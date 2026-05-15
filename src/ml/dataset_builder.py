"""
src/ml/dataset_builder.py — 训练数据集构建

流程：
  1. 拉沪深 300 当前成分股代码（akshare）
  2. 对每只股票拉 N 年日线 + Spot + 财务数据（用 cache_manager 缓存）
  3. 在历史每个采样点（如每周五）计算所有已注册因子的值 → 特征矩阵 X
  4. 计算未来 20 日个股收益 - 沪深 300 同期收益 → 标签 y（超额收益 %）
  5. 输出 parquet：列 [date, code, feat_*, y_excess_ret_20d]

设计要点：
  - **Purged**：每个样本的 label 用未来 20 日，因此样本日期 >= 截止日 - 20 不可用，避免泄漏
  - **数据缓存**：完整复用 cache_manager.CacheManager，重复跑只多 1 次增量拉取
  - **采样频率**：默认每周五，避免日频带来的样本相关性 + 训练耗时
  - **特征齐全度**：用所有已注册的非 OHLCV 重的因子（PE/PB/PS/MarketCap 等是 Spot 类，
    历史不可回放；本模块只用历史可回放的"基于 OHLCV + 历史财务"的因子）
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from src.data.providers.cache_manager import CacheManager
from src.utils.logger import get_logger

logger = get_logger("ml.dataset")

# 输出根目录
_DEFAULT_OUTPUT_DIR = Path("./cache/ml_dataset")

# 沪深 300 指数代码（akshare/东方财富）
_CSI300_INDEX_CODE = "000300"

# 标签：未来 N 日超额收益
DEFAULT_LABEL_HORIZON_DAYS = 20


class DatasetBuilder:
    """构建 ML 训练数据集（features + 未来超额收益标签）"""

    def __init__(
        self,
        output_dir: Path | str = _DEFAULT_OUTPUT_DIR,
        cache_dir: str = ".cache/ml_dataset",
        cache_ttl_hours: int = 24 * 7,  # 数据缓存一周
        label_horizon_days: int = DEFAULT_LABEL_HORIZON_DAYS,
        sample_freq: str = "W-FRI",  # 每周五采样
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.cache = CacheManager(cache_dir=cache_dir, ttl_hours=cache_ttl_hours, max_mb=2048)
        self.label_horizon = int(label_horizon_days)
        self.sample_freq = sample_freq

    # ---------------- 数据源 ----------------

    def fetch_csi300_codes(self) -> list[str]:
        """拉沪深 300 成分股代码"""
        cache_key = "csi300_codes"

        def _fetch() -> list[str]:
            try:
                import akshare as ak
                df = ak.index_stock_cons_csindex(symbol="000300")
                if df is None or df.empty:
                    return []
                col = next((c for c in ("成分券代码", "代码", "ConstCode") if c in df.columns), None)
                if col is None:
                    logger.warning(f"沪深300成分股 DataFrame 缺关键列: {df.columns.tolist()}")
                    return []
                return [str(c).zfill(6) for c in df[col].tolist()]
            except Exception as e:
                logger.error(f"拉取沪深 300 成分股失败: {e}", exc_info=True)
                return []

        codes = self.cache.get_or_fetch(cache_key, _fetch, ttl_hours=24 * 30)
        logger.info(f"沪深 300 成分股: {len(codes)} 只")
        return codes or []

    def fetch_daily_ohlcv(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """拉单只股票日线（带缓存）"""
        cache_key = f"daily_{code}_{start_date}_{end_date}"

        def _fetch() -> pd.DataFrame:
            try:
                import akshare as ak
                df = ak.stock_zh_a_hist(
                    symbol=code, period="daily",
                    start_date=start_date.replace("-", ""),
                    end_date=end_date.replace("-", ""),
                    adjust="qfq",
                )
                if df is None or df.empty:
                    return pd.DataFrame()
                df["日期"] = pd.to_datetime(df["日期"])
                df = df.sort_values("日期").reset_index(drop=True)
                return df
            except Exception as e:
                logger.debug(f"{code} 日线拉取失败: {e}")
                return pd.DataFrame()

        return self.cache.get_or_fetch(cache_key, _fetch)

    def fetch_csi300_returns(self, start_date: str, end_date: str) -> pd.Series:
        """拉沪深 300 指数日线 → 计算日收益率 Series（日期为 DatetimeIndex）"""
        cache_key = f"csi300_returns_{start_date}_{end_date}"

        def _fetch() -> pd.Series:
            try:
                import akshare as ak
                df = ak.index_zh_a_hist(
                    symbol=_CSI300_INDEX_CODE, period="daily",
                    start_date=start_date.replace("-", ""),
                    end_date=end_date.replace("-", ""),
                )
                if df is None or df.empty:
                    return pd.Series(dtype=float)
                df["日期"] = pd.to_datetime(df["日期"])
                df = df.sort_values("日期").set_index("日期")
                close = pd.to_numeric(df["收盘"], errors="coerce")
                return close
            except Exception as e:
                logger.error(f"沪深 300 指数拉取失败: {e}", exc_info=True)
                return pd.Series(dtype=float)

        return self.cache.get_or_fetch(cache_key, _fetch)

    # ---------------- 特征工程 ----------------

    @staticmethod
    def _compute_features(df_daily: pd.DataFrame) -> pd.DataFrame:
        """
        在日线 OHLCV 上计算可历史回放的特征。

        说明：PE/PB/MarketCap 是当日截面快照，历史回放需要单独的"历史财务+流通股"接口，
        本版本先用纯 OHLCV 衍生特征，后续可扩展到接 baostock 历史 PE 接口。

        返回：列 [日期, 代码, ret_5d, ret_20d, ret_60d, vol_20d, ma5_dev, ma20_dev, rsi_14,
                 macd, macd_signal, macd_hist, amp_20d, turnover_ma20]
        """
        if df_daily is None or df_daily.empty or len(df_daily) < 250:
            return pd.DataFrame()

        df = df_daily.copy()
        rename = {"日期": "date", "开盘": "open", "最高": "high", "最低": "low",
                  "收盘": "close", "成交量": "volume", "换手率": "turnover"}
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        for col in ("open", "high", "low", "close", "volume"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["close"]).reset_index(drop=True)

        close = df["close"]
        feats = pd.DataFrame({"date": df["date"]})

        # 收益
        feats["ret_5d"] = close.pct_change(5) * 100
        feats["ret_20d"] = close.pct_change(20) * 100
        feats["ret_60d"] = close.pct_change(60) * 100
        feats["ret_120d"] = close.pct_change(120) * 100

        # 波动率（20 日年化）
        feats["vol_20d"] = close.pct_change().rolling(20).std() * np.sqrt(252) * 100

        # 均线偏离度
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        ma250 = close.rolling(250).mean()
        feats["ma5_dev"] = (close / ma5 - 1) * 100
        feats["ma20_dev"] = (close / ma20 - 1) * 100
        feats["ma60_dev"] = (close / ma60 - 1) * 100
        feats["ma250_dev"] = (close / ma250 - 1) * 100

        # RSI 14
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss.replace(0, np.nan)
        feats["rsi_14"] = 100 - 100 / (1 + rs)

        # MACD 12-26-9
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        feats["macd"] = macd
        feats["macd_signal"] = signal
        feats["macd_hist"] = macd - signal

        # 振幅 20 日均值
        if "high" in df.columns and "low" in df.columns:
            amp = (df["high"] - df["low"]) / df["close"].shift(1) * 100
            feats["amp_20d"] = amp.rolling(20).mean()

        # 换手率 20 日均
        if "turnover" in df.columns:
            feats["turnover_ma20"] = pd.to_numeric(df["turnover"], errors="coerce").rolling(20).mean()
        else:
            feats["turnover_ma20"] = np.nan

        return feats

    # ---------------- 标签（未来超额收益）----------------

    def _compute_future_excess_return(
        self,
        df_daily: pd.DataFrame,
        csi300_close: pd.Series,
    ) -> pd.Series:
        """
        计算未来 self.label_horizon 日的个股收益 vs 沪深 300 超额（百分比）。

        Returns:
            Series，索引为日期，值为该日往后 N 日的超额收益（%）
        """
        if df_daily.empty:
            return pd.Series(dtype=float)
        df = df_daily.copy()
        df["日期"] = pd.to_datetime(df["日期"])
        df = df.sort_values("日期").set_index("日期")
        close = pd.to_numeric(df["收盘"], errors="coerce")
        # 个股未来 N 日收益
        stock_fwd = close.shift(-self.label_horizon) / close - 1
        # 大盘未来 N 日收益（按日期对齐）
        idx_fwd = csi300_close.shift(-self.label_horizon) / csi300_close - 1
        # 对齐索引
        excess = (stock_fwd - idx_fwd.reindex(stock_fwd.index)) * 100
        return excess

    # ---------------- 主流程 ----------------

    def build(
        self,
        start_date: str = "2020-01-01",
        end_date: str | None = None,
        codes: list[str] | None = None,
        max_codes: int | None = None,
    ) -> pd.DataFrame:
        """
        生成数据集。

        Args:
            start_date: 历史起点（标签开始往前推 max_lookback 天）
            end_date: 历史终点，None=今天
            codes: 股票池，None=沪深 300
            max_codes: 上限（调试用）

        Returns:
            DataFrame，列 [date, code, feat_*, y_excess_ret_20d]
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        if codes is None:
            codes = self.fetch_csi300_codes()
        if not codes:
            logger.error("股票池为空，无法构建数据集")
            return pd.DataFrame()
        if max_codes:
            codes = codes[:max_codes]

        # 预加载沪深 300 指数
        # 多拉 30 天确保未来收益覆盖
        index_end = (pd.to_datetime(end_date) + timedelta(days=self.label_horizon + 10)).strftime("%Y-%m-%d")
        csi300_close = self.fetch_csi300_returns(start_date, index_end)
        if csi300_close.empty:
            logger.error("沪深 300 指数为空，无法构建数据集")
            return pd.DataFrame()

        all_rows = []
        total = len(codes)
        for i, code in enumerate(codes):
            if i % 20 == 0:
                logger.info(f"数据集构建进度 {i}/{total}: {code}")
            df_daily = self.fetch_daily_ohlcv(code, start_date, index_end)
            if df_daily.empty or len(df_daily) < 250:
                continue

            feats = self._compute_features(df_daily)
            if feats.empty:
                continue

            label = self._compute_future_excess_return(df_daily, csi300_close)
            label = label.rename(f"y_excess_ret_{self.label_horizon}d").reset_index()
            label.columns = ["date", f"y_excess_ret_{self.label_horizon}d"]

            merged = feats.merge(label, on="date", how="inner")
            merged["code"] = code

            # 重采样到每周五（减少样本相关性）
            merged["date"] = pd.to_datetime(merged["date"])
            merged = merged.set_index("date")
            # 取每周五的样本
            try:
                weekly = merged.resample(self.sample_freq).last()
            except Exception:
                weekly = merged
            weekly = weekly.dropna(subset=[f"y_excess_ret_{self.label_horizon}d"])

            # 过滤 end_date 之后的样本（标签会泄漏未来）
            cutoff = pd.to_datetime(end_date) - timedelta(days=self.label_horizon)
            weekly = weekly[weekly.index <= cutoff]
            weekly = weekly.reset_index()
            all_rows.append(weekly)

        if not all_rows:
            logger.error("无任何有效样本")
            return pd.DataFrame()

        dataset = pd.concat(all_rows, ignore_index=True)
        logger.info(f"数据集构建完成：共 {len(dataset)} 条样本，覆盖 {dataset['code'].nunique()} 只股票")
        return dataset

    def save(self, dataset: pd.DataFrame, version: str | None = None) -> Path:
        """保存为 parquet，文件名带时间戳便于多版本共存"""
        if version is None:
            version = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = self.output_dir / f"dataset_{version}.parquet"
        dataset.to_parquet(path, index=False)
        latest = self.output_dir / "dataset_latest.parquet"
        try:
            if latest.exists():
                latest.unlink()
            # Windows 不支持 symlink，直接 copy
            import shutil
            shutil.copy2(path, latest)
        except OSError:
            pass
        logger.info(f"数据集已保存: {path}（latest -> {latest.name}）")
        return path

    @staticmethod
    def load_latest(output_dir: Path | str = _DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
        """加载最新数据集"""
        path = Path(output_dir) / "dataset_latest.parquet"
        if not path.exists():
            raise FileNotFoundError(f"未找到最新数据集: {path}")
        return pd.read_parquet(path)

    @staticmethod
    def feature_columns(dataset: pd.DataFrame) -> list[str]:
        """从数据集 DataFrame 提取特征列名（排除 date/code/y_*）"""
        return [c for c in dataset.columns if not (c == "date" or c == "code" or c.startswith("y_"))]
