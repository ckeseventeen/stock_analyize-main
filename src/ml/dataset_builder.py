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

from src.core.cache_policy import ttl_for
from src.data.providers.cache_manager import CacheManager
from src.utils.logger import get_logger

logger = get_logger("ml.dataset")


# 给 Baostock 调用加 wall-clock 超时（库本身无 timeout 控制，限频时会 hang）
# 用 ThreadPoolExecutor 包装实现强制超时
_BAOSTOCK_TIMEOUT_POOL = None


def _baostock_with_timeout(code: str, start_date: str, end_date: str,
                             fields: str, timeout: float = 8.0):
    """
    用 ThreadPoolExecutor + future.result(timeout) 给 Baostock 加超时。
    避免服务端限频时进程无限挂住。

    超时返回 None，调用方自己处理。
    """
    from concurrent.futures import ThreadPoolExecutor
    from concurrent.futures import TimeoutError as FT
    global _BAOSTOCK_TIMEOUT_POOL
    if _BAOSTOCK_TIMEOUT_POOL is None:
        _BAOSTOCK_TIMEOUT_POOL = ThreadPoolExecutor(max_workers=2,
                                                      thread_name_prefix="bs-timeout")

    def _call():
        from src.data.providers.baostock_provider import BaostockProvider
        with BaostockProvider() as bp:
            return bp.get_k_data(
                code,
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                fields=fields,
            )

    fut = _BAOSTOCK_TIMEOUT_POOL.submit(_call)
    try:
        return fut.result(timeout=timeout)
    except FT:
        logger.debug(f"{code} Baostock {fields} 超时 {timeout}s")
        # 不能 cancel 已经在跑的 worker，但让它自然 die，避免阻塞主线程
        return None
    except Exception as e:
        logger.debug(f"{code} Baostock 调用异常: {e}")
        return None

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
        data_provider=None,
        use_alpha158: bool = True,
    ):
        """
        Args:
            data_provider: 行情数据源（需提供 get_scope_codes / get_daily_ohlcv 等）。
                缺省时按需自建 ScreenerDataProvider。**显式注入是解开
                analysis↔ml 循环依赖的关键**：ml 不再在模块层依赖 analysis。
            use_alpha158: 是否把 31 个 Alpha158 标准因子并入特征（默认开）。
                手写特征偏基础，Alpha158 覆盖 K线形态/量价/趋势斜率等维度，
                对 IC 的贡献通常比再手搓指标更实在。
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.cache = CacheManager(cache_dir=cache_dir, ttl_hours=cache_ttl_hours, max_mb=2048)
        self.label_horizon = int(label_horizon_days)
        self.use_alpha158 = bool(use_alpha158)
        self.sample_freq = sample_freq
        self._screener_provider = data_provider

    # ---------------- 数据源 ----------------

    def _get_screener_provider(self):
        """
        取行情数据源：优先用注入的实例；未注入时才按需自建。

        自建路径是**函数内**导入 analysis 层，属运行期依赖而非模块级依赖，
        因此 `import src.ml.*` 不会拉起 analysis，静态依赖图上无环。
        """
        if self._screener_provider is None:
            from src.analysis.screening.data_provider import ScreenerDataProvider
            self._screener_provider = ScreenerDataProvider()
        return self._screener_provider

    def fetch_csi300_codes(self) -> list[str]:
        """拉沪深 300 成分股代码 — 复用 ScreenerDataProvider 的 scope_codes 接口"""
        cache_key = "csi300_codes"

        def _fetch() -> list[str]:
            try:
                provider = self._get_screener_provider()
                codes_set = provider.get_scope_codes(["csi300"])
                if not codes_set:
                    logger.warning("沪深 300 成分股拉取返回空")
                    return []
                return sorted(str(c).zfill(6) for c in codes_set)
            except Exception as e:
                logger.error(f"拉取沪深 300 成分股失败: {e}", exc_info=True)
                return []

        codes = self.cache.get_or_fetch(cache_key, _fetch, ttl_hours=ttl_for("ml_dataset"))
        logger.info(f"沪深 300 成分股: {len(codes)} 只")
        return codes or []

    def fetch_daily_ohlcv(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        拉单只股票日线 — 优先 pytdx（不限频，800 条 ≈ 3.2 年活跃股），
        失败兜底 Baostock（带 socket timeout，避免限频时卡死）。

        训练数据范围建议 2023-01 起（pytdx 800 条够）。
        """
        cache_key = f"daily_{code}_{start_date}_{end_date}"

        def _fetch() -> pd.DataFrame:
            # 1. pytdx 优先（无限频，~40ms/只）
            try:
                from src.data.providers.pytdx_provider import get_global_pytdx
                pytdx = get_global_pytdx()
                if pytdx.is_available():
                    df = pytdx.get_k_data(code, days_back=800, frequency="d")
                    if df is not None and not df.empty:
                        # pytdx 日期带 15:00:00 时分，下游 merge 时要求纯日期
                        df["日期"] = pd.to_datetime(df["日期"]).dt.normalize()
                        start = pd.to_datetime(start_date)
                        end = pd.to_datetime(end_date)
                        df = df[(df["日期"] >= start) & (df["日期"] <= end)]
                        if not df.empty:
                            return df.sort_values("日期").reset_index(drop=True)
            except Exception as e:
                logger.debug(f"{code} pytdx K 线失败: {e}")

            # 2. Baostock 兜底（受限频风险，加 5s 超时）
            try:
                df = _baostock_with_timeout(
                    code, start_date, end_date,
                    fields="date,open,high,low,close,volume,amount,turn",
                    timeout=5,
                )
                if df is None or df.empty:
                    return pd.DataFrame()
                df = df.rename(columns={
                    "date": "日期", "open": "开盘", "high": "最高",
                    "low": "最低", "close": "收盘",
                    "volume": "成交量", "amount": "成交额", "turn": "换手率",
                })
                df["日期"] = pd.to_datetime(df["日期"])
                return df.sort_values("日期").reset_index(drop=True)
            except Exception as e:
                logger.debug(f"{code} Baostock K 线失败: {e}")
                return pd.DataFrame()

        return self.cache.get_or_fetch(cache_key, _fetch)

    def fetch_valuation_history(self, code: str, start_date: str,
                                  end_date: str) -> pd.DataFrame:
        """
        拉单只股票历史估值（PE/PB/PS）+ 换手率 — Baostock + 10s 超时

        返回列 [date, pe_ttm, pb_mrq, ps_ttm, turn]，date 为 normalized datetime（00:00）。
        turn = 当日换手率 % — pytdx 不带这个字段，所以一起从 Baostock 拉。
        超时返回空 DataFrame，下游用 NaN 占位（LightGBM 容忍）。
        """
        cache_key = f"valuation_{code}_{start_date}_{end_date}_v2"  # v2: 加了 turn

        def _fetch() -> pd.DataFrame:
            df = _baostock_with_timeout(
                code, start_date, end_date,
                fields="date,peTTM,pbMRQ,psTTM,turn",
                timeout=10,
            )
            if df is None or df.empty:
                return pd.DataFrame()
            df = df.rename(columns={
                "peTTM": "pe_ttm",
                "pbMRQ": "pb_mrq",
                "psTTM": "ps_ttm",
            })
            df["date"] = pd.to_datetime(df["date"]).dt.normalize()
            for col in ("pe_ttm", "pb_mrq", "ps_ttm", "turn"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            df.loc[df["pe_ttm"] <= 0, "pe_ttm"] = np.nan
            df.loc[df["ps_ttm"] <= 0, "ps_ttm"] = np.nan
            df.loc[df["pb_mrq"] <= 0, "pb_mrq"] = np.nan
            return df.sort_values("date").reset_index(drop=True)

        return self.cache.get_or_fetch(cache_key, _fetch)

    @staticmethod
    def _compute_valuation_features(df_val: pd.DataFrame) -> pd.DataFrame:
        """
        在 daily 估值序列上派生 ML 特征。

        每个估值指标产生 3 个特征：
          - {indicator}: 当日绝对值
          - {indicator}_z252: 250 日 Z-score（[价值 - 250日均值] / 250日标准差）
                              ≈ 该股票自身估值历史区间内的相对位置（核心特征）
          - {indicator}_chg_60d: 60 日相对变化率 %（估值趋势）

        共 9 个特征（pe / pb / ps × 3）

        Z-score 比绝对值好的原因：消费股 PE 通常 30+ 而银行股 5-10，
        绝对值对模型是噪音；自身历史的偏离才有"贵了/便宜了"的语义。
        """
        if df_val is None or df_val.empty:
            return pd.DataFrame()

        out = pd.DataFrame({"date": df_val["date"]})
        for col in ("pe_ttm", "pb_mrq", "ps_ttm"):
            if col not in df_val.columns:
                continue
            series = df_val[col]
            out[col] = series
            # 250 日 Z-score
            roll_mean = series.rolling(252, min_periods=60).mean()
            roll_std = series.rolling(252, min_periods=60).std()
            out[f"{col}_z252"] = (series - roll_mean) / roll_std.replace(0, np.nan)
            # 60 日变化率
            out[f"{col}_chg_60d"] = (series / series.shift(60) - 1) * 100

        return out

    @staticmethod
    def _fetch_index_kline_raw(code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """拉指数日线 — 委托给统一接口 data.providers.index_kline"""
        from src.data.providers.index_kline import fetch_index_kline
        return fetch_index_kline(code, start_date, end_date)

    def fetch_csi300_returns(self, start_date: str, end_date: str) -> pd.Series:
        """
        拉沪深 300 指数日线 → 收盘价 Series（日期为 DatetimeIndex）。

        多级 fallback：akshare 新浪 → pytdx 直连 → Baostock → 沪深300ETF 代理。

        **缓存键刻意不含 end_date**（历史事故）：原本键是
        `csi300_returns_{start}_{end}`，而调用方传的 end_date 每天都在变，
        导致缓存永远 miss、每次都要重新联网拉取。一旦指数源限流（东财/pytdx
        都有动态限流），标签就算不出来 → build 返回空 → **训练永远没有模型**。
        指数历史数据不会变，改为按起点缓存 + 命中后只判断覆盖范围是否够用。
        """
        cache_key = f"csi300_close_from_{start_date}"

        cached = self.cache.get(cache_key, ttl_hours=ttl_for("ml_dataset"))
        if cached is not None and len(cached) > 0:
            need_end = pd.to_datetime(end_date)
            have_end = pd.to_datetime(cached.index.max())
            # 缓存覆盖到需求终点、或已到最近 10 天内（指数不会有更新的数据了）
            if have_end >= need_end - pd.Timedelta(days=10):
                logger.info(f"沪深 300 指数走缓存: {len(cached)} 个交易日 "
                            f"(~{have_end.date()})")
                return cached

        df = self._fetch_index_kline_raw(_CSI300_INDEX_CODE, start_date, end_date)
        if df is None or df.empty:
            df = self._fetch_index_proxy_etf(start_date, end_date)

        if df is None or df.empty:
            if cached is not None and len(cached) > 0:
                logger.warning(
                    f"沪深 300 所有数据源均失败，沿用缓存（覆盖到 "
                    f"{pd.to_datetime(cached.index.max()).date()}）")
                return cached
            logger.error(
                f"沪深 300 ({_CSI300_INDEX_CODE}) 所有数据源都失败且无缓存——"
                f"标签无法计算，数据集会为空")
            return pd.Series(dtype=float)

        df = df.sort_values("日期").set_index("日期")
        close = pd.to_numeric(df["收盘"], errors="coerce").dropna()
        logger.info(f"沪深 300 指数加载: {len(close)} 个交易日 "
                    f"({close.index.min().date()} ~ {close.index.max().date()})")
        self.cache.set(cache_key, close)
        return close

    @staticmethod
    def _fetch_index_proxy_etf(start_date: str, end_date: str) -> pd.DataFrame:
        """
        用沪深300ETF(510300) 作为指数代理。

        为什么有效：ETF 是**普通证券**，走 pytdx 的 get_security_bars 通道，
        与指数专用通道的限流策略不同——指数被限流时 ETF 往往仍可拉。
        ETF 净值走势与指数高度一致，用于计算"相对大盘超额收益"完全够用。
        """
        try:
            from src.data.providers.pytdx_provider import get_global_pytdx

            pytdx = get_global_pytdx()
            if not pytdx.is_available():
                return pd.DataFrame()
            df = pytdx.get_k_data("510300", days_back=800, frequency="d")
            if df is None or df.empty:
                return pd.DataFrame()
            df = df.copy()
            df["日期"] = pd.to_datetime(df["日期"]).dt.normalize()
            mask = ((df["日期"] >= pd.to_datetime(start_date))
                    & (df["日期"] <= pd.to_datetime(end_date)))
            df = df[mask]
            if df.empty:
                return pd.DataFrame()
            logger.info(f"沪深300ETF(510300) 代理指数成功: {len(df)} 个交易日")
            return df.reset_index(drop=True)
        except Exception as e:
            logger.debug(f"ETF 代理指数失败: {type(e).__name__}: {e}")
            return pd.DataFrame()

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

    @staticmethod
    def _compute_alpha158_features(df_daily: pd.DataFrame) -> pd.DataFrame:
        """
        用 Alpha158 表达式因子引擎产出特征（QLib 标准因子集）。

        为什么加这个：手写特征只有 24 个且偏基础（收益/均线/RSI/MACD），
        而项目已有 31 个标准 Alpha 因子（K线形态/量价/趋势斜率/RSV…）
        却一直没喂给模型。这些因子经过学术与业界检验，对 IC 的贡献
        通常比再手搓几个指标更实在。

        单个因子求值失败（数据太短等）只跳过该因子，不影响其余。
        """
        if df_daily is None or df_daily.empty:
            return pd.DataFrame()
        try:
            from src.analysis.factor.expression import (
                evaluate_expression,
                load_alpha158_config,
            )
        except Exception as e:
            logger.debug(f"Alpha158 引擎不可用，跳过: {type(e).__name__}: {e}")
            return pd.DataFrame()

        entries = load_alpha158_config()
        if not entries:
            return pd.DataFrame()

        out: dict[str, pd.Series] = {}
        for e in entries:
            name, expr = e.get("name"), e.get("expr")
            if not name or not expr:
                continue
            try:
                series = evaluate_expression(df_daily, expr)
                # 列名加前缀，避免与手写特征重名
                out[f"a158_{name}"] = series.reset_index(drop=True)
            except Exception as ex:
                logger.debug(f"Alpha158 因子 {name} 求值失败: {type(ex).__name__}: {ex}")
        if not out:
            return pd.DataFrame()
        return pd.DataFrame(out)

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

            # 基本面 + 换手率（Baostock 拉，10s 超时）
            # 注意：先拉 valuation，把 turn 合并到 daily_df 再算特征
            df_val = self.fetch_valuation_history(code, start_date, end_date)
            if not df_val.empty and "turn" in df_val.columns:
                turn_df = df_val[["date", "turn"]].rename(
                    columns={"date": "日期", "turn": "换手率"}
                )
                df_daily = df_daily.merge(turn_df, on="日期", how="left")

            feats = self._compute_features(df_daily)
            if feats.empty:
                continue

            # Alpha158 标准因子（按行拼接：与 feats 同为日线逐行，索引对齐）
            if self.use_alpha158:
                a158 = self._compute_alpha158_features(df_daily)
                if not a158.empty and len(a158) == len(feats):
                    feats = pd.concat([feats.reset_index(drop=True), a158], axis=1)

            val_feats = self._compute_valuation_features(df_val)
            if not val_feats.empty:
                feats = feats.merge(val_feats, on="date", how="left")

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
