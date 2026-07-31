"""
src/analysis/screening/conditions_pkg/factor.py — 因子/排序类选股条件

与 entry.py 的区别是**范式不同**：entry 里的条件回答"当天有没有出现某个
技术形态"（金叉/突破/背离），这里的条件回答"这只股票在某个因子维度上
排第几/够不够格"（ML 预测分、动量排名、波动率、股息率、资金流向）。

因子类条件通常跨市场比较，其中 RankingCondition 子类还需要全局排序，
由 StockScreener 在 Pass2 结束后统一做 top-K 截断。
"""
from __future__ import annotations

import pandas as pd

from src.analysis.screening.conditions_pkg.base import (
    BaseCondition,
    RankingCondition,
)
from src.utils.logger import get_logger

logger = get_logger("screener_conditions")


class MLTopKCondition(BaseCondition):
    """
    ML 预测排名 Top-K 筛选条件（B 路径自学习）。

    工作方式：
      - Spot 阶段：所有候选股放行
      - Full 阶段：用 daily_df 算 ML 预测分，缓存到 spot_row['_ml_score']
      - 实际筛选发生在 evaluate_full：用每只股票的 ml_score 排名，取 top_k

    注意：因为筛选条件接口是逐股调用，本条件单独评估时无法"排名"，
          需要 screener 在 pass2 后做一次批量重排（已在 screener.py 处理）。
    """
    name = "ml_top_k"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, top_k: int = 50, min_score: float | None = None):
        """
        Args:
            top_k: 取预测分前 K 名
            min_score: 同时要求 ml_score >= min_score（None 不限制）
        """
        self.top_k = int(top_k)
        self.min_score = min_score

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < 250:
            return False
        try:
            from src.ml.predictor import get_predictor
            predictor = get_predictor()
            if predictor is None:
                # 模型未训练时不应该静默放行，否则 _ml_scores 始终为空，top-K 截断永远不执行
                logger.warning(
                    "ml_top_k 条件需要 ML 模型但未训练，该条件返回 False。"
                    "请先运行：python -m src.ml.cli train"
                )
                return False
            score = predictor.predict_from_daily_df(ohlcv_df)
            # 把分数挂到 spot_row 上，供 screener 做最终 top-K 排名
            try:
                spot_row["_ml_score"] = score
            except Exception:
                pass
            # 同时保存到条件对象上，作为 spot_row 的备份
            self.last_computed_score = score
            if self.min_score is not None and score < self.min_score:
                return False
            # 真正的 top-K 截断在 screener pass2 完成后进行；这里先放行
            return True
        except Exception as e:
            logger.debug(f"ml_top_k 计算失败: {e}")
            return False

class NorthboundFlowCondition(BaseCondition):
    """
    北向资金持续净买入筛选

    检测近 N 日北向资金对个股的净买入趋势。
    注意：需要 akshare 接口支持，部分股票可能无数据。
    """
    name = "northbound_flow"
    requires_ohlcv = False

    def __init__(self, lookback_days: int = 5, min_net_buy: float = 1e8):
        """
        Args:
            lookback_days: 回溯天数
            min_net_buy: 最小累计净买入金额（元），默认 1 亿
        """
        self.lookback_days = lookback_days
        self.min_net_buy = min_net_buy

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        return pd.Series(True, index=df.index)

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        code = str(spot_row.get("代码", "")).strip()
        if not code:
            return False
        try:
            import akshare as ak
            df = ak.stock_hsgt_individual_em(symbol=code)
            if df is None or df.empty:
                return False
            if "当日净买入" in df.columns:
                recent = df.tail(self.lookback_days)
                net_buy = pd.to_numeric(recent["当日净买入"], errors="coerce").sum()
                return net_buy >= self.min_net_buy
            return False
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False

class LowVolatilityCondition(BaseCondition):
    """
    低波动因子筛选（低波动异象：长期看低波动组合风险调整后收益更优）

    用近 N 日收益率的年化标准差衡量。区间可配，便于同时表达
    "足够低波动"与"排除僵尸股（波动过低往往是无成交）"。
    """

    name = "low_volatility"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, period: int = 60, max_vol: float = 30.0,
                 min_vol: float = 5.0):
        self.period = int(period)
        self.max_vol = float(max_vol)
        self.min_vol = float(min_vol)

    def required_bars(self) -> int:
        return max(self._DEFAULT_MIN_BARS, self.period + self._WARMUP_BUFFER_BARS)

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < self.period + 1:
            return False
        try:
            import numpy as np

            from src.core.columns import get_close_col

            col = get_close_col(ohlcv_df.columns)
            if col is None:
                return False
            close = pd.to_numeric(ohlcv_df[col], errors="coerce").dropna()
            if len(close) < self.period + 1:
                return False
            ann_vol = float(
                close.pct_change().tail(self.period).std() * np.sqrt(252) * 100)
            return self.min_vol <= ann_vol <= self.max_vol
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False

class DividendYieldCondition(BaseCondition):
    """
    股息率筛选（红利策略核心条件）

    数据来源优先级：
      1. spot 行情自带的"股息率"列（部分数据源提供）
      2. 用 PE 与派息率估算：股息率 ≈ 派息率 / PE × 100

    估算路径的默认派息率取 30%（A 股中位数附近），可配。
    """

    name = "dividend_yield"
    requires_ohlcv = False

    def __init__(self, min_yield: float = 2.0, max_yield: float = 15.0,
                 assumed_payout_ratio: float = 0.3):
        self.min_yield = float(min_yield)
        self.max_yield = float(max_yield)
        self.assumed_payout_ratio = float(assumed_payout_ratio)

    def _yield_of(self, row_or_df, vectorized: bool = False):
        """取股息率：优先真实列，否则用 派息率/PE 估算"""
        if vectorized:
            df = row_or_df
            if "股息率" in df.columns:
                return pd.to_numeric(df["股息率"], errors="coerce")
            pe = pd.to_numeric(df.get("市盈率-动态"), errors="coerce")
            est = self.assumed_payout_ratio / pe * 100
            return est.where(pe > 0)
        row = row_or_df
        if "股息率" in row.index:
            try:
                return float(row.get("股息率") or 0)
            except (TypeError, ValueError):
                return 0.0
        try:
            pe = float(row.get("市盈率-动态", 0) or 0)
        except (TypeError, ValueError):
            return 0.0
        return (self.assumed_payout_ratio / pe * 100) if pe > 0 else 0.0

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        y = self._yield_of(spot_row)
        return self.min_yield <= y <= self.max_yield

    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        y = self._yield_of(df, vectorized=True)
        if y is None:
            return pd.Series(True, index=df.index)
        return ((y >= self.min_yield) & (y <= self.max_yield)).fillna(False)

class MomentumRankCondition(RankingCondition):
    """
    动量轮动：按 N 日涨幅在全市场排序，取最强的前 K 只。

    动量效应（Jegadeesh & Titman）是最经典的横截面异象之一。
    与"涨幅 > X%"这类绝对阈值不同，排序方式能自动适配牛熊——
    熊市里最强的 20 只依然选得出来。

    可选 skip_recent_days 跳过最近若干日（经典动量因子做法：用
    t-12月 ~ t-1月 的收益，避开短期反转效应）。
    """

    name = "momentum_rank"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, period: int = 60, top_k: int = 20,
                 skip_recent_days: int = 0, min_score: float | None = None):
        self.period = int(period)
        self.top_k = int(top_k)
        self.skip_recent_days = int(skip_recent_days)
        self.min_score = min_score

    def required_bars(self) -> int:
        need = self.period + self.skip_recent_days + self._WARMUP_BUFFER_BARS
        return max(self._DEFAULT_MIN_BARS, need)

    def score(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> float:
        """N 日涨幅（%）；skip_recent_days>0 时窗口整体前移"""
        from src.core.columns import get_close_col

        if ohlcv_df is None or ohlcv_df.empty:
            return float("nan")
        col = get_close_col(ohlcv_df.columns)
        if col is None:
            return float("nan")
        close = pd.to_numeric(ohlcv_df[col], errors="coerce").dropna()
        need = self.period + self.skip_recent_days + 1
        if len(close) < need:
            return float("nan")

        end_idx = len(close) - 1 - self.skip_recent_days
        start_idx = end_idx - self.period
        start_price = float(close.iloc[start_idx])
        end_price = float(close.iloc[end_idx])
        if start_price <= 0:
            return float("nan")
        return (end_price / start_price - 1) * 100

