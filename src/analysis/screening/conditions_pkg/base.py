"""
src/analysis/screening/conditions_pkg/base.py — 条件基类与共享抽象

BaseCondition 定义所有筛选条件的契约（含 required_bars() 数据深度协议）；
_MACrossCondition 是金叉/死叉两个条件的共享实现。
"""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from src.analysis.technical.indicators import TechnicalAnalyzer
from src.utils.logger import get_logger

logger = get_logger("screener_conditions")


class BaseCondition(ABC):
    """
    筛选条件抽象基类

    Attributes:
        name: 条件名称
        requires_ohlcv: 是否需要历史K线数据
        ohlcv_period: 所需K线周期 "daily" / "weekly"
    """

    name: str = ""
    requires_ohlcv: bool = False
    ohlcv_period: str = "daily"

    # required_bars() 默认实现扫描的"回看窗口"参数属性名（标量 / 列表）
    _LOOKBACK_SCALAR_ATTRS = (
        "ma_period", "period", "lookback_bars", "lookback",
        "slow_period", "long_period", "box_period",
    )
    _LOOKBACK_LIST_ATTRS = ("ma_list", "ma_periods", "periods")
    # 指标热身余量 + 无法推断时的保守基线
    _WARMUP_BUFFER_BARS = 20
    _DEFAULT_MIN_BARS = 60

    def required_bars(self) -> int:
        """
        评估本条件所需的最小 K 线 bar 数（按 ohlcv_period 对应周期计）。

        数据提供层按所有条件的 max(required_bars()) 决定拉取深度——bar 数
        不足时条件只能恒 False（历史 bug：MA120 拿 81 根 bar 永远失败）。

        默认实现按常见回看参数属性推断 + 热身余量；窗口语义特殊的子类
        （如用非常规参数名、或窗口需要级联叠加的）应覆写本方法。
        """
        vals: list[int] = []
        for attr in self._LOOKBACK_SCALAR_ATTRS:
            v = getattr(self, attr, None)
            if isinstance(v, (int, float)) and v > 0:
                vals.append(int(v))
        for attr in self._LOOKBACK_LIST_ATTRS:
            v = getattr(self, attr, None)
            if isinstance(v, (list, tuple)) and v:
                try:
                    vals.append(int(max(v)))
                except (TypeError, ValueError):
                    pass
        if not vals:
            return self._DEFAULT_MIN_BARS
        return max(self._DEFAULT_MIN_BARS, max(vals) + self._WARMUP_BUFFER_BARS)

    @abstractmethod
    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        """
        基于实时行情数据快速评估。
        对于 OHLCV 条件，始终返回 True（跳过，留待 evaluate_full 判断）。
        """

    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        """
        向量化评估（批量对 5000+ 行做同一个条件判断）。

        默认实现是 df.apply(evaluate_spot, axis=1) —— Python 级循环，慢。
        子类若能用 pandas 列运算重写此方法，可带来 50-200x 加速。

        Returns:
            布尔 Series，index 与 df.index 对齐
        """
        return df.apply(self.evaluate_spot, axis=1).astype(bool)

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        """
        基于完整数据评估（含K线）。
        默认委托给 evaluate_spot。OHLCV条件需覆写此方法。
        """
        return self.evaluate_spot(spot_row)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"


# ========================
# Spot 条件（快速，无需K线）
# ========================


class _MACrossCondition(BaseCondition):
    """
    均线交叉筛选基类（DRY 提取）

    子类仅需设置 `_cross_direction`:
      - "golden": 短上穿长（金叉）
      - "death":  短下穿长（死叉）
    """
    requires_ohlcv = True
    ohlcv_period = "daily"
    _cross_direction: str = "golden"

    def __init__(self, fast_period: int = 5, slow_period: int = 20):
        self.fast_period = fast_period
        self.slow_period = slow_period

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < self.slow_period + 4:
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_moving_averages([self.fast_period, self.slow_period])
            df = ta.get_dataframe()
            if len(df) < 4:
                return False
            fast_col = f"ma_{self.fast_period}"
            slow_col = f"ma_{self.slow_period}"
            # 允许最近3根K线内出现交叉
            for offset in range(3):
                idx = -(offset + 1)
                prev_idx = idx - 1
                if abs(prev_idx) <= len(df):
                    prev_fast = float(df[fast_col].iloc[prev_idx])
                    prev_slow = float(df[slow_col].iloc[prev_idx])
                    curr_fast = float(df[fast_col].iloc[idx])
                    curr_slow = float(df[slow_col].iloc[idx])
                    if self._cross_direction == "golden":
                        if prev_fast <= prev_slow and curr_fast > curr_slow:
                            return True
                    else:
                        if prev_fast >= prev_slow and curr_fast < curr_slow:
                            return True
            return False
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False




class RankingCondition(BaseCondition):
    """
    **打分排序条件**基类——把"取前 N 名"从 ml_top_k 的专属能力泛化出来。

    与普通条件的区别：普通条件只回答"通过/不通过"，无法表达
    "全市场动量最强的 20 只"这类**相对**筛选（动量轮动、因子选股都需要）。

    子类实现 `score()` 返回打分，筛选器会：
      1. 先用 evaluate_full 做硬性初筛（可选，默认全通过）
      2. 收集所有候选的分数，降序取前 `top_k`

    注意：排序是**全局**的，因此该条件必然要等所有候选都算完才能截断，
    由 StockScreener 在 Pass2 结束后统一处理。
    """

    #: 取分数最高的前 N 名
    top_k: int = 50
    #: 分数低于此值直接淘汰（None 表示不限制）
    min_score: float | None = None
    #: True=分数越大越好
    higher_is_better: bool = True

    def score(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> float:
        """返回该股票的打分；无法计算时返回 NaN（会被排到最后）"""
        raise NotImplementedError

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        """
        硬性初筛：算出分数并暂存到 spot_row，供筛选器收集。
        分数算不出（NaN）或低于 min_score 时直接淘汰。
        """
        try:
            value = self.score(spot_row, ohlcv_df)
        except Exception as e:
            logger.debug(f"[{self.name}] 打分异常，按不通过处理: {type(e).__name__}: {e}")
            return False
        if value is None or pd.isna(value):
            return False
        if self.min_score is not None and value < self.min_score:
            return False
        # 借 spot_row 回传分数（与 ml_top_k 的既有约定一致）
        try:
            spot_row["_rank_score"] = float(value)
        except Exception:
            pass
        return True


class CompositeCondition(BaseCondition):
    """
    组合条件：把若干子条件用 AND / OR / NOT 组合起来。

    解决什么问题：策略的买入条件此前恒为 AND、卖出恒为 OR，写不出
    "站上均线 **且** (RSI超卖 **或** 触及布林下轨)" 这类表达。真实策略里
    "多个入场信号取其一"是很常见的诉求。

    YAML 写法（`logic` + `conditions` 即为组合节点，可任意嵌套）::

        conditions:
          - type: market_cap          # 普通条件
            min: 300
          - logic: any                # 组合节点：任一满足即可
            conditions:
              - type: rsi_oversold
              - type: bollinger_breakout

    语义：
      all / and  → 全部子条件为真
      any / or   → 任一子条件为真
      none / not → 全部子条件为假（用于排除）

    数据需求（requires_ohlcv / ohlcv_period / required_bars）自动从子条件
    聚合，因此筛选器的两轮架构与数据深度推断都不需要为组合条件特判。
    """

    name = "composite"

    _ALL = ("all", "and")
    _ANY = ("any", "or")
    _NONE = ("none", "not")

    def __init__(self, logic: str = "all",
                 conditions: list[BaseCondition] | None = None):
        raw = str(logic or "all").strip().lower()
        if raw not in self._ALL + self._ANY + self._NONE:
            raise ValueError(
                f"未知组合逻辑 {logic!r}，支持 "
                f"{list(self._ALL + self._ANY + self._NONE)}")
        self.logic = raw
        self.children: list[BaseCondition] = list(conditions or [])
        self.name = f"composite({raw})"

    # ---- 数据需求：从子条件聚合，无需调用方特判 ----

    @property
    def requires_ohlcv(self) -> bool:
        return any(getattr(c, "requires_ohlcv", False) for c in self.children)

    @property
    def ohlcv_period(self) -> str:
        """任一子条件需要周线，则整体按周线取数（周线含日线信息的超集需求）"""
        for c in self.children:
            if getattr(c, "requires_ohlcv", False) and \
                    getattr(c, "ohlcv_period", "daily") == "weekly":
                return "weekly"
        return "daily"

    def required_bars(self) -> int:
        need = [c.required_bars() for c in self.children
                if getattr(c, "requires_ohlcv", False)]
        return max(need) if need else self._DEFAULT_MIN_BARS

    # ---- 求值 ----

    def _combine(self, results: list[bool]) -> bool:
        if not results:
            return True          # 空组合视为不约束
        if self.logic in self._ALL:
            return all(results)
        if self.logic in self._ANY:
            return any(results)
        return not any(results)  # none / not

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        """
        Spot 阶段：**需要 K 线的子条件一律先视为通过**，留到 evaluate_full 再判。

        这样才不会在第一轮就把"技术信号尚未确认"的股票误杀——与筛选器
        两轮架构（Pass1 快速过滤 / Pass2 精筛）的语义保持一致。
        """
        results = []
        for c in self.children:
            if getattr(c, "requires_ohlcv", False):
                continue
            try:
                results.append(bool(c.evaluate_spot(spot_row)))
            except Exception as e:
                logger.debug(f"[{self.name}] 子条件 {c.name} spot 异常: {e}")
                results.append(False)
        # OR 组合里若含 K 线子条件，spot 阶段无法定论 → 放行到第二轮
        if self.logic in self._ANY and any(
                getattr(c, "requires_ohlcv", False) for c in self.children):
            return True
        return self._combine(results)

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        results = []
        for c in self.children:
            try:
                if getattr(c, "requires_ohlcv", False):
                    results.append(bool(c.evaluate_full(spot_row, ohlcv_df)))
                else:
                    results.append(bool(c.evaluate_spot(spot_row)))
            except Exception as e:
                logger.debug(f"[{self.name}] 子条件 {c.name} 求值异常: {e}")
                results.append(False)
        return self._combine(results)

    def __repr__(self) -> str:
        inner = ", ".join(getattr(c, "name", "?") for c in self.children)
        return f"Composite[{self.logic}]({inner})"
