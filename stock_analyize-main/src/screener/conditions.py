"""
src/screener/conditions.py — 可组合筛选条件

两类条件：
  1. Spot条件 — 仅需实时行情数据，内存过滤（快速）
  2. OHLCV条件 — 需要历史K线数据，逐只检测（慢速）

所有条件通过 AND 组合。
"""
from abc import ABC, abstractmethod

import pandas as pd

from src.analysis.technical.divergence import MACDDivergenceDetector
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

    @abstractmethod
    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        """
        基于实时行情数据快速评估。
        对于 OHLCV 条件，始终返回 True（跳过，留待 evaluate_full 判断）。
        """

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

class MarketCapCondition(BaseCondition):
    """市值范围筛选（单位：亿元）"""

    name = "market_cap"
    requires_ohlcv = False

    def __init__(self, min_cap: float = 0, max_cap: float = float("inf")):
        self.min_cap = min_cap
        self.max_cap = max_cap

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        # 若数据源不提供总市值（如 Baostock），跳过本条件（视为通过），避免误过滤
        if "总市值" not in spot_row.index:
            return True
        cap = float(spot_row.get("总市值", 0) or 0) / 1e8
        return self.min_cap <= cap <= self.max_cap


class PERangeCondition(BaseCondition):
    """市盈率(动态)范围筛选"""

    name = "pe_range"
    requires_ohlcv = False

    def __init__(self, min_pe: float = 0, max_pe: float = float("inf")):
        self.min_pe = min_pe
        self.max_pe = max_pe

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        pe = float(spot_row.get("市盈率-动态", 0) or 0)
        if pe <= 0:
            return False  # 排除亏损股（PE为负）
        return self.min_pe <= pe <= self.max_pe


class PBRangeCondition(BaseCondition):
    """市净率范围筛选"""

    name = "pb_range"
    requires_ohlcv = False

    def __init__(self, min_pb: float = 0, max_pb: float = float("inf")):
        self.min_pb = min_pb
        self.max_pb = max_pb

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        pb = float(spot_row.get("市净率", 0) or 0)
        if pb <= 0:
            return False
        return self.min_pb <= pb <= self.max_pb


class PriceRangeCondition(BaseCondition):
    """股价范围筛选"""

    name = "price_range"
    requires_ohlcv = False

    def __init__(self, min_price: float = 0, max_price: float = float("inf")):
        self.min_price = min_price
        self.max_price = max_price

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        price = float(spot_row.get("最新价", 0) or 0)
        if price <= 0:
            return False
        return self.min_price <= price <= self.max_price


class TurnoverRateCondition(BaseCondition):
    """换手率范围筛选"""

    name = "turnover_rate"
    requires_ohlcv = False

    def __init__(self, min_rate: float = 0, max_rate: float = float("inf")):
        self.min_rate = min_rate
        self.max_rate = max_rate

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        rate = float(spot_row.get("换手率", 0) or 0)
        return self.min_rate <= rate <= self.max_rate


# ========================
# OHLCV 条件（慢速，需要K线数据）
# ========================

class WeeklyMACDBottomDivergenceCondition(BaseCondition):
    """
    周线MACD底背离筛选

    检测周线级别的 MACD 底背离信号：
    价格创新低，但 MACD 柱状图未创新低。
    """

    name = "weekly_macd_divergence"
    requires_ohlcv = True
    ohlcv_period = "weekly"

    def __init__(self, lookback_bars: int = 60):
        self.lookback_bars = lookback_bars

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True  # Spot阶段无法判断，放行到OHLCV阶段

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < 20:
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_macd()
            detector = MACDDivergenceDetector(ta.get_dataframe())
            return detector.detect_bottom_divergence(lookback_bars=self.lookback_bars)
        except Exception as e:
            logger.debug(f"底背离检测异常: {e}")
            return False


class DailyMACDBottomDivergenceCondition(BaseCondition):
    """日线MACD底背离筛选"""

    name = "daily_macd_divergence"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, lookback_bars: int = 120):
        self.lookback_bars = lookback_bars

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < 20:
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_macd()
            detector = MACDDivergenceDetector(ta.get_dataframe())
            return detector.detect_bottom_divergence(lookback_bars=self.lookback_bars)
        except Exception as e:
            logger.debug(f"日线底背离检测异常: {e}")
            return False


class RSIOversoldCondition(BaseCondition):
    """RSI超卖筛选（日线RSI低于阈值）"""

    name = "rsi_oversold"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, threshold: float = 30, period: int = 14):
        self.threshold = threshold
        self.period = period

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < self.period + 1:
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_rsi(self.period)
            df = ta.get_dataframe()
            rsi_col = f"rsi_{self.period}"
            rsi_val = df[rsi_col].dropna().iloc[-1]
            return bool(rsi_val < self.threshold)
        except Exception as e:
            logger.debug(f"RSI超卖检测异常: {e}")
            return False


class PriceAboveMACondition(BaseCondition):
    """价格站上均线筛选"""

    name = "price_above_ma"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, ma_period: int = 20):
        self.ma_period = ma_period

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < self.ma_period:
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_moving_averages([self.ma_period])
            df = ta.get_dataframe()
            ma_col = f"ma_{self.ma_period}"
            last = df.dropna(subset=[ma_col, "close"]).iloc[-1]
            return bool(last["close"] > last[ma_col])
        except Exception as e:
            logger.debug(f"均线站上检测异常: {e}")
            return False


# ========================
# 条件注册表（用于YAML配置解析）
# ========================

CONDITION_REGISTRY: dict[str, type] = {
    "market_cap": MarketCapCondition,
    "pe_range": PERangeCondition,
    "pb_range": PBRangeCondition,
    "price_range": PriceRangeCondition,
    "turnover_rate": TurnoverRateCondition,
    "weekly_macd_divergence": WeeklyMACDBottomDivergenceCondition,
    "daily_macd_divergence": DailyMACDBottomDivergenceCondition,
    "rsi_oversold": RSIOversoldCondition,
    "price_above_ma": PriceAboveMACondition,
}
