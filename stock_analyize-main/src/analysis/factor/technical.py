"""
src/analysis/factor/technical.py — 技术因子

封装 TechnicalAnalyzer / MACDDivergenceDetector 为因子接口。
"""

from src.analysis.factor.base import BaseFactor
from src.analysis.technical.divergence import MACDDivergenceDetector
from src.analysis.technical.indicators import TechnicalAnalyzer


class RSIFactor(BaseFactor):
    """RSI 因子，返回最新 RSI 值"""

    higher_is_better = False
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, period: int = 14):
        self.period = period
        self.name = f"rsi_{period}"
        self.description = f"RSI({period})"

    def validate(self, data: dict) -> bool:
        df = data.get("daily_df")
        return df is not None and not df.empty and len(df) > self.period

    def calculate(self, data: dict) -> float:
        ta = TechnicalAnalyzer(data["daily_df"])
        ta.add_rsi(self.period)
        rsi_col = f"rsi_{self.period}"
        series = ta.get_dataframe()[rsi_col].dropna()
        if series.empty:
            return float("nan")
        return float(series.iloc[-1])


class MACDDivergenceFactor(BaseFactor):
    """
    MACD 底背离因子

    返回 1.0 表示检测到底背离，0.0 表示未检测到。
    默认使用周线数据。
    """

    name = "macd_bottom_divergence"
    description = "MACD底背离(周线)"
    higher_is_better = True
    requires_ohlcv = True
    ohlcv_period = "weekly"

    def __init__(self, lookback_bars: int = 60):
        self.lookback_bars = lookback_bars

    def validate(self, data: dict) -> bool:
        df = data.get("weekly_df")
        return df is not None and not df.empty and len(df) >= 20

    def calculate(self, data: dict) -> float:
        ta = TechnicalAnalyzer(data["weekly_df"])
        ta.add_macd()
        detector = MACDDivergenceDetector(ta.get_dataframe())
        return 1.0 if detector.detect_bottom_divergence(self.lookback_bars) else 0.0


class PriceAboveMAFactor(BaseFactor):
    """
    价格站上均线因子

    返回 1.0 表示当前收盘价 > MA(period)，否则 0.0。
    """

    higher_is_better = True
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, ma_period: int = 20):
        self.ma_period = ma_period
        self.name = f"price_above_ma{ma_period}"
        self.description = f"价格站上MA{ma_period}"

    def validate(self, data: dict) -> bool:
        df = data.get("daily_df")
        return df is not None and not df.empty and len(df) > self.ma_period

    def calculate(self, data: dict) -> float:
        ta = TechnicalAnalyzer(data["daily_df"])
        ta.add_moving_averages([self.ma_period])
        df = ta.get_dataframe()
        ma_col = f"ma_{self.ma_period}"
        if ma_col not in df.columns or "close" not in df.columns:
            return float("nan")
        last = df.dropna(subset=[ma_col, "close"]).iloc[-1]
        return 1.0 if last["close"] > last[ma_col] else 0.0
