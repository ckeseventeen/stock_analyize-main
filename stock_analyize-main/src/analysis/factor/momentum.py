"""
src/analysis/factor/momentum.py — 动量因子

基于日线收盘价计算 N 日收益率。
"""
import pandas as pd

from src.analysis.factor.base import BaseFactor


class ReturnFactor(BaseFactor):
    """N日收益率因子"""

    higher_is_better = True
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, period: int = 20):
        self.period = period
        self.name = f"return_{period}d"
        self.description = f"近{period}日收益率(%)"

    def validate(self, data: dict) -> bool:
        df = data.get("daily_df")
        if df is None or df.empty:
            return False
        close_col = self._get_close_col(df)
        return close_col is not None and len(df) > self.period

    def calculate(self, data: dict) -> float:
        df = data["daily_df"]
        close_col = self._get_close_col(df)
        close = pd.to_numeric(df[close_col], errors="coerce").dropna()
        if len(close) <= self.period:
            return float("nan")
        return (close.iloc[-1] / close.iloc[-self.period - 1] - 1) * 100

    @staticmethod
    def _get_close_col(df: pd.DataFrame) -> str | None:
        for col in ("close", "收盘"):
            if col in df.columns:
                return col
        return None


class Return5D(ReturnFactor):
    def __init__(self):
        super().__init__(period=5)


class Return20D(ReturnFactor):
    def __init__(self):
        super().__init__(period=20)


class Return60D(ReturnFactor):
    def __init__(self):
        super().__init__(period=60)


class Return120D(ReturnFactor):
    def __init__(self):
        super().__init__(period=120)
