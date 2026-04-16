"""
src/analysis/technical/indicators.py — 技术指标计算器

基于 ta 库封装常用技术指标，支持中英文列名自动识别。
"""
import numpy as np
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import MACD
from ta.volatility import BollingerBands

from src.utils.logger import get_logger

logger = get_logger("technical")

# 中文列名 -> 英文标准化映射
_CN_COL_MAP = {
    "日期": "date",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
    "成交额": "amount",
}


class TechnicalAnalyzer:
    """
    技术指标计算器，接收 OHLCV DataFrame，逐步叠加技术指标列。

    使用示例:
        ta = TechnicalAnalyzer(df)
        ta.add_macd().add_rsi().add_bollinger()
        enriched_df = ta.get_dataframe()
    """

    def __init__(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("输入 DataFrame 不能为空")
        self._df = df.copy()
        self._normalize_columns()
        self._ensure_numeric()

    def _normalize_columns(self) -> None:
        """自动识别中文列名并映射为英文标准名"""
        rename_map = {}
        for col in self._df.columns:
            col_lower = str(col).strip().lower()
            if col_lower in _CN_COL_MAP:
                rename_map[col] = _CN_COL_MAP[col_lower]
            elif col_lower in ("open", "high", "low", "close", "volume", "date", "amount"):
                rename_map[col] = col_lower
        if rename_map:
            self._df.rename(columns=rename_map, inplace=True)

    def _ensure_numeric(self) -> None:
        """确保 OHLCV 列为数值类型"""
        for col in ("open", "high", "low", "close", "volume"):
            if col in self._df.columns:
                self._df[col] = pd.to_numeric(self._df[col], errors="coerce")

    def add_macd(self, fast: int = 12, slow: int = 26, signal: int = 9) -> "TechnicalAnalyzer":
        """添加 MACD 指标：macd, macd_signal, macd_hist"""
        if "close" not in self._df.columns:
            logger.warning("缺少 close 列，无法计算 MACD")
            return self
        macd = MACD(self._df["close"], window_slow=slow, window_fast=fast, window_sign=signal)
        self._df["macd"] = macd.macd()
        self._df["macd_signal"] = macd.macd_signal()
        self._df["macd_hist"] = macd.macd_diff()
        return self

    def add_rsi(self, period: int = 14) -> "TechnicalAnalyzer":
        """添加 RSI 指标"""
        if "close" not in self._df.columns:
            logger.warning("缺少 close 列，无法计算 RSI")
            return self
        rsi = RSIIndicator(self._df["close"], window=period)
        self._df[f"rsi_{period}"] = rsi.rsi()
        return self

    def add_kdj(self, n: int = 9, m1: int = 3, m2: int = 3) -> "TechnicalAnalyzer":
        """
        添加 KDJ 指标（手动实现，ta库无原生KDJ）

        算法：
          RSV = (close - low_n) / (high_n - low_n) * 100
          K = EMA(RSV, m1)  （实际用 2/3 * prev_K + 1/3 * RSV 递推）
          D = EMA(K, m2)
          J = 3*K - 2*D
        """
        if not all(col in self._df.columns for col in ("high", "low", "close")):
            logger.warning("缺少 high/low/close 列，无法计算 KDJ")
            return self

        low_n = self._df["low"].rolling(window=n, min_periods=1).min()
        high_n = self._df["high"].rolling(window=n, min_periods=1).max()
        rsv = (self._df["close"] - low_n) / (high_n - low_n) * 100
        rsv = rsv.fillna(50)

        k_values = np.zeros(len(rsv))
        d_values = np.zeros(len(rsv))
        k_values[0] = 50.0
        d_values[0] = 50.0

        for i in range(1, len(rsv)):
            k_values[i] = (m1 - 1) / m1 * k_values[i - 1] + 1 / m1 * rsv.iloc[i]
            d_values[i] = (m2 - 1) / m2 * d_values[i - 1] + 1 / m2 * k_values[i]

        self._df["kdj_k"] = k_values
        self._df["kdj_d"] = d_values
        self._df["kdj_j"] = 3 * self._df["kdj_k"] - 2 * self._df["kdj_d"]
        return self

    def add_bollinger(self, period: int = 20, std_dev: float = 2.0) -> "TechnicalAnalyzer":
        """添加布林带指标：bb_upper, bb_middle, bb_lower"""
        if "close" not in self._df.columns:
            logger.warning("缺少 close 列，无法计算布林带")
            return self
        bb = BollingerBands(self._df["close"], window=period, window_dev=std_dev)
        self._df["bb_upper"] = bb.bollinger_hband()
        self._df["bb_middle"] = bb.bollinger_mavg()
        self._df["bb_lower"] = bb.bollinger_lband()
        return self

    def add_moving_averages(self, periods: list[int] | None = None) -> "TechnicalAnalyzer":
        """添加简单移动平均线"""
        if periods is None:
            periods = [5, 10, 20, 60, 120, 250]
        if "close" not in self._df.columns:
            logger.warning("缺少 close 列，无法计算均线")
            return self
        for p in periods:
            self._df[f"ma_{p}"] = self._df["close"].rolling(window=p, min_periods=1).mean()
        return self

    def add_volume_analysis(self) -> "TechnicalAnalyzer":
        """添加成交量分析指标：vol_ma5, vol_ma10, vol_ratio"""
        if "volume" not in self._df.columns:
            logger.warning("缺少 volume 列，无法计算量能指标")
            return self
        self._df["vol_ma5"] = self._df["volume"].rolling(window=5, min_periods=1).mean()
        self._df["vol_ma10"] = self._df["volume"].rolling(window=10, min_periods=1).mean()
        self._df["vol_ratio"] = self._df["volume"] / self._df["vol_ma5"]
        self._df["vol_ratio"] = self._df["vol_ratio"].replace([np.inf, -np.inf], np.nan)
        return self

    def add_all(self) -> "TechnicalAnalyzer":
        """一次性添加全部技术指标"""
        return (
            self.add_macd()
            .add_rsi()
            .add_kdj()
            .add_bollinger()
            .add_moving_averages()
            .add_volume_analysis()
        )

    def get_dataframe(self) -> pd.DataFrame:
        """返回叠加指标后的完整 DataFrame"""
        return self._df
