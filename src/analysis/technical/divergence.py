"""
src/analysis/technical/divergence.py — MACD 背离检测

支持底背离（价格创新低 + MACD柱状图未创新低）和顶背离检测。
用于股票筛选器的周线底背离条件。
"""
import numpy as np
import pandas as pd
from scipy.signal import argrelmax, argrelmin

from src.utils.logger import get_logger

logger = get_logger("divergence")

# 两个极值点之间的最小间隔（K线根数）
_MIN_TROUGH_DISTANCE = 5


class MACDDivergenceDetector:
    """
    MACD 背离检测器

    使用示例:
        from src.analysis.technical.indicators import TechnicalAnalyzer
        ta = TechnicalAnalyzer(weekly_df)
        ta.add_macd()
        detector = MACDDivergenceDetector(ta.get_dataframe())
        if detector.detect_bottom_divergence():
            print("检测到底背离")
    """

    def __init__(self, df: pd.DataFrame, price_col: str = "close", macd_hist_col: str = "macd_hist"):
        if df is None or df.empty:
            self._valid = False
            self._df = pd.DataFrame()
            return

        self._df = df.copy()
        self._price_col = price_col
        self._macd_hist_col = macd_hist_col
        self._valid = price_col in df.columns and macd_hist_col in df.columns
        self._details: dict = {}

    def detect_bottom_divergence(
        self, 
        lookback_bars: int = 60, 
        order: int = 5, 
        max_bars_since_trough: int | None = None,
        zero_axis_filter: bool = False,
        multi_level_check: bool = False
    ) -> bool:
        """
        检测底背离：价格创新低，但 MACD 柱状图未创新低（更高的低点）

        Args:
            lookback_bars: 向前搜索的K线根数

        Returns:
            True 表示检测到底背离
        """
        if not self._valid or len(self._df) < 20:
            return False

        df = self._df.iloc[-lookback_bars:] if len(self._df) > lookback_bars else self._df
        price = df[self._price_col].values.astype(float)
        macd_hist = df[self._macd_hist_col].values.astype(float)

        # 去除 NaN
        valid_mask = ~(np.isnan(price) | np.isnan(macd_hist))
        if valid_mask.sum() < 20:
            return False

        price = price[valid_mask]
        macd_hist = macd_hist[valid_mask]

        # 多级检测：尝试多种 order 敏感度
        orders_to_try = [1, 2, 3] if multi_level_check else [order]
        
        for cur_order in orders_to_try:
            # 找价格局部最低点
            price_troughs = self._find_troughs(price, order=cur_order)
            if len(price_troughs) < 2:
                continue

            # 取最近两个低点
            t2_idx = price_troughs[-1]  # 最近的低点
            t1_idx = price_troughs[-2]  # 前一个低点

            # 检查底背离是否是最近发生的
            if max_bars_since_trough is not None:
                if len(price) - 1 - t2_idx > max_bars_since_trough:
                    continue

            # 两个低点间距需 >= _MIN_TROUGH_DISTANCE
            if t2_idx - t1_idx < _MIN_TROUGH_DISTANCE:
                continue
                
            # 零轴过滤：两次低点之间，MACD柱必须要穿过零轴（即存在正的柱子）
            if zero_axis_filter:
                if np.max(macd_hist[t1_idx:t2_idx+1]) <= 0:
                    continue

            # 判断底背离条件：价格创新低，MACD柱状图未创新低
            price_lower_low = price[t2_idx] <= price[t1_idx]
            macd_higher_low = self._get_min_macd_near(macd_hist, t2_idx) > self._get_min_macd_near(macd_hist, t1_idx)

            if price_lower_low and macd_higher_low:
                self._details = {
                    "type": "bottom_divergence",
                    "price_trough_1": float(price[t1_idx]),
                    "price_trough_2": float(price[t2_idx]),
                    "macd_trough_1": float(self._get_min_macd_near(macd_hist, t1_idx)),
                    "macd_trough_2": float(self._get_min_macd_near(macd_hist, t2_idx)),
                    "bars_between": int(t2_idx - t1_idx),
                }
                return True

        return False

    def detect_top_divergence(self, lookback_bars: int = 60) -> bool:
        """
        检测顶背离：价格创新高，但 MACD 柱状图未创新高（更低的高点）

        Args:
            lookback_bars: 向前搜索的K线根数

        Returns:
            True 表示检测到顶背离
        """
        if not self._valid or len(self._df) < 20:
            return False

        df = self._df.iloc[-lookback_bars:] if len(self._df) > lookback_bars else self._df
        price = df[self._price_col].values.astype(float)
        macd_hist = df[self._macd_hist_col].values.astype(float)

        valid_mask = ~(np.isnan(price) | np.isnan(macd_hist))
        if valid_mask.sum() < 20:
            return False

        price = price[valid_mask]
        macd_hist = macd_hist[valid_mask]

        # 找价格局部最高点
        price_peaks = self._find_peaks(price, order=5)
        if len(price_peaks) < 2:
            return False

        p2_idx = price_peaks[-1]
        p1_idx = price_peaks[-2]

        if p2_idx - p1_idx < _MIN_TROUGH_DISTANCE:
            return False

        # 顶背离：价格创新高，MACD柱状图未创新高
        price_higher_high = price[p2_idx] >= price[p1_idx]
        macd_lower_high = self._get_max_macd_near(macd_hist, p2_idx) < self._get_max_macd_near(macd_hist, p1_idx)

        if price_higher_high and macd_lower_high:
            self._details = {
                "type": "top_divergence",
                "price_peak_1": float(price[p1_idx]),
                "price_peak_2": float(price[p2_idx]),
                "macd_peak_1": float(self._get_max_macd_near(macd_hist, p1_idx)),
                "macd_peak_2": float(self._get_max_macd_near(macd_hist, p2_idx)),
                "bars_between": int(p2_idx - p1_idx),
            }
            return True

        return False

    def get_divergence_details(self) -> dict:
        """返回最近一次检测到的背离详情"""
        return self._details

    @staticmethod
    def _find_troughs(data: np.ndarray, order: int = 5) -> np.ndarray:
        """找局部最低点的索引"""
        if len(data) < 2 * order + 1:
            return np.array([], dtype=int)
        result = argrelmin(data, order=order)[0]
        return result

    @staticmethod
    def _find_peaks(data: np.ndarray, order: int = 5) -> np.ndarray:
        """找局部最高点的索引"""
        if len(data) < 2 * order + 1:
            return np.array([], dtype=int)
        result = argrelmax(data, order=order)[0]
        return result

    @staticmethod
    def _get_min_macd_near(macd_hist: np.ndarray, idx: int, window: int = 3) -> float:
        """获取 idx 附近 window 范围内 MACD 柱状图的最小值"""
        start = max(0, idx - window)
        end = min(len(macd_hist), idx + window + 1)
        return float(np.min(macd_hist[start:end]))

    @staticmethod
    def _get_max_macd_near(macd_hist: np.ndarray, idx: int, window: int = 3) -> float:
        """获取 idx 附近 window 范围内 MACD 柱状图的最大值"""
        start = max(0, idx - window)
        end = min(len(macd_hist), idx + window + 1)
        return float(np.max(macd_hist[start:end]))
