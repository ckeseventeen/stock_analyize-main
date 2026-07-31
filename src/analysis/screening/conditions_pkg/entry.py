"""
src/analysis/screening/conditions_pkg/entry.py — 买入向条件（技术形态 / 突破 / 超卖 / 资金）

需要历史 K 线，第二轮逐只检测。
"""
from __future__ import annotations

import pandas as pd

from src.analysis.screening.conditions_pkg.base import (
    BaseCondition,
    _MACrossCondition,
)
from src.analysis.technical.divergence import MACDDivergenceDetector
from src.analysis.technical.indicators import TechnicalAnalyzer
from src.utils.logger import get_logger

logger = get_logger("screener_conditions")


class WeeklyMACDBottomDivergenceCondition(BaseCondition):
    """
    周线MACD底背离筛选

    检测周线级别的 MACD 底背离信号：
    价格创新低，但 MACD 柱状图未创新低。
    """

    name = "weekly_macd_divergence"
    requires_ohlcv = True
    ohlcv_period = "weekly"

    def __init__(self, lookback_bars: int = 60, zero_axis_filter: bool = False,
                 multi_level_check: bool = False,
                 order: int = 2, max_bars_since_trough: int = 4):
        """
        B19/S7 修复：order 和 max_bars_since_trough 改为可配，YAML 可覆盖。
        - order: 局部极值识别窗口（前后多少根 K 线没有更低点即认定为底）
        - max_bars_since_trough: 背离必须发生在最近多少根 K 线内
        """
        self.lookback_bars = lookback_bars
        self.zero_axis_filter = zero_axis_filter
        self.multi_level_check = multi_level_check
        self.order = order
        self.max_bars_since_trough = max_bars_since_trough

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True  # Spot阶段无法判断，放行到OHLCV阶段

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < 20:
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_macd()
            detector = MACDDivergenceDetector(ta.get_dataframe())
            return detector.detect_bottom_divergence(
                lookback_bars=self.lookback_bars,
                order=self.order,
                max_bars_since_trough=self.max_bars_since_trough,
                zero_axis_filter=self.zero_axis_filter,
                multi_level_check=self.multi_level_check
            )
        except Exception as e:
            logger.debug(f"底背离检测异常: {e}")
            return False


class DailyMACDBottomDivergenceCondition(BaseCondition):
    """日线MACD底背离筛选"""

    name = "daily_macd_divergence"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, lookback_bars: int = 120, zero_axis_filter: bool = False,
                 multi_level_check: bool = False,
                 order: int = 5, max_bars_since_trough: int = 5):
        """
        B19/S7 修复：参数与 Weekly 对齐，order/max_bars_since_trough 可配。
        """
        self.lookback_bars = lookback_bars
        self.zero_axis_filter = zero_axis_filter
        self.multi_level_check = multi_level_check
        self.order = order
        self.max_bars_since_trough = max_bars_since_trough

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < 20:
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_macd()
            detector = MACDDivergenceDetector(ta.get_dataframe())
            return detector.detect_bottom_divergence(
                lookback_bars=self.lookback_bars,
                order=self.order,
                max_bars_since_trough=self.max_bars_since_trough,
                zero_axis_filter=self.zero_axis_filter,
                multi_level_check=self.multi_level_check
            )
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


class BoxBreakoutCondition(BaseCondition):
    """
    箱体突破筛选（日线）

    逻辑：
      1. 取最近 lookback_bars 根 K 线（不含最新一根）识别"箱体"
         - 箱顶 = 区间内最高价
         - 箱底 = 区间内最低价
         - 振幅 = (箱顶 - 箱底) / 箱底，超过 consolidation_pct 则不是震荡箱体
      2. 最新收盘价 > 箱顶 * (1 + breakout_pct) → 有效向上突破
    """

    name = "box_breakout"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(
        self,
        lookback_bars: int = 20,
        breakout_pct: float = 0.02,
        consolidation_pct: float = 0.10,
    ):
        self.lookback_bars = lookback_bars
        self.breakout_pct = breakout_pct
        self.consolidation_pct = consolidation_pct

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True  # Spot 阶段无法判断，放行到 OHLCV 阶段

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < self.lookback_bars + 1:
            return False
        try:
            df = ohlcv_df.copy()
            # 标准化列名（中文 → 英文）
            rename = {"最高": "high", "最低": "low", "收盘": "close"}
            df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)

            if not all(c in df.columns for c in ("high", "low", "close")):
                logger.debug("box_breakout: 缺少 high/low/close 列")
                return False

            # 取识别区间（倒数 lookback_bars+1 到倒数第 2 根，不含最新一根）
            box_df = df.iloc[-(self.lookback_bars + 1):-1]
            box_high = float(box_df["high"].max())
            box_low = float(box_df["low"].min())

            if box_low <= 0:
                return False

            # 振幅检测：箱体过宽则不视为震荡箱体
            amplitude = (box_high - box_low) / box_low
            if amplitude > self.consolidation_pct:
                logger.debug(
                    f"box_breakout: 振幅 {amplitude:.2%} > {self.consolidation_pct:.2%}，不是震荡箱体"
                )
                return False

            # 突破判断：最新收盘价 > 箱顶 * (1 + breakout_pct)
            last_close = float(df["close"].iloc[-1])
            threshold = box_high * (1 + self.breakout_pct)
            result = last_close > threshold
            if result:
                logger.debug(
                    f"box_breakout: 收盘 {last_close:.2f} > 箱顶 {box_high:.2f} * "
                    f"(1+{self.breakout_pct:.2%}) = {threshold:.2f} ✓"
                )
            return result
        except Exception as e:
            logger.debug(f"箱体突破检测异常: {e}")
            return False


class DowntrendBreakoutCondition(BaseCondition):
    """
    下降趋势线突破筛选（日线）

    算法：
      1. 在 lookback_bars 根 K 线中找局部高点（峰值）
      2. 对这些高点用最小二乘法拟合下降趋势线（斜率 < 0 才有效）
      3. 趋势线触碰高点数 >= min_touches 才有效
      4. 最新收盘价 > 趋势线当前值 * (1 + breakout_pct) → 有效向上突破
    """

    name = "downtrend_breakout"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(
        self,
        lookback_bars: int = 60,
        min_touches: int = 2,
        breakout_pct: float = 0.01,
    ):
        self.lookback_bars = lookback_bars
        self.min_touches = min_touches
        self.breakout_pct = breakout_pct

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < self.lookback_bars + 1:
            return False
        try:
            import numpy as np

            df = ohlcv_df.copy()
            rename = {"最高": "high", "收盘": "close"}
            df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)

            if not all(c in df.columns for c in ("high", "close")):
                logger.debug("downtrend_breakout: 缺少 high/close 列")
                return False

            # 取识别区间（不含最新一根）
            window = df.iloc[-(self.lookback_bars + 1):-1].reset_index(drop=True)
            highs = window["high"].values.astype(float)
            n = len(highs)

            # 找局部高点：前后各 2 根都比它低（简单 argrelmax）
            peak_indices = []
            for i in range(2, n - 2):
                if highs[i] >= highs[i - 1] and highs[i] >= highs[i - 2] \
                        and highs[i] >= highs[i + 1] and highs[i] >= highs[i + 2]:
                    peak_indices.append(i)

            if len(peak_indices) < self.min_touches:
                logger.debug(
                    f"downtrend_breakout: 仅找到 {len(peak_indices)} 个高点峰值，"
                    f"< min_touches={self.min_touches}"
                )
                return False

            # 对高点拟合趋势线（x=序号，y=价格）
            xs = np.array(peak_indices, dtype=float)
            ys = highs[peak_indices]
            slope, intercept = np.polyfit(xs, ys, 1)

            # 斜率必须向下（下降趋势线）
            if slope >= 0:
                logger.debug(f"downtrend_breakout: 斜率 {slope:.4f} >= 0，非下降趋势")
                return False

            # 趋势线在最新一根 K 线位置的值（外推到 n，即最新根）
            trend_val = slope * n + intercept

            # 突破判断
            last_close = float(df["close"].iloc[-1])
            threshold = trend_val * (1 + self.breakout_pct)
            result = last_close > threshold
            if result:
                logger.debug(
                    f"downtrend_breakout: 收盘 {last_close:.2f} > 趋势线 {trend_val:.2f} * "
                    f"(1+{self.breakout_pct:.2%}) = {threshold:.2f} ✓ (slope={slope:.4f})"
                )
            return result
        except Exception as e:
            logger.debug(f"下降趋势线突破检测异常: {e}")
            return False


# ========================
# 新增条件（为了支持 yaml 中新增的方案）
# ========================

class MultiMABullCondition(BaseCondition):
    name = "multi_ma_bull"
    requires_ohlcv = True
    ohlcv_period = "daily"
    def __init__(self, ma_list: list = None, require_all_above: bool = True,
                 require_up_trend: bool = True, tolerance: float = 0.005):
        self.ma_list = ma_list or [5, 10, 20, 60, 250]
        self.require_all_above = require_all_above
        self.require_up_trend = require_up_trend
        self.tolerance = tolerance  # 均线值接近时允许的容差比例
    def evaluate_spot(self, spot_row: pd.Series) -> bool: return True
    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < max(self.ma_list):
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_moving_averages(self.ma_list)
            df = ta.get_dataframe()
            if len(df) < 3:
                return False
            last_row = df.iloc[-1]

            # 判断多头排列 MA5 > MA10 > MA20...（允许 tolerance 容差）
            sorted_mas = sorted(self.ma_list)
            for i in range(len(sorted_mas) - 1):
                fast_val = float(last_row[f"ma_{sorted_mas[i]}"])
                slow_val = float(last_row[f"ma_{sorted_mas[i+1]}"])
                if fast_val < slow_val * (1 - self.tolerance):
                    return False

            # 收盘价在所有均线之上（允许容差）
            if self.require_all_above:
                close_val = float(last_row["close"])
                for ma in self.ma_list:
                    if close_val < float(last_row[f"ma_{ma}"]) * (1 - self.tolerance):
                        return False

            # 均线发散向上：检查最近3日趋势，允许多数均线上行即可
            if self.require_up_trend:
                up_count = 0
                for ma in self.ma_list:
                    vals = [float(df.iloc[j][f"ma_{ma}"]) for j in range(-3, 0)]
                    if vals[-1] >= vals[0]:  # 3日前 vs 最新
                        up_count += 1
                # 至少 60% 的均线在上行
                if up_count < len(self.ma_list) * 0.6:
                    return False
            return True
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False

class VolumeBreakCondition(BaseCondition):
    name = "volume_break"
    requires_ohlcv = True
    ohlcv_period = "daily"
    def __init__(self, lookback_bars: int = 20, vol_multiple: float = 1.5):
        self.lookback_bars = lookback_bars
        self.vol_multiple = vol_multiple
    def evaluate_spot(self, spot_row: pd.Series) -> bool: return True
    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < self.lookback_bars + 1:
            return False
        try:
            vol_col = "成交量" if "成交量" in ohlcv_df.columns else "volume"
            if vol_col not in ohlcv_df.columns:
                return False
            vols = pd.to_numeric(ohlcv_df[vol_col], errors="coerce").fillna(0).values
            current_vol = vols[-1]
            avg_vol = vols[-(self.lookback_bars+1):-1].mean()
            if avg_vol <= 0:
                return False
            return current_vol > avg_vol * self.vol_multiple
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False

class WeeklyMACDGoldCrossCondition(BaseCondition):
    name = "weekly_macd_gold_cross"
    requires_ohlcv = True
    ohlcv_period = "weekly"
    def __init__(self, require_zero_near: bool = True):
        self.require_zero_near = require_zero_near
    def evaluate_spot(self, spot_row: pd.Series) -> bool: return True
    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < 30:
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_macd()
            df = ta.get_dataframe()
            macd = df["macd"].values
            signal = df["macd_signal"].values
            if len(macd) < 3 or pd.isna(macd[-1]) or pd.isna(signal[-1]):
                return False
            # 允许最近2根K线内出现金叉（不要求恰好是最后一根）
            for offset in range(2):
                idx = -(offset + 1)
                prev_idx = idx - 1
                if abs(prev_idx) <= len(macd):
                    if macd[prev_idx] < signal[prev_idx] and macd[idx] >= signal[idx]:
                        return True
            return False
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False

class VolumeShrinkCondition(BaseCondition):
    name = "volume_shrink"
    requires_ohlcv = True
    ohlcv_period = "daily"
    def __init__(self, lookback_bars: int = 10, shrink_ratio: float = 0.6):
        self.lookback_bars = lookback_bars
        self.shrink_ratio = shrink_ratio
    def evaluate_spot(self, spot_row: pd.Series) -> bool: return True
    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < self.lookback_bars + 1:
            return False
        try:
            vol_col = "成交量" if "成交量" in ohlcv_df.columns else "volume"
            if vol_col not in ohlcv_df.columns:
                return False
            vols = pd.to_numeric(ohlcv_df[vol_col], errors="coerce").fillna(0).values
            current_vol = vols[-1]
            avg_vol = vols[-(self.lookback_bars+1):-1].mean()
            if avg_vol <= 0:
                return False
            return current_vol < avg_vol * self.shrink_ratio
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False

class SupportMACondition(BaseCondition):
    name = "support_ma"
    requires_ohlcv = True
    ohlcv_period = "daily"
    def __init__(self, ma_period: int = 20, close_touch: bool = True):
        self.ma_period = ma_period
        self.close_touch = close_touch
    def evaluate_spot(self, spot_row: pd.Series) -> bool: return True
    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < self.ma_period:
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_moving_averages([self.ma_period])
            df = ta.get_dataframe()
            if len(df) < 1:
                return False

            # 判断是否包含需要的列（统一大小写兼容）
            cols = [c.lower() for c in df.columns]
            if "low" not in cols or "close" not in cols:
                return False

            last_row = df.iloc[-1]
            ma_val = float(last_row[f"ma_{self.ma_period}"])
            close_val = float(last_row.get("close", last_row.get("收盘", 0)))
            low_val = float(last_row.get("low", last_row.get("最低", 0)))

            if self.close_touch:
                # 最低价触及或击穿均线（允许2%误差），且收盘价在均线附近或之上（企稳）
                return low_val <= ma_val * 1.02 and close_val >= ma_val * 0.97
            else:
                return low_val <= ma_val * 1.03
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False

# ========================
# 新增条件（Phase 3 扩展）
# ========================

class BollingerBreakoutCondition(BaseCondition):
    """
    布林带突破筛选（日线）

    逻辑：
      - direction="upper": 收盘价突破上轨 → 看多突破
      - direction="lower": 收盘价跌破下轨 → 超卖反弹机会
    """
    name = "bollinger_breakout"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, period: int = 20, std_dev: float = 2.0, direction: str = "upper"):
        self.period = period
        self.std_dev = std_dev
        self.direction = direction  # "upper" or "lower"

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < self.period:
            return False
        try:
            close_col = "close" if "close" in ohlcv_df.columns else "收盘"
            if close_col not in ohlcv_df.columns:
                return False
            closes = pd.to_numeric(ohlcv_df[close_col], errors="coerce")
            ma = closes.rolling(self.period).mean()
            std = closes.rolling(self.period).std()
            upper = ma + self.std_dev * std
            lower = ma - self.std_dev * std
            last_close = float(closes.iloc[-1])
            if self.direction == "upper":
                return last_close > float(upper.iloc[-1])
            else:
                return last_close < float(lower.iloc[-1])
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False


class KDJGoldCrossCondition(BaseCondition):
    """
    KDJ 金叉筛选（日线）

    K 线上穿 D 线，且 J 值 < j_threshold 时视为有效（避免高位金叉假信号）
    """
    name = "kdj_gold_cross"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, n: int = 9, m1: int = 3, m2: int = 3, j_threshold: float = 80):
        self.n = n
        self.m1 = m1
        self.m2 = m2
        self.j_threshold = j_threshold

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < self.n + 5:
            return False
        try:
            df = ohlcv_df.copy()
            rename = {"最高": "high", "最低": "low", "收盘": "close"}
            df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)
            if not all(c in df.columns for c in ("high", "low", "close")):
                return False

            high = pd.to_numeric(df["high"], errors="coerce")
            low = pd.to_numeric(df["low"], errors="coerce")
            close = pd.to_numeric(df["close"], errors="coerce")

            low_n = low.rolling(self.n).min()
            high_n = high.rolling(self.n).max()
            rsv = (close - low_n) / (high_n - low_n) * 100
            rsv = rsv.fillna(50)

            k = rsv.ewm(alpha=1.0 / self.m1, adjust=False).mean()
            d = k.ewm(alpha=1.0 / self.m2, adjust=False).mean()
            j = 3 * k - 2 * d

            if len(k) < 4:
                return False

            # K 上穿 D（允许最近3根K线内出现金叉），且 J 不在超买区
            for offset in range(3):
                idx = -(offset + 1)
                prev_idx = idx - 1
                if abs(prev_idx) <= len(k):
                    k_prev, d_prev = float(k.iloc[prev_idx]), float(d.iloc[prev_idx])
                    k_curr, d_curr = float(k.iloc[idx]), float(d.iloc[idx])
                    j_curr = float(j.iloc[idx])
                    if k_prev <= d_prev and k_curr > d_curr and j_curr < self.j_threshold:
                        return True
            return False
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False


class MAGoldCrossCondition(_MACrossCondition):
    """均线金叉筛选：短期均线上穿长期均线"""
    name = "ma_gold_cross"
    _cross_direction = "golden"


class PriceChangeCondition(BaseCondition):
    """当日涨跌幅范围筛选（Spot条件）"""
    name = "price_change"
    requires_ohlcv = False

    def __init__(self, min_change: float = -100, max_change: float = 100):
        self.min_change = min_change
        self.max_change = max_change

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        change = float(spot_row.get("涨跌幅", 0) or 0)
        return self.min_change <= change <= self.max_change

    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        if "涨跌幅" not in df.columns:
            return pd.Series(True, index=df.index)
        c = pd.to_numeric(df["涨跌幅"], errors="coerce").fillna(0)
        return (c >= self.min_change) & (c <= self.max_change)


class MACDHistPositiveCondition(BaseCondition):
    """
    MACD 柱状图翻红筛选（日线）

    检测 MACD 柱状图（MACD-Signal）从负值转为正值（或连续 n 根为正）。
    """
    name = "macd_hist_positive"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, consecutive: int = 1):
        self.consecutive = max(1, consecutive)

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < 30:
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_macd()
            df = ta.get_dataframe()
            hist = df["macd_hist"].dropna()
            if len(hist) < self.consecutive + 1:
                return False
            # 最近 consecutive 根都为正，且最近5根内存在由负转正
            recent = hist.iloc[-self.consecutive:]
            if not all(float(v) > 0 for v in recent):
                return False
            # 检查最近5根K线内是否有从负值翻正的转折点
            lookback = min(5 + self.consecutive, len(hist))
            check_range = hist.iloc[-lookback:]
            for i in range(1, len(check_range)):
                if float(check_range.iloc[i-1]) <= 0 and float(check_range.iloc[i]) > 0:
                    return True
            return False
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False


class BoxBreakoutWithVolumeCondition(BaseCondition):
    """
    箱体放量突破筛选（日线）

    在 BoxBreakoutCondition 基础上增加成交量验证：
    突破日成交量 > 近 N 日均量 × vol_multiple，过滤无量假突破。
    """
    name = "box_breakout_volume"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(
        self,
        lookback_bars: int = 20,
        breakout_pct: float = 0.02,
        consolidation_pct: float = 0.10,
        vol_lookback: int = 20,
        vol_multiple: float = 1.5,
    ):
        self.lookback_bars = lookback_bars
        self.breakout_pct = breakout_pct
        self.consolidation_pct = consolidation_pct
        self.vol_lookback = vol_lookback
        self.vol_multiple = vol_multiple

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < self.lookback_bars + 1:
            return False
        try:
            df = ohlcv_df.copy()
            rename = {"最高": "high", "最低": "low", "收盘": "close"}
            df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)
            vol_col = "成交量" if "成交量" in df.columns else "volume"
            if not all(c in df.columns for c in ("high", "low", "close")) or vol_col not in df.columns:
                return False

            # 箱体识别
            box_df = df.iloc[-(self.lookback_bars + 1):-1]
            box_high = float(box_df["high"].max())
            box_low = float(box_df["low"].min())
            if box_low <= 0:
                return False
            amplitude = (box_high - box_low) / box_low
            if amplitude > self.consolidation_pct:
                return False

            # 突破判断
            last_close = float(df["close"].iloc[-1])
            if last_close <= box_high * (1 + self.breakout_pct):
                return False

            # 成交量验证：突破日放量
            vols = pd.to_numeric(df[vol_col], errors="coerce").fillna(0).values
            current_vol = vols[-1]
            avg_vol = vols[-(self.vol_lookback + 1):-1].mean()
            if avg_vol <= 0 or current_vol <= avg_vol * self.vol_multiple:
                return False

            return True
        except Exception as e:
            logger.debug(f"箱体放量突破检测异常: {e}")
            return False


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


# ========================
# 卖出 / 回调预警类条件（Phase 5 扩展）
# ========================
# 用于"个股卖点扫描"和回测的卖出信号。
# 设计原则：与买入条件同结构（继承 BaseCondition），通过 SIGNAL_DIRECTION 标 "sell"。


