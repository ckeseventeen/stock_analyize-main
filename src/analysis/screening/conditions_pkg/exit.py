"""
src/analysis/screening/conditions_pkg/exit.py — 卖出向条件（止损 / 超买 / death cross / 顶背离）

卖出引擎与回测卖出侧使用。
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


class MADeathCrossCondition(_MACrossCondition):
    """均线死叉筛选：短期均线下穿长期均线"""
    name = "ma_death_cross"
    _cross_direction = "death"


class RSIOverboughtCondition(BaseCondition):
    """RSI 超买筛选（日线 RSI 高于阈值）— 可作为卖出信号"""
    name = "rsi_overbought"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, threshold: float = 70, period: int = 14):
        self.threshold = threshold
        self.period = period

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < self.period + 1:
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_rsi(self.period)
            df = ta.get_dataframe()
            rsi_col = f"rsi_{self.period}"
            rsi_val = df[rsi_col].dropna().iloc[-1]
            return bool(rsi_val > self.threshold)
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False


# ========================
# 新增条件（策略改进 Phase 4）
# ========================

class StopLossCondition(BaseCondition):
    """
    止损条件（Spot）：当前亏损超过阈值时触发卖出。

    在回测中使用：当持仓亏损超过 max_loss_pct 时卖出。
    """
    name = "stop_loss"
    requires_ohlcv = False

    def __init__(self, max_loss_pct: float = 8.0, cost_key: str = "cost_basis"):
        """
        Args:
            max_loss_pct: 最大允许亏损百分比（正数，如 8 表示 -8%）
            cost_key: 持仓成本价所在的 spot_row 键名
        """
        self.max_loss_pct = max_loss_pct
        self.cost_key = cost_key

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        price = float(spot_row.get("最新价", 0) or 0)
        cost = float(spot_row.get(self.cost_key, 0) or 0)
        if cost <= 0 or price <= 0:
            return False
        loss_pct = (cost - price) / cost * 100
        return loss_pct >= self.max_loss_pct

    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        if "最新价" not in df.columns or self.cost_key not in df.columns:
            return pd.Series(False, index=df.index)
        price = pd.to_numeric(df["最新价"], errors="coerce").fillna(0)
        cost = pd.to_numeric(df[self.cost_key], errors="coerce").fillna(0)
        loss_pct = (cost - price) / cost * 100
        return (cost > 0) & (price > 0) & (loss_pct >= self.max_loss_pct)


class TrailingStopCondition(BaseCondition):
    """
    移动止损条件（OHLCV）：从买入后的最高收盘价回落超过阈值时触发卖出。
    """
    name = "trailing_stop"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, callback_pct: float = 5.0, lookback: int = 60):
        """
        Args:
            callback_pct: 从最高价回落的百分比阈值（正数，如 5 表示回落 5%）
            lookback: 回溯最高价的K线根数
        """
        self.callback_pct = callback_pct
        self.lookback = lookback

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < 2:
            return False
        try:
            close_col = "close" if "close" in ohlcv_df.columns else "收盘"
            closes = pd.to_numeric(ohlcv_df[close_col], errors="coerce").fillna(0)
            recent_high = closes.iloc[-self.lookback:].max()
            current = closes.iloc[-1]
            if recent_high <= 0 or current <= 0:
                return False
            drawdown = (recent_high - current) / recent_high * 100
            return drawdown >= self.callback_pct
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False


class VolumePriceDivergenceCondition(BaseCondition):
    """
    量价背离筛选（日线）

    顶背离：价格创新高但成交量萎缩 → 趋势衰竭信号
    底背离：价格创新低但成交量放大 → 底部反转信号
    """
    name = "volume_price_divergence"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, lookback_bars: int = 30, direction: str = "top"):
        """
        Args:
            lookback_bars: 回溯区间
            direction: "top" 顶背离 或 "bottom" 底背离
        """
        self.lookback_bars = lookback_bars
        self.direction = direction

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < self.lookback_bars:
            return False
        try:
            df = ohlcv_df.tail(self.lookback_bars).copy()
            close_col = "close" if "close" in df.columns else "收盘"
            vol_col = "成交量" if "成交量" in df.columns else "volume"
            if close_col not in df.columns or vol_col not in df.columns:
                return False

            closes = pd.to_numeric(df[close_col], errors="coerce")
            vols = pd.to_numeric(df[vol_col], errors="coerce")

            # 将区间分为前半段和后半段
            half = len(df) // 2
            if half < 3:
                return False

            first_half_close = closes.iloc[:half]
            second_half_close = closes.iloc[half:]
            first_half_vol = vols.iloc[:half]
            second_half_vol = vols.iloc[half:]

            if self.direction == "top":
                # 顶背离：后半段价格更高但量更小
                price_new_high = second_half_close.max() > first_half_close.max()
                vol_shrink = second_half_vol.mean() < first_half_vol.mean() * 0.8
                return price_new_high and vol_shrink
            else:
                # 底背离：后半段价格更低但量更大
                price_new_low = second_half_close.min() < first_half_close.min()
                vol_expand = second_half_vol.mean() > first_half_vol.mean() * 1.2
                return price_new_low and vol_expand
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False


class WeeklyMACDTopDivergenceCondition(BaseCondition):
    """
    周线 MACD 顶背离 — 中期趋势衰竭信号

    价格创新高但 MACD 柱状图未创新高。
    在持仓 / 看顶判断时是高优先级信号。
    """
    name = "weekly_macd_top_divergence"
    requires_ohlcv = True
    ohlcv_period = "weekly"

    def __init__(self, lookback_bars: int = 60):
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
            return detector.detect_top_divergence(lookback_bars=self.lookback_bars)
        except Exception as e:
            logger.debug(f"周线顶背离检测异常: {e}")
            return False


class DailyMACDTopDivergenceCondition(BaseCondition):
    """日线 MACD 顶背离 — 短期超买衰竭"""
    name = "daily_macd_top_divergence"
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
            return detector.detect_top_divergence(lookback_bars=self.lookback_bars)
        except Exception as e:
            logger.debug(f"日线顶背离检测异常: {e}")
            return False


class KDJDeathCrossCondition(BaseCondition):
    """
    KDJ 死叉（高位）— 与 KDJGoldCrossCondition 对偶

    K 下穿 D 且 J > j_threshold 视为有效（避免低位假死叉）
    """
    name = "kdj_death_cross"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, n: int = 9, m1: int = 3, m2: int = 3, j_threshold: float = 70):
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
            # 检查最近3根K线内死叉，且 J 处于高位（确认是高位死叉，非低位震荡）
            for offset in range(3):
                idx = -(offset + 1)
                prev_idx = idx - 1
                if abs(prev_idx) <= len(k):
                    k_prev, d_prev = float(k.iloc[prev_idx]), float(d.iloc[prev_idx])
                    k_curr, d_curr = float(k.iloc[idx]), float(d.iloc[idx])
                    j_prev = float(j.iloc[prev_idx])
                    if k_prev >= d_prev and k_curr < d_curr and j_prev > self.j_threshold:
                        return True
            return False
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False


class BIASCondition(BaseCondition):
    """
    乖离率（BIAS）— 短线均值回归预警

    BIAS = (close - MA_n) / MA_n × 100%
    bias > threshold ⇒ 偏离均线过远，5-10 日内回踩概率 60-80%

    A 股常用阈值：
      - MA20: ±8% （短线敏感）
      - MA60: ±15% （中线参考）
    """
    name = "bias"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, ma_period: int = 20, threshold: float = 8.0, direction: str = "above"):
        """
        Args:
            ma_period: 均线周期
            threshold: BIAS 百分比阈值（绝对值，如 8 = 8%）
            direction: "above" 偏离向上（卖出信号）/ "below" 偏离向下（买入信号）/ "both" 任一方向
        """
        self.ma_period = ma_period
        self.threshold = threshold
        self.direction = direction

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < self.ma_period:
            return False
        try:
            close_col = "close" if "close" in ohlcv_df.columns else "收盘"
            if close_col not in ohlcv_df.columns:
                return False
            close = pd.to_numeric(ohlcv_df[close_col], errors="coerce")
            ma = close.rolling(self.ma_period).mean()
            last_close = float(close.iloc[-1])
            last_ma = float(ma.iloc[-1])
            if last_ma <= 0:
                return False
            bias_pct = (last_close - last_ma) / last_ma * 100
            if self.direction == "above":
                return bias_pct > self.threshold
            if self.direction == "below":
                return bias_pct < -self.threshold
            return abs(bias_pct) > self.threshold
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False


class BreakBelowMACondition(BaseCondition):
    """
    跌破均线 — 趋势破位预警

    与 PriceAboveMACondition 对偶：
    收盘价从均线之上跌破到均线之下。
    """
    name = "break_below_ma"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, ma_period: int = 60, lookback: int = 3):
        """
        Args:
            ma_period: 均线周期（常用 20 / 60 / 250）
            lookback: 最近 N 根 K 线内跌破即触发
        """
        self.ma_period = ma_period
        self.lookback = max(1, lookback)

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < self.ma_period + self.lookback:
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_moving_averages([self.ma_period])
            df = ta.get_dataframe()
            ma_col = f"ma_{self.ma_period}"
            close_col = "close" if "close" in df.columns else "收盘"
            if close_col not in df.columns or ma_col not in df.columns:
                return False
            recent = df.iloc[-(self.lookback + 1):].dropna(subset=[ma_col, close_col])
            if len(recent) < 2:
                return False
            # 找最近一次"上一根在均线之上 且 这一根跌到均线之下"的转折
            closes = recent[close_col].astype(float).values
            mas = recent[ma_col].astype(float).values
            for i in range(1, len(recent)):
                if closes[i - 1] >= mas[i - 1] and closes[i] < mas[i]:
                    return True
            return False
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False


class ATRTrailingStopCondition(BaseCondition):
    """
    ATR 动态移动止损（机构常用）

    比固定百分比止损更自适应：
    - 高波动股：止损线更宽，不容易被洗
    - 低波动股：止损线更紧，避免无意义的回撤

    触发条件: 收盘价 < (最近 N 日最高价 - K × ATR(period))
    经验值: K=2.0~2.5, period=14
    """
    name = "atr_trailing_stop"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, atr_period: int = 14, multiplier: float = 2.5,
                 lookback: int = 30):
        """
        Args:
            atr_period: ATR 计算窗口
            multiplier: K 值，2.0 紧 / 2.5 中 / 3.0 松
            lookback: 移动止损"高点"的回溯窗口（30 日 = ~1.5 个月）
        """
        self.atr_period = atr_period
        self.multiplier = multiplier
        self.lookback = lookback

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < max(self.atr_period, self.lookback) + 1:
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
            prev_close = close.shift(1)

            # True Range
            tr = pd.concat([
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ], axis=1).max(axis=1)
            atr = tr.rolling(self.atr_period).mean()

            recent_high = high.tail(self.lookback).max()
            cur_close = float(close.iloc[-1])
            cur_atr = float(atr.iloc[-1])
            if cur_atr <= 0 or pd.isna(cur_atr):
                return False
            stop_line = recent_high - self.multiplier * cur_atr
            return bool(cur_close < stop_line)
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False


class VolumeBlowoffCondition(BaseCondition):
    """
    天量天价 — 派发 / 顶部信号

    单日成交量 > N 日均量 × 倍数，且当日有大涨。
    经典的"天量见天价"信号，常出现于趋势末端的恐慌性买入。
    """
    name = "volume_blowoff"
    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, lookback_bars: int = 60, vol_multiple: float = 3.0,
                 min_price_change_pct: float = 5.0):
        """
        Args:
            lookback_bars: 均量回溯窗口
            vol_multiple: 当日量 / 均量 倍数（默认 3 倍）
            min_price_change_pct: 当日最小涨幅（默认 5%，配合放量才算 blowoff）
        """
        self.lookback_bars = lookback_bars
        self.vol_multiple = vol_multiple
        self.min_price_change_pct = min_price_change_pct

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < self.lookback_bars + 1:
            return False
        try:
            vol_col = "成交量" if "成交量" in ohlcv_df.columns else "volume"
            close_col = "close" if "close" in ohlcv_df.columns else "收盘"
            if vol_col not in ohlcv_df.columns or close_col not in ohlcv_df.columns:
                return False
            vols = pd.to_numeric(ohlcv_df[vol_col], errors="coerce").fillna(0).values
            closes = pd.to_numeric(ohlcv_df[close_col], errors="coerce").fillna(0).values
            if len(vols) < 2 or len(closes) < 2:
                return False
            cur_vol = vols[-1]
            avg_vol = vols[-(self.lookback_bars + 1):-1].mean()
            if avg_vol <= 0:
                return False
            vol_ratio = cur_vol / avg_vol
            if vol_ratio < self.vol_multiple:
                return False
            # 当日涨幅
            prev_close = closes[-2]
            if prev_close <= 0:
                return False
            chg_pct = (closes[-1] - prev_close) / prev_close * 100
            return bool(chg_pct >= self.min_price_change_pct)
        except Exception as e:
            logger.debug(f"[{self.name}] 条件评估异常，按不通过处理: {type(e).__name__}: {e}")
            return False
