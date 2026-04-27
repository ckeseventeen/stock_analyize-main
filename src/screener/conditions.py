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

    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        if "总市值" not in df.columns:
            return pd.Series(True, index=df.index)
        cap = pd.to_numeric(df["总市值"], errors="coerce").fillna(0) / 1e8
        return (cap >= self.min_cap) & (cap <= self.max_cap)


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

    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        if "市盈率-动态" not in df.columns:
            return pd.Series(False, index=df.index)
        pe = pd.to_numeric(df["市盈率-动态"], errors="coerce").fillna(0)
        return (pe > 0) & (pe >= self.min_pe) & (pe <= self.max_pe)


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

    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        if "市净率" not in df.columns:
            return pd.Series(False, index=df.index)
        pb = pd.to_numeric(df["市净率"], errors="coerce").fillna(0)
        return (pb > 0) & (pb >= self.min_pb) & (pb <= self.max_pb)


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

    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        if "最新价" not in df.columns:
            return pd.Series(False, index=df.index)
        p = pd.to_numeric(df["最新价"], errors="coerce").fillna(0)
        return (p > 0) & (p >= self.min_price) & (p <= self.max_price)


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

    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        if "换手率" not in df.columns:
            return pd.Series(True, index=df.index)
        r = pd.to_numeric(df["换手率"], errors="coerce").fillna(0)
        return (r >= self.min_rate) & (r <= self.max_rate)


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

    def __init__(self, lookback_bars: int = 60, zero_axis_filter: bool = False, multi_level_check: bool = False):
        self.lookback_bars = lookback_bars
        self.zero_axis_filter = zero_axis_filter
        self.multi_level_check = multi_level_check

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True  # Spot阶段无法判断，放行到OHLCV阶段

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < 20:
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_macd()
            detector = MACDDivergenceDetector(ta.get_dataframe())
            # 对于周线，order=2 即可（前后2周没有更低点即为底），且背离必须发生在最近4周内
            return detector.detect_bottom_divergence(
                lookback_bars=self.lookback_bars, 
                order=2, 
                max_bars_since_trough=4,
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

    def __init__(self, lookback_bars: int = 120, zero_axis_filter: bool = False, multi_level_check: bool = False):
        self.lookback_bars = lookback_bars
        self.zero_axis_filter = zero_axis_filter
        self.multi_level_check = multi_level_check

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True

    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < 20:
            return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_macd()
            detector = MACDDivergenceDetector(ta.get_dataframe())
            # 对于日线，order=5（前后1周没有更低点即为底），背离必须发生在最近5个交易日内
            return detector.detect_bottom_divergence(
                lookback_bars=self.lookback_bars,
                order=5,
                max_bars_since_trough=5,
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

class ExcludeSTCondition(BaseCondition):
    name = "exclude_st"
    requires_ohlcv = False
    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        name = str(spot_row.get("名称", ""))
        return "ST" not in name and "退" not in name
    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        if "名称" not in df.columns: return pd.Series(True, index=df.index)
        return ~df["名称"].astype(str).str.contains("ST|退", na=False)

class ExcludeDelistingRiskCondition(BaseCondition):
    name = "exclude_delisting_risk"
    requires_ohlcv = False
    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        name = str(spot_row.get("名称", ""))
        return "*ST" not in name and "退" not in name
    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        if "名称" not in df.columns: return pd.Series(True, index=df.index)
        return ~df["名称"].astype(str).str.contains(r"\*ST|退", na=False)

class ExcludeRecentUnlockCondition(BaseCondition):
    name = "exclude_recent_unlock"
    requires_ohlcv = False
    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        return True # 占位，需接入额外解禁数据源才能真正实现
    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        return pd.Series(True, index=df.index)

class ROEFilterCondition(BaseCondition):
    name = "roe_filter"
    requires_ohlcv = False
    def __init__(self, min_roe: float = 10):
        self.min_roe = min_roe
    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        if "ROE" not in spot_row.index and "净资产收益率" not in spot_row.index:
            return True
        val = spot_row.get("ROE", spot_row.get("净资产收益率", 0))
        return float(val or 0) >= self.min_roe
    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        col = "ROE" if "ROE" in df.columns else ("净资产收益率" if "净资产收益率" in df.columns else None)
        if not col: return pd.Series(True, index=df.index)
        val = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return val >= self.min_roe

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
        if ohlcv_df is None or len(ohlcv_df) < max(self.ma_list): return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_moving_averages(self.ma_list)
            df = ta.get_dataframe()
            if len(df) < 3: return False
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
        except Exception:
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
        if ohlcv_df is None or len(ohlcv_df) < self.lookback_bars + 1: return False
        try:
            vol_col = "成交量" if "成交量" in ohlcv_df.columns else "volume"
            if vol_col not in ohlcv_df.columns: return False
            vols = pd.to_numeric(ohlcv_df[vol_col], errors="coerce").fillna(0).values
            current_vol = vols[-1]
            avg_vol = vols[-(self.lookback_bars+1):-1].mean()
            if avg_vol <= 0: return False
            return current_vol > avg_vol * self.vol_multiple
        except Exception: return False

class WeeklyMACDGoldCrossCondition(BaseCondition):
    name = "weekly_macd_gold_cross"
    requires_ohlcv = True
    ohlcv_period = "weekly"
    def __init__(self, require_zero_near: bool = True):
        self.require_zero_near = require_zero_near
    def evaluate_spot(self, spot_row: pd.Series) -> bool: return True
    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < 30: return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_macd()
            df = ta.get_dataframe()
            macd = df["macd"].values
            signal = df["macd_signal"].values
            if len(macd) < 3 or pd.isna(macd[-1]) or pd.isna(signal[-1]): return False
            # 允许最近2根K线内出现金叉（不要求恰好是最后一根）
            for offset in range(2):
                idx = -(offset + 1)
                prev_idx = idx - 1
                if abs(prev_idx) <= len(macd):
                    if macd[prev_idx] < signal[prev_idx] and macd[idx] >= signal[idx]:
                        return True
            return False
        except Exception: return False

class VolumeShrinkCondition(BaseCondition):
    name = "volume_shrink"
    requires_ohlcv = True
    ohlcv_period = "daily"
    def __init__(self, lookback_bars: int = 10, shrink_ratio: float = 0.6):
        self.lookback_bars = lookback_bars
        self.shrink_ratio = shrink_ratio
    def evaluate_spot(self, spot_row: pd.Series) -> bool: return True
    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < self.lookback_bars + 1: return False
        try:
            vol_col = "成交量" if "成交量" in ohlcv_df.columns else "volume"
            if vol_col not in ohlcv_df.columns: return False
            vols = pd.to_numeric(ohlcv_df[vol_col], errors="coerce").fillna(0).values
            current_vol = vols[-1]
            avg_vol = vols[-(self.lookback_bars+1):-1].mean()
            if avg_vol <= 0: return False
            return current_vol < avg_vol * self.shrink_ratio
        except Exception: return False

class SupportMACondition(BaseCondition):
    name = "support_ma"
    requires_ohlcv = True
    ohlcv_period = "daily"
    def __init__(self, ma_period: int = 20, close_touch: bool = True):
        self.ma_period = ma_period
        self.close_touch = close_touch
    def evaluate_spot(self, spot_row: pd.Series) -> bool: return True
    def evaluate_full(self, spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        if ohlcv_df is None or len(ohlcv_df) < self.ma_period: return False
        try:
            ta = TechnicalAnalyzer(ohlcv_df)
            ta.add_moving_averages([self.ma_period])
            df = ta.get_dataframe()
            if len(df) < 1: return False
            
            # 判断是否包含需要的列（统一大小写兼容）
            cols = [c.lower() for c in df.columns]
            if "low" not in cols or "close" not in cols: return False
            
            last_row = df.iloc[-1]
            ma_val = float(last_row[f"ma_{self.ma_period}"])
            close_val = float(last_row.get("close", last_row.get("收盘", 0)))
            low_val = float(last_row.get("low", last_row.get("最低", 0)))
            
            if self.close_touch:
                # 最低价触及或击穿均线（允许2%误差），且收盘价在均线附近或之上（企稳）
                return low_val <= ma_val * 1.02 and close_val >= ma_val * 0.97
            else:
                return low_val <= ma_val * 1.03
        except Exception: return False

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
        except Exception:
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
        except Exception:
            return False


class MAGoldCrossCondition(BaseCondition):
    """均线金叉筛选：短期均线上穿长期均线"""
    name = "ma_gold_cross"
    requires_ohlcv = True
    ohlcv_period = "daily"

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
            # 允许最近3根K线内出现金叉
            for offset in range(3):
                idx = -(offset + 1)
                prev_idx = idx - 1
                if abs(prev_idx) <= len(df):
                    prev_fast = float(df[fast_col].iloc[prev_idx])
                    prev_slow = float(df[slow_col].iloc[prev_idx])
                    curr_fast = float(df[fast_col].iloc[idx])
                    curr_slow = float(df[slow_col].iloc[idx])
                    if prev_fast <= prev_slow and curr_fast > curr_slow:
                        return True
            return False
        except Exception:
            return False


class MADeathCrossCondition(BaseCondition):
    """均线死叉筛选：短期均线下穿长期均线"""
    name = "ma_death_cross"
    requires_ohlcv = True
    ohlcv_period = "daily"

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
            # 允许最近3根K线内出现死叉
            for offset in range(3):
                idx = -(offset + 1)
                prev_idx = idx - 1
                if abs(prev_idx) <= len(df):
                    prev_fast = float(df[fast_col].iloc[prev_idx])
                    prev_slow = float(df[slow_col].iloc[prev_idx])
                    curr_fast = float(df[fast_col].iloc[idx])
                    curr_slow = float(df[slow_col].iloc[idx])
                    if prev_fast >= prev_slow and curr_fast < curr_slow:
                        return True
            return False
        except Exception:
            return False


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
        except Exception:
            return False


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
        except Exception:
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
    "rsi_overbought": RSIOverboughtCondition,
    "price_above_ma": PriceAboveMACondition,
    "box_breakout": BoxBreakoutCondition,
    "downtrend_breakout": DowntrendBreakoutCondition,
    "exclude_st": ExcludeSTCondition,
    "exclude_delisting_risk": ExcludeDelistingRiskCondition,
    "exclude_recent_unlock": ExcludeRecentUnlockCondition,
    "exclude_recent_large_unlock": ExcludeRecentUnlockCondition,
    "roe_filter": ROEFilterCondition,
    "multi_ma_bull": MultiMABullCondition,
    "volume_break": VolumeBreakCondition,
    "weekly_macd_gold_cross": WeeklyMACDGoldCrossCondition,
    "volume_shrink": VolumeShrinkCondition,
    "support_ma": SupportMACondition,
    "bollinger_breakout": BollingerBreakoutCondition,
    "kdj_gold_cross": KDJGoldCrossCondition,
    "ma_gold_cross": MAGoldCrossCondition,
    "ma_death_cross": MADeathCrossCondition,
    "price_change": PriceChangeCondition,
    "macd_hist_positive": MACDHistPositiveCondition,
}


# ========================
# 条件元数据（用于前端渲染表单）
# ========================

# 条件分类，供前端分组展示
CONDITION_CATEGORIES = {
    "基本面/排除": [
        "exclude_st", "exclude_delisting_risk", "exclude_recent_unlock",
        "exclude_recent_large_unlock", "roe_filter",
    ],
    "估值/财务": [
        "market_cap", "pe_range", "pb_range", "price_range",
        "turnover_rate", "price_change",
    ],
    "均线": [
        "price_above_ma", "multi_ma_bull", "ma_gold_cross",
        "ma_death_cross", "support_ma",
    ],
    "MACD": [
        "weekly_macd_divergence", "daily_macd_divergence",
        "weekly_macd_gold_cross", "macd_hist_positive",
    ],
    "RSI": [
        "rsi_oversold", "rsi_overbought",
    ],
    "量价/突破": [
        "volume_break", "volume_shrink", "box_breakout",
        "downtrend_breakout", "bollinger_breakout",
    ],
    "KDJ": [
        "kdj_gold_cross",
    ],
}

# 条件的中文标签
CONDITION_LABELS: dict[str, str] = {
    "market_cap": "市值范围",
    "pe_range": "市盈率(PE)范围",
    "pb_range": "市净率(PB)范围",
    "price_range": "股价范围",
    "turnover_rate": "换手率范围",
    "price_change": "涨跌幅范围",
    "weekly_macd_divergence": "周线MACD底背离",
    "daily_macd_divergence": "日线MACD底背离",
    "weekly_macd_gold_cross": "周线MACD金叉",
    "macd_hist_positive": "MACD柱状图翻红",
    "rsi_oversold": "RSI超卖",
    "rsi_overbought": "RSI超买",
    "price_above_ma": "价格站上均线",
    "multi_ma_bull": "均线多头排列",
    "ma_gold_cross": "均线金叉",
    "ma_death_cross": "均线死叉",
    "support_ma": "均线支撑",
    "volume_break": "放量突破",
    "volume_shrink": "缩量",
    "box_breakout": "箱体突破",
    "downtrend_breakout": "下降趋势线突破",
    "bollinger_breakout": "布林带突破",
    "kdj_gold_cross": "KDJ金叉",
    "exclude_st": "排除ST股",
    "exclude_delisting_risk": "排除退市风险股",
    "exclude_recent_unlock": "排除近期解禁",
    "exclude_recent_large_unlock": "排除近期大额解禁",
    "roe_filter": "ROE筛选",
}
