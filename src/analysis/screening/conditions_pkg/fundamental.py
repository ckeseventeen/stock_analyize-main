"""
src/analysis/screening/conditions_pkg/fundamental.py — 基本面 / Spot 快照条件

仅依赖实时行情快照（市值/PE/PB/换手率等），第一轮内存向量化过滤。
"""
from __future__ import annotations

import pandas as pd

from src.analysis.screening.conditions_pkg.base import BaseCondition
from src.utils.logger import get_logger

logger = get_logger("screener_conditions")


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
        # 列缺失时跳过本条件（与 MarketCapCondition 一致），避免数据源退化时全表清零
        if "市盈率-动态" not in spot_row.index:
            return True
        pe = float(spot_row.get("市盈率-动态", 0) or 0)
        if pe <= 0:
            return False  # 排除亏损股（PE为负）
        return self.min_pe <= pe <= self.max_pe

    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        if "市盈率-动态" not in df.columns:
            return pd.Series(True, index=df.index)
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
        if "市净率" not in spot_row.index:
            return True
        pb = float(spot_row.get("市净率", 0) or 0)
        if pb <= 0:
            return False
        return self.min_pb <= pb <= self.max_pb

    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        if "市净率" not in df.columns:
            return pd.Series(True, index=df.index)
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
        if "最新价" not in spot_row.index:
            return True
        price = float(spot_row.get("最新价", 0) or 0)
        if price <= 0:
            return False
        return self.min_price <= price <= self.max_price

    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        if "最新价" not in df.columns:
            return pd.Series(True, index=df.index)
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

class ExcludeRiskCondition(BaseCondition):
    """
    排除风险股票（ST/退市/退市风险）

    Args:
        strict: True=排除所有ST(含*ST)+退; False=仅排除*ST+退
    """
    name = "exclude_risk"
    requires_ohlcv = False

    def __init__(self, strict: bool = True):
        self.strict = strict

    def evaluate_spot(self, spot_row: pd.Series) -> bool:
        name = str(spot_row.get("名称", ""))
        if self.strict:
            return "ST" not in name and "退" not in name
        return "*ST" not in name and "退" not in name

    def evaluate_vectorized(self, df: pd.DataFrame) -> pd.Series:
        if "名称" not in df.columns:
            return pd.Series(True, index=df.index)
        if self.strict:
            return ~df["名称"].astype(str).str.contains(r"ST|退", na=False)
        return ~df["名称"].astype(str).str.contains(r"\*ST|退", na=False)


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
        if not col:
            return pd.Series(True, index=df.index)
        val = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return val >= self.min_roe

