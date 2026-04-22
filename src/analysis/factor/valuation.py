"""
src/analysis/factor/valuation.py — 估值类因子

从 ak.stock_zh_a_spot_em() 实时行情中直接提取，无需额外API调用。
"""
from src.analysis.factor.base import BaseFactor


class PEFactor(BaseFactor):
    """市盈率(动态)因子"""

    name = "pe_ttm"
    description = "市盈率(动态)"
    higher_is_better = False

    def validate(self, data: dict) -> bool:
        spot = data.get("spot")
        return spot is not None and "市盈率-动态" in spot.index

    def calculate(self, data: dict) -> float:
        return float(data["spot"]["市盈率-动态"])


class PBFactor(BaseFactor):
    """市净率因子"""

    name = "pb"
    description = "市净率"
    higher_is_better = False

    def validate(self, data: dict) -> bool:
        spot = data.get("spot")
        return spot is not None and "市净率" in spot.index

    def calculate(self, data: dict) -> float:
        return float(data["spot"]["市净率"])


class PSFactor(BaseFactor):
    """市销率因子（总市值 / 营业总收入TTM）

    注意：ak.stock_zh_a_spot_em() 实时行情中不含营收字段，
    因此需要调用方在 data["financial"] 中提供 ttm_revenue（TTM营业总收入）。
    若未提供 financial 数据，则返回 NaN。
    """

    name = "ps"
    description = "市销率"
    higher_is_better = False

    def validate(self, data: dict) -> bool:
        spot = data.get("spot")
        if spot is None:
            return False
        # 必须有总市值；营收数据来自 financial 字段（可选）
        return "总市值" in spot.index

    def calculate(self, data: dict) -> float:
        spot = data["spot"]
        market_cap = float(spot.get("总市值", 0) or 0)
        if market_cap <= 0:
            return float("nan")

        # 尝试从 financial 字典获取 TTM 营收数据
        financial = data.get("financial", {})
        ttm_revenue = float(financial.get("ttm_revenue", 0) or 0)
        if ttm_revenue <= 0:
            # spot 行情无营收字段，且调用方未提供 financial 数据，无法计算 PS
            return float("nan")

        # PS = 总市值 / TTM营业总收入
        return market_cap / ttm_revenue


class MarketCapFactor(BaseFactor):
    """总市值因子（亿元）"""

    name = "market_cap"
    description = "总市值(亿元)"
    higher_is_better = True

    def validate(self, data: dict) -> bool:
        spot = data.get("spot")
        return spot is not None and "总市值" in spot.index

    def calculate(self, data: dict) -> float:
        cap = float(data["spot"]["总市值"] or 0)
        return cap / 1e8


class TurnoverRateFactor(BaseFactor):
    """换手率因子"""

    name = "turnover_rate"
    description = "换手率(%)"
    higher_is_better = False

    def validate(self, data: dict) -> bool:
        spot = data.get("spot")
        return spot is not None and "换手率" in spot.index

    def calculate(self, data: dict) -> float:
        return float(data["spot"]["换手率"])
