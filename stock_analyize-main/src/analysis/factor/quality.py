"""
src/analysis/factor/quality.py — 质量因子

ROE、毛利率、营收增长率等。部分可从 spot 数据直接提取。
"""
from src.analysis.factor.base import BaseFactor


class ROEFactor(BaseFactor):
    """净资产收益率因子"""

    name = "roe"
    description = "ROE(%)"
    higher_is_better = True

    def validate(self, data: dict) -> bool:
        spot = data.get("spot")
        return spot is not None and "ROE" in spot.index

    def calculate(self, data: dict) -> float:
        return float(data["spot"]["ROE"])


class GrossMarginFactor(BaseFactor):
    """毛利率因子（需要营收和成本数据）"""

    name = "gross_margin"
    description = "毛利率(%)"
    higher_is_better = True

    def validate(self, data: dict) -> bool:
        spot = data.get("spot")
        return spot is not None and "毛利率" in spot.index

    def calculate(self, data: dict) -> float:
        return float(data["spot"]["毛利率"])


class RevenueGrowthFactor(BaseFactor):
    """营收同比增长率因子"""

    name = "revenue_growth"
    description = "营收同比增长率(%)"
    higher_is_better = True

    def validate(self, data: dict) -> bool:
        spot = data.get("spot")
        # 部分 akshare 接口可能包含此字段
        if spot is None:
            return False
        return "营收同比" in spot.index or "营业收入同比增长率" in spot.index

    def calculate(self, data: dict) -> float:
        spot = data["spot"]
        for col_name in ("营收同比", "营业收入同比增长率"):
            if col_name in spot.index:
                return float(spot[col_name])
        return float("nan")


class AmplitudeFactor(BaseFactor):
    """振幅因子"""

    name = "amplitude"
    description = "振幅(%)"
    higher_is_better = False

    def validate(self, data: dict) -> bool:
        spot = data.get("spot")
        return spot is not None and "振幅" in spot.index

    def calculate(self, data: dict) -> float:
        return float(data["spot"]["振幅"])
