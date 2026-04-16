"""
src/analysis/factor/engine.py — 因子计算引擎

组合多个因子，对批量股票数据进行因子值计算。
"""
import pandas as pd

from src.analysis.factor.base import BaseFactor
from src.utils.logger import get_logger

logger = get_logger("factor_engine")


class FactorEngine:
    """
    可组合因子计算引擎

    使用示例:
        engine = FactorEngine()
        engine.add_factor(PEFactor())
        engine.add_factor(MarketCapFactor())
        engine.add_factor(MACDDivergenceFactor())

        # stocks_data: {code: {"spot": Series, "daily_df": DataFrame, ...}}
        result_df = engine.compute(stocks_data)
    """

    def __init__(self):
        self.factors: list[BaseFactor] = []

    def add_factor(self, factor: BaseFactor) -> "FactorEngine":
        """链式添加因子"""
        self.factors.append(factor)
        return self

    def compute(self, stocks_data: dict[str, dict]) -> pd.DataFrame:
        """
        批量计算所有因子

        Args:
            stocks_data: {股票代码: {"spot": Series, "daily_df": DataFrame, "weekly_df": DataFrame}}

        Returns:
            DataFrame，索引为股票代码，列为各因子值
        """
        results = {}
        for code, data in stocks_data.items():
            row = {}
            for factor in self.factors:
                row[factor.name] = factor.safe_calculate(data)
            results[code] = row

        df = pd.DataFrame.from_dict(results, orient="index")
        df.index.name = "代码"
        return df

    def compute_single(self, code: str, data: dict) -> dict[str, float]:
        """单只股票因子计算"""
        result = {}
        for factor in self.factors:
            result[factor.name] = factor.safe_calculate(data)
        return result

    def get_factor_names(self) -> list[str]:
        """返回所有已添加因子的名称列表"""
        return [f.name for f in self.factors]

    def get_ohlcv_requirements(self) -> set[str]:
        """返回因子所需的OHLCV周期集合（如 {"daily", "weekly"}）"""
        periods = set()
        for f in self.factors:
            if f.requires_ohlcv:
                periods.add(f.ohlcv_period)
        return periods
