"""
tests/test_factors.py — 因子计算测试
"""
import numpy as np
import pandas as pd
import pytest

from src.analysis.factor.engine import FactorEngine
from src.analysis.factor.momentum import Return20D, ReturnFactor
from src.analysis.factor.valuation import MarketCapFactor, PBFactor, PEFactor, TurnoverRateFactor


@pytest.mark.unit
class TestValuationFactors:
    """估值因子测试"""

    def test_pe_factor_extraction(self, spot_data_row):
        """PE因子应正确提取市盈率"""
        factor = PEFactor()
        data = {"spot": spot_data_row}
        assert factor.validate(data) is True
        val = factor.calculate(data)
        assert val == pytest.approx(28.5)

    def test_pb_factor_extraction(self, spot_data_row):
        """PB因子应正确提取市净率"""
        factor = PBFactor()
        data = {"spot": spot_data_row}
        val = factor.calculate(data)
        assert val == pytest.approx(8.2)

    def test_market_cap_factor(self, spot_data_row):
        """市值因子应转为亿元"""
        factor = MarketCapFactor()
        data = {"spot": spot_data_row}
        val = factor.calculate(data)
        assert val == pytest.approx(22600.0, rel=0.01)

    def test_turnover_rate_factor(self, spot_data_row):
        """换手率因子应正确提取"""
        factor = TurnoverRateFactor()
        data = {"spot": spot_data_row}
        val = factor.calculate(data)
        assert val == pytest.approx(0.35)

    def test_missing_column_returns_nan(self):
        """缺少必要列时 safe_calculate 应返回 NaN"""
        factor = PEFactor()
        data = {"spot": pd.Series({"名称": "测试", "最新价": 10.0})}
        val = factor.safe_calculate(data)
        assert np.isnan(val)

    def test_none_spot_returns_nan(self):
        """spot 为 None 时应返回 NaN"""
        factor = PEFactor()
        data = {"spot": None}
        val = factor.safe_calculate(data)
        assert np.isnan(val)


@pytest.mark.unit
class TestMomentumFactors:
    """动量因子测试"""

    def test_return_factor_calculation(self, synthetic_ohlcv_en):
        """收益率因子应正确计算"""
        factor = ReturnFactor(period=20)
        data = {"daily_df": synthetic_ohlcv_en}
        assert factor.validate(data) is True
        val = factor.calculate(data)
        assert isinstance(val, float)
        assert not np.isnan(val)

    def test_return_factor_insufficient_data(self):
        """数据不足时应验证失败"""
        factor = ReturnFactor(period=20)
        df = pd.DataFrame({"close": [10.0] * 5})
        data = {"daily_df": df}
        assert factor.validate(data) is False

    def test_return_20d_convenience(self, synthetic_ohlcv_en):
        """Return20D 便捷类应正常工作"""
        factor = Return20D()
        data = {"daily_df": synthetic_ohlcv_en}
        val = factor.safe_calculate(data)
        assert isinstance(val, float)


@pytest.mark.unit
class TestFactorEngine:
    """FactorEngine 测试"""

    def test_compute_multiple_factors(self, spot_data_row, synthetic_ohlcv_en):
        """批量计算多个因子"""
        engine = FactorEngine()
        engine.add_factor(PEFactor())
        engine.add_factor(MarketCapFactor())

        stocks_data = {
            "600519": {"spot": spot_data_row, "daily_df": synthetic_ohlcv_en},
        }
        result = engine.compute(stocks_data)
        assert "pe_ttm" in result.columns
        assert "market_cap" in result.columns
        assert len(result) == 1

    def test_compute_single_stock(self, spot_data_row):
        """单只股票因子计算"""
        engine = FactorEngine()
        engine.add_factor(PEFactor())
        engine.add_factor(PBFactor())

        result = engine.compute_single("600519", {"spot": spot_data_row})
        assert "pe_ttm" in result
        assert "pb" in result

    def test_get_factor_names(self):
        """因子名称列表"""
        engine = FactorEngine()
        engine.add_factor(PEFactor())
        engine.add_factor(MarketCapFactor())
        names = engine.get_factor_names()
        assert names == ["pe_ttm", "market_cap"]

    def test_chaining_api(self):
        """链式添加因子"""
        engine = FactorEngine()
        result = engine.add_factor(PEFactor()).add_factor(PBFactor())
        assert result is engine
        assert len(engine.factors) == 2

    def test_ohlcv_requirements(self):
        """正确识别OHLCV需求"""
        engine = FactorEngine()
        engine.add_factor(PEFactor())
        assert engine.get_ohlcv_requirements() == set()

        engine.add_factor(Return20D())
        assert "daily" in engine.get_ohlcv_requirements()
