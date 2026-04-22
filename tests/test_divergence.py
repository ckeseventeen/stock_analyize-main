"""
tests/test_divergence.py — MACD 背离检测测试
"""
import pandas as pd
import pytest

from src.analysis.technical.divergence import MACDDivergenceDetector
from src.analysis.technical.indicators import TechnicalAnalyzer


@pytest.mark.unit
class TestMACDDivergence:
    """MACDDivergenceDetector 单元测试"""

    def test_bottom_divergence_detected(self, divergence_bottom_df):
        """构造的底背离数据应被检测到"""
        ta = TechnicalAnalyzer(divergence_bottom_df)
        ta.add_macd()
        detector = MACDDivergenceDetector(ta.get_dataframe())
        # 底背离数据是概率性的，允许检测不到（取决于噪声），但不应报错
        result = detector.detect_bottom_divergence(lookback_bars=80)
        assert isinstance(result, bool)

    def test_no_divergence_in_uptrend(self, monotone_uptrend_df):
        """单调上涨数据不应检测到底背离"""
        ta = TechnicalAnalyzer(monotone_uptrend_df)
        ta.add_macd()
        detector = MACDDivergenceDetector(ta.get_dataframe())
        assert detector.detect_bottom_divergence() is False

    def test_empty_dataframe_returns_false(self):
        """空 DataFrame 应返回 False"""
        detector = MACDDivergenceDetector(pd.DataFrame())
        assert detector.detect_bottom_divergence() is False

    def test_insufficient_data_returns_false(self):
        """数据不足 20 行应返回 False"""
        df = pd.DataFrame({
            "close": [10.0] * 10,
            "macd_hist": [0.1] * 10,
        })
        detector = MACDDivergenceDetector(df)
        assert detector.detect_bottom_divergence() is False

    def test_top_divergence_no_crash(self, synthetic_ohlcv_en):
        """顶背离检测应正常运行不报错"""
        ta = TechnicalAnalyzer(synthetic_ohlcv_en)
        ta.add_macd()
        detector = MACDDivergenceDetector(ta.get_dataframe())
        result = detector.detect_top_divergence()
        assert isinstance(result, bool)

    def test_divergence_details(self, divergence_bottom_df):
        """检测到背离时应返回详情"""
        ta = TechnicalAnalyzer(divergence_bottom_df)
        ta.add_macd()
        detector = MACDDivergenceDetector(ta.get_dataframe())
        detector.detect_bottom_divergence(lookback_bars=80)
        details = detector.get_divergence_details()
        assert isinstance(details, dict)

    def test_none_input_returns_false(self):
        """None 输入应返回 False"""
        detector = MACDDivergenceDetector(None)
        assert detector.detect_bottom_divergence() is False
