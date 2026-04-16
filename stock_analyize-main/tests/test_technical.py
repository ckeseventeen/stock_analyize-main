"""
tests/test_technical.py — 技术指标计算测试
"""
import pandas as pd
import pytest

from src.analysis.technical.indicators import TechnicalAnalyzer


@pytest.mark.unit
class TestTechnicalAnalyzer:
    """TechnicalAnalyzer 单元测试"""

    def test_chinese_column_normalization(self, synthetic_ohlcv_df):
        """中文列名应自动映射为英文"""
        ta = TechnicalAnalyzer(synthetic_ohlcv_df)
        df = ta.get_dataframe()
        assert "close" in df.columns
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "volume" in df.columns

    def test_english_columns_pass_through(self, synthetic_ohlcv_en):
        """英文列名应保持不变"""
        ta = TechnicalAnalyzer(synthetic_ohlcv_en)
        df = ta.get_dataframe()
        assert "close" in df.columns

    def test_macd_columns_added(self, synthetic_ohlcv_df):
        """add_macd 应添加 macd, macd_signal, macd_hist 三列"""
        ta = TechnicalAnalyzer(synthetic_ohlcv_df)
        ta.add_macd()
        df = ta.get_dataframe()
        assert "macd" in df.columns
        assert "macd_signal" in df.columns
        assert "macd_hist" in df.columns

    def test_rsi_range_0_to_100(self, synthetic_ohlcv_df):
        """RSI 值应在 0-100 范围内"""
        ta = TechnicalAnalyzer(synthetic_ohlcv_df)
        ta.add_rsi(14)
        df = ta.get_dataframe()
        rsi = df["rsi_14"].dropna()
        assert rsi.min() >= 0
        assert rsi.max() <= 100

    def test_kdj_range(self, synthetic_ohlcv_df):
        """KDJ K/D 值应大致在 0-100 范围"""
        ta = TechnicalAnalyzer(synthetic_ohlcv_df)
        ta.add_kdj()
        df = ta.get_dataframe()
        assert "kdj_k" in df.columns
        assert "kdj_d" in df.columns
        assert "kdj_j" in df.columns
        # K 和 D 通常在 0-100，J 可以超出
        k_vals = df["kdj_k"].dropna()
        assert k_vals.min() >= -10  # 允许微小偏移
        assert k_vals.max() <= 110

    def test_bollinger_bands_ordering(self, synthetic_ohlcv_df):
        """布林带: upper > middle > lower"""
        ta = TechnicalAnalyzer(synthetic_ohlcv_df)
        ta.add_bollinger()
        df = ta.get_dataframe().dropna(subset=["bb_upper", "bb_middle", "bb_lower"])
        assert (df["bb_upper"] >= df["bb_middle"]).all()
        assert (df["bb_middle"] >= df["bb_lower"]).all()

    def test_moving_averages_correct_periods(self, synthetic_ohlcv_df):
        """均线列名应包含对应周期数"""
        ta = TechnicalAnalyzer(synthetic_ohlcv_df)
        ta.add_moving_averages([5, 20, 60])
        df = ta.get_dataframe()
        assert "ma_5" in df.columns
        assert "ma_20" in df.columns
        assert "ma_60" in df.columns
        assert "ma_10" not in df.columns  # 未指定的不应出现

    def test_volume_analysis(self, synthetic_ohlcv_df):
        """成交量分析应添加 vol_ma5, vol_ma10, vol_ratio"""
        ta = TechnicalAnalyzer(synthetic_ohlcv_df)
        ta.add_volume_analysis()
        df = ta.get_dataframe()
        assert "vol_ma5" in df.columns
        assert "vol_ma10" in df.columns
        assert "vol_ratio" in df.columns

    def test_add_all_no_exception(self, synthetic_ohlcv_df):
        """add_all 应无异常地添加全部指标"""
        ta = TechnicalAnalyzer(synthetic_ohlcv_df)
        ta.add_all()
        df = ta.get_dataframe()
        # 至少应有 MACD + RSI + KDJ + BB + MA + Volume 指标列
        assert len(df.columns) > 15

    def test_chaining_api(self, synthetic_ohlcv_df):
        """链式调用应正常工作"""
        ta = TechnicalAnalyzer(synthetic_ohlcv_df)
        result = ta.add_macd().add_rsi().add_bollinger()
        assert result is ta  # 链式返回自身

    def test_empty_df_raises(self):
        """空 DataFrame 应抛出 ValueError"""
        with pytest.raises(ValueError):
            TechnicalAnalyzer(pd.DataFrame())
