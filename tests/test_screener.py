"""
tests/test_screener.py — 筛选器条件测试
"""

import pandas as pd
import pytest

from src.screener.conditions import (
    MarketCapCondition,
    PBRangeCondition,
    PERangeCondition,
    PriceAboveMACondition,
    PriceRangeCondition,
    RSIOversoldCondition,
    TurnoverRateCondition,
    WeeklyMACDBottomDivergenceCondition,
)
from src.screener.config_schema import parse_screen_config


@pytest.mark.unit
class TestSpotConditions:
    """Spot 条件（快速过滤）测试"""

    def test_market_cap_in_range(self, spot_data_row):
        """市值在范围内应通过"""
        cond = MarketCapCondition(min_cap=10000, max_cap=30000)
        assert cond.evaluate_spot(spot_data_row) is True

    def test_market_cap_below_range(self, spot_data_row):
        """市值低于范围应不通过"""
        cond = MarketCapCondition(min_cap=30000, max_cap=50000)
        assert cond.evaluate_spot(spot_data_row) is False

    def test_market_cap_above_range(self, spot_data_row):
        """市值高于范围应不通过"""
        cond = MarketCapCondition(min_cap=0, max_cap=100)
        assert cond.evaluate_spot(spot_data_row) is False

    def test_pe_range_normal(self, spot_data_row):
        """正常PE范围"""
        cond = PERangeCondition(min_pe=20, max_pe=35)
        assert cond.evaluate_spot(spot_data_row) is True

    def test_pe_range_excludes_loss(self, spot_data_row_loss):
        """亏损股（PE为负）应被排除"""
        cond = PERangeCondition(min_pe=0, max_pe=100)
        assert cond.evaluate_spot(spot_data_row_loss) is False

    def test_pb_range(self, spot_data_row):
        """PB范围"""
        cond = PBRangeCondition(min_pb=5, max_pb=10)
        assert cond.evaluate_spot(spot_data_row) is True

    def test_price_range(self, spot_data_row):
        """股价范围"""
        cond = PriceRangeCondition(min_price=1000, max_price=2000)
        assert cond.evaluate_spot(spot_data_row) is True

    def test_price_range_out(self, spot_data_row):
        """股价超出范围"""
        cond = PriceRangeCondition(min_price=0, max_price=100)
        assert cond.evaluate_spot(spot_data_row) is False

    def test_turnover_rate(self, spot_data_row):
        """换手率范围"""
        cond = TurnoverRateCondition(min_rate=0, max_rate=1.0)
        assert cond.evaluate_spot(spot_data_row) is True


@pytest.mark.unit
class TestOHLCVConditions:
    """OHLCV 条件测试"""

    def test_weekly_divergence_spot_always_true(self, spot_data_row):
        """OHLCV条件在spot阶段应始终返回True"""
        cond = WeeklyMACDBottomDivergenceCondition()
        assert cond.evaluate_spot(spot_data_row) is True
        assert cond.requires_ohlcv is True

    def test_weekly_divergence_empty_df(self, spot_data_row):
        """空K线数据应返回False"""
        cond = WeeklyMACDBottomDivergenceCondition()
        assert cond.evaluate_full(spot_data_row, pd.DataFrame()) is False

    def test_weekly_divergence_with_data(self, spot_data_row, divergence_bottom_df):
        """有数据时应正常运行不报错"""
        cond = WeeklyMACDBottomDivergenceCondition(lookback_bars=80)
        result = cond.evaluate_full(spot_data_row, divergence_bottom_df)
        assert isinstance(result, bool)

    def test_rsi_oversold_empty(self, spot_data_row):
        """RSI条件空数据应返回False"""
        cond = RSIOversoldCondition(threshold=30)
        assert cond.evaluate_full(spot_data_row, pd.DataFrame()) is False

    def test_rsi_oversold_with_data(self, spot_data_row, synthetic_ohlcv_en):
        """RSI条件有数据时应正常运行"""
        cond = RSIOversoldCondition(threshold=80)  # 高阈值以确保通过
        result = cond.evaluate_full(spot_data_row, synthetic_ohlcv_en)
        assert isinstance(result, bool)

    def test_price_above_ma(self, spot_data_row, synthetic_ohlcv_en):
        """均线条件应正常运行"""
        cond = PriceAboveMACondition(ma_period=20)
        result = cond.evaluate_full(spot_data_row, synthetic_ohlcv_en)
        assert isinstance(result, bool)


@pytest.mark.unit
class TestConfigParsing:
    """筛选配置解析测试"""

    def test_parse_valid_config(self, tmp_path):
        """解析有效配置"""
        config_content = """
screen:
  conditions:
    - type: market_cap
      min: 50
      max: 500
    - type: pe_range
      min: 0
      max: 30
  output:
    sort_by: "总市值(亿)"
    limit: 20
"""
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(config_content, encoding="utf-8")

        conditions, output_config = parse_screen_config(str(config_file))
        assert len(conditions) == 2
        assert output_config["limit"] == 20

    def test_parse_with_ohlcv_conditions(self, tmp_path):
        """解析含OHLCV条件的配置"""
        config_content = """
screen:
  conditions:
    - type: weekly_macd_divergence
      lookback_bars: 40
    - type: rsi_oversold
      threshold: 25
      period: 14
"""
        config_file = tmp_path / "test_config2.yaml"
        config_file.write_text(config_content, encoding="utf-8")

        conditions, _ = parse_screen_config(str(config_file))
        assert len(conditions) == 2
        assert conditions[0].requires_ohlcv is True
        assert conditions[1].requires_ohlcv is True

    def test_parse_unknown_condition_skipped(self, tmp_path):
        """未知条件类型应被跳过"""
        config_content = """
screen:
  conditions:
    - type: market_cap
      min: 50
    - type: unknown_type
      foo: bar
"""
        config_file = tmp_path / "test_config3.yaml"
        config_file.write_text(config_content, encoding="utf-8")

        conditions, _ = parse_screen_config(str(config_file))
        assert len(conditions) == 1  # 只有 market_cap

    def test_parse_missing_file(self):
        """不存在的配置文件应返回空"""
        conditions, output = parse_screen_config("/nonexistent/path.yaml")
        assert conditions == []
        assert output == {}
