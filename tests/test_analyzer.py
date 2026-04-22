"""
tests/test_analyzer.py — Analyzer TTM 计算与财年检测测试
"""
import os
import sys

import pandas as pd
import pytest

# 将项目根目录加入 sys.path 以支持根级模块导入
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from analyzer import BaseAnalyzer


class ConcreteAnalyzer(BaseAnalyzer):
    """用于测试的具体 Analyzer（直接使用已清洗好的数据）"""

    def _clean_financial_data(self) -> pd.DataFrame:
        return self.raw_fin


@pytest.mark.unit
class TestFiscalYearDetection:
    """财年月份检测测试"""

    def test_detect_december_fiscal_year(self, sample_financial_df):
        """标准12月财年应检测为12"""
        market_data = {"price": 100.0, "market_cap": 1e12}
        config = {"name": "test", "code": "000001", "valuation": "pe", "pe_range": [10, 20, 30]}
        analyzer = ConcreteAnalyzer(sample_financial_df, pd.DataFrame(), market_data, config)
        month = analyzer._detect_fiscal_year_month(sample_financial_df)
        assert month == 12

    def test_detect_september_fiscal_year(self, sample_financial_df_sept_fiscal):
        """9月财年（如苹果）应检测为9"""
        market_data = {"price": 100.0, "market_cap": 1e12}
        config = {"name": "test", "code": "AAPL", "valuation": "pe", "pe_range": [10, 20, 30]}
        analyzer = ConcreteAnalyzer(sample_financial_df_sept_fiscal, pd.DataFrame(), market_data, config)
        month = analyzer._detect_fiscal_year_month(sample_financial_df_sept_fiscal)
        assert month == 9

    def test_tie_prefers_december(self):
        """相同月份出现次数相同时应优先选择12月"""
        # 构造：6月和12月各出现在2个不同年份
        dates = pd.to_datetime(["2023-12-31", "2022-12-31", "2023-06-30", "2022-06-30"])
        df = pd.DataFrame({"归母净利润": [100, 90, 50, 45]}, index=dates)
        df.sort_index(ascending=False, inplace=True)

        market_data = {"price": 100.0, "market_cap": 1e12}
        config = {"name": "test", "code": "000001", "valuation": "pe", "pe_range": [10, 20, 30]}
        analyzer = ConcreteAnalyzer(df, pd.DataFrame(), market_data, config)
        month = analyzer._detect_fiscal_year_month(df)
        assert month == 12

    def test_empty_df_defaults_to_12(self):
        """空数据应默认返回12"""
        market_data = {"price": 100.0, "market_cap": 1e12}
        config = {"name": "test", "code": "000001", "valuation": "pe", "pe_range": [10, 20, 30]}
        empty_df = pd.DataFrame()
        analyzer = ConcreteAnalyzer(empty_df, pd.DataFrame(), market_data, config)
        month = analyzer._detect_fiscal_year_month(empty_df)
        assert month == 12


@pytest.mark.unit
class TestTTMCalculation:
    """TTM 计算测试"""

    def test_ttm_annual_report_direct(self, sample_financial_df):
        """最新报告为年报时应直接取值"""
        # 修改数据使最新日期为12月年报
        dates = pd.to_datetime([
            "2023-12-31", "2022-12-31", "2021-12-31", "2020-12-31", "2019-12-31",
        ])
        data = {
            "归母净利润": [600e8, 550e8, 500e8, 460e8, 400e8],
            "营业总收入": [1200e8, 1100e8, 1000e8, 920e8, 800e8],
            "营业成本": [500e8, 480e8, 450e8, 420e8, 380e8],
        }
        df = pd.DataFrame(data, index=dates)
        df.sort_index(ascending=False, inplace=True)

        market_data = {"price": 1800.0, "market_cap": 2.26e12}
        config = {"name": "test", "code": "000001", "valuation": "pe", "pe_range": [10, 20, 30]}
        analyzer = ConcreteAnalyzer(df, pd.DataFrame(), market_data, config)
        ttm = analyzer._calc_ttm(df, "归母净利润")
        assert ttm == pytest.approx(600e8)

    def test_ttm_quarterly_calculation(self, sample_financial_df):
        """季报数据的 TTM = 当期YTD + 上年年报 - 上年同期YTD"""
        market_data = {"price": 1800.0, "market_cap": 2.26e12}
        config = {"name": "test", "code": "000001", "valuation": "pe", "pe_range": [10, 20, 30]}
        analyzer = ConcreteAnalyzer(sample_financial_df, pd.DataFrame(), market_data, config)

        ttm = analyzer._calc_ttm(sample_financial_df, "归母净利润")
        # 最新Q3(2024-09-30) = 450e8
        # TTM = Q3_2024 + Annual_2023 - Q3_2023 = 450 + 600 - 420 = 630 (亿)
        expected = 450e8 + 600e8 - 420e8
        assert ttm == pytest.approx(expected)

    def test_ttm_missing_metric_returns_zero(self, sample_financial_df):
        """指标不存在时应返回 0.0"""
        market_data = {"price": 100.0, "market_cap": 1e12}
        config = {"name": "test", "code": "000001", "valuation": "pe", "pe_range": [10, 20, 30]}
        analyzer = ConcreteAnalyzer(sample_financial_df, pd.DataFrame(), market_data, config)
        ttm = analyzer._calc_ttm(sample_financial_df, "不存在的指标")
        assert ttm == 0.0

    def test_process_returns_dict(self, sample_financial_df):
        """process 应返回包含关键字段的字典"""
        market_data = {"price": 1800.0, "market_cap": 2.26e12}
        config = {
            "name": "测试股票",
            "code": "000001",
            "market_name": "A股",
            "category_name": "测试",
            "valuation": "pe",
            "pe_range": [10, 20, 30],
        }
        analyzer = ConcreteAnalyzer(sample_financial_df, pd.DataFrame(), market_data, config)
        result = analyzer.process()
        assert isinstance(result, dict)
        assert "ttm_net_profit" in result
        assert "ttm_revenue" in result
        assert "scenarios" in result
        assert "price" in result

    def test_process_empty_data_returns_empty(self):
        """空数据应返回空字典"""
        market_data = {"price": 100.0, "market_cap": 1e12}
        config = {"name": "test", "code": "000001", "valuation": "pe", "pe_range": [10, 20, 30]}
        analyzer = ConcreteAnalyzer(pd.DataFrame(), pd.DataFrame(), market_data, config)
        result = analyzer.process()
        assert result == {}


@pytest.mark.unit
class TestBugFixes:
    """验证本次修复的 bug 是否生效"""

    def test_process_called_twice_no_error(self, sample_financial_df):
        """修复：process() 二次调用不应因 hist_val 被原地修改而崩溃"""
        # 构造带 trade_date 列的历史估值数据
        hist_val = pd.DataFrame({
            "trade_date": pd.date_range("2020-01-01", periods=100, freq="W"),
            "pe_ttm": [20 + i * 0.1 for i in range(100)],
        })
        market_data = {"price": 1800.0, "market_cap": 2.26e12}
        config = {
            "name": "测试股票", "code": "000001", "market_name": "A股",
            "category_name": "测试", "valuation": "pe", "pe_range": [10, 20, 30],
        }
        analyzer = ConcreteAnalyzer(sample_financial_df, hist_val, market_data, config)

        # 第一次调用应正常返回
        result1 = analyzer.process()
        assert isinstance(result1, dict)
        assert "ttm_net_profit" in result1

        # 第二次调用不应崩溃（修复前会因 trade_date 已是索引而 KeyError）
        result2 = analyzer.process()
        assert isinstance(result2, dict)
        assert result2["ttm_net_profit"] == result1["ttm_net_profit"]

    def test_process_zero_price_returns_empty(self, sample_financial_df):
        """修复：股价为0时应提前终止返回空字典，不产生无意义结果"""
        market_data = {"price": 0.0, "market_cap": 0.0}
        config = {
            "name": "测试股票", "code": "000001", "market_name": "A股",
            "category_name": "测试", "valuation": "pe", "pe_range": [10, 20, 30],
        }
        analyzer = ConcreteAnalyzer(sample_financial_df, pd.DataFrame(), market_data, config)
        result = analyzer.process()
        assert result == {}

    def test_process_zero_market_cap_returns_empty(self, sample_financial_df):
        """修复：市值为0时应提前终止"""
        market_data = {"price": 100.0, "market_cap": 0.0}
        config = {
            "name": "测试股票", "code": "000001", "market_name": "A股",
            "category_name": "测试", "valuation": "pe", "pe_range": [10, 20, 30],
        }
        analyzer = ConcreteAnalyzer(sample_financial_df, pd.DataFrame(), market_data, config)
        result = analyzer.process()
        assert result == {}

    def test_process_ps_valuation(self, sample_financial_df):
        """PS 估值路径应正常返回结果（不崩溃）"""
        market_data = {"price": 1800.0, "market_cap": 2.26e12}
        config = {
            "name": "测试股票", "code": "000001", "market_name": "A股",
            "category_name": "测试", "valuation": "ps", "ps_range": [5, 10, 15],
        }
        analyzer = ConcreteAnalyzer(sample_financial_df, pd.DataFrame(), market_data, config)
        result = analyzer.process()
        assert isinstance(result, dict)
        assert "current_ps" in result
        assert "scenarios" in result
        assert len(result["scenarios"]) == 3

    def test_hist_val_none_handled(self, sample_financial_df):
        """hist_val 为 None 时不应崩溃"""
        market_data = {"price": 1800.0, "market_cap": 2.26e12}
        config = {
            "name": "测试股票", "code": "000001", "market_name": "A股",
            "category_name": "测试", "valuation": "pe", "pe_range": [10, 20, 30],
        }
        analyzer = ConcreteAnalyzer(sample_financial_df, None, market_data, config)
        result = analyzer.process()
        assert isinstance(result, dict)
        assert result["hist_percentile"] == 0.0


@pytest.mark.unit
class TestInternationalAnalyzer:
    """测试合并后的 InternationalStockAnalyzer（港股/美股共用）"""

    def test_international_analyzer_hk(self):
        """港股 analyzer 应正确映射英文指标并返回结果"""
        from analyzer import InternationalStockAnalyzer

        # 模拟 akshare 东方财富返回的港股财务数据
        dates = pd.to_datetime(["2024-06-30", "2023-12-31", "2022-12-31", "2021-12-31"])
        raw_fin = pd.DataFrame({
            "Net Income Common Stockholders": [100e8, 200e8, 180e8, 160e8],
            "Total Revenue": [500e8, 900e8, 800e8, 700e8],
            "Cost Of Revenue": [300e8, 500e8, 450e8, 400e8],
        }, index=dates)
        raw_fin.sort_index(ascending=False, inplace=True)

        market_data = {"price": 350.0, "market_cap": 3.3e12}
        config = {
            "name": "腾讯控股", "code": "00700", "market_name": "港股",
            "category_name": "科技", "valuation": "pe", "pe_range": [15, 25, 35],
        }
        analyzer = InternationalStockAnalyzer(raw_fin, pd.DataFrame(), market_data, config)
        result = analyzer.process()
        assert isinstance(result, dict)
        assert result["market_name"] == "港股"
        assert "ttm_net_profit" in result

    def test_international_analyzer_us(self):
        """美股 analyzer 应以 market_name 区分日志，功能与港股一致"""
        from analyzer import InternationalStockAnalyzer

        dates = pd.to_datetime(["2024-09-30", "2023-09-30", "2022-09-30", "2021-09-30"])
        raw_fin = pd.DataFrame({
            "Net Income Common Stockholders": [95e9, 97e9, 99e9, 94e9],
            "Total Revenue": [380e9, 383e9, 394e9, 365e9],
            "Cost Of Revenue": [160e9, 160e9, 170e9, 155e9],
        }, index=dates)
        raw_fin.sort_index(ascending=False, inplace=True)

        market_data = {"price": 230.0, "market_cap": 3.5e12}
        config = {
            "name": "Apple", "code": "AAPL", "market_name": "美股",
            "category_name": "科技", "valuation": "pe", "pe_range": [20, 30, 40],
        }
        analyzer = InternationalStockAnalyzer(raw_fin, pd.DataFrame(), market_data, config)
        result = analyzer.process()
        assert isinstance(result, dict)
        assert result["market_name"] == "美股"
