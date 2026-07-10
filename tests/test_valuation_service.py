"""
tests/test_valuation_service.py — 估值服务层单元测试（纯计算部分，无网络）
"""
from __future__ import annotations

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.services import valuation_service as vsvc


@pytest.mark.unit
class TestPercentileBadge:
    def test_bands(self):
        assert vsvc.percentile_badge(5) == ("极度低估", "low")
        assert vsvc.percentile_badge(35) == ("合理偏低", "low")
        assert vsvc.percentile_badge(65) == ("合理偏高", "mid")
        assert vsvc.percentile_badge(95) == ("极度高估", "high")

    def test_boundaries(self):
        assert vsvc.percentile_badge(20)[0] == "合理偏低"
        assert vsvc.percentile_badge(50)[0] == "合理偏高"
        assert vsvc.percentile_badge(80)[0] == "极度高估"


@pytest.mark.unit
class TestTargetPriceRows:
    def test_normal(self):
        result = {"scenarios": [100.0, 150.0, 200.0], "price": 120.0}
        rows = vsvc.target_price_rows(result)
        assert [r["档位"] for r in rows] == ["保守", "中性", "乐观"]
        assert rows[0]["目标价"] == 100.0
        assert rows[1]["相对当前"] == "+25.0%"
        assert rows[0]["相对当前"] == "-16.7%"

    def test_no_price(self):
        rows = vsvc.target_price_rows({"scenarios": [1, 2, 3]})
        assert all(r["相对当前"] == "" for r in rows)

    def test_missing_scenarios(self):
        rows = vsvc.target_price_rows({})
        assert len(rows) == 3
        assert all(r["目标价"] == 0 for r in rows)


@pytest.mark.unit
class TestSummaryRows:
    def test_full_data(self):
        annual_df = pd.DataFrame(
            {"x": [1, 2]},
            index=pd.to_datetime(["2024-12-31", "2025-12-31"]),
        )
        result = {
            "annual_df": annual_df,
            "ttm_revenue": 5e10, "ttm_net_profit": 1e10,
            "price": 88.5, "current_pe": 25.5, "hist_percentile": 42.0,
        }
        fin_df = pd.DataFrame([{"营业总收入": 100.0, "营业成本": 60.0}])
        rows = vsvc.summary_rows(result, fin_df, "pe")
        by_key = {r["关键指标"]: r["数据"] for r in rows}
        assert by_key["财报最新年度"] == "2025"
        assert by_key["营业总收入 (亿元)"] == "500.00"
        assert by_key["毛利率 (%)"] == "40.00%"
        assert by_key["当前 PE (TTM)"] == "25.50"
        assert by_key["历史 PE 分位数"] == "42.00%"

    def test_missing_data_degrades(self):
        rows = vsvc.summary_rows({}, None, "ps")
        by_key = {r["关键指标"]: r["数据"] for r in rows}
        assert by_key["财报最新年度"] == "-"
        assert by_key["当前 PS (TTM)"] == "N/A"


@pytest.mark.unit
class TestBuildStockConfig:
    def test_a_market(self):
        cfg = vsvc.build_stock_config(" 600519 ", " 茅台 ", "a", "pe", [10, 20, 30])
        assert cfg["code"] == "600519"
        assert cfg["name"] == "茅台"
        assert cfg["pe_range"] == [10, 20, 30]
        assert cfg["market_name"]  # 来自 market_registry 的 label

    def test_unknown_market_raises(self):
        with pytest.raises(KeyError):
            vsvc.build_stock_config("AAPL", "苹果", "jp", "pe", [10, 20, 30])
