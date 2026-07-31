"""
tests/test_fundamental_data.py — 基本面深度数据（三大报表/财务指标/业绩预告/快报）

全离线 mock akshare。覆盖：
  - 代码 → 东财 symbol 前缀转换
  - latest_report_period 季度报告期推算
  - fetch 分发/缓存/必填参数/未知源/异常吞并
  - /api/fundamental/sources 与 /api/fundamental/{source} 端点
"""
from __future__ import annotations

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import src.data.providers.fundamental_data as fund_mod
from src.data.providers.fundamental_data import (
    FUNDAMENTAL_SOURCES,
    FundamentalDataProvider,
    _to_em_symbol,
    latest_report_period,
)


def _sample_df(n: int = 4) -> pd.DataFrame:
    return pd.DataFrame({
        "报告期": [f"2025-0{i+1}-01" for i in range(n)],
        "净资产收益率(%)": [10.0 + i for i in range(n)],
        "毛利率(%)": [40.0 + i for i in range(n)],
    })


@pytest.fixture
def provider(tmp_path):
    return FundamentalDataProvider(cache_dir=str(tmp_path / "fund"))


@pytest.mark.unit
class TestHelpers:
    @pytest.mark.parametrize("code,expected", [
        ("600519", "SH600519"),
        ("000001", "SZ000001"),
        ("300750", "SZ300750"),
        ("688981", "SH688981"),
        ("830799", "BJ830799"),
        ("430047", "BJ430047"),
        ("600519.SS", "SH600519"),
        ("sh600519", "SH600519"),
    ])
    def test_to_em_symbol(self, code, expected):
        assert _to_em_symbol(code) == expected

    @pytest.mark.parametrize("now,expected", [
        ("2026-07-17", "20260630"),
        ("2026-04-01", "20260331"),
        ("2026-01-15", "20251231"),
        ("2026-12-31", "20261231"),
        ("2026-03-30", "20251231"),
    ])
    def test_latest_report_period(self, now, expected):
        assert latest_report_period(pd.Timestamp(now)) == expected


@pytest.mark.unit
class TestFetch:
    def test_registry_has_six_sources(self):
        assert set(FUNDAMENTAL_SOURCES) == {
            "balance_sheet", "profit_sheet", "cash_flow_sheet",
            "indicators", "forecast", "express",
        }

    def test_statement_sources_use_em_symbol(self, provider, monkeypatch):
        seen = {}
        monkeypatch.setattr(fund_mod.ak, "stock_balance_sheet_by_report_em",
                            lambda symbol: (seen.update(bs=symbol), _sample_df())[1])
        monkeypatch.setattr(fund_mod.ak, "stock_profit_sheet_by_report_em",
                            lambda symbol: (seen.update(ps=symbol), _sample_df())[1])
        monkeypatch.setattr(fund_mod.ak, "stock_cash_flow_sheet_by_report_em",
                            lambda symbol: (seen.update(cf=symbol), _sample_df())[1])
        assert not provider.fetch("balance_sheet", code="600519").empty
        assert not provider.fetch("profit_sheet", code="000001").empty
        assert not provider.fetch("cash_flow_sheet", code="300750").empty
        assert seen == {"bs": "SH600519", "ps": "SZ000001", "cf": "SZ300750"}

    def test_indicators_default_start_year(self, provider, monkeypatch):
        seen = {}

        def fake(symbol, start_year):
            seen.update(symbol=symbol, start_year=start_year)
            return _sample_df()

        monkeypatch.setattr(fund_mod.ak, "stock_financial_analysis_indicator", fake)
        provider.fetch("indicators", code="600519")
        assert seen["symbol"] == "600519"
        assert int(seen["start_year"]) == pd.Timestamp.now().year - 5

    def test_forecast_defaults_to_latest_period(self, provider, monkeypatch):
        seen = {}
        monkeypatch.setattr(fund_mod.ak, "stock_yjyg_em",
                            lambda date: (seen.update(date=date), _sample_df())[1])
        provider.fetch("forecast")
        assert seen["date"] == latest_report_period()
        provider.fetch("forecast", period="20251231")
        assert seen["date"] == "20251231"

    def test_cache_hit(self, provider, monkeypatch):
        calls = {"n": 0}

        def fake(date):
            calls["n"] += 1
            return _sample_df()

        monkeypatch.setattr(fund_mod.ak, "stock_yjkb_em", fake)
        provider.fetch("express", period="20260630")
        provider.fetch("express", period="20260630")
        assert calls["n"] == 1

    def test_missing_code_raises(self, provider):
        with pytest.raises(ValueError, match="缺少必填参数"):
            provider.fetch("indicators")

    def test_unknown_source_raises(self, provider):
        with pytest.raises(KeyError, match="未知基本面数据源"):
            provider.fetch("nope")

    def test_upstream_error_raises_runtime_error(self, provider, monkeypatch):
        """上游异常 → RuntimeError（与合法空结果区分，前端能报数据源故障）"""
        def boom(symbol):
            raise ConnectionError("接口挂了")

        monkeypatch.setattr(fund_mod.ak, "stock_balance_sheet_by_report_em", boom)
        with pytest.raises(RuntimeError, match="拉取失败"):
            provider.fetch("balance_sheet", code="600519")

    def test_unknown_param_raises(self, provider):
        # indicators 不接受 period（那是业绩预告/快报的参数）→ 明确 422 而非空表
        with pytest.raises(ValueError, match="不支持参数"):
            provider.fetch("indicators", code="600519", period="20251231")


@pytest.mark.unit
class TestFundamentalApi:
    @pytest.fixture
    def client(self, tmp_path, monkeypatch):
        from fastapi.testclient import TestClient

        import src.api.main as api_main
        monkeypatch.setattr(fund_mod, "_GLOBAL_FUND_PROVIDER",
                            FundamentalDataProvider(cache_dir=str(tmp_path / "f")))
        return TestClient(api_main.app)

    def test_sources_endpoint(self, client):
        r = client.get("/api/fundamental/sources")
        assert r.status_code == 200
        assert len(r.json()) == 6

    def test_unknown_source_404(self, client):
        assert client.get("/api/fundamental/nope").status_code == 404

    def test_missing_param_422(self, client):
        assert client.get("/api/fundamental/indicators").status_code == 422

    def test_query_returns_rows(self, client, monkeypatch):
        monkeypatch.setattr(fund_mod.ak, "stock_financial_analysis_indicator",
                            lambda symbol, start_year: _sample_df(6))
        r = client.get("/api/fundamental/indicators?code=600519")
        assert r.status_code == 200
        data = r.json()
        assert data["count"] == 6
        assert "净资产收益率(%)" in data["columns"]
