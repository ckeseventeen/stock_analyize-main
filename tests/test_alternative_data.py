"""
tests/test_alternative_data.py — 另类数据接入（8 项数据源，全离线 mock）

覆盖：
  - AlternativeDataProvider.fetch：分发、缓存、必填参数校验、未知源、异常吞并
  - margin 日期自动回溯（跳过无数据的周末/节假日）
  - /api/altdata/sources 与 /api/altdata/{source} 端点
"""
from __future__ import annotations

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import src.data.providers.alternative_data as alt_mod
from src.data.providers.alternative_data import (
    ALT_DATA_SOURCES,
    AlternativeDataProvider,
)


def _sample_df(n: int = 3) -> pd.DataFrame:
    return pd.DataFrame({
        "代码": [f"60000{i}" for i in range(n)],
        "名称": [f"股票{i}" for i in range(n)],
        "数值": [float(i) for i in range(n)],
    })


@pytest.fixture
def provider(tmp_path):
    return AlternativeDataProvider(cache_dir=str(tmp_path / "altdata"))


@pytest.mark.unit
class TestSourceRegistry:
    def test_eight_roadmap_sources_present(self):
        # 路线图 8 项 + 北向汇总
        for key in ("lhb", "northbound", "northbound_stock", "block_trade",
                    "margin", "restricted_release", "holder_number",
                    "dividend", "concept_cons"):
            assert key in ALT_DATA_SOURCES
            spec = ALT_DATA_SOURCES[key]
            assert spec["label"] and "params" in spec and spec["ttl_hours"] > 0


@pytest.mark.unit
class TestFetch:
    def test_dispatch_and_cache(self, provider, monkeypatch):
        calls = {"n": 0}

        def fake_lhb(**kwargs):
            calls["n"] += 1
            return _sample_df()

        monkeypatch.setattr(alt_mod.ak, "stock_lhb_detail_em", fake_lhb)
        df1 = provider.fetch("lhb", days=5)
        df2 = provider.fetch("lhb", days=5)
        assert not df1.empty and len(df2) == len(df1)
        assert calls["n"] == 1, "第二次应命中缓存"
        # 不同参数是独立缓存键
        provider.fetch("lhb", days=10)
        assert calls["n"] == 2

    def test_unknown_source_raises(self, provider):
        with pytest.raises(KeyError, match="未知另类数据源"):
            provider.fetch("nope")

    def test_missing_required_param_raises(self, provider):
        with pytest.raises(ValueError, match="缺少必填参数"):
            provider.fetch("holder_number")

    def test_upstream_error_raises_runtime_error(self, provider, monkeypatch):
        """上游异常 → RuntimeError（与合法空结果区分，前端能报数据源故障）"""
        def boom(**kwargs):
            raise ConnectionError("接口挂了")

        monkeypatch.setattr(alt_mod.ak, "stock_zh_a_gdhs_detail_em", boom)
        with pytest.raises(RuntimeError, match="拉取失败"):
            provider.fetch("holder_number", code="600519")

    def test_unknown_param_raises(self, provider):
        with pytest.raises(ValueError, match="不支持参数"):
            provider.fetch("northbound", code="600519")

    def test_per_stock_sources(self, provider, monkeypatch):
        monkeypatch.setattr(alt_mod.ak, "stock_hsgt_individual_em",
                            lambda symbol: _sample_df())
        monkeypatch.setattr(alt_mod.ak, "stock_restricted_release_queue_em",
                            lambda symbol: _sample_df())
        monkeypatch.setattr(alt_mod.ak, "stock_history_dividend_detail",
                            lambda symbol, indicator: _sample_df())
        monkeypatch.setattr(alt_mod.ak, "stock_board_concept_cons_em",
                            lambda symbol: _sample_df())
        assert not provider.fetch("northbound_stock", code="600519").empty
        assert not provider.fetch("restricted_release", code="600519").empty
        assert not provider.fetch("dividend", code="600519").empty
        assert not provider.fetch("concept_cons", symbol="融资融券").empty


@pytest.mark.unit
class TestMarginDateRollback:
    def test_rolls_back_to_last_trading_day(self, monkeypatch):
        seen_dates = []

        def fake_margin(date):
            seen_dates.append(date)
            # 前两天（周末）无数据，第三天有
            if len(seen_dates) < 3:
                return pd.DataFrame()
            return _sample_df()

        monkeypatch.setattr(alt_mod.ak, "stock_margin_detail_szse", fake_margin)
        df = AlternativeDataProvider.get_margin()
        assert not df.empty
        assert len(seen_dates) == 3

    def test_explicit_date_no_rollback(self, monkeypatch):
        seen = []
        monkeypatch.setattr(alt_mod.ak, "stock_margin_detail_szse",
                            lambda date: (seen.append(date), _sample_df())[1])
        AlternativeDataProvider.get_margin(date="20260710")
        assert seen == ["20260710"]


@pytest.mark.unit
class TestAltDataApi:
    @pytest.fixture
    def client(self, tmp_path, monkeypatch):
        from fastapi.testclient import TestClient

        import src.api.main as api_main
        # API 用的全局单例替换成 tmp 缓存目录的实例
        monkeypatch.setattr(alt_mod, "_GLOBAL_ALT_PROVIDER",
                            AlternativeDataProvider(cache_dir=str(tmp_path / "alt")))
        return TestClient(api_main.app)

    def test_sources_endpoint(self, client):
        r = client.get("/api/altdata/sources")
        assert r.status_code == 200
        data = r.json()
        assert len(data) == len(ALT_DATA_SOURCES)
        assert {"key", "label", "params"} <= set(data[0])

    def test_unknown_source_404(self, client):
        assert client.get("/api/altdata/nope").status_code == 404

    def test_missing_param_422(self, client):
        assert client.get("/api/altdata/holder_number").status_code == 422

    def test_query_returns_rows(self, client, monkeypatch):
        monkeypatch.setattr(alt_mod.ak, "stock_lhb_detail_em",
                            lambda **kw: _sample_df(5))
        r = client.get("/api/altdata/lhb?days=3")
        assert r.status_code == 200
        data = r.json()
        assert data["count"] == 5
        assert data["columns"] == ["代码", "名称", "数值"]
        assert len(data["rows"]) == 5

    def test_empty_result_shape(self, client, monkeypatch):
        monkeypatch.setattr(alt_mod.ak, "stock_zh_a_gdhs_detail_em",
                            lambda symbol: pd.DataFrame())
        r = client.get("/api/altdata/holder_number?code=600519")
        assert r.status_code == 200
        assert r.json() == {"count": 0, "columns": [], "rows": []}

    def test_upstream_failure_returns_502_not_empty(self, client, monkeypatch):
        """数据源故障 → 502 带原因（而不是伪装成'共 0 条'）"""
        def boom(symbol):
            raise ConnectionError("blocked")

        monkeypatch.setattr(alt_mod.ak, "stock_zh_a_gdhs_detail_em", boom)
        r = client.get("/api/altdata/holder_number?code=600519")
        assert r.status_code == 502
        assert "拉取失败" in r.json()["detail"]
