"""
tests/test_api.py — REST API 层测试（TestClient，无网络路径优先）
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi.testclient import TestClient

import src.api.main as api_main
from src.trading import PaperBroker


@pytest.fixture
def client(tmp_path, monkeypatch):
    # 模拟盘隔离到 tmp + 固定价格源（无网络）
    prices = {"000001": 10.0}
    broker = PaperBroker(
        state_path=tmp_path / "paper.json",
        price_fetcher=lambda c: prices.get(c, 0.0),
    )
    monkeypatch.setattr(api_main, "_broker", broker)
    return TestClient(api_main.app)


@pytest.mark.unit
class TestBasics:
    def test_health(self, client):
        assert client.get("/api/health").json() == {"status": "ok"}

    def test_index_serves_spa(self, client):
        r = client.get("/")
        assert r.status_code == 200
        assert "量化投研平台" in r.text

    def test_strategies_list(self, client):
        r = client.get("/api/strategies")
        assert r.status_code == 200
        data = r.json()
        assert len(data) >= 1
        assert {"id", "name", "fundamental", "technical"} <= set(data[0])

    def test_screening_validation(self, client):
        r = client.post("/api/screening/run", json={"strategy_ids": []})
        assert r.status_code == 422  # 至少一个策略


@pytest.mark.unit
class TestTradingApi:
    def test_account_initial(self, client):
        r = client.get("/api/trading/account").json()
        assert r["summary"]["cash"] == 1_000_000
        assert r["positions"] == []

    def test_order_lifecycle(self, client):
        # 市价买入 → 成交
        r = client.post("/api/trading/orders", json={
            "code": "000001", "side": "buy", "qty": 500}).json()
        assert r["status"] == "filled"
        assert r["filled_price"] == 10.0

        acc = client.get("/api/trading/account").json()
        assert acc["positions"][0]["qty"] == 500

        # 限价挂单 → 撤单
        r2 = client.post("/api/trading/orders", json={
            "code": "000001", "side": "buy", "qty": 100, "price": 9.0}).json()
        assert r2["status"] == "pending"
        rc = client.delete(f"/api/trading/orders/{r2['order_id']}")
        assert rc.status_code == 200

        orders = client.get("/api/trading/orders").json()
        assert len(orders) == 2

    def test_order_rejects_bad_side(self, client):
        r = client.post("/api/trading/orders", json={
            "code": "000001", "side": "hold", "qty": 100})
        assert r.status_code == 422

    def test_cancel_missing_order_404(self, client):
        assert client.delete("/api/trading/orders/nope").status_code == 404


@pytest.fixture
def strat_client(tmp_path, monkeypatch):
    """策略管理端点：把 PATH_SCREEN 指到 tmp 副本，隔离真实配置"""
    import yaml

    from src.services import screening_service as svc
    p = tmp_path / "screen_config.yaml"
    p.write_text(yaml.safe_dump({"strategies": {
        "s1": {"name": "测试策略",
               "conditions": [{"type": "exclude_st"},
                               {"type": "ma_gold_cross", "fast_period": 5, "slow_period": 20}],
               "backtest": {"sell_conditions": [{"type": "stop_loss", "max_loss_pct": 8}]}},
    }}, allow_unicode=True), encoding="utf-8")
    monkeypatch.setattr(svc, "PATH_SCREEN", p)
    return TestClient(api_main.app)


@pytest.mark.unit
class TestStrategyManagementApi:
    def test_rename_id_and_name(self, strat_client):
        r = strat_client.patch("/api/strategies/s1",
                               json={"new_id": "s1_renamed", "new_name": "新名字"})
        assert r.status_code == 200
        ids = {s["id"]: s["name"] for s in strat_client.get("/api/strategies").json()}
        assert ids.get("s1_renamed") == "新名字"
        assert "s1" not in ids

    def test_duplicate_then_delete(self, strat_client):
        r = strat_client.post("/api/strategies/s1/duplicate")
        assert r.status_code == 200
        new_id = r.json()["id"]
        assert new_id == "s1_copy"

        r2 = strat_client.delete(f"/api/strategies/{new_id}")
        assert r2.status_code == 200
        ids = [s["id"] for s in strat_client.get("/api/strategies").json()]
        assert new_id not in ids and "s1" in ids

    def test_patch_missing_404(self, strat_client):
        r = strat_client.patch("/api/strategies/nope", json={"new_name": "x"})
        assert r.status_code == 404

    def test_delete_missing_404(self, strat_client):
        assert strat_client.delete("/api/strategies/nope").status_code == 404

    def test_backtest_rejects_no_tech_strategy(self, strat_client, monkeypatch):
        # 纯基本面策略（无技术条件）→ 400
        import yaml

        from src.services import screening_service as svc
        cfg = yaml.safe_load(svc.PATH_SCREEN.read_text(encoding="utf-8"))
        cfg["strategies"]["fund_only"] = {
            "name": "纯基本面",
            "conditions": [{"type": "market_cap", "min": 100}],
            "backtest": {"sell_conditions": [{"type": "stop_loss"}]},
        }
        svc.PATH_SCREEN.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
        r = strat_client.post("/api/strategies/fund_only/backtest", json={"code": "600519"})
        assert r.status_code == 400
        assert "技术条件" in r.json()["detail"]

    def test_backtest_missing_strategy_400(self, strat_client):
        r = strat_client.post("/api/strategies/nope/backtest", json={"code": "600519"})
        assert r.status_code == 400


@pytest.mark.unit
class TestStockSearchApi:
    def test_search_route(self, client, monkeypatch):
        from src.services import stock_service as ssvc
        monkeypatch.setattr(ssvc, "_a_share_names",
                            lambda: {"600519": "贵州茅台"})
        r = client.get("/api/stocks/search", params={"q": "茅台"})
        assert r.status_code == 200
        assert r.json() == [{"code": "600519", "name": "贵州茅台"}]

    def test_search_not_shadowed_by_market_route(self, client):
        # /api/stocks/search 不应被 /{market}/{code} 吞掉
        r = client.get("/api/stocks/search", params={"q": "x", "market": "hk"})
        assert r.status_code == 200
        assert isinstance(r.json(), list)


@pytest.mark.unit
class TestKlineAndFcfApi:
    def test_kline_shape(self, client, monkeypatch):
        import numpy as np
        import pandas as pd

        from src.services import backtest_service as btsvc
        idx = pd.date_range("2025-01-01", periods=80, freq="D")
        close = pd.Series(100 + np.arange(80.0), index=idx)
        df = pd.DataFrame({"open": close - 1, "high": close + 1,
                           "low": close - 2, "close": close,
                           "volume": 1000}, index=idx)
        monkeypatch.setattr(btsvc, "fetch_ohlcv", lambda *a, **k: df.reset_index(names="date"))
        r = client.get("/api/stocks/a/600519/kline?days=80")
        assert r.status_code == 200
        j = r.json()
        assert len(j["dates"]) == 80
        assert len(j["k"][0]) == 4          # [开, 收, 低, 高]
        assert j["ma20"][0] is None         # 前 19 根无 MA20 → null
        assert j["ma20"][-1] is not None

    def test_kline_404_when_empty(self, client, monkeypatch):
        import pandas as pd

        from src.services import backtest_service as btsvc
        monkeypatch.setattr(btsvc, "fetch_ohlcv", lambda *a, **k: pd.DataFrame())
        assert client.get("/api/stocks/a/999999/kline").status_code == 404

    def test_fcf_shape(self, client, monkeypatch):
        import pandas as pd

        from src.services import valuation_service as vsvc
        idx = pd.to_datetime(["2023-12-31", "2024-12-31"])
        adf = pd.DataFrame({
            "operating_cash_flow": [2e9, 3e9], "capex": [5e8, 6e8],
            "fcf": [1.5e9, 2.4e9], "net_profit": [1.8e9, 2.2e9],
            "fcf_margin": [15.0, 18.0],
        }, index=idx)
        fake = vsvc.FCFRun(analyzed_df=adf, market_cap=1e11, score_res={
            "scores": {"total": 80}, "summary": {"rating": "优", "judgement": "j",
                                                  "main_risk": "r", "current_fcf_yield": 2.4},
        })
        monkeypatch.setattr(vsvc, "run_fcf", lambda *a, **k: fake)
        r = client.get("/api/stocks/a/600519/fcf")
        assert r.status_code == 200
        j = r.json()
        assert j["scores"]["total"] == 80
        assert j["series"]["fcf"] == [15.0, 24.0]   # 亿元换算
        assert len(j["dates"]) == 2

    def test_fcf_404_when_empty(self, client, monkeypatch):
        import pandas as pd

        from src.services import valuation_service as vsvc
        fake = vsvc.FCFRun(analyzed_df=pd.DataFrame(), score_res={}, market_cap=0)
        monkeypatch.setattr(vsvc, "run_fcf", lambda *a, **k: fake)
        assert client.get("/api/stocks/a/999999/fcf").status_code == 404
