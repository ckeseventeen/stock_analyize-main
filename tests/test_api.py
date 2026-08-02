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
        body = client.get("/api/health").json()
        assert body["status"] == "ok"
        # 健康检查会带上 V8 守卫状态（它决定问财数据源是否可用）
        assert "v8_guard" in body

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

    def test_us_name_search_is_case_insensitive(self, client, monkeypatch):
        """线上报障：搜 "roblox" 查不到 Roblox。

        中文名无大小写之分，所以 A 股一直没暴露这个 bug；港美股名称是英文，
        原实现拿原样子串比对，大小写不一致就永远搜不到。
        """
        import src.core.config_io as cio
        monkeypatch.setattr(
            cio, "list_stocks_from_market_config",
            lambda m: [{"code": "RBLX", "name": "Roblox"}] if m == "us" else [])
        for q in ("roblox", "ROBLOX", "Roblox", "blox"):
            r = client.get("/api/stocks/search", params={"q": q, "market": "us"})
            assert r.json() == [{"code": "RBLX", "name": "Roblox"}], f"查询 {q!r} 落空"

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


@pytest.mark.unit
class TestValuationBasisFallback:
    """
    线上报障：美股 Roblox 的估值面板显示 "PE(TTM) …" + 一排负目标价。

    根因是亏损股（TTM 净利润为负）仍按 PE 估值，负 EPS 推出负目标价；
    且分析器对亏损股给出的 current_pe 是 **NaN 而非 None**，
    `v is None or v <= 0` 恒判 False，自动回退逻辑整个被绕过。
    """

    def _run(self, pe_value, ps_value=5.0):
        import pandas as pd

        from src.services import valuation_service as vsvc
        fin = pd.DataFrame({"x": [1]})

        def _fake_run(market, cfg):
            is_ps = cfg.get("valuation") == "ps"
            return vsvc.ValuationRun(
                result={"price": 35.6, "current_pe": pe_value, "current_ps": ps_value,
                        "hist_percentile": 10.0, "ttm_net_profit": -1e9,
                        "scenarios": [1.0, 2.0, 3.0] if is_ps else [-15.4, -30.8, -46.2]},
                fin_df=fin, hist_val_df=None, market_data={}, stock_config=cfg)

        return _fake_run

    def test_nan_pe_triggers_ps_fallback(self, client, monkeypatch):
        """NaN 才是亏损股的真实取值——这条断言就是那个 bug 的回归"""
        from src.services import valuation_service as vsvc
        monkeypatch.setattr(vsvc, "run_valuation", self._run(float("nan")))
        monkeypatch.setattr(vsvc, "target_price_rows", lambda r: [])
        monkeypatch.setattr(vsvc, "summary_rows", lambda *a: [])
        j = client.get("/api/stocks/us/RBLX/valuation").json()
        assert j["basis"] == "ps"
        assert "PS" in j["basis_note"]

    def test_none_pe_triggers_ps_fallback(self, client, monkeypatch):
        from src.services import valuation_service as vsvc
        monkeypatch.setattr(vsvc, "run_valuation", self._run(None))
        monkeypatch.setattr(vsvc, "target_price_rows", lambda r: [])
        monkeypatch.setattr(vsvc, "summary_rows", lambda *a: [])
        assert client.get("/api/stocks/us/RBLX/valuation").json()["basis"] == "ps"

    def test_profitable_stock_keeps_pe(self, client, monkeypatch):
        """盈利股不许被误切到 PS"""
        from src.services import valuation_service as vsvc
        monkeypatch.setattr(vsvc, "run_valuation", self._run(15.0))
        monkeypatch.setattr(vsvc, "target_price_rows", lambda r: [])
        monkeypatch.setattr(vsvc, "summary_rows", lambda *a: [])
        j = client.get("/api/stocks/a/600519/valuation").json()
        assert j["basis"] == "pe"
        assert j["basis_note"] == ""

    def test_empty_findata_reports_source_failure_not_bad_code(self, client, monkeypatch):
        """数据源限流不能报成"请检查代码"——用户会以为是自己输错了"""
        import pandas as pd

        from src.services import valuation_service as vsvc
        monkeypatch.setattr(vsvc, "run_valuation", lambda *a, **k: vsvc.ValuationRun(
            result={}, fin_df=pd.DataFrame(), hist_val_df=None,
            market_data={}, stock_config={}))
        r = client.get("/api/stocks/us/RBLX/valuation")
        assert r.status_code == 502
        assert "稍后重试" in r.json()["detail"]


@pytest.mark.unit
class TestFcfErrorAttribution:
    """FCF 取数失败与"确实无记录"必须给不同状态码与文案"""

    def test_source_failure_is_502(self, client, monkeypatch):
        import pandas as pd

        from src.services import valuation_service as vsvc
        monkeypatch.setattr(vsvc, "run_fcf", lambda *a, **k: vsvc.FCFRun(
            analyzed_df=pd.DataFrame(), score_res={}, market_cap=0.0,
            fetch_status="failed", fetch_reason="yfinance 连续 3 次取数失败"))
        r = client.get("/api/stocks/us/RBLX/fcf")
        assert r.status_code == 502
        assert "yfinance" in r.json()["detail"]

    def test_genuinely_empty_is_404(self, client, monkeypatch):
        import pandas as pd

        from src.services import valuation_service as vsvc
        monkeypatch.setattr(vsvc, "run_fcf", lambda *a, **k: vsvc.FCFRun(
            analyzed_df=pd.DataFrame(), score_res={}, market_cap=0.0,
            fetch_status="empty", fetch_reason="可能是新股/已退市"))
        assert client.get("/api/stocks/us/RBLX/fcf").status_code == 404


@pytest.mark.unit
class TestMlPanelEndpoints:
    """
    线上缺口：模型训练完后前端「训练」按钮被隐藏（只在未训练时显示），
    想重训没有入口；训练是 5-15 分钟的后台线程，也没有进度可轮询。
    """

    def test_training_status_is_pollable(self, client):
        """必须有独立的轮询端点，否则前端只知道"已启动"，不知何时结束"""
        r = client.get("/api/ml/training-status")
        assert r.status_code == 200
        body = r.json()
        assert "training" in body and "model" in body
        assert "running" in body["training"]

    def test_status_exposes_feature_importance(self, client, monkeypatch):
        """特征重要性一直写在 meta 里却没往外暴露，页面因此只有一行 IC 摘要"""
        from src.services import ml_service
        monkeypatch.setattr(ml_service, "model_status", lambda: {
            "trained": True,
            "feature_importance": [{"feature": "pe_ttm", "gain": 992867.0}],
            "cv_ic_mean": 0.028, "n_features": 55, "train_samples": 48409,
        })
        j = client.get("/api/ml/status").json()
        assert j["feature_importance"][0]["feature"] == "pe_ttm"

    def test_untrained_status_has_no_crash(self, client, monkeypatch):
        from src.services import ml_service
        monkeypatch.setattr(ml_service, "model_status",
                            lambda: {"trained": False, "message": "ML 模型未训练"})
        assert client.get("/api/ml/status").json()["trained"] is False
