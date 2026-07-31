"""
tests/test_service_layering.py — 分层约束的可执行守卫（A3）

背景：API 层曾有 9 处直接 import src.analysis / src.data / src.strategy /
src.ml / src.portfolio，绕过服务层。每加一个新功能就多一处越层调用。
本测试把"API 只依赖 services + trading"这条架构约束变成会失败的测试，
防止腐化复发。
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

ROOT = Path(__file__).resolve().parents[1]

# API 层允许直接依赖的内部模块（services 是正门；trading/core 是薄公共层）
_API_ALLOWED = ("src.services", "src.trading", "src.core", "src.utils")


@pytest.mark.unit
class TestApiLayering:
    def test_api_only_depends_on_services(self):
        src = (ROOT / "src" / "api" / "main.py").read_text(encoding="utf-8")
        bad = []
        for m in re.finditer(r'^\s*(?:from|import)\s+(src\.[\w.]+)', src, re.M):
            mod = m.group(1)
            if not mod.startswith(_API_ALLOWED):
                bad.append(mod)
        assert not bad, (
            f"API 层越层依赖了 {sorted(set(bad))}；"
            f"请在 src/services 下加一个服务函数，由 API 调用它")

    def test_required_services_exist(self):
        """A3 引入的服务层模块必须可导入且暴露关键入口"""
        from src.services import backtest_service, factor_service, ml_service, portfolio_service
        from src.services import market_data_service as mdsvc

        assert callable(factor_service.list_alpha158)
        assert callable(mdsvc.list_alt_sources)
        assert callable(mdsvc.list_fundamental_sources)
        assert callable(mdsvc.is_minute_freq)
        assert callable(ml_service.model_status)
        assert callable(ml_service.start_training)
        assert callable(portfolio_service.run_portfolio_diagnosis)
        assert callable(backtest_service.run_optimization)
        assert callable(backtest_service.list_optimizable_strategies)


@pytest.mark.unit
class TestServiceBehaviourParity:
    """服务层下沉后，端点行为必须与下沉前一致"""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient

        import src.api.main as api_main
        return TestClient(api_main.app)

    def test_alpha158_endpoint(self, client):
        r = client.get("/api/factors/alpha158")
        assert r.status_code == 200 and len(r.json()) == 31

    def test_altdata_sources(self, client):
        r = client.get("/api/altdata/sources")
        assert r.status_code == 200
        assert {"key", "label", "params"} <= set(r.json()[0])

    def test_fundamental_sources(self, client):
        r = client.get("/api/fundamental/sources")
        assert r.status_code == 200 and len(r.json()) == 6

    def test_optimizable(self, client):
        r = client.get("/api/backtest/optimizable")
        assert r.status_code == 200 and r.json()[0]["key"] == "ma_crossover"

    def test_ml_status_shape(self, client):
        r = client.get("/api/ml/status")
        assert r.status_code == 200 and "trained" in r.json()

    def test_unknown_source_still_404(self, client):
        assert client.get("/api/altdata/nope").status_code == 404
        assert client.get("/api/fundamental/nope").status_code == 404

    def test_optimize_unknown_strategy_404(self, client):
        r = client.post("/api/backtest/optimize",
                        json={"code": "600519", "strategy": "nope"})
        assert r.status_code == 404

    def test_optimize_unknown_method_422(self, client):
        r = client.post("/api/backtest/optimize",
                        json={"code": "600519", "method": "magic"})
        assert r.status_code == 422


@pytest.mark.unit
class TestMlServiceConcurrency:
    """下沉时顺带修的真 bug：训练状态曾是路由函数局部变量，并发点击会重复启动"""

    def test_training_state_is_process_level(self, monkeypatch):
        from src.services import ml_service

        started_threads = []

        class FakeThread:
            def __init__(self, *a, **k):
                started_threads.append(k.get("name"))

            def start(self):
                pass

        monkeypatch.setattr(ml_service.threading, "Thread", FakeThread)
        ml_service._STATE.running = False
        try:
            ok1, msg1 = ml_service.start_training()
            ok2, msg2 = ml_service.start_training()   # 第二次应被拒
            assert ok1 is True
            assert ok2 is False and "进行中" in msg2
            assert len(started_threads) == 1, "并发调用不应启动第二个训练线程"
        finally:
            ml_service._STATE.running = False

    def test_status_snapshot_keys(self):
        from src.services import ml_service

        s = ml_service.training_status()
        assert {"running", "done", "error", "started_at", "finished_at"} <= set(s)
