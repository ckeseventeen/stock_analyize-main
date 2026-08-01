"""
tests/test_v8_guard.py — V8 崩溃守卫（线上事故回归 + 误伤回归）

## 事故一：进程猝死

uvicorn 跑起来后访问个股页，进程毫无征兆消失，用户侧表现为"所有功能都用不了"。
日志尾部是

    [FATAL:partition_address_space.cc(243)] Check failed:
    !IsConfigurablePoolInitialized()

即 py_mini_racer 的 V8 初始化 FATAL 中止——**不是异常，try/except 拦不住，
进程被当场杀死**。

## 事故二：一刀切禁用误伤美股

初版守卫把 MiniRacer 换成"实例化即抛"的桩，崩溃是挡住了，但 `ak.stock_us_daily`
（新浪美股，要 V8 解密）也一起废了。东财美股接口在国内常年不通，新浪一断
**美股就彻底没有数据源**——用户报"美股 roblox 查不到"。

## 因此现在的契约是双向的

  - 探测到本环境 V8 会崩 → 必须抛**可捕获**异常（保住事故一的修复）
  - 探测到本环境 V8 正常 → 必须**放行**真实 MiniRacer（不制造事故二）
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core import v8_guard
from src.core.v8_guard import V8Disabled, apply_guard, guard_status, v8_usable


@pytest.fixture
def clean_probe():
    """每个用例跑在干净的探测状态上，避免相互污染"""
    v8_guard._reset_probe_for_tests()
    yield
    v8_guard._reset_probe_for_tests()


@pytest.mark.unit
class TestGuardApplied:
    def test_guard_is_active_by_default(self):
        """包导入时（src/__init__.py）就应装好守卫"""
        st = guard_status()
        assert st["applied"] is True, "V8 守卫未生效——服务可能被 FATAL 杀死"
        assert "MiniRacer" in st["reason"]
        assert st["mode"] in ("lazy", "hard")

    def test_apply_guard_is_idempotent(self):
        assert apply_guard() is True
        assert apply_guard() is True

    def test_error_is_catchable_as_runtime_error(self):
        """降级链靠 except Exception 兜底，故异常须继承 RuntimeError"""
        assert issubclass(V8Disabled, RuntimeError)


@pytest.mark.unit
class TestCrashProtection:
    """事故一回归：探测判定不安全时，绝不能让 V8 真的初始化"""

    def test_raises_catchable_error_when_probe_says_unsafe(self, clean_probe, monkeypatch):
        py_mini_racer = pytest.importorskip("py_mini_racer")
        monkeypatch.setattr(v8_guard, "probe_v8", lambda *a, **k: False)
        with pytest.raises(V8Disabled):
            py_mini_racer.MiniRacer()

    def test_v8_usable_follows_probe(self, clean_probe, monkeypatch):
        monkeypatch.setattr(v8_guard, "probe_v8", lambda *a, **k: False)
        assert v8_usable() is False
        monkeypatch.setattr(v8_guard, "probe_v8", lambda *a, **k: True)
        assert v8_usable() is True

    def test_hard_stub_never_touches_v8(self):
        """已知会崩的环境用 STOCK_ANALYZE_DISABLE_V8=1 硬禁用：不探测、直接抛"""
        stub = v8_guard._make_hard_stub()
        with pytest.raises(V8Disabled):
            stub()

    def test_probe_treats_crashed_child_as_unsafe(self, clean_probe, monkeypatch):
        """子进程被 FATAL 杀死表现为非零退出码，必须判为不安全"""
        class _Crashed:
            returncode = 3221226505    # Windows 上 abort 的典型退出码
            stderr = b"[FATAL:partition_address_space.cc(243)] Check failed"

        monkeypatch.setattr(v8_guard.subprocess, "run", lambda *a, **k: _Crashed())
        monkeypatch.setattr(v8_guard, "_write_cached_probe", lambda *a, **k: None)
        monkeypatch.setattr(v8_guard, "_read_cached_probe", lambda: None)
        assert v8_guard.probe_v8(force=True) is False
        assert "FATAL" in v8_guard.probe_status()["reason"]

    def test_probe_timeout_is_unsafe(self, clean_probe, monkeypatch):
        """探测卡死也按不安全处理——宁可少数据源，不可猝死"""
        def _timeout(*a, **k):
            raise v8_guard.subprocess.TimeoutExpired(cmd="x", timeout=30)

        monkeypatch.setattr(v8_guard.subprocess, "run", _timeout)
        monkeypatch.setattr(v8_guard, "_write_cached_probe", lambda *a, **k: None)
        monkeypatch.setattr(v8_guard, "_read_cached_probe", lambda: None)
        assert v8_guard.probe_v8(force=True) is False


@pytest.mark.unit
class TestNoCollateralDamage:
    """事故二回归：V8 正常的环境不许被守卫拖累"""

    def test_passes_through_real_minirager_when_probe_says_safe(self, clean_probe, monkeypatch):
        py_mini_racer = pytest.importorskip("py_mini_racer")
        monkeypatch.setattr(v8_guard, "probe_v8", lambda *a, **k: True)
        ctx = py_mini_racer.MiniRacer()
        assert ctx.eval("1+1") == 2, "V8 安全时必须直通真实 MiniRacer"

    def test_wencai_availability_follows_probe(self, clean_probe, monkeypatch):
        """问财可用性应跟着探测结论走，而不是"装了守卫就一律不可用" """
        from src.data.providers.wencai_provider import WencaiProvider

        WencaiProvider._V8_PROBE_RESULT = None
        monkeypatch.setattr(v8_guard, "v8_usable", lambda: False)
        try:
            assert WencaiProvider._probe_mini_racer() is False
        finally:
            WencaiProvider._V8_PROBE_RESULT = None


@pytest.mark.unit
class TestProbeCache:
    def test_cache_roundtrip(self, clean_probe, tmp_path, monkeypatch):
        """探测结果落盘，避免每个进程都付一次子进程启动成本"""
        monkeypatch.setattr(v8_guard, "_probe_cache_path",
                            lambda: tmp_path / "v8_probe.json")
        v8_guard._write_cached_probe(True, "子进程 V8 初始化成功")
        cached = v8_guard._read_cached_probe()
        assert cached is not None and cached[0] is True

    def test_cache_ignored_on_version_change(self, clean_probe, tmp_path, monkeypatch):
        """换 mini-racer 版本 → V8 行为可能变，旧结论作废"""
        monkeypatch.setattr(v8_guard, "_probe_cache_path",
                            lambda: tmp_path / "v8_probe.json")
        monkeypatch.setattr(v8_guard, "_mini_racer_version", lambda: "0.14.1")
        v8_guard._write_cached_probe(True, "ok")
        monkeypatch.setattr(v8_guard, "_mini_racer_version", lambda: "0.99.0")
        assert v8_guard._read_cached_probe() is None


@pytest.mark.unit
class TestDownstreamDegradation:
    def test_spot_chain_works_without_wencai(self, tmp_path, monkeypatch):
        """问财不可用时，全A行情降级链仍能从其他源取到合格数据"""
        import pandas as pd

        import src.analysis.screening.data_provider as dp

        healthy = pd.DataFrame({
            "代码": ["600519", "000001", "600036"]
                    + [f"{600100 + i:06d}" for i in range(4200)],
            "名称": ["贵州茅台", "平安银行", "招商银行"]
                    + [f"股{i}" for i in range(4200)],
            "总市值": [1.6e12, 3e11, 8e11] + [5e10] * 4200,
        })
        monkeypatch.setattr(dp, "_ensure_no_proxy_disable", lambda: None)
        monkeypatch.setattr(
            "src.data.providers.eastmoney_spot.fetch_all_a_spot", lambda: healthy)

        p = dp.ScreenerDataProvider(cache_dir=str(tmp_path / "c"))
        df = p.get_all_a_shares()
        assert len(df) >= 4000
        assert not df.attrs.get("degraded", False)


@pytest.mark.unit
class TestHealthEndpointExposesGuard:
    def test_health_reports_guard_state(self):
        """线上要能一眼确认守卫是否生效、探测结论是什么"""
        from fastapi.testclient import TestClient

        import src.api.main as api_main
        body = TestClient(api_main.app).get("/api/health").json()
        assert body["status"] == "ok"
        assert body["v8_guard"]["applied"] is True
        assert "probe" in body["v8_guard"]
