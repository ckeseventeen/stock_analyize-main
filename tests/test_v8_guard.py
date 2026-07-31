"""
tests/test_v8_guard.py — V8 崩溃守卫（线上事故回归）

事故：uvicorn 跑起来后访问个股页，进程毫无征兆消失，用户侧表现为
"所有功能都用不了"。日志尾部是

    [FATAL:partition_address_space.cc(243)] Check failed:
    !IsConfigurablePoolInitialized()

即 py_mini_racer 的 V8 初始化 FATAL 中止——**不是异常，try/except 拦不住，
进程被当场杀死**。akshare 自身依赖 mini-racer（不只是 pywencai），因此
任何走到 JS 解密的数据源路径都是定时炸弹。
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core.v8_guard import V8Disabled, apply_guard, guard_status


@pytest.mark.unit
class TestGuardApplied:
    def test_guard_is_active_by_default(self):
        """包导入时（src/__init__.py）就应装好守卫"""
        st = guard_status()
        assert st["applied"] is True, "V8 守卫未生效——服务可能被 FATAL 杀死"
        assert "MiniRacer" in st["reason"]

    def test_minirager_raises_instead_of_killing_process(self):
        """核心契约：实例化必须抛**可捕获**异常，而不是 FATAL 杀进程"""
        py_mini_racer = pytest.importorskip("py_mini_racer")
        with pytest.raises(V8Disabled):
            py_mini_racer.MiniRacer()

    def test_error_is_catchable_as_runtime_error(self):
        """降级链靠 except Exception 兜底，故异常须继承 RuntimeError"""
        assert issubclass(V8Disabled, RuntimeError)

    def test_apply_guard_is_idempotent(self):
        assert apply_guard() is True
        assert apply_guard() is True


@pytest.mark.unit
class TestDownstreamDegradation:
    """守卫生效后，依赖 V8 的数据源应优雅降级而非报错崩溃"""

    def test_wencai_reports_unavailable(self):
        from src.data.providers.wencai_provider import WencaiProvider

        WencaiProvider._V8_PROBE_RESULT = None   # 清掉缓存的探测结果
        try:
            assert WencaiProvider().is_available() is False, \
                "V8 被禁用时问财必须自报不可用，否则会走到崩溃路径"
        finally:
            WencaiProvider._V8_PROBE_RESULT = None

    def test_spot_chain_skips_wencai_and_still_works(self, tmp_path, monkeypatch):
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
        """线上要能一眼确认守卫是否生效"""
        from fastapi.testclient import TestClient

        import src.api.main as api_main
        body = TestClient(api_main.app).get("/api/health").json()
        assert body["status"] == "ok"
        assert body["v8_guard"]["applied"] is True
