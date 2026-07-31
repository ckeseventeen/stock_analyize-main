"""
tests/test_data_health.py — 数据健康度契约 + 统一降级链（A1/A4）

背景：项目曾有 162 处静默吞异常，"拿不到数据"和"确实没数据"不可区分，
导致筛选归零查了四轮才定位。这两个抽象把故障信号变成类型层面的契约。
"""
from __future__ import annotations

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core.data_health import DataResult, HealthStatus
from src.core.fallback import DataSource, FallbackChain


@pytest.mark.unit
class TestDataResult:
    def test_ok(self):
        r = DataResult.ok([1, 2, 3], source="东财")
        assert r.is_ok and r.usable
        assert not r.is_degraded and not r.is_failed
        assert r.user_message == ""
        assert r.unwrap() == [1, 2, 3]

    def test_degraded_carries_reason(self):
        r = DataResult.degraded([1], "仅 2649 只", source="问财")
        assert r.is_degraded and r.usable and not r.is_failed
        assert "数据不完整" in r.user_message and "2649" in r.user_message
        assert "问财" in r.user_message

    def test_empty_vs_failed_are_distinct(self):
        """核心契约：'确实没数据' 与 '获取失败' 必须可区分"""
        empty = DataResult.empty("当日非交易日")
        failed = DataResult.failed("接口被限流")
        assert empty.is_empty and not empty.is_failed
        assert failed.is_failed and not failed.is_empty
        assert not empty.usable and not failed.usable
        assert "无数据" in empty.user_message
        assert "失败" in failed.user_message

    def test_unwrap_default(self):
        assert DataResult.failed("x").unwrap("fallback") == "fallback"
        assert DataResult.empty().unwrap([]) == []

    def test_raise_if_failed(self):
        DataResult.ok(1).raise_if_failed()          # 不抛
        DataResult.degraded(1, "r").raise_if_failed()
        with pytest.raises(RuntimeError, match="限流"):
            DataResult.failed("接口被限流").raise_if_failed()

    def test_raise_chains_original_error(self):
        err = ConnectionError("boom")
        with pytest.raises(RuntimeError) as ei:
            DataResult.failed("网络失败", error=err).raise_if_failed()
        assert ei.value.__cause__ is err

    def test_to_dict_serializable(self):
        d = DataResult.degraded([1], "半份数据", source="问财", size=2649).to_dict()
        assert d["status"] == "degraded"
        assert d["meta"]["size"] == 2649
        assert HealthStatus.DEGRADED.value == "degraded"


def _df(n: int) -> pd.DataFrame:
    return pd.DataFrame({"code": [f"{i:06d}" for i in range(n)]})


@pytest.mark.unit
class TestFallbackChain:
    def test_first_healthy_source_wins(self):
        calls = []

        def mk(label, n):
            def f():
                calls.append(label)
                return _df(n)
            return f

        chain = FallbackChain(
            name="T", validator=lambda d: (len(d) >= 100, f"仅 {len(d)}"),
            sources=[DataSource("A", mk("A", 500)), DataSource("B", mk("B", 999))])
        r = chain.run()
        assert r.is_ok and r.source == "A" and len(r.data) == 500
        assert calls == ["A"], "合格即停，不应继续调用后续源"

    def test_skips_unhealthy_then_uses_healthy(self):
        chain = FallbackChain(
            name="T", validator=lambda d: (len(d) >= 100, f"仅 {len(d)} 条"),
            sources=[
                DataSource("残缺源", lambda: _df(10)),
                DataSource("完整源", lambda: _df(500)),
            ])
        r = chain.run()
        assert r.is_ok and r.source == "完整源"

    def test_all_degraded_returns_biggest_with_reason(self):
        chain = FallbackChain(
            name="T", validator=lambda d: (len(d) >= 1000, f"仅 {len(d)} 条"),
            sources=[
                DataSource("小", lambda: _df(10)),
                DataSource("中", lambda: _df(300)),
                DataSource("大", lambda: _df(800)),
            ])
        r = chain.run()
        assert r.is_degraded and r.source == "大" and len(r.data) == 800
        assert "800" in r.reason

    def test_all_failed_returns_failed_not_empty(self):
        """全失败必须是 FAILED，不能伪装成空数据"""
        def boom():
            raise ConnectionError("blocked")

        chain = FallbackChain(name="T", sources=[
            DataSource("A", boom), DataSource("B", lambda: pd.DataFrame())])
        r = chain.run()
        assert r.is_failed and not r.usable

    def test_exception_does_not_abort_chain(self):
        def boom():
            raise TimeoutError("t")

        chain = FallbackChain(name="T", sources=[
            DataSource("崩的", boom), DataSource("好的", lambda: _df(50))])
        assert chain.run().is_ok

    def test_skip_if_bypasses_source(self):
        called = []
        chain = FallbackChain(name="T", sources=[
            DataSource("熔断中", lambda: called.append("x") or _df(9),
                       skip_if=lambda: True),
            DataSource("正常", lambda: _df(50)),
        ])
        r = chain.run()
        assert r.is_ok and r.source == "正常" and called == []

    def test_callbacks_fire(self):
        events = []
        chain = FallbackChain(
            name="T", validator=lambda d: (len(d) >= 100, "太少"),
            sources=[
                DataSource("坏", lambda: pd.DataFrame(),
                           on_failure=lambda: events.append("fail")),
                DataSource("好", lambda: _df(200),
                           on_success=lambda: events.append("ok")),
            ])
        chain.run()
        assert events == ["fail", "ok"]

    def test_validator_exception_treated_as_unhealthy(self):
        def bad_validator(d):
            raise ValueError("validator bug")

        chain = FallbackChain(name="T", validator=bad_validator,
                              sources=[DataSource("A", lambda: _df(50))])
        r = chain.run()
        assert r.is_degraded and "validator bug" in r.reason

    def test_no_validator_means_nonempty_is_ok(self):
        chain = FallbackChain(name="T", sources=[DataSource("A", lambda: _df(1))])
        assert chain.run().is_ok

    def test_meta_records_size_and_source(self):
        chain = FallbackChain(name="T", sources=[DataSource("A", lambda: _df(42))])
        r = chain.run()
        assert r.meta["size"] == 42 and r.source == "A"


@pytest.mark.unit
class TestSpotChainIntegration:
    """data_provider 已改用 FallbackChain，行为须与手写版一致"""

    def test_degraded_attrs_still_set(self, tmp_path, monkeypatch):
        import src.analysis.screening.data_provider as dp

        small = pd.DataFrame({
            "代码": [f"{600000 + i:06d}" for i in range(100)],
            "名称": [f"股{i}" for i in range(100)],
            "总市值": [5e10] * 100,
        })
        monkeypatch.setattr(dp, "_ensure_no_proxy_disable", lambda: None)
        monkeypatch.setattr(
            "src.data.providers.eastmoney_spot.fetch_all_a_spot", lambda: small)
        monkeypatch.setattr(dp.ak, "stock_zh_a_spot_em", lambda: pd.DataFrame())
        monkeypatch.setattr(dp.ak, "stock_zh_a_spot", lambda: pd.DataFrame())
        monkeypatch.setattr(dp.WencaiProvider, "is_available", lambda self: False)
        monkeypatch.setattr(dp.BaostockProvider, "get_all_stocks",
                            lambda self: pd.DataFrame())

        p = dp.ScreenerDataProvider(cache_dir=str(tmp_path / "c"))
        df = p.get_all_a_shares()
        assert not df.empty
        assert df.attrs.get("degraded") is True
        assert "覆盖不全" in df.attrs.get("degrade_reason", "")
        assert df.attrs.get("degrade_source")
