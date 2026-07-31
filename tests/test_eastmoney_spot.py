"""
tests/test_eastmoney_spot.py — 东财全A行情直连 Provider（全离线 mock）

背景：akshare 的 stock_zh_a_spot_em 索要 100+ 字段且无常规 UA，易被东财反爬
掐连接 → 降级到问财 → 问财分页残缺 → 筛选恒 0。本 provider 只取 9 个必要
字段并发翻页，实测 5888 只 / 1.5 秒（未被限流时）。
"""
from __future__ import annotations

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import src.data.providers.eastmoney_spot as em


def _row(code: str, name: str = "测试股", mktcap: float = 5e10) -> dict:
    return {"f12": code, "f14": name, "f2": 10.5, "f3": 1.2, "f8": 0.8,
            "f9": 15.4, "f20": mktcap, "f21": mktcap * 0.8, "f23": 3.2}


@pytest.mark.unit
class TestFetchAllASpot:
    def test_paginates_and_maps_fields(self, monkeypatch):
        total = 250   # 3 页（100/100/50）

        def fake_page(sess, host, pn, timeout=12.0):
            start = (pn - 1) * 100
            rows = [_row(f"{600000 + i:06d}") for i in range(start, min(start + 100, total))]
            return rows, total

        monkeypatch.setattr(em, "_fetch_page", fake_page)
        df = em.fetch_all_a_spot()
        assert len(df) == total
        # f-code 已映射为项目统一中文列名
        assert list(df.columns) == ["代码", "名称", "最新价", "涨跌幅", "换手率",
                                    "市盈率-动态", "总市值", "流通市值", "市净率"]
        assert df["代码"].iloc[0] == "600000"
        assert pd.api.types.is_numeric_dtype(df["总市值"])

    def test_filters_st_and_dedups(self, monkeypatch):
        rows = [_row("600519", "贵州茅台"), _row("000001", "ST黑股"),
                _row("600519", "贵州茅台"), _row("000002", "某某退市")]

        monkeypatch.setattr(em, "_fetch_page",
                            lambda s, h, pn, timeout=12.0: (rows, len(rows)))
        df = em.fetch_all_a_spot()
        codes = set(df["代码"])
        assert codes == {"600519"}, "ST/退市应过滤，重复代码应去重"

    def test_all_hosts_down_returns_empty(self, monkeypatch):
        def boom(sess, host, pn, timeout=12.0):
            raise ConnectionError("Remote end closed connection")

        monkeypatch.setattr(em, "_fetch_page", boom)
        assert em.fetch_all_a_spot().empty

    def test_falls_through_to_next_host(self, monkeypatch):
        calls = []

        def flaky(sess, host, pn, timeout=12.0):
            calls.append(host)
            if host == em._HOSTS[0]:
                raise ConnectionError("blocked")
            return [_row("600519", "贵州茅台")], 1

        monkeypatch.setattr(em, "_fetch_page", flaky)
        df = em.fetch_all_a_spot()
        assert len(df) == 1
        assert calls[0] == em._HOSTS[0] and calls[1] == em._HOSTS[1]

    def test_partial_page_failure_still_returns_data(self, monkeypatch):
        total = 300

        def flaky_page(sess, host, pn, timeout=12.0):
            if pn == 2:
                raise TimeoutError("page 2 timeout")
            start = (pn - 1) * 100
            return [_row(f"{600000 + i:06d}") for i in
                    range(start, min(start + 100, total))], total

        monkeypatch.setattr(em, "_fetch_page", flaky_page)
        df = em.fetch_all_a_spot()
        assert 0 < len(df) < total, "部分页失败时应返回已取到的数据"


@pytest.mark.unit
class TestIntegrationWithProvider:
    def test_used_as_primary_source(self, tmp_path, monkeypatch):
        """东财直连合格时应被直接采用，不再降级到问财/Baostock"""
        import src.analysis.screening.data_provider as dp

        healthy = pd.DataFrame({
            "代码": ["600519", "000001", "600036"] + [f"{600100 + i:06d}" for i in range(4200)],
            "名称": ["贵州茅台", "平安银行", "招商银行"] + [f"股{i}" for i in range(4200)],
            "总市值": [1.6e12, 3e11, 8e11] + [5e10] * 4200,
        })
        monkeypatch.setattr(dp, "_ensure_no_proxy_disable", lambda: None)
        monkeypatch.setattr(
            "src.data.providers.eastmoney_spot.fetch_all_a_spot", lambda: healthy)

        def should_not_run(*a, **k):
            raise AssertionError("东财合格时不应降级到其他源")

        monkeypatch.setattr(dp.ak, "stock_zh_a_spot_em", should_not_run)
        p = dp.ScreenerDataProvider(cache_dir=str(tmp_path / "c"))
        df = p.get_all_a_shares()
        assert len(df) == len(healthy)
        assert not df.attrs.get("degraded", False)
