"""
tests/test_minute_data.py — 分钟线数据支持（Phase 1 路线图）

覆盖：
  - 分钟 DataFrame 归一化（时间→日期 / 列过滤 / 排序）
  - get_minute_ohlcv：akshare 主源、pytdx 兜底、缓存命中、非法频率
  - backtest_service.fetch_ohlcv 分钟频率分发
  - pytdx 分钟周期映射
  - /api/stocks/{market}/{code}/kline 的 freq 参数
"""
from __future__ import annotations

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import src.analysis.screening.data_provider as dp_mod
from src.analysis.screening.data_provider import ScreenerDataProvider


def _minute_df_em(n: int = 10) -> pd.DataFrame:
    """模拟 ak.stock_zh_a_hist_min_em 返回（乱序 + 含额外列）"""
    ts = pd.date_range("2026-07-16 09:30", periods=n, freq="5min")
    df = pd.DataFrame({
        "时间": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "开盘": [10.0 + i * 0.01 for i in range(n)],
        "收盘": [10.02 + i * 0.01 for i in range(n)],
        "最高": [10.05 + i * 0.01 for i in range(n)],
        "最低": [9.98 + i * 0.01 for i in range(n)],
        "成交量": [1000 + i for i in range(n)],
        "成交额": [10000 + i for i in range(n)],
        "涨跌幅": [0.1] * n,
    })
    return df.iloc[::-1].reset_index(drop=True)  # 倒序，验证归一化会重新排序


@pytest.fixture
def provider(tmp_path):
    return ScreenerDataProvider(cache_dir=str(tmp_path / "cache"))


@pytest.fixture(autouse=True)
def _reset_circuit_breaker():
    """每个用例前复位 akshare 熔断器全局状态，避免用例间串扰"""
    dp_mod._akshare_cb_state["fail_count"] = 0
    dp_mod._akshare_cb_state["cool_until"] = 0.0
    yield
    dp_mod._akshare_cb_state["fail_count"] = 0
    dp_mod._akshare_cb_state["cool_until"] = 0.0


@pytest.mark.unit
class TestNormalizeMinuteDf:
    def test_rename_sort_and_filter(self):
        out = ScreenerDataProvider._normalize_minute_df(_minute_df_em())
        assert "日期" in out.columns and "时间" not in out.columns
        assert "涨跌幅" not in out.columns  # 非标准列被剔除
        assert list(out.columns) == ["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额"]
        assert out["日期"].is_monotonic_increasing
        assert pd.api.types.is_datetime64_any_dtype(out["日期"])

    def test_empty_and_missing_date(self):
        assert ScreenerDataProvider._normalize_minute_df(pd.DataFrame()).empty
        assert ScreenerDataProvider._normalize_minute_df(
            pd.DataFrame({"开盘": [1.0]})).empty


@pytest.mark.unit
class TestGetMinuteOhlcv:
    def test_akshare_success_and_cache(self, provider, monkeypatch):
        calls = {"n": 0}

        def fake_min_em(**kwargs):
            calls["n"] += 1
            assert kwargs["period"] == "5"
            assert kwargs["adjust"] == "qfq"
            return _minute_df_em()

        monkeypatch.setattr(dp_mod.ak, "stock_zh_a_hist_min_em", fake_min_em)
        df = provider.get_minute_ohlcv("600519", freq="5m", days_back=5)
        assert not df.empty
        assert list(df.columns)[0] == "日期"
        # 第二次调用命中缓存，不再触发 akshare
        df2 = provider.get_minute_ohlcv("600519", freq="5m", days_back=5)
        assert calls["n"] == 1
        assert len(df2) == len(df)

    def test_1m_uses_no_adjust(self, provider, monkeypatch):
        seen = {}

        def fake_min_em(**kwargs):
            seen.update(kwargs)
            return _minute_df_em()

        monkeypatch.setattr(dp_mod.ak, "stock_zh_a_hist_min_em", fake_min_em)
        provider.get_minute_ohlcv("600519", freq="1m", days_back=3)
        assert seen["adjust"] == ""  # 东财 1 分钟接口不支持复权
        assert seen["period"] == "1"

    def test_fallback_to_pytdx(self, provider, monkeypatch):
        def fail_min_em(**kwargs):
            raise RuntimeError("EM blocked")

        class FakePytdx:
            def is_available(self):
                return True

            def get_k_data(self, code, days_back, frequency):
                assert frequency == "15m"
                assert days_back <= 800  # bar 数上限
                df = _minute_df_em()
                return df.rename(columns={"时间": "日期"})

        monkeypatch.setattr(dp_mod.ak, "stock_zh_a_hist_min_em", fail_min_em)
        # K 线获取已拆分到 kline_mixin，patch 真实调用位置
        import src.analysis.screening.kline_mixin as kline_mod
        monkeypatch.setattr(kline_mod, "get_global_pytdx", lambda: FakePytdx())
        df = provider.get_minute_ohlcv("000001", freq="15m", days_back=5)
        assert not df.empty

    def test_invalid_freq_raises(self, provider):
        with pytest.raises(ValueError, match="未知分钟频率"):
            provider.get_minute_ohlcv("600519", freq="3m")

    def test_code_suffix_normalized(self, provider, monkeypatch):
        seen = {}

        def fake_min_em(**kwargs):
            seen.update(kwargs)
            return _minute_df_em()

        monkeypatch.setattr(dp_mod.ak, "stock_zh_a_hist_min_em", fake_min_em)
        provider.get_minute_ohlcv("600519.SS", freq="5m", days_back=5)
        assert seen["symbol"] == "600519"


@pytest.mark.unit
class TestFetchOhlcvDispatch:
    def test_minute_freq_routes_to_minute_fetcher(self, monkeypatch):
        from src.services import backtest_service as btsvc

        seen = {}

        def fake_minute(self, code, freq="5m", days_back=5, market="a"):
            seen.update(code=code, freq=freq, days_back=days_back, market=market)
            return _minute_df_em().rename(columns={"时间": "日期"})

        monkeypatch.setattr(ScreenerDataProvider, "get_minute_ohlcv", fake_minute)
        df = btsvc.fetch_ohlcv("600519", "a", 5, frequency="30m")
        assert not df.empty
        assert seen == {"code": "600519", "freq": "30m", "days_back": 5, "market": "a"}

    def test_daily_path_unchanged(self, monkeypatch):
        from src.services import backtest_service as btsvc

        monkeypatch.setattr(
            ScreenerDataProvider, "get_daily_ohlcv",
            lambda self, code, days_back, market="a": _minute_df_em().rename(
                columns={"时间": "日期"}),
        )
        df = btsvc.fetch_ohlcv("600519", "a", 120, frequency="d")
        assert not df.empty


@pytest.mark.unit
class TestPytdxMinuteCategory:
    def test_minute_categories_registered(self):
        from src.data.providers.pytdx_provider import _PERIOD_CATEGORY

        assert _PERIOD_CATEGORY["1m"] == 8
        assert _PERIOD_CATEGORY["5m"] == 0
        assert _PERIOD_CATEGORY["15m"] == 1
        assert _PERIOD_CATEGORY["30m"] == 2
        assert _PERIOD_CATEGORY["60m"] == 3


@pytest.mark.unit
class TestKlineApiFreq:
    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient

        import src.api.main as api_main
        return TestClient(api_main.app)

    def test_unknown_freq_422(self, client):
        r = client.get("/api/stocks/a/600519/kline?freq=3m")
        assert r.status_code == 422

    def test_minute_freq_returns_time_axis(self, client, monkeypatch):
        from src.services import backtest_service as btsvc

        def fake_fetch(code, market, days, frequency="d"):
            assert frequency == "5m"
            assert days <= 30  # 分钟回溯窗口被夹紧
            return _minute_df_em().rename(columns={"时间": "日期"})

        monkeypatch.setattr(btsvc, "fetch_ohlcv", fake_fetch)
        r = client.get("/api/stocks/a/600519/kline?days=250&freq=5m")
        assert r.status_code == 200
        data = r.json()
        # 分钟轴保留 时:分（格式 MM-DD HH:MM）
        assert ":" in data["dates"][0]
        assert len(data["k"]) == len(data["dates"])

    def test_daily_freq_date_only(self, client, monkeypatch):
        from src.services import backtest_service as btsvc

        def fake_fetch(code, market, days, frequency="d"):
            df = _minute_df_em().rename(columns={"时间": "日期"})
            df["日期"] = pd.date_range("2026-01-01", periods=len(df), freq="D")
            return df

        monkeypatch.setattr(btsvc, "fetch_ohlcv", fake_fetch)
        r = client.get("/api/stocks/a/600519/kline?days=100")
        assert r.status_code == 200
        assert ":" not in r.json()["dates"][0]
