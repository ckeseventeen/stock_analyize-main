"""tests/test_cache_policy.py — 缓存 TTL 单点真相（B3）"""
from __future__ import annotations

import importlib
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core.cache_policy import CacheTTL, all_policies, ttl_for


@pytest.mark.unit
class TestCachePolicy:
    def test_known_kinds(self):
        assert ttl_for("kline_minute") == CacheTTL.KLINE_MINUTE
        assert ttl_for("kline_daily") == CacheTTL.KLINE_DAILY
        assert ttl_for("spot") == CacheTTL.SPOT
        assert ttl_for("fundamental") == CacheTTL.FUNDAMENTAL

    def test_case_insensitive(self):
        assert ttl_for("KLINE_DAILY") == ttl_for("kline_daily")

    def test_unknown_kind_falls_back_not_raises(self):
        """拼错种类不能让缓存整个失效，回退默认值"""
        assert ttl_for("nonexistent_kind") == CacheTTL.DEFAULT

    def test_ordering_matches_data_freshness(self):
        """越易变的数据 TTL 越短"""
        assert ttl_for("kline_minute") < ttl_for("kline_daily")
        assert ttl_for("kline_daily") < ttl_for("kline_weekly")
        assert ttl_for("kline_weekly") <= ttl_for("kline_monthly")
        assert ttl_for("alt_intraday") < ttl_for("alt_daily") < ttl_for("fundamental")

    def test_all_policies_covers_every_kind(self):
        pol = all_policies()
        assert "spot" in pol and "ml_dataset" in pol
        assert all(v >= 1 for v in pol.values())

    def test_scale_env_var(self, monkeypatch):
        monkeypatch.setenv("STOCK_ANALYZE_TTL_SCALE", "0.5")
        import src.core.cache_policy as cp
        importlib.reload(cp)
        try:
            assert cp.ttl_for("fundamental") == max(1, round(CacheTTL.FUNDAMENTAL * 0.5))
        finally:
            monkeypatch.delenv("STOCK_ANALYZE_TTL_SCALE", raising=False)
            importlib.reload(cp)


@pytest.mark.unit
class TestProvidersUsePolicy:
    def test_providers_read_from_policy(self, tmp_path):
        """各 provider 的默认 TTL 必须来自统一策略，而非各自硬编码"""
        from src.analysis.screening.data_provider import ScreenerDataProvider
        from src.data.providers.alternative_data import ALT_DATA_SOURCES
        from src.data.providers.fundamental_data import FUNDAMENTAL_SOURCES

        p = ScreenerDataProvider(cache_dir=str(tmp_path / "c"))
        assert p._spot_ttl == ttl_for("spot")
        assert p._ttl_by_frequency["minute"] == ttl_for("kline_minute")
        assert p._ttl_by_frequency["weekly"] == ttl_for("kline_weekly")
        assert ALT_DATA_SOURCES["northbound"]["ttl_hours"] == ttl_for("alt_intraday")
        assert ALT_DATA_SOURCES["lhb"]["ttl_hours"] == ttl_for("alt_daily")
        assert FUNDAMENTAL_SOURCES["balance_sheet"]["ttl_hours"] == ttl_for("fundamental")
        assert FUNDAMENTAL_SOURCES["forecast"]["ttl_hours"] == ttl_for("earnings")

    def test_explicit_override_still_wins(self, tmp_path):
        from src.analysis.screening.data_provider import ScreenerDataProvider

        p = ScreenerDataProvider(cache_dir=str(tmp_path / "c2"), spot_ttl_hours=99)
        assert p._spot_ttl == 99
