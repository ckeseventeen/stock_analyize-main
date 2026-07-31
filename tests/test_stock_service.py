"""
tests/test_stock_service.py — 个股中心服务层单元测试（本地部分，无网络）
"""
from __future__ import annotations

import os
import sys

import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.services import stock_service as ssvc


@pytest.fixture
def tmp_alerts(tmp_path, monkeypatch):
    """把 PATH_PRICE_ALERTS 指到 tmp，隔离真实配置"""
    p = tmp_path / "price_alerts.yaml"
    p.write_text(yaml.safe_dump({
        "buy_alerts": [
            {"id": "x1", "name": "已有规则", "code": "600519",
             "signal": {"type": "rsi_oversold"}, "enabled": True},
        ],
        "sell_alerts": [],
    }, allow_unicode=True), encoding="utf-8")
    monkeypatch.setattr(ssvc, "PATH_PRICE_ALERTS", p)
    return p


@pytest.mark.unit
class TestAlertRules:
    def test_rules_for_code_filters(self, tmp_alerts):
        buy, sell = ssvc.alert_rules_for_code("600519")
        assert len(buy) == 1 and buy[0]["id"] == "x1"
        assert sell == []
        buy2, _ = ssvc.alert_rules_for_code("000001")
        assert buy2 == []

    def test_add_rule_roundtrip(self, tmp_alerts):
        ok, msg = ssvc.add_price_alert_rule(
            "000001", "平安银行 金叉", "buy", "ma_gold_cross")
        assert ok, msg
        buy, _ = ssvc.alert_rules_for_code("000001")
        assert len(buy) == 1
        rule = buy[0]
        assert rule["signal"]["type"] == "ma_gold_cross"
        assert rule["enabled"] is True
        assert rule["id"] == "hub_000001_ma_gold_cross"

    def test_add_sell_rule(self, tmp_alerts):
        ok, _ = ssvc.add_price_alert_rule("000001", "", "sell", "kdj_death_cross")
        assert ok
        _, sell = ssvc.alert_rules_for_code("000001")
        assert len(sell) == 1

    def test_duplicate_rejected(self, tmp_alerts):
        ssvc.add_price_alert_rule("000001", "", "buy", "ma_gold_cross")
        ok, msg = ssvc.add_price_alert_rule("000001", "", "buy", "ma_gold_cross")
        assert not ok
        assert "已存在" in msg

    def test_unknown_signal_rejected(self, tmp_alerts):
        ok, msg = ssvc.add_price_alert_rule("000001", "", "buy", "no_such_signal")
        assert not ok
        assert "未知信号" in msg

    def test_invalid_direction_rejected(self, tmp_alerts):
        ok, _ = ssvc.add_price_alert_rule("000001", "", "hold", "ma_gold_cross")
        assert not ok

    def test_existing_rules_preserved(self, tmp_alerts):
        """新增不应破坏其他股票的已有规则"""
        ssvc.add_price_alert_rule("000001", "", "buy", "ma_gold_cross")
        cfg = yaml.safe_load(tmp_alerts.read_text(encoding="utf-8"))
        ids = [r["id"] for r in cfg["buy_alerts"]]
        assert "x1" in ids and "hub_000001_ma_gold_cross" in ids


@pytest.mark.unit
class TestResolveName:
    def test_empty_code(self):
        assert ssvc.resolve_name("") == ""

    def test_buy_profile_types_registered(self):
        """买点清单里的条件类型必须全部已注册（防拼错）"""
        from src.analysis.screening.conditions import CONDITION_REGISTRY
        for cond_type, _ in ssvc._BUY_PROFILE:
            assert cond_type in CONDITION_REGISTRY, f"未注册: {cond_type}"


@pytest.mark.unit
class TestScanStrategySignals:
    def _synthetic(self, n=300):
        import numpy as np
        import pandas as pd
        idx = pd.date_range("2025-01-01", periods=n, freq="D")
        base = 100 + 10 * np.sin(np.linspace(0, 8 * np.pi, n))
        close = pd.Series(base, index=idx)
        return pd.DataFrame({
            "date": idx, "open": close.shift(1).fillna(close.iloc[0]),
            "high": close + 1, "low": close - 1, "close": close,
            "volume": 1_000_000,
        })

    def test_per_strategy_buy_sell_sides(self, tmp_path, monkeypatch):
        import yaml

        from src.analysis.screening.data_provider import ScreenerDataProvider

        cfg = tmp_path / "screen.yaml"
        cfg.write_text(yaml.safe_dump({"strategies": {
            "s1": {
                "name": "双侧策略",
                "conditions": [
                    {"type": "market_cap", "min": 100},          # spot-only → 应被跳过
                    {"type": "ma_gold_cross", "fast_period": 5, "slow_period": 20},
                    {"type": "rsi_oversold", "threshold": 30},
                ],
                "backtest": {
                    "buy_logic": "any",
                    "sell_logic": "any",
                    "sell_conditions": [
                        {"type": "rsi_overbought", "threshold": 70},
                        {"type": "ma_death_cross", "fast_period": 5, "slow_period": 20},
                    ],
                },
            },
        }}, allow_unicode=True), encoding="utf-8")

        df = self._synthetic()
        monkeypatch.setattr(ScreenerDataProvider, "get_daily_ohlcv",
                            lambda self, *a, **k: df)
        monkeypatch.setattr(ScreenerDataProvider, "get_weekly_ohlcv",
                            lambda self, *a, **k: df)

        res = ssvc.scan_strategy_signals("600519", "a", config_path=cfg)
        assert len(res) == 1
        s = res[0]
        assert s["sid"] == "s1" and s["name"] == "双侧策略"
        # 买卖两侧分离，spot-only 条件被剔除
        buy_types = [c["type"] for c in s["buy_conditions"]]
        assert "market_cap" not in buy_types
        assert set(buy_types) == {"ma_gold_cross", "rsi_oversold"}
        assert {c["type"] for c in s["sell_conditions"]} == {"rsi_overbought", "ma_death_cross"}
        # hit 是 bool，聚合逻辑与 any 一致
        assert s["buy_hit"] == any(c["hit"] for c in s["buy_conditions"])
        assert s["sell_hit"] == any(c["hit"] for c in s["sell_conditions"])
        assert isinstance(s["buy_hit"], bool)

    def test_empty_kline_returns_empty(self, tmp_path, monkeypatch):
        import pandas as pd
        import yaml

        from src.analysis.screening.data_provider import ScreenerDataProvider
        cfg = tmp_path / "screen.yaml"
        cfg.write_text(yaml.safe_dump({"strategies": {"s1": {
            "name": "x", "conditions": [{"type": "ma_gold_cross"}]}}},
            allow_unicode=True), encoding="utf-8")
        monkeypatch.setattr(ScreenerDataProvider, "get_daily_ohlcv",
                            lambda self, *a, **k: pd.DataFrame())
        monkeypatch.setattr(ScreenerDataProvider, "get_weekly_ohlcv",
                            lambda self, *a, **k: pd.DataFrame())
        assert ssvc.scan_strategy_signals("600519", "a", config_path=cfg) == []


@pytest.mark.unit
class TestSearchStocks:
    @pytest.fixture(autouse=True)
    def _fake_universe(self, monkeypatch):
        monkeypatch.setattr(ssvc, "_a_share_names", lambda: {
            "600519": "贵州茅台", "600518": "康美药业",
            "000001": "平安银行", "002156": "通富微电",
        })

    def test_code_prefix(self):
        res = ssvc.search_stocks("6005")
        codes = [r["code"] for r in res]
        assert set(codes) == {"600519", "600518"}

    def test_name_substring(self):
        res = ssvc.search_stocks("茅台")
        assert res == [{"code": "600519", "name": "贵州茅台"}]

    def test_code_hits_rank_first(self):
        res = ssvc.search_stocks("000001")
        assert res[0]["code"] == "000001"

    def test_empty_query(self):
        assert ssvc.search_stocks("  ") == []

    def test_limit(self):
        assert len(ssvc.search_stocks("0", limit=1)) == 1

    def test_spaced_name_matches_compact_query(self, monkeypatch):
        """老数据源名称带对齐空格（"五 粮 液"）时，搜"五粮液"必须命中
        且返回的名称已去空格（2026-07 用户报"按名称搜不到"回归）"""
        monkeypatch.setattr(ssvc, "_a_share_names", lambda: {
            "000858": "五 粮 液", "000002": "万 科Ａ", "600519": "贵州茅台",
        })
        res = ssvc.search_stocks("五粮液")
        assert res == [{"code": "000858", "name": "五粮液"}]
        assert ssvc.search_stocks("万科")[0]["code"] == "000002"

    def test_concept_word_returns_empty(self, monkeypatch):
        """概念词（如"白酒"）不在任何股票名称里 → 返回空，由前端明确报错"""
        monkeypatch.setattr(ssvc, "_a_share_names", lambda: {
            "600519": "贵州茅台", "000858": "五粮液",
        })
        assert ssvc.search_stocks("白酒") == []
