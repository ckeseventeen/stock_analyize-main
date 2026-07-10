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
