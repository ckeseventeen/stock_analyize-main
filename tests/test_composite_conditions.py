"""
tests/test_composite_conditions.py — 嵌套条件逻辑 + 策略新建（Task 21 收尾）

背景：策略的买入条件此前恒为 AND、卖出恒为 OR，写不出
"站上均线 且 (RSI超卖 或 触及布林下轨)" 这类表达；前端也只能克隆策略，
无法从零新建。
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.analysis.screening.conditions import (
    CompositeCondition,
    MarketCapCondition,
    PERangeCondition,
    PriceAboveMACondition,
)


def _spot(mktcap: float = 5e10, pe: float = 20.0) -> pd.Series:
    return pd.Series({"代码": "600519", "名称": "测试", "总市值": mktcap,
                      "市盈率-动态": pe})


def _uptrend(n: int = 200) -> pd.DataFrame:
    close = np.linspace(10, 30, n)
    return pd.DataFrame({
        "日期": pd.bdate_range("2025-01-02", periods=n),
        "开盘": close, "最高": close * 1.01, "最低": close * 0.99,
        "收盘": close, "成交量": np.full(n, 1e6),
    })


def _downtrend(n: int = 200) -> pd.DataFrame:
    close = np.linspace(30, 10, n)
    return pd.DataFrame({
        "日期": pd.bdate_range("2025-01-02", periods=n),
        "开盘": close, "最高": close * 1.01, "最低": close * 0.99,
        "收盘": close, "成交量": np.full(n, 1e6),
    })


@pytest.mark.unit
class TestCompositeLogic:
    def test_all_requires_every_child(self):
        c = CompositeCondition("all", [
            MarketCapCondition(min_cap=100, max_cap=1000),   # 亿元
            PERangeCondition(min_pe=0, max_pe=30),
        ])
        assert c.evaluate_spot(_spot(mktcap=5e10, pe=20)) is True     # 500亿/PE20
        assert c.evaluate_spot(_spot(mktcap=5e10, pe=50)) is False    # PE 超限

    def test_any_needs_only_one(self):
        c = CompositeCondition("any", [
            PERangeCondition(min_pe=0, max_pe=10),
            PERangeCondition(min_pe=50, max_pe=100),
        ])
        assert c.evaluate_spot(_spot(pe=5)) is True
        assert c.evaluate_spot(_spot(pe=80)) is True
        assert c.evaluate_spot(_spot(pe=30)) is False   # 两个区间都不在

    def test_none_excludes(self):
        c = CompositeCondition("none", [PERangeCondition(min_pe=0, max_pe=10)])
        assert c.evaluate_spot(_spot(pe=30)) is True    # 不在区间内 → none 通过
        assert c.evaluate_spot(_spot(pe=5)) is False

    def test_logic_aliases(self):
        for alias, expect in (("and", False), ("or", True)):
            c = CompositeCondition(alias, [
                PERangeCondition(min_pe=0, max_pe=10),
                PERangeCondition(min_pe=15, max_pe=25),
            ])
            assert c.evaluate_spot(_spot(pe=20)) is expect

    def test_invalid_logic_rejected(self):
        with pytest.raises(ValueError, match="未知组合逻辑"):
            CompositeCondition("xor", [PERangeCondition()])

    def test_empty_composite_is_permissive(self):
        assert CompositeCondition("all", []).evaluate_spot(_spot()) is True

    def test_nesting(self):
        """A 且 (B 或 C) —— 本次要解决的核心表达"""
        inner = CompositeCondition("any", [
            PERangeCondition(min_pe=0, max_pe=10),
            PERangeCondition(min_pe=50, max_pe=100),
        ])
        outer = CompositeCondition("all", [
            MarketCapCondition(min_cap=100, max_cap=10000), inner])
        assert outer.evaluate_spot(_spot(mktcap=5e10, pe=5)) is True
        assert outer.evaluate_spot(_spot(mktcap=5e10, pe=30)) is False   # 内层否
        assert outer.evaluate_spot(_spot(mktcap=1e9, pe=5)) is False     # 外层否


@pytest.mark.unit
class TestCompositeDataRequirements:
    """数据需求要从子条件聚合，否则筛选器两轮架构与深度推断会失效"""

    def test_requires_ohlcv_aggregates(self):
        assert CompositeCondition("all", [PERangeCondition()]).requires_ohlcv is False
        mixed = CompositeCondition("all", [
            PERangeCondition(), PriceAboveMACondition(ma_period=20)])
        assert mixed.requires_ohlcv is True

    def test_required_bars_takes_max(self):
        c = CompositeCondition("any", [
            PriceAboveMACondition(ma_period=20),
            PriceAboveMACondition(ma_period=120),
        ])
        assert c.required_bars() == 140     # 120 + 20 热身

    def test_ohlcv_period_prefers_weekly(self):
        from src.analysis.screening.conditions import (
            WeeklyMACDBottomDivergenceCondition,
        )

        c = CompositeCondition("any", [
            PriceAboveMACondition(ma_period=20),
            WeeklyMACDBottomDivergenceCondition(),
        ])
        assert c.ohlcv_period == "weekly"


@pytest.mark.unit
class TestCompositeTwoPassSemantics:
    """与筛选器 Pass1(快)/Pass2(精) 两轮架构的配合"""

    def test_or_with_kline_child_passes_first_pass(self):
        """OR 组合里有 K 线子条件时，第一轮无法定论 → 必须放行到第二轮，
        否则技术信号还没来得及算就被误杀"""
        c = CompositeCondition("any", [
            PERangeCondition(min_pe=0, max_pe=1),        # spot 阶段就不满足
            PriceAboveMACondition(ma_period=20),          # 需要 K 线
        ])
        assert c.evaluate_spot(_spot(pe=50)) is True, "OR+K线 应放行到第二轮"

    def test_full_evaluation_uses_kline(self):
        c = CompositeCondition("any", [
            PERangeCondition(min_pe=0, max_pe=1),
            PriceAboveMACondition(ma_period=20),
        ])
        assert c.evaluate_full(_spot(pe=50), _uptrend()) is True    # 均线满足
        assert c.evaluate_full(_spot(pe=50), _downtrend()) is False  # 都不满足

    def test_and_still_filters_early_on_spot(self):
        """AND 组合的纯 spot 子条件不满足时，第一轮就该淘汰（省掉拉 K 线）"""
        c = CompositeCondition("all", [
            PERangeCondition(min_pe=0, max_pe=1),
            PriceAboveMACondition(ma_period=20),
        ])
        assert c.evaluate_spot(_spot(pe=50)) is False

    def test_child_exception_does_not_propagate(self):
        class Boom(PERangeCondition):
            name = "boom"

            def evaluate_spot(self, spot_row):
                raise RuntimeError("child failed")

        c = CompositeCondition("any", [Boom(), PERangeCondition(min_pe=0, max_pe=30)])
        assert c.evaluate_spot(_spot(pe=20)) is True, "单个子条件异常不应拖垮组合"


@pytest.mark.unit
class TestCompositeConfigParsing:
    def test_parses_nested_yaml(self, tmp_path):
        from src.analysis.screening.config_schema import parse_screen_config

        cfg = {"strategies": {"s1": {"conditions": [
            {"type": "market_cap", "min": 100, "max": 10000},
            {"logic": "any", "conditions": [
                {"type": "rsi_oversold", "period": 14, "threshold": 30},
                {"type": "ma_gold_cross", "fast_period": 5, "slow_period": 20},
            ]},
        ]}}}
        p = tmp_path / "screen_config.yaml"
        p.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")

        conds, _ = parse_screen_config(str(p), strategy_ids=["s1"])
        assert len(conds) == 2
        comp = conds[1]
        assert isinstance(comp, CompositeCondition)
        assert comp.logic == "any" and len(comp.children) == 2
        assert comp.requires_ohlcv is True

    def test_shipped_nested_strategy_parses(self):
        """仓库里的 multi_signal_entry 用了嵌套逻辑，必须能解析"""
        from src.analysis.screening.config_schema import parse_screen_config

        conds, _ = parse_screen_config("config/screen_config.yaml",
                                       strategy_ids=["multi_signal_entry"])
        assert any(isinstance(c, CompositeCondition) for c in conds)


@pytest.mark.unit
class TestStrategyCreate:
    """前端此前只能克隆策略，缺 CRUD 的 C"""

    def test_create_from_scratch(self, tmp_path):
        from src.services import screening_service as svc

        p = tmp_path / "screen_config.yaml"
        p.write_text(yaml.safe_dump({"strategies": {}}, allow_unicode=True),
                     encoding="utf-8")

        sid, err = svc.create_strategy(name="我的新策略", config_path=p)
        assert not err and sid
        body = yaml.safe_load(p.read_text(encoding="utf-8"))["strategies"][sid]
        assert body["name"] == "我的新策略"
        assert body["conditions"], "新策略要带可用骨架，否则一保存就过不了校验"
        assert body["backtest"]["sell_conditions"], "要能直接回测"

    def test_id_collision_gets_suffix(self, tmp_path):
        from src.services import screening_service as svc

        p = tmp_path / "screen_config.yaml"
        p.write_text(yaml.safe_dump({"strategies": {}}, allow_unicode=True),
                     encoding="utf-8")
        a, _ = svc.create_strategy(name="dup", config_path=p)
        b, _ = svc.create_strategy(name="dup", config_path=p)
        assert a != b, "重名不能互相覆盖"

    def test_created_strategy_is_parsable(self, tmp_path):
        from src.analysis.screening.config_schema import parse_screen_config
        from src.services import screening_service as svc

        p = tmp_path / "screen_config.yaml"
        p.write_text(yaml.safe_dump({"strategies": {}}, allow_unicode=True),
                     encoding="utf-8")
        sid, _ = svc.create_strategy(name="可解析", config_path=p)
        conds, _ = parse_screen_config(str(p), strategy_ids=[sid])
        assert conds, "新建的策略应当能立刻解析出条件"

    def test_api_endpoint(self, tmp_path, monkeypatch):
        from fastapi.testclient import TestClient

        import src.api.main as api_main
        import src.services.screening_service as svc

        p = tmp_path / "screen_config.yaml"
        p.write_text(yaml.safe_dump({"strategies": {}}, allow_unicode=True),
                     encoding="utf-8")
        monkeypatch.setattr(svc, "PATH_SCREEN", p, raising=False)
        orig = svc.create_strategy
        monkeypatch.setattr(svc, "create_strategy",
                            lambda name="", sid="", config_path=None:
                            orig(name=name, sid=sid, config_path=p))

        r = TestClient(api_main.app).post("/api/strategies", json={"name": "接口新建"})
        assert r.status_code == 200 and r.json()["ok"] is True
