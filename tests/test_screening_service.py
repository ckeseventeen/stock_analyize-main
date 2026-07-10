"""
tests/test_screening_service.py — 筛选服务层单元测试

覆盖：
  - 条件 schema 查询（分类/标签/参数规格/默认值）
  - 条件校验 validate_conditions
  - 策略 CRUD（隔离到 tmp yaml）
"""
from __future__ import annotations

import os
import sys

import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.services import screening_service as svc


# ========================
# 条件 schema
# ========================

@pytest.mark.unit
class TestConditionSchema:
    def test_condition_types_nonempty(self):
        types = svc.condition_types()
        assert "market_cap" in types
        assert "weekly_macd_divergence" in types

    def test_categories_cover_registered_types(self):
        cats = svc.condition_categories()
        assert cats, "条件分类不应为空"
        all_in_cats = {t for types in cats.values() for t in types}
        # 分类中的类型都必须是已注册类型
        assert all_in_cats <= set(svc.condition_types())

    def test_label_fallback(self):
        assert svc.condition_label("nonexistent_type_xyz") == "nonexistent_type_xyz"

    def test_param_specs_market_cap(self):
        specs = {s.yaml_key: s for s in svc.condition_param_specs("market_cap")}
        assert "min" in specs and "max" in specs

    def test_param_specs_bool_kind(self):
        specs = {s.yaml_key: s for s in svc.condition_param_specs("weekly_macd_divergence")}
        assert specs["zero_axis_filter"].kind == "bool"
        # order 是 int（极值窗口），不能被误判为枚举
        assert specs["order"].kind == "int"

    def test_param_specs_choice_per_condition(self):
        """direction 参数在不同条件下取值集不同"""
        boll = {s.yaml_key: s for s in svc.condition_param_specs("bollinger_breakout")}
        assert boll["direction"].kind == "choice"
        assert boll["direction"].choices == ["upper", "lower"]

        bias = {s.yaml_key: s for s in svc.condition_param_specs("bias")}
        assert bias["direction"].choices == ["above", "below", "both"]

    def test_new_condition_dict_has_defaults(self):
        cond = svc.new_condition_dict("rsi_oversold")
        assert cond["type"] == "rsi_oversold"
        assert "threshold" in cond  # __init__ 默认值应被填入

    def test_sellable_excludes_spot_only(self):
        sellable = set(svc.sellable_condition_types())
        assert "market_cap" not in sellable
        assert "exclude_st" not in sellable


# ========================
# 条件校验
# ========================

@pytest.mark.unit
class TestValidateConditions:
    def test_valid_conditions_pass(self):
        conds = [
            {"type": "exclude_st"},
            {"type": "market_cap", "min": 50, "max": 500},
        ]
        assert svc.validate_conditions(conds) == []

    def test_unknown_type_reported(self):
        errors = svc.validate_conditions([{"type": "no_such_condition"}])
        assert len(errors) == 1
        assert "no_such_condition" in errors[0]

    def test_unknown_param_reported(self):
        errors = svc.validate_conditions([{"type": "market_cap", "bogus_param": 1}])
        assert len(errors) == 1
        assert "bogus_param" in errors[0]

    def test_not_a_list(self):
        assert svc.validate_conditions("not a list") == ["conditions 必须是列表"]


# ========================
# 策略 CRUD（tmp yaml 隔离）
# ========================

@pytest.fixture
def tmp_config(tmp_path):
    p = tmp_path / "screen_config.yaml"
    p.write_text(
        yaml.safe_dump({
            "strategies": {
                "s1": {
                    "name": "测试策略",
                    "conditions": [{"type": "exclude_st"}],
                    "output": {"sort_by": "总市值(亿)", "limit": 10},
                },
            },
        }, allow_unicode=True),
        encoding="utf-8",
    )
    return p


@pytest.mark.unit
class TestStrategyCrud:
    def test_load_all(self, tmp_config):
        strategies = svc.load_all_strategies(tmp_config)
        assert "s1" in strategies
        assert strategies["s1"]["name"] == "测试策略"

    def test_get_strategy(self, tmp_config):
        body = svc.get_strategy("s1", tmp_config)
        assert body["name"] == "测试策略"
        assert svc.get_strategy("nonexistent", tmp_config) == {}

    def test_upsert_and_reload(self, tmp_config):
        body = svc.default_strategy_body("新策略")
        ok, msg = svc.upsert_strategy("s2", body, tmp_config)
        assert ok, msg
        reloaded = svc.load_all_strategies(tmp_config)
        assert "s2" in reloaded
        assert reloaded["s2"]["name"] == "新策略"
        # 原有策略不受影响
        assert "s1" in reloaded

    def test_upsert_rejects_invalid_conditions(self, tmp_config):
        body = {"name": "坏策略", "conditions": [{"type": "bad_type"}]}
        ok, msg = svc.upsert_strategy("bad", body, tmp_config)
        assert not ok
        assert "bad_type" in msg
        assert "bad" not in svc.load_all_strategies(tmp_config)

    def test_upsert_empty_sid_rejected(self, tmp_config):
        ok, _ = svc.upsert_strategy("  ", svc.default_strategy_body(), tmp_config)
        assert not ok

    def test_delete_strategy(self, tmp_config):
        ok, _ = svc.delete_strategy("s1", tmp_config)
        assert ok
        assert svc.load_all_strategies(tmp_config) == {}
        ok2, msg = svc.delete_strategy("s1", tmp_config)
        assert not ok2

    def test_save_preserves_other_top_level_keys(self, tmp_config):
        # 顶层加一个额外键
        cfg = yaml.safe_load(tmp_config.read_text(encoding="utf-8"))
        cfg["some_global_option"] = {"foo": 1}
        tmp_config.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")

        svc.upsert_strategy("s3", svc.default_strategy_body("x"), tmp_config)
        after = yaml.safe_load(tmp_config.read_text(encoding="utf-8"))
        assert after["some_global_option"] == {"foo": 1}

    def test_duplicate_in_memory(self):
        strategies = {"a": {"name": "原始", "conditions": []}}
        new_sid = svc.duplicate_strategy_in_memory(strategies, "a")
        assert new_sid == "a_copy"
        assert strategies[new_sid]["name"] == "原始 (副本)"
        # 再复制一次不冲突
        new_sid2 = svc.duplicate_strategy_in_memory(strategies, "a")
        assert new_sid2 == "a_copy1"

    def test_list_strategy_names(self, tmp_config):
        names = svc.list_strategy_names(tmp_config)
        assert names == {"s1": "测试策略"}


@pytest.mark.unit
class TestRenameStrategy:
    def test_rename_roundtrip(self, tmp_config):
        ok, msg = svc.rename_strategy("s1", "s1_new", tmp_config)
        assert ok, msg
        names = svc.list_strategy_names(tmp_config)
        assert "s1_new" in names and "s1" not in names
        assert names["s1_new"] == "测试策略"

    def test_rename_missing_source(self, tmp_config):
        ok, msg = svc.rename_strategy("nope", "x", tmp_config)
        assert not ok

    def test_rename_target_exists(self, tmp_config):
        svc.upsert_strategy("s2", svc.default_strategy_body("b"), tmp_config)
        ok, msg = svc.rename_strategy("s1", "s2", tmp_config)
        assert not ok
        assert "已存在" in msg

    def test_rename_empty_target(self, tmp_config):
        ok, _ = svc.rename_strategy("s1", "  ", tmp_config)
        assert not ok
