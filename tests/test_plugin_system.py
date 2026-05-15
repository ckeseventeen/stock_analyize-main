"""
tests/test_plugin_system.py — PR 2 插件系统测试

覆盖：
  - PluginRegistry: register / get / 重复检测 / replace / labels / schema_for
  - autodiscover: 子包文件自动 import 触发装饰器
  - schema_inspect: 从 dataclass / backtrader params / __init__ 签名推导 schema
  - STRATEGY_REGISTRY 向后兼容代理（dict-like 接口）
"""
from __future__ import annotations

import os
import sys
import tempfile
import textwrap
from dataclasses import dataclass, field
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core.plugin import PluginRegistry, autodiscover
from src.core.schema_inspect import derive_schema, instantiate_from_dict


# =============================================================================
# PluginRegistry
# =============================================================================

@pytest.mark.unit
class TestPluginRegistry:
    def setup_method(self):
        self.reg = PluginRegistry[object]("test")

    def test_register_and_get(self):
        @self.reg.register("foo", label="Foo Strategy")
        class FooCls:
            pass
        assert self.reg.get("foo") is FooCls
        assert self.reg.labels()["foo"] == "Foo Strategy"

    def test_duplicate_raises(self):
        @self.reg.register("dup")
        class A: pass
        with pytest.raises(ValueError, match="已被"):
            @self.reg.register("dup")
            class B: pass

    def test_replace_succeeds(self):
        @self.reg.register("rep")
        class A: pass

        @self.reg.register("rep", replace=True)
        class B: pass
        assert self.reg.get("rep") is B

    def test_get_unknown_raises_with_available(self):
        self.reg.register_class("known", object)
        with pytest.raises(KeyError, match="known"):
            self.reg.get("unknown")

    def test_register_class_imperative(self):
        self.reg.register_class("cmd", object, label="命令式")
        assert "cmd" in self.reg
        assert self.reg.labels()["cmd"] == "命令式"

    def test_dict_like_interface(self):
        self.reg.register_class("a", object)
        self.reg.register_class("b", object)
        assert len(self.reg) == 2
        assert "a" in self.reg
        assert set(self.reg.keys()) == {"a", "b"}

    def test_unregister(self):
        self.reg.register_class("tmp", object)
        self.reg.unregister("tmp")
        assert "tmp" not in self.reg

    def test_empty_key_rejected(self):
        with pytest.raises(ValueError, match="不能为空"):
            self.reg.register_class("", object)


# =============================================================================
# autodiscover
# =============================================================================

@pytest.mark.unit
class TestAutodiscover:
    def test_autodiscover_loads_package(self, tmp_path, monkeypatch):
        # 临时建一个 fake 包
        pkg_root = tmp_path / "fake_plugins"
        pkg_root.mkdir()
        (pkg_root / "__init__.py").write_text("")
        (pkg_root / "p1.py").write_text("LOADED_P1 = True\n")
        (pkg_root / "p2.py").write_text("LOADED_P2 = True\n")

        monkeypatch.syspath_prepend(str(tmp_path))
        n = autodiscover("fake_plugins")
        assert n == 2

        import fake_plugins.p1 as p1
        import fake_plugins.p2 as p2
        assert p1.LOADED_P1 is True
        assert p2.LOADED_P2 is True

    def test_autodiscover_unknown_package_silent(self):
        # silent_errors=True (默认) 时不抛
        n = autodiscover("nonexistent_pkg_xyz")
        assert n == 0


# =============================================================================
# schema_inspect: 多种来源
# =============================================================================

@pytest.mark.unit
class TestSchemaFromDataclass:
    def test_dataclass_with_metadata(self):
        @dataclass
        class MyDC:
            fast: int = field(default=10, metadata={"label": "短", "min": 1, "max": 60})
            slow: int = field(default=30, metadata={"label": "长", "min": 5, "max": 250})

        schema = derive_schema(MyDC)
        assert len(schema) == 2
        s1 = schema[0]
        assert s1["key"] == "fast"
        assert s1["label"] == "短"
        assert s1["default"] == 10
        assert s1["min"] == 1
        assert s1["max"] == 60
        assert s1["type"] == "int"

    def test_dataclass_skips_hidden(self):
        @dataclass
        class HasHidden:
            visible: int = field(default=1)
            _internal: int = field(default=0, metadata={"hidden": True})
        schema = derive_schema(HasHidden)
        keys = [s["key"] for s in schema]
        assert "visible" in keys
        assert "_internal" not in keys


@pytest.mark.unit
class TestSchemaFromBacktraderParams:
    def test_backtrader_tuple_params(self):
        class FakeStrat:
            params = (("fast_period", 10), ("slow_period", 30))

        schema = derive_schema(FakeStrat)
        keys = {s["key"] for s in schema}
        assert keys == {"fast_period", "slow_period"}
        assert all(s["type"] == "int" for s in schema)

    def test_backtrader_float_inferred(self):
        class FakeStrat:
            params = (("threshold", 0.95),)

        schema = derive_schema(FakeStrat)
        assert schema[0]["type"] == "float"


@pytest.mark.unit
class TestSchemaFromInitSignature:
    def test_init_signature_fallback(self):
        class NoBacktraderParams:
            def __init__(self, foo: int = 5, bar: str = "x"):
                ...

        schema = derive_schema(NoBacktraderParams)
        keys = {s["key"] for s in schema}
        assert keys == {"foo", "bar"}


@pytest.mark.unit
class TestSchemaExplicitOverride:
    def test_param_schema_class_attr_overrides(self):
        class Explicit:
            _param_schema = [{"key": "x", "type": "int", "default": 1, "label": "X"}]
            def __init__(self, x=2, y=3): ...

        schema = derive_schema(Explicit)
        assert schema == [{"key": "x", "type": "int", "default": 1, "label": "X"}]


# =============================================================================
# instantiate_from_dict
# =============================================================================

@pytest.mark.unit
class TestInstantiateFromDict:
    def test_basic(self):
        class C:
            def __init__(self, a=1, b=2):
                self.a, self.b = a, b
        inst = instantiate_from_dict(C, {"a": 10, "b": 20})
        assert inst.a == 10
        assert inst.b == 20

    def test_with_aliases(self):
        class C:
            _param_aliases = {"min": "min_pe", "max": "max_pe"}
            def __init__(self, min_pe=0, max_pe=100):
                self.min_pe, self.max_pe = min_pe, max_pe
        inst = instantiate_from_dict(C, {"min": 10, "max": 30})
        assert inst.min_pe == 10
        assert inst.max_pe == 30

    def test_unknown_params_filtered(self):
        class C:
            def __init__(self, a=1):
                self.a = a
        # 不在 __init__ 签名里的 b 应被过滤而不报错
        inst = instantiate_from_dict(C, {"a": 5, "b": "ignored"})
        assert inst.a == 5

    def test_kwargs_accepts_unknown(self):
        class C:
            def __init__(self, a=1, **kwargs):
                self.a, self.extras = a, kwargs
        inst = instantiate_from_dict(C, {"a": 1, "anything": 2})
        assert inst.extras == {"anything": 2}


# =============================================================================
# STRATEGY_REGISTRY 向后兼容
# =============================================================================

@pytest.mark.unit
class TestStrategyRegistryBackcompat:
    def test_dict_like_getitem(self):
        from src.strategy.backtest import STRATEGY_REGISTRY
        cls = STRATEGY_REGISTRY["ma_crossover"]
        assert cls.__name__ == "MACrossoverStrategy"

    def test_contains(self):
        from src.strategy.backtest import STRATEGY_REGISTRY
        assert "ma_crossover" in STRATEGY_REGISTRY
        assert "totally_not_a_strategy" not in STRATEGY_REGISTRY

    def test_keys_iter(self):
        from src.strategy.backtest import STRATEGY_REGISTRY
        keys = set(STRATEGY_REGISTRY.keys())
        assert {"ma_crossover", "factor_rebalance", "rule_based",
                "screener_rule", "ml_rebalance"} <= keys

    def test_strategy_labels_dict_like(self):
        from src.strategy.backtest import STRATEGY_LABELS
        assert STRATEGY_LABELS["ma_crossover"].startswith("双均线")
        assert "ma_crossover" in STRATEGY_LABELS

    def test_strategy_param_schemas_returns_list(self):
        from src.strategy.backtest import STRATEGY_PARAM_SCHEMAS
        schema = STRATEGY_PARAM_SCHEMAS["ma_crossover"]
        assert isinstance(schema, list)
        assert all("key" in s for s in schema)
        keys = {s["key"] for s in schema}
        assert "fast_period" in keys
        assert "slow_period" in keys


# =============================================================================
# 端到端：通过 strategies/ 自动发现新策略
# =============================================================================

@pytest.mark.unit
class TestAutodiscoverIntegration:
    def test_new_strategy_via_drop_in_file(self, tmp_path, monkeypatch):
        """
        模拟用户在 strategies/ 目录下 drop 一个新策略文件，
        autodiscover 后该策略自动出现在 STRATEGY_REGISTRY。
        """
        from src.strategy.backtest import STRATEGY_REGISTRY

        # 临时挂一个测试用的策略文件
        strategies_dir = Path(__file__).resolve().parent.parent / "src" / "strategy" / "backtest" / "strategies"
        test_file = strategies_dir / "_pytest_demo.py"
        test_file.write_text(textwrap.dedent("""
            from src.strategy.backtest import STRATEGY_REGISTRY
            from src.strategy.backtest.base_strategy import BaseStrategy

            @STRATEGY_REGISTRY.register("pytest_demo", label="测试用 Demo")
            class PytestDemoStrategy(BaseStrategy):
                params = (("x", 1),)
                def next(self): pass
        """), encoding="utf-8")
        try:
            # 强制重新 autodiscover
            from src.core.plugin import autodiscover
            # 卸载之前的 demo（如果有）
            STRATEGY_REGISTRY.unregister("pytest_demo")
            # 重新 import 子模块
            import importlib
            mod_name = "src.strategy.backtest.strategies._pytest_demo"
            if mod_name in sys.modules:
                importlib.reload(sys.modules[mod_name])
            else:
                importlib.import_module(mod_name)

            assert "pytest_demo" in STRATEGY_REGISTRY
            assert STRATEGY_REGISTRY["pytest_demo"].__name__ == "PytestDemoStrategy"
        finally:
            test_file.unlink(missing_ok=True)
            STRATEGY_REGISTRY.unregister("pytest_demo")
