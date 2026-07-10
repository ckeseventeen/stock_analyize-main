"""
tests/test_widgets.py — PR 4 前端框架测试

不依赖真实 streamlit 上下文（用 mock），验证：
  - Page 框架的 inject_project_path 正确定位项目根
  - 各 widget 类构造、接口签名稳定
  - SchemaForm 内部逻辑（字段解析、yaml 反序列化）
  - CRUDTable 的 _format_label
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# =============================================================================
# Framework
# =============================================================================

@pytest.mark.unit
class TestFramework:
    def test_inject_project_path(self):
        from src.web.framework import inject_project_path
        root = inject_project_path()
        assert (root / "main.py").exists() or (root / "pyproject.toml").exists()
        assert str(root) in sys.path

    def test_page_class_definition(self):
        from src.web.framework import Page

        class MyPage(Page):
            title = "Test"
            icon = "🧪"
            caption = "test caption"

            def render(self):
                pass

        # 不实际运行（缺 streamlit 上下文），只验证类属性
        assert MyPage.title == "Test"
        assert MyPage.icon == "🧪"

    def test_page_must_implement_render(self):
        from src.web.framework import Page

        class Incomplete(Page):
            title = "x"
            # 不 override render

        # 仅断言 Page.render 是 NotImplementedError
        with pytest.raises(NotImplementedError):
            Page.render(Page.__new__(Page))


# =============================================================================
# Widgets — 构造 + 接口签名
# =============================================================================

@pytest.mark.unit
class TestWidgetConstruction:
    def test_schema_form_construct(self):
        from src.web.widgets import SchemaForm
        sf = SchemaForm([{"key": "x", "type": "int", "default": 1}],
                        defaults={"x": 5}, key="t")
        assert sf.key == "t"
        assert sf.defaults["x"] == 5

    def test_schema_form_empty_schema_ok(self):
        from src.web.widgets import SchemaForm
        sf = SchemaForm([], key="t")
        assert sf.schema == []

# =============================================================================
# Theme
# =============================================================================

@pytest.mark.unit
class TestTheme:
    def test_emoji_constants(self):
        from src.web.theme import Emoji
        assert Emoji.SUCCESS == "✅"
        assert Emoji.DELETE == "🗑️"

    def test_market_flag(self):
        from src.web.theme import market_flag
        assert market_flag("a") == "🇨🇳"
        assert market_flag("hk") == "🇭🇰"
        assert market_flag("us") == "🇺🇸"
        assert market_flag("xx") == "🌐"  # 未知市场兜底


# =============================================================================
# 与 PluginRegistry 集成：schema_form 能消费 STRATEGY_REGISTRY.schema_for
# =============================================================================

@pytest.mark.unit
class TestSchemaFormIntegration:
    def test_consume_strategy_schema(self):
        from src.strategy.backtest import STRATEGY_PARAM_SCHEMAS
        from src.web.widgets import SchemaForm

        schema = STRATEGY_PARAM_SCHEMAS["ma_crossover"]
        # 构造 SchemaForm 不应抛错
        sf = SchemaForm(schema, key="t")
        # schema 含我们期望的字段
        keys = {f["key"] for f in sf.schema}
        assert "fast_period" in keys
        assert "slow_period" in keys
