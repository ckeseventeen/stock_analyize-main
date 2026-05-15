"""
src/web/widgets/schema_form.py — Schema 驱动的表单渲染器

消费 PluginRegistry.schema_for(key) 返回的字段列表，自动渲染对应 Streamlit 控件。
取代 4_策略回测.py / 2_策略配置.py 等页面的手写参数表单。

支持的 schema type：
  - int / float                 → number_input
  - str                         → text_input（option 列表则用 selectbox）
  - bool                        → checkbox
  - list / dict / yaml          → text_area（解析 YAML）
  - 含 options 列表             → selectbox

Schema 字段示例：
    {"key": "fast_period", "label": "短期均线", "type": "int",
     "default": 10, "min": 3, "max": 60, "step": 1, "help": "金叉快线"}
"""
from __future__ import annotations

from typing import Any

import yaml


class SchemaForm:
    """
    根据 schema list 渲染表单，返回用户填入的 dict。

    用法：
        schema = STRATEGY_REGISTRY.schema_for("ma_crossover")
        result = SchemaForm(schema, defaults={"fast_period": 5}, key="bt").render()
        # result = {"fast_period": 5, "slow_period": 30}
    """

    def __init__(
        self,
        schema: list[dict],
        defaults: dict | None = None,
        key: str = "schemaform",
        columns: int = 1,
    ):
        self.schema = list(schema or [])
        self.defaults = dict(defaults or {})
        self.key = key
        self.columns = max(1, int(columns))

    def render(self) -> dict[str, Any]:
        """渲染表单并返回 {field_key: value}"""
        import streamlit as st

        values: dict[str, Any] = {}
        if not self.schema:
            st.caption("（无可配置参数）")
            return values

        # 多列布局
        if self.columns > 1:
            cols = st.columns(self.columns)
        else:
            cols = [st]

        for idx, field in enumerate(self.schema):
            target = cols[idx % self.columns]
            with target:
                value = self._render_field(field)
                values[field["key"]] = value

        return values

    def _render_field(self, field: dict) -> Any:
        """渲染单个字段"""
        import streamlit as st

        key = field["key"]
        label = field.get("label", key)
        ftype = field.get("type", "str")
        default = self.defaults.get(key, field.get("default"))
        help_text = field.get("help")
        widget_key = f"{self.key}__{key}"

        options = field.get("options")
        if options:
            # 下拉选择
            if default in options:
                idx = options.index(default)
            else:
                idx = 0
            return st.selectbox(
                label, options=options, index=idx,
                key=widget_key, help=help_text,
            )

        if ftype == "int":
            return st.number_input(
                label, value=int(default) if default is not None else 0,
                min_value=int(field["min"]) if "min" in field else None,
                max_value=int(field["max"]) if "max" in field else None,
                step=int(field.get("step", 1)),
                key=widget_key, help=help_text,
            )

        if ftype == "float":
            return st.number_input(
                label, value=float(default) if default is not None else 0.0,
                min_value=float(field["min"]) if "min" in field else None,
                max_value=float(field["max"]) if "max" in field else None,
                step=float(field.get("step", 0.1)),
                format=field.get("format", "%.4f"),
                key=widget_key, help=help_text,
            )

        if ftype == "bool":
            return st.checkbox(
                label, value=bool(default) if default is not None else False,
                key=widget_key, help=help_text,
            )

        if ftype in ("yaml", "list", "dict"):
            # 复杂结构用 text_area + yaml 解析
            default_text = (
                yaml.dump(default, allow_unicode=True, default_flow_style=False)
                if default is not None else ""
            )
            text = st.text_area(
                label, value=default_text, height=200,
                key=widget_key, help=help_text or "用 YAML 格式编辑",
            )
            try:
                return yaml.safe_load(text) if text.strip() else (
                    [] if ftype == "list" else {} if ftype == "dict" else None
                )
            except yaml.YAMLError as e:
                st.error(f"⚠️ YAML 解析失败: {e}")
                return default

        # 默认 str
        return st.text_input(
            label, value=str(default) if default is not None else "",
            key=widget_key, help=help_text,
        )
