"""
src/web/widgets/crud_table.py — 通用 CRUD 表格 widget

取代 5_价格预警 / 9_配置管理 / 12_关注标的 等页面手写的：
  - 表格展示 + 删除按钮
  - 表单新增（含二次确认）
  - YAML 持久化样板

用法：
    rules = load_yaml(PATH_PRICE_ALERTS).get("rules", [])
    schema = [
        {"key": "code", "type": "str", "label": "股票代码"},
        {"key": "name", "type": "str", "label": "名称"},
        ...
    ]

    new_rules = CRUDTable(
        items=rules,
        schema=schema,
        title="📋 现有规则",
        id_field="code",
        key="alerts",
    ).render()
    if new_rules is not rules:    # 用户做了增删
        cfg["rules"] = new_rules
        atomic_save_yaml(PATH_PRICE_ALERTS, cfg)
"""
from __future__ import annotations

import pandas as pd

from src.web.widgets.schema_form import SchemaForm


class CRUDTable:
    """
    数据列表 + 新增表单 + 删除按钮一体化 widget。

    Args:
        items:       现有数据列表（list[dict]）
        schema:      字段 schema（同 SchemaForm 格式）
        title:       表格上方标题
        id_field:    用于识别记录的关键字段（删除/编辑用），默认 "code"
        key:         widget 唯一前缀，避免 session_state 冲突
        read_only:   True 时只展示，禁用增删
        display_cols: 表格显示哪些列（None=全部）
    """

    def __init__(
        self,
        items: list[dict],
        schema: list[dict],
        *,
        title: str = "",
        id_field: str = "code",
        key: str = "crud",
        read_only: bool = False,
        display_cols: list[str] | None = None,
        on_add_label: str = "➕ 新增",
        empty_hint: str = "暂无数据，请使用下方表单添加",
    ):
        self.items = list(items or [])
        self.schema = list(schema or [])
        self.title = title
        self.id_field = id_field
        self.key = key
        self.read_only = read_only
        self.display_cols = display_cols
        self.on_add_label = on_add_label
        self.empty_hint = empty_hint

    def render(self) -> list[dict]:
        """渲染表格 + 新增/删除控件，返回更新后的 items 列表"""
        import streamlit as st

        if self.title:
            st.subheader(self.title)

        # 表格
        if not self.items:
            st.info(self.empty_hint)
        else:
            df = pd.DataFrame(self.items)
            if self.display_cols:
                df = df[[c for c in self.display_cols if c in df.columns]]
            st.dataframe(df, width="stretch", hide_index=True)

        if self.read_only:
            return self.items

        new_items = list(self.items)

        # 删除
        if self.items:
            with st.expander("🗑 删除记录"):
                labels = [self._format_label(item) for item in self.items]
                if labels:
                    chosen = st.selectbox(
                        "选择要删除的记录",
                        options=range(len(labels)),
                        format_func=lambda i: labels[i],
                        key=f"{self.key}_del_sel",
                    )
                    confirm_key = f"{self.key}_del_confirm"
                    do_confirm = st.checkbox("我确认删除", key=confirm_key)
                    if st.button("🗑️ 执行删除", key=f"{self.key}_del_btn",
                                 disabled=not do_confirm):
                        removed = new_items.pop(int(chosen))
                        st.success(f"已删除: {self._format_label(removed)}")
                        st.session_state[confirm_key] = False
                        return new_items   # 返回新版本，调用方负责持久化

        # 新增
        with st.expander(self.on_add_label, expanded=not self.items):
            with st.form(key=f"{self.key}_add_form", clear_on_submit=True):
                form = SchemaForm(self.schema, key=f"{self.key}_add").render()
                submit = st.form_submit_button("✅ 添加", type="primary")
                if submit:
                    # 简单校验：id_field 必填
                    if self.id_field and not form.get(self.id_field):
                        st.error(f"{self.id_field} 不能为空")
                    else:
                        new_items.append(dict(form))
                        st.success(f"已添加: {self._format_label(form)}")
                        return new_items

        return new_items

    def _format_label(self, item: dict) -> str:
        """生成易读的标签，用于删除选择框 / 提示"""
        parts = []
        # 优先 id_field + name
        if item.get(self.id_field):
            parts.append(str(item[self.id_field]))
        if item.get("name") and item.get("name") != item.get(self.id_field):
            parts.append(str(item["name"]))
        if not parts:
            # 取前两个非空字段
            for k, v in item.items():
                if v not in (None, "", []):
                    parts.append(f"{k}={v}")
                if len(parts) >= 2:
                    break
        return " · ".join(parts)
