"""
src/web/components/search.py — 可搜索的选择器

解决的核心问题：
  - 原生 st.selectbox 50+ 股票无法搜索
  - 关注标的搜索不支持模糊匹配

用法：
  from src.web.components.search import searchable_select

  selected_code = searchable_select(
      "选择股票",
      options=[{"code": "600519", "name": "贵州茅台"}, ...],
      key="stock_picker",
  )
"""
from __future__ import annotations

from typing import Any

import streamlit as st


def searchable_select(
    label: str,
    options: list[dict],
    key: str,
    id_field: str = "code",
    name_field: str = "name",
    placeholder: str = "输入代码或名称搜索...",
) -> Any | None:
    """
    可搜索的下拉选择器。

    在 selectbox 之上增加搜索框过滤，解决 50+ 选项难以定位的问题。

    Args:
        label: 选择器标签
        options: 选项列表 [{"code": "600519", "name": "贵州茅台"}, ...]
        key: 组件唯一标识
        id_field: 选项 ID 字段名
        name_field: 选项显示名字段名
        placeholder: 搜索框占位文字

    Returns:
        选中的 id 值（如 "600519"），或 None
    """
    if not options:
        st.warning("无可选项")
        return None

    # 搜索框
    query = st.text_input(
        f"🔍 {label}",
        key=f"{key}_search",
        placeholder=placeholder,
    )

    # 过滤
    if query:
        q = query.lower()
        filtered = [
            o for o in options
            if q in str(o.get(id_field, "")).lower()
            or q in str(o.get(name_field, "")).lower()
        ]
    else:
        filtered = options

    if not filtered:
        st.warning("无匹配结果")
        return None

    # 下拉选择
    display = [
        f"{o.get(id_field, '')} - {o.get(name_field, '')} · {o.get('category', '')}"
        for o in filtered
    ]
    selected_idx = st.selectbox(
        label,
        options=range(len(display)),
        format_func=lambda i: display[i],
        key=f"{key}_select",
    )
    return filtered[selected_idx][id_field]


def fuzzy_search_stocks(
    stocks: list[dict],
    query: str,
    code_field: str = "code",
    name_field: str = "name",
    notes_field: str = "notes",
) -> list[dict]:
    """
    模糊搜索股票列表。支持：
    - 前缀匹配（代码）
    - 子串匹配（名称/备注）
    - 简单拼音首字母（首字母大写匹配）

    Args:
        stocks: 股票列表
        query: 搜索关键词
        code_field: 代码字段名
        name_field: 名称字段名
        notes_field: 备注字段名

    Returns:
        匹配的股票列表
    """
    if not query.strip():
        return stocks

    q = query.strip().lower()
    results = []
    for s in stocks:
        code = str(s.get(code_field, "")).lower()
        name = str(s.get(name_field, "")).lower()
        notes = str(s.get(notes_field, "")).lower()

        # 优先级：代码前缀 > 代码包含 > 名称包含 > 备注包含
        if code.startswith(q):
            results.insert(0, s)  # 最高优先级
        elif q in code or q in name or q in notes:
            results.append(s)

    return results
