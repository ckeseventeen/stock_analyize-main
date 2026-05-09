"""
src/web/components/unsaved.py — 未保存变更提示

解决的核心问题：
  - 策略配置编辑后切换页面即丢失，无任何提示

用法：
  from src.web.components.unsaved import mark_dirty, mark_clean, unsaved_badge

  # 编辑操作后标记为 dirty
  mark_dirty("strategy_editor")

  # 保存后标记为 clean
  mark_clean("strategy_editor")

  # 在页面顶部显示提示
  unsaved_badge("strategy_editor")
"""
from __future__ import annotations

import streamlit as st


def mark_dirty(key: str) -> None:
    """标记为有未保存变更"""
    st.session_state[f"_dirty_{key}"] = True


def mark_clean(key: str) -> None:
    """标记为已保存"""
    st.session_state[f"_dirty_{key}"] = False


def is_dirty(key: str) -> bool:
    """是否存在未保存变更"""
    return st.session_state.get(f"_dirty_{key}", False)


def unsaved_badge(key: str) -> None:
    """
    显示未保存提示横幅（仅在有未保存变更时显示）
    """
    if is_dirty(key):
        st.markdown(
            '<div style="background:#fff3cd;color:#856404;padding:10px 16px;'
            'border-radius:8px;font-size:13px;font-weight:600;'
            'border:1px solid #ffc10740;margin-bottom:12px;">'
            "⚠️ 有未保存的变更 — 离开此页面将丢失！"
            "</div>",
            unsafe_allow_html=True,
        )
