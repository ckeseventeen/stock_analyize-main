"""
src/web/components/confirm.py — 二次确认对话框组件

解决的核心问题：
  - 10+ 处删除/停止操作无二次确认
  - 误操作无法撤销

用法：
  from src.web.components.confirm import confirm_action

  result = confirm_action("del_strategy", "确定删除该策略？此操作不可撤销。", "删除策略")
  if result is True:
      # 用户确认，执行删除
      ...
  elif result is False:
      # 用户取消
      pass
  # result is None → 未触发
"""
from __future__ import annotations

import streamlit as st


def confirm_action(
    key: str,
    message: str,
    button_label: str = "确认执行",
    danger: bool = True,
) -> bool | None:
    """
    两步确认组件：
    1. 第一次点击按钮 → 显示确认消息
    2. 第二次点击 → 返回 True
    3. 取消 → 返回 False
    未触发 → 返回 None

    Args:
        key: 唯一标识（避免多组件冲突）
        message: 确认时显示的警告消息
        button_label: 触发按钮的文字
        danger: 是否为危险操作（影响颜色）

    Returns:
        True=确认, False=取消, None=未触发
    """
    state_key = f"_confirm_{key}"

    if st.button(button_label, key=f"_btn_{key}"):
        st.session_state[state_key] = True

    if st.session_state.get(state_key):
        color = "#dc3545" if danger else "#856404"
        bg = "#fff5f5" if danger else "#fff3cd"
        st.markdown(
            f'<div style="background:{bg};color:{color};padding:12px 16px;'
            f'border-radius:8px;font-size:14px;font-weight:600;'
            f'border:1px solid {color}30;">'
            f"⚠️ {message}"
            f"</div>",
            unsafe_allow_html=True,
        )
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✓ 确认", key=f"_yes_{key}", type="primary"):
                st.session_state[state_key] = False
                return True
        with col2:
            if st.button("✗ 取消", key=f"_no_{key}"):
                st.session_state[state_key] = False
                return False
    return None
