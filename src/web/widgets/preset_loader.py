"""
src/web/widgets/preset_loader.py — 预设加载/保存/删除

取代 4_策略回测.py 等页面手写的预设管理样板。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable


class PresetLoader:
    """
    预设加载/保存 widget。

    Args:
        list_func:   () -> list[str]，返回所有预设名
        load_func:   (name) -> dict，加载某预设
        save_func:   (name, config) -> bool，保存预设
        delete_func: (name) -> bool，删除预设（可选）
    """

    def __init__(
        self,
        list_func: Callable[[], list[str]],
        load_func: Callable[[str], dict],
        save_func: Callable[[str, dict], bool] | None = None,
        delete_func: Callable[[str], bool] | None = None,
        *,
        key: str = "preset",
        label: str = "📁 预设",
    ):
        self.list_func = list_func
        self.load_func = load_func
        self.save_func = save_func
        self.delete_func = delete_func
        self.key = key
        self.label = label

    def render_load_section(self) -> tuple[str, dict]:
        """
        渲染"加载预设"区域。
        Returns: (preset_name, loaded_config_dict)
            preset_name == "<不使用预设>" 时 loaded_config_dict 为 {}
        """
        import streamlit as st

        presets = ["<不使用预设>"] + list(self.list_func() or [])
        chosen = st.selectbox(
            self.label,
            options=presets,
            index=0,
            key=f"{self.key}_load_sel",
        )
        if chosen == "<不使用预设>":
            return chosen, {}
        try:
            return chosen, dict(self.load_func(chosen) or {})
        except Exception as e:
            st.warning(f"加载预设失败: {e}")
            return chosen, {}

    def render_save_section(self, current_config: dict) -> None:
        """
        渲染"保存当前配置为预设"区域。
        """
        import streamlit as st
        if self.save_func is None:
            return
        with st.expander("💾 另存为预设"):
            name = st.text_input(
                "预设名", key=f"{self.key}_save_name",
                placeholder="例如：my_strategy_v1",
            )
            desc = st.text_input(
                "描述（可选）", key=f"{self.key}_save_desc",
            )
            if st.button("✅ 保存", key=f"{self.key}_save_btn"):
                if not name:
                    st.error("预设名不能为空")
                else:
                    cfg = dict(current_config)
                    if desc:
                        cfg["description"] = desc
                    if self.save_func(name, cfg):
                        st.success(f"已保存: {name}")
                        st.rerun()
                    else:
                        st.error("保存失败")

    def render_delete_section(self) -> None:
        """渲染"删除预设"区域"""
        import streamlit as st
        if self.delete_func is None:
            return
        presets = list(self.list_func() or [])
        if not presets:
            return
        with st.expander("🗑 删除预设"):
            choice = st.selectbox("选择要删除的预设", options=presets,
                                  key=f"{self.key}_del_sel")
            confirm = st.checkbox("我确认删除", key=f"{self.key}_del_confirm")
            if st.button("🗑️ 执行删除", key=f"{self.key}_del_btn",
                         disabled=not confirm):
                if self.delete_func(choice):
                    st.success(f"已删除: {choice}")
                    st.session_state[f"{self.key}_del_confirm"] = False
                    st.rerun()
                else:
                    st.error("删除失败")
