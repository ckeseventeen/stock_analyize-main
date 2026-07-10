"""
pages/13_因子库.py — 因子库管理（单一职责）

从 9_配置管理 拆出：因子 profile 增删改 + 启用切换。
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from src.web.utils import (  # noqa: E402
    PATH_FACTORS,
    delete_factor_profile,
    list_factor_profiles,
    load_factor_profile,
    load_yaml,
    save_factor_profile,
    set_active_factor_profile,
)

st.title("🎯 因子库")
st.caption(
    f"配置文件：`{PATH_FACTORS}` · 代码端通过 `build_engine_from_config()` 读取"
)

# 触发 FACTOR_REGISTRY 填充
from src.analysis.factor import FACTOR_REGISTRY  # noqa: E402

# ============================================================================
# Profile 管理
# ============================================================================
profiles = list_factor_profiles() or ["default"]
cfg_raw = load_yaml(PATH_FACTORS) or {}
active = cfg_raw.get("active_profile", "default")

c1, c2, c3 = st.columns([2, 1, 1])
with c1:
    picked = st.selectbox(
        "Profile",
        options=profiles,
        index=profiles.index(active) if active in profiles else 0,
        key="fac_profile",
    )
with c2:
    if picked == active:
        st.success(f"✅ 当前激活：`{active}`")
    else:
        if st.button("⚡ 激活", key="set_active_fac", width="stretch"):
            if set_active_factor_profile(picked):
                st.toast(f"已激活 {picked}", icon="✅")
                st.rerun()
with c3:
    if picked != "default":
        if st.button("🗑 删除", key="del_fac", width="stretch"):
            if delete_factor_profile(picked):
                st.toast(f"已删除 {picked}", icon="🗑️")
                st.rerun()
    else:
        st.caption("default 不可删")

factors = load_factor_profile(picked)

# ============================================================================
# 因子列表（带行内启用切换 / 删除）
# ============================================================================
st.markdown("---")
st.markdown(f"#### 📋 `{picked}` 包含的因子 ({len(factors)} 个)")

if not factors:
    st.info(f"profile `{picked}` 暂无因子。使用下方表单添加。")
else:
    # 行内操作表头
    h = st.columns([0.6, 2.2, 3, 0.8, 0.8])
    h[0].markdown("**状态**")
    h[1].markdown("**类型**")
    h[2].markdown("**参数**")
    h[3].markdown("**操作**")
    h[4].markdown("")

    for i, f in enumerate(factors):
        ftype = f.get("type", "?")
        enabled = f.get("enabled", True)
        params = f.get("params", {})

        cols = st.columns([0.6, 2.2, 3, 0.8, 0.8])
        with cols[0]:
            label = "✅" if enabled else "⏸"
            if st.button(label, key=f"toggle_fac_{i}", help="点击切换启用/禁用"):
                factors[i]["enabled"] = not enabled
                save_factor_profile(picked, factors)
                st.toast(f"{'启用' if not enabled else '禁用'} {ftype}", icon="✅")
                st.rerun()
        with cols[1]:
            tag = ftype if enabled else f"~~{ftype}~~"
            st.markdown(tag)
        with cols[2]:
            if params:
                st.caption(", ".join(f"{k}={v}" for k, v in params.items()))
            else:
                st.caption("（无参数）")
        with cols[3]:
            confirm_key = f"confirm_del_fac_{i}"
            if st.session_state.get(confirm_key):
                if st.button("⚠️确定", key=f"del_confirm_{i}"):
                    factors.pop(i)
                    save_factor_profile(picked, factors)
                    st.toast(f"已删除 {ftype}", icon="🗑️")
                    st.session_state[confirm_key] = False
                    st.rerun()
            else:
                if st.button("🗑️", key=f"del_fac_{i}", help="删除（点 2 次）"):
                    st.session_state[confirm_key] = True
                    st.rerun()

# ============================================================================
# 新增因子
# ============================================================================
st.markdown("---")
st.markdown("#### ➕ 添加因子到当前 profile")

all_types = sorted(FACTOR_REGISTRY.keys())

with st.form("add_factor_form", clear_on_submit=True):
    c1, c2 = st.columns([2, 1])
    with c1:
        type_key = st.selectbox(
            "因子类型",
            options=all_types,
            help="从 FACTOR_REGISTRY 中所有已注册因子选择",
        )
    with c2:
        enabled = st.checkbox("立即启用", value=True)

    params_str = st.text_input(
        "参数 (key=value, 逗号分隔；留空 = 用默认值)",
        placeholder="period=14, lookback_bars=60",
    )

    submit = st.form_submit_button("✅ 添加", type="primary")

    if submit:
        params_dict = {}
        if params_str.strip():
            try:
                for pair in params_str.split(","):
                    if "=" not in pair:
                        continue
                    k, v = pair.split("=", 1)
                    k = k.strip()
                    v = v.strip()
                    if v.lstrip("-").isdigit():
                        params_dict[k] = int(v)
                    else:
                        try:
                            params_dict[k] = float(v)
                        except ValueError:
                            params_dict[k] = v
            except Exception as e:
                st.error(f"参数解析失败: {e}")
                params_dict = None

        if params_dict is not None:
            entry = {"type": type_key, "enabled": enabled}
            if params_dict:
                entry["params"] = params_dict
            factors.append(entry)
            if save_factor_profile(picked, factors):
                st.toast(f"已添加 {type_key}", icon="✅")
                st.rerun()
            else:
                st.error("写入失败")

# ============================================================================
# 全部可用因子参考
# ============================================================================
with st.expander("📚 全部可用因子类型（FACTOR_REGISTRY）"):
    rows = [{"type": k, "类名": cls.__name__} for k, cls in sorted(FACTOR_REGISTRY.items())]
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

# 跳转
st.markdown("---")
nav = st.columns(3)
with nav[0]:
    st.page_link("pages/9_指标参数.py", label="📐 指标参数", icon="📐")
with nav[1]:
    st.page_link("pages/4_策略回测.py", label="🧪 策略回测", icon="🧪")
with nav[2]:
    st.page_link("pages/12_关注标的.py", label="📌 关注标的", icon="📌")
