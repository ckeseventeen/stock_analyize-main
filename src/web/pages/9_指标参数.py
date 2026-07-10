"""
pages/9_指标参数.py — 技术指标参数配置（单一职责）

只做技术指标 profile 管理：MACD/RSI/KDJ/BOLL/MA 的参数。
原 9_配置管理 的另两个职责已拆出：
  - 因子库 → 13_因子库.py
  - 回测预设 → 14_回测预设.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st  # noqa: E402

from src.web.utils import (  # noqa: E402
    PATH_INDICATORS,
    delete_indicator_profile,
    list_indicator_profiles,
    load_indicator_profile,
    load_yaml,
    save_indicator_profile,
    set_active_indicator_profile,
)

st.title("📐 技术指标参数")
st.caption(
    f"配置文件：`{PATH_INDICATORS}` · 代码端通过 `TechnicalAnalyzer.add_all_from_config()` 读取"
)

# ============================================================================
# Profile 管理
# ============================================================================
profiles = list_indicator_profiles() or ["default"]
cfg_raw = load_yaml(PATH_INDICATORS) or {}
active = cfg_raw.get("active_profile", "default")

c1, c2, c3 = st.columns([2, 1, 1])
with c1:
    picked = st.selectbox(
        "Profile",
        options=profiles,
        index=profiles.index(active) if active in profiles else 0,
        help="选择要查看/编辑的参数集；可在下方'另存为'里创建新 profile",
        key="ind_profile",
    )
with c2:
    if picked == active:
        st.success(f"✅ 当前激活：`{active}`")
    else:
        if st.button("⚡ 激活此 profile", key="set_active_ind", width="stretch"):
            if set_active_indicator_profile(picked):
                st.toast(f"已激活 {picked}", icon="✅")
                st.rerun()
            else:
                st.toast("激活失败", icon="⚠️")
with c3:
    if picked != "default":
        if st.button("🗑 删除", key="del_ind", width="stretch"):
            if delete_indicator_profile(picked):
                st.toast(f"已删除 {picked}", icon="🗑️")
                st.rerun()
            else:
                st.toast("删除失败", icon="⚠️")
    else:
        st.caption("default 不可删")

params = load_indicator_profile(picked)

# ============================================================================
# 编辑表单
# ============================================================================
st.markdown("---")
st.markdown(f"#### ✏️ 编辑 `{picked}`")

with st.form("edit_ind_profile"):
    # ───── MACD ─────
    st.markdown("##### 📊 MACD")
    cm1, cm2, cm3 = st.columns(3)
    macd_fast = cm1.slider("快速 EMA (fast)",
                           min_value=2, max_value=30,
                           value=int(params["macd"]["fast"]), step=1)
    macd_slow = cm2.slider("慢速 EMA (slow)",
                           min_value=5, max_value=60,
                           value=int(params["macd"]["slow"]), step=1)
    macd_signal = cm3.slider("信号线周期 (signal)",
                             min_value=2, max_value=20,
                             value=int(params["macd"]["signal"]), step=1)

    # ───── RSI ─────
    st.markdown("##### 📈 RSI")
    rsi_period = st.slider("RSI 周期",
                           min_value=2, max_value=50,
                           value=int(params["rsi"]["period"]), step=1)

    # ───── KDJ ─────
    st.markdown("##### 🎲 KDJ")
    ck1, ck2, ck3 = st.columns(3)
    kdj_n = ck1.slider("n",
                       min_value=2, max_value=30,
                       value=int(params["kdj"]["n"]), step=1)
    kdj_m1 = ck2.slider("m1",
                        min_value=1, max_value=10,
                        value=int(params["kdj"]["m1"]), step=1)
    kdj_m2 = ck3.slider("m2",
                        min_value=1, max_value=10,
                        value=int(params["kdj"]["m2"]), step=1)

    # ───── Bollinger ─────
    st.markdown("##### 📊 Bollinger Bands")
    cb1, cb2 = st.columns(2)
    bb_period = cb1.slider("周期",
                           min_value=5, max_value=60,
                           value=int(params["bollinger"]["period"]), step=1)
    bb_std = cb2.slider("std_dev (倍数)",
                        min_value=0.5, max_value=4.0,
                        value=float(params["bollinger"]["std_dev"]), step=0.1)

    # ───── 均线 ─────
    st.markdown("##### 📏 均线（Moving Averages）")
    ma_periods_str = st.text_input(
        "均线周期（逗号分隔）",
        value=",".join(str(p) for p in (params["moving_averages"].get("periods") or [])),
        help="例如：5,10,20,60,120,250",
    )

    # ───── 保存 ─────
    st.markdown("---")
    save_as_name = st.text_input(
        "另存为新 profile（留空 = 覆盖当前 profile）",
        value="",
        placeholder=f"my_{picked}_v2",
    )
    submitted = st.form_submit_button("💾 保存", type="primary")

    if submitted:
        try:
            ma_periods = [int(x.strip()) for x in ma_periods_str.split(",") if x.strip()]
        except ValueError:
            st.error("均线周期必须为整数列表")
        else:
            new_params = {
                "macd": {"fast": int(macd_fast), "slow": int(macd_slow),
                         "signal": int(macd_signal)},
                "rsi": {"period": int(rsi_period)},
                "kdj": {"n": int(kdj_n), "m1": int(kdj_m1), "m2": int(kdj_m2)},
                "bollinger": {"period": int(bb_period), "std_dev": float(bb_std)},
                "moving_averages": {"periods": ma_periods},
            }
            target = save_as_name.strip() or picked
            if save_indicator_profile(target, new_params):
                st.toast(f"已保存到 profile `{target}`", icon="✅")
                st.rerun()
            else:
                st.error("写入 YAML 失败")

# ============================================================================
# 当前 profile 详情（只读）
# ============================================================================
with st.expander("📋 当前参数（只读 JSON）"):
    st.json(params)

# 跳转到其他配置页
st.markdown("---")
st.markdown("**🔗 相关配置**")
nav_cols = st.columns(3)
with nav_cols[0]:
    st.page_link("pages/13_因子库.py", label="🎯 因子库", icon="🎯")
with nav_cols[1]:
    st.page_link("pages/4_策略回测.py", label="🧪 策略回测", icon="🧪")
with nav_cols[2]:
    st.page_link("pages/12_关注标的.py", label="📌 关注标的", icon="📌")
