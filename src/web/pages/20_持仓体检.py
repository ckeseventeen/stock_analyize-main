"""
pages/20_持仓体检.py — 组合诊断（持仓体检报告）

四维打分（卖出信号 / 集中度 / 盈亏结构 / 大盘环境）→ 综合健康分 + 问题清单。
引擎在 src/portfolio/diagnostics.py，本页仅渲染。
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

st.title("🩺 持仓体检")
st.caption("四维诊断：卖出信号 · 集中度 · 盈亏结构 · 大盘环境 → 一份健康分报告")

_GRADE_COLORS = {"A": "🟢", "B": "🟡", "C": "🟠", "D": "🔴"}

run_btn = st.button("🩺 开始体检", type="primary")

if run_btn:
    with st.spinner("正在拉取行情并逐项诊断（首次约 1-2 分钟）..."):
        try:
            from src.portfolio.diagnostics import run_diagnosis
            st.session_state["_diag_result"] = run_diagnosis()
        except Exception as e:
            st.error(f"体检失败: {e}")
            st.exception(e)
            st.stop()

diag = st.session_state.get("_diag_result")

if diag is None:
    st.info("点击上方按钮开始体检。需要已在「持仓监控」页录入持仓（config/holdings.yaml）。")
    st.stop()

# ============================================================
# 总览
# ============================================================
st.markdown("---")
g_icon = _GRADE_COLORS.get(diag.grade, "⚪")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("综合健康分", f"{diag.score:.0f} / 100")
c2.metric("等级", f"{g_icon} {diag.grade}")
c3.metric("持仓市值", f"{diag.total_market_value:,.0f}")
c4.metric("总浮盈", f"{diag.total_pnl:+,.0f} ({diag.total_pnl_pct:+.1f}%)")
c5.metric("持仓数", diag.position_count)

st.progress(min(1.0, diag.score / 100))
st.caption(f"生成时间：{diag.generated_at}")

# ============================================================
# 四维得分
# ============================================================
st.markdown("---")
st.subheader("📐 分维度得分")

dim_cols = st.columns(len(diag.dimensions))
for col, d in zip(dim_cols, diag.dimensions):
    with col:
        st.metric(f"{d.label}（权重 {d.weight:.0%}）", f"{d.score:.0f}")
        st.progress(min(1.0, d.score / 100))
        st.caption(d.detail or "—")

# 大盘环境摘要
if diag.regime is not None:
    with st.expander(f"{diag.regime.emoji} 大盘环境详情", expanded=False):
        st.markdown(diag.regime.summary)

# ============================================================
# 问题清单 / 良好项
# ============================================================
st.markdown("---")
col_issues, col_good = st.columns(2)
with col_issues:
    st.subheader("⚠️ 需要处理")
    if diag.issues:
        for issue in diag.issues:
            st.markdown(f"- {issue}")
    else:
        st.success("没有需要立即处理的问题")
with col_good:
    st.subheader("✅ 良好项")
    if diag.highlights:
        for h in diag.highlights:
            st.markdown(f"- {h}")
    else:
        st.caption("—")

# ============================================================
# 持仓明细
# ============================================================
st.markdown("---")
st.subheader("📋 持仓明细")

rows = []
for e in diag.holdings:
    rows.append({
        "代码": e.code,
        "名称": e.name,
        "行业": e.industry,
        "现价": e.price if e.price > 0 else None,
        "占比%": round(e.weight_pct, 1),
        "浮盈%": round(e.pnl_pct, 1),
        "风险分%": round(e.risk_pct, 0) if e.verdict else None,
        "建议": {
            "hold": "持有", "reduce_half": "减仓50%",
            "reduce_all": "清仓", "stop_loss": "止损",
        }.get(e.verdict.action, "—") if e.verdict else "未评估",
    })
df = pd.DataFrame(rows)
st.dataframe(df, width="stretch", hide_index=True)

# 触发信号详情
_with_signals = [e for e in diag.holdings if e.verdict and e.verdict.signals]
if _with_signals:
    st.subheader("🔍 触发的卖出信号")
    for e in _with_signals:
        with st.expander(f"{e.name}({e.code}) — 风险分 {e.risk_pct:.0f}% · {e.verdict.risk_level}"):
            for sig in e.verdict.signals:
                st.markdown(f"- **{sig['label']}**（优先级 P{sig['priority']}，权重 {sig['weight']}）")
            st.caption(e.verdict.advice)

st.markdown("---")
st.caption("💡 体检结果只是数据工具输出，不构成投资建议。综合健康分权重："
           "卖出信号 40% · 集中度 25% · 盈亏结构 20% · 大盘环境 15%。")
