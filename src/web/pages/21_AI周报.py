"""
pages/21_AI周报.py — AI 解读周报

聚合持仓体检 +（可选）最近筛选结果 → Claude 生成解读 → 导出 md/html/pdf。
未配置 ANTHROPIC_API_KEY 时降级为规则模板模式（无 AI 点评段）。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st  # noqa: E402

st.title("📰 AI 解读周报")
st.caption("持仓体检 + 筛选结果 → AI 深度解读 → 一键导出报告")

# ── 状态提示 ──
_has_key = bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())
if _has_key:
    st.success("✅ 已检测到 ANTHROPIC_API_KEY，将使用 Claude 生成 AI 解读")
else:
    st.warning("未配置 ANTHROPIC_API_KEY —— 将以**模板模式**生成（含完整数据段，无 AI 点评）。"
               "在 .env 中配置后重启即可启用 AI 解读。")

# ── 数据源选择 ──
col1, col2 = st.columns(2)
with col1:
    _cached_diag = st.session_state.get("_diag_result")
    use_cached_diag = st.checkbox(
        "复用「持仓体检」页的最近结果",
        value=_cached_diag is not None,
        disabled=_cached_diag is None,
        help="未勾选或无缓存时，将现场重新拉行情做一次完整体检（约 1-2 分钟）",
    )
with col2:
    _cached_screen = st.session_state.get("_screen_last_results")
    include_screening = st.checkbox(
        "附上「股票筛选」页的最近结果",
        value=_cached_screen is not None,
        disabled=_cached_screen is None,
    )

if st.button("📰 生成周报", type="primary"):
    from src.report import build_weekly_context, export_report, generate_report

    try:
        with st.spinner("正在聚合数据..."):
            diag_dict = None
            if use_cached_diag and _cached_diag is not None:
                diag_dict = _cached_diag.to_report_dict()
            ctx = build_weekly_context(
                diagnosis_dict=diag_dict,
                screening_df=_cached_screen if include_screening else None,
            )
        with st.spinner("正在生成报告（AI 模式约 30-60 秒）..."):
            markdown, provider_name = generate_report(ctx)
        with st.spinner("正在导出..."):
            paths = export_report(markdown)
        st.session_state["_report_md"] = markdown
        st.session_state["_report_paths"] = paths
        st.session_state["_report_provider"] = provider_name
        st.toast(f"✅ 周报已生成（{provider_name} 模式）", icon="📰")
    except Exception as e:
        st.error(f"周报生成失败: {e}")
        st.exception(e)
        st.stop()

# ── 展示与下载 ──
_md = st.session_state.get("_report_md")
if _md:
    paths = st.session_state.get("_report_paths", {})
    provider_name = st.session_state.get("_report_provider", "?")

    st.markdown("---")
    dl_cols = st.columns(4)
    with dl_cols[0]:
        st.download_button("💾 下载 Markdown", data=_md,
                           file_name=paths.get("md", Path("report.md")).name,
                           mime="text/markdown", use_container_width=True)
    with dl_cols[1]:
        html_p = paths.get("html")
        if html_p and html_p.exists():
            st.download_button("💾 下载 HTML（可打印为 PDF）",
                               data=html_p.read_text(encoding="utf-8"),
                               file_name=html_p.name, mime="text/html",
                               use_container_width=True)
    with dl_cols[2]:
        pdf_p = paths.get("pdf")
        if pdf_p and pdf_p.exists():
            st.download_button("💾 下载 PDF", data=pdf_p.read_bytes(),
                               file_name=pdf_p.name, mime="application/pdf",
                               use_container_width=True)
        else:
            st.caption("PDF：打开 HTML → 浏览器打印 → 另存为 PDF")
    with dl_cols[3]:
        st.caption(f"生成模式：`{provider_name}`\n\n文件已保存到 `output/reports/`")

    st.markdown("---")
    st.markdown(_md)
else:
    st.info("点击上方「生成周报」。建议先到「🩺 持仓体检」页跑一次体检，本页可直接复用结果。")
