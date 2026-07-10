"""
pages/1_估值分析.py — 单股深度分析（合并估值 + FCF）

合并自原 1_估值分析 + 11_FCF分析。
sidebar 只选股一次，主区两个 Tab：
  📊 估值分析（PE/PS TTM、目标价、四格图、历史分位）
  💰 FCF 分析（自由现金流评分卡 + 趋势图）
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.web.utils import setup_matplotlib_chinese  # noqa: E402

setup_matplotlib_chinese()

import pandas as pd  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402

from src.web.components.search import searchable_select  # noqa: E402
from src.web.components.state import load_result, save_result, stale_warning  # noqa: E402
from src.web.utils import (  # noqa: E402
    MARKET_LABELS,
    list_stocks_from_market_config,
    quick_add_stock_widget,
)

st.title("🎯 个股中心")
st.caption("一次选股 → 估值 · 基本面 · 回测 · 财报 · 资讯 · 买卖信号 · 预警 全视角")

# 注入全局 CSS 样式实现高级毛玻璃与暗黑卡片效果
st.markdown("""
    <style>
    .metric-card-glass {
        background: rgba(30, 41, 59, 0.45);
        border-radius: 12px;
        padding: 16px 20px;
        border: 1px solid rgba(255, 255, 255, 0.08);
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.2);
        backdrop-filter: blur(8px);
        -webkit-backdrop-filter: blur(8px);
        margin-bottom: 15px;
    }
    .metric-card-title {
        color: #94a3b8;
        font-size: 14px;
        font-weight: 500;
        margin-bottom: 6px;
    }
    .metric-card-value {
        color: #f8fafc;
        font-size: 26px;
        font-weight: 700;
    }
    .badge-green {
        background: linear-gradient(135deg, #10b981 0%, #059669 100%);
        color: white;
        padding: 4px 10px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: bold;
        display: inline-block;
    }
    .badge-yellow {
        background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%);
        color: white;
        padding: 4px 10px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: bold;
        display: inline-block;
    }
    .badge-red {
        background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
        color: white;
        padding: 4px 10px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: bold;
        display: inline-block;
    }
    </style>
""", unsafe_allow_html=True)


# ============================================================================
# Sidebar：选股（estate + fcf 共用）
# ============================================================================

# 全局焦点股票（Pri 3 准备）：从 session_state['focus_stock'] 优先读
_focus = st.session_state.get("focus_stock") or {}

market_default = _focus.get("market") or "a"
market = st.sidebar.selectbox(
    "市场",
    options=list(MARKET_LABELS.keys()),
    format_func=lambda k: MARKET_LABELS[k],
    index=list(MARKET_LABELS.keys()).index(market_default)
        if market_default in MARKET_LABELS else 0,
)

stocks = list_stocks_from_market_config(market)

with st.sidebar:
    st.markdown("---")
    st.markdown("**🔍 股票选择**")
    default_code, default_name = "", ""
    default_val, default_range = "pe", [10, 20, 30]
    selected = None

    # FE-1 修复：添加明确的输入模式切换，避免手动输入被列表覆盖
    input_mode_options = ["📋 从关注列表选择", "✏️ 手动输入代码"]
    if not stocks:
        input_mode_options = ["✏️ 手动输入代码"]  # 无关注列表时只能手动
    # 首页「直达」带来的焦点代码若不在关注列表 → 默认落到手动模式（否则会被列表首项覆盖）
    _focus_code = str(_focus.get("code", "")).strip()
    _focus_in_list = any(str(s.get("code", "")).strip() == _focus_code for s in stocks)
    _default_mode_idx = (
        len(input_mode_options) - 1
        if (_focus_code and not _focus_in_list) else 0
    )
    input_mode = st.radio(
        "选股方式", options=input_mode_options,
        index=_default_mode_idx,
        horizontal=True, key="page1_input_mode",
        label_visibility="collapsed",
    )
    use_watchlist = input_mode == "📋 从关注列表选择" and stocks

    # 名称自动解析（关注列表 → resolver → 全市场 spot）
    # 注意：st.cache_data 会忽略下划线开头的参数（不参与缓存键），
    # 参数名绝不能写成 _code/_market，否则所有代码共享同一条缓存！
    @st.cache_data(ttl=600, show_spinner=False)
    def _resolve_name(query_code: str, query_market: str) -> str:
        from src.services import stock_service as ssvc
        return ssvc.resolve_name(query_code, query_market)

    # 两种模式彻底分离：代码只有一个来源，杜绝"输入被列表选中项顶回"的 bug
    if use_watchlist:
        # 列表模式：代码/名称直接取自选中项，不再提供可编辑输入框
        selected_code = searchable_select(
            "从关注列表选择",
            options=stocks,
            key="page1_stock",
            id_field="code",
            name_field="name",
        )
        if selected_code:
            selected = next((s for s in stocks if s["code"] == selected_code), stocks[0])
        else:
            selected = stocks[0]
        code = str(selected["code"]).strip()
        name = str(selected.get("name", "")).strip()
        default_val = selected.get("valuation", "pe")
        default_range = selected.get(f"{default_val}_range", [10, 20, 30])
        st.caption(f"✅ 当前：**{name}**（{code}）· 想输任意代码请切「✏️ 手动输入代码」")
    else:
        # 手动模式：输入框带稳定 key；焦点股票只做首次预填
        _manual_default = ""
        if _focus.get("code") and _focus.get("market") == market:
            _manual_default = str(_focus["code"])
        code = st.text_input(
            "股票代码", value=_manual_default,
            key="page1_code_manual",
            placeholder="600519 / 00700 / AAPL",
        ).strip()

        name = ""
        if code:
            name = _resolve_name(code, market)
            if name:
                st.caption(f"✅ 已识别：**{name}**")
            else:
                name = st.text_input("名称未识别，请手动输入",
                                     key="page1_name_manual").strip()

    # 同步焦点股票（让其他页继承）
    if code.strip():
        st.session_state["focus_stock"] = {
            "code": code.strip(), "name": name.strip(), "market": market,
        }

    # FE-2 修复：分离为两个按钮，避免一键同时拉取两个 Tab 的数据
    btn_c1, btn_c2 = st.columns(2)
    with btn_c1:
        run_btn_val = st.button("📊 估值分析", type="primary", use_container_width=True)
    with btn_c2:
        run_btn_fcf = st.button("💰 FCF 分析", type="secondary", use_container_width=True)

    # ───── 估值专属：估值方式 + 档位 ─────
    with st.expander("📊 估值参数", expanded=False):
        val_type = st.radio("估值方式", options=["pe", "ps"],
                            index=0 if default_val == "pe" else 1, horizontal=True)
        st.caption("目标价档位（低 / 合理 / 高）")
        c1, c2, c3 = st.columns(3)
        with c1:
            r_low = st.number_input("低", value=float(default_range[0]), step=0.5)
        with c2:
            r_mid = st.number_input("中", value=float(default_range[1]), step=0.5)
        with c3:
            r_high = st.number_input("高", value=float(default_range[2]), step=0.5)

    # ───── FCF 专属：年度/季度 ─────
    with st.expander("💰 FCF 参数（仅 FCF Tab 用）"):
        period = st.radio("时间区间", options=["年度", "季度"], index=0)
        is_annual = (period == "年度")

    # ───── 加入关注列表 ─────
    st.markdown("---")
    if quick_add_stock_widget(
        key_prefix="page1_sidebar",
        default_market=market,
        default_code=code,
        default_name=name,
        default_valuation=val_type,
        default_range=[r_low, r_mid, r_high],
    ):
        st.rerun()


# ============================================================================
# 顶部状态条
# ============================================================================

if code and name:
    st.info(f"📌 当前分析：**{name}**（{code}） · 市场 {MARKET_LABELS[market]}")
elif code:
    st.info(f"📌 当前分析：**{code}** · 市场 {MARKET_LABELS[market]}")
else:
    st.warning("👈 请在侧边栏选股或手动输入代码")


# ============================================================================
# Tab：估值分析 / FCF 分析
# ============================================================================

# 4 个主 Tab；相关内容合并（同一 Tab 变量可多次 with，内容顺序拼接）
tab_main, tab_bt, tab_sig_alert, tab_info = st.tabs([
    "📊 估值与基本面", "🧪 回测", "🚦 信号与预警", "📅 财报与资讯",
])
tab_val = tab_main        # 估值段
tab_fcf = tab_main        # FCF 段（紧随其后）
tab_sig = tab_sig_alert   # 买卖信号段
tab_alert = tab_sig_alert # 预警段
tab_earn = tab_info       # 财报段
tab_news = tab_info       # 资讯段


# ─────────────────────────────────────────────────────────────────
# Tab 1：估值分析
# ─────────────────────────────────────────────────────────────────
with tab_val:

    @st.cache_data(ttl=300, show_spinner=False)
    def _run_valuation_pipeline(market: str, stock_config: dict):
        from src.services import valuation_service as vsvc
        run = vsvc.run_valuation(market, stock_config)
        return run.result, run.fin_df, run.hist_val_df, run.market_data

    if run_btn_val:
        if not code or not name:
            st.error("请填入股票代码和名称")
            st.stop()

        from src.services import valuation_service as _vsvc
        stock_config = _vsvc.build_stock_config(
            code, name, market, val_type, [r_low, r_mid, r_high],
        )
        with st.spinner(f"正在拉取 {name} ({code}) 估值数据..."):
            try:
                result, fin_df, hist_val_df, market_data = _run_valuation_pipeline(
                    market, stock_config,
                )
                params = {"code": code.strip(), "valuation": val_type,
                          "range": [r_low, r_mid, r_high]}
                save_result("valuation", code.strip(), market, params,
                            (result, fin_df, hist_val_df, market_data, stock_config))
            except Exception as e:
                st.error(f"数据拉取或分析异常: {e}")
                st.exception(e)
                st.stop()

    params = {"code": code.strip(), "valuation": val_type,
              "range": [r_low, r_mid, r_high]}
    stale_warning("valuation", code.strip())

    _val_data = load_result("valuation", code.strip(), market, params)
    if _val_data:
        result, fin_df, hist_val_df, market_data, stock_config = _val_data
        if not result:
            st.warning("分析结果为空，可能数据缺失或 price=0。请检查代码是否正确。")
        else:
            _name = stock_config.get("name", code)
            _code = stock_config.get("code", code)
            st.toast(f"✅ 估值分析完成：{_name} ({_code})", icon="📊")

            # 渲染核心指标高级卡片
            m1, m2, m3, m4 = st.columns(4)
            with m1:
                st.markdown(f"""
                    <div class="metric-card-glass">
                        <div class="metric-card-title">当前价</div>
                        <div class="metric-card-value">{result.get('price', 0):.2f} 元</div>
                    </div>
                """, unsafe_allow_html=True)
            with m2:
                pe_ttm = result.get("current_pe")
                pe_val = f"{pe_ttm:.2f}" if pe_ttm and pe_ttm > 0 else "N/A"
                st.markdown(f"""
                    <div class="metric-card-glass">
                        <div class="metric-card-title">PE (TTM)</div>
                        <div class="metric-card-value">{pe_val}</div>
                    </div>
                """, unsafe_allow_html=True)
            with m3:
                ps_ttm = result.get("current_ps")
                ps_val = f"{ps_ttm:.2f}" if ps_ttm and ps_ttm > 0 else "N/A"
                st.markdown(f"""
                    <div class="metric-card-glass">
                        <div class="metric-card-title">PS (TTM)</div>
                        <div class="metric-card-value">{ps_val}</div>
                    </div>
                """, unsafe_allow_html=True)
            with m4:
                cap_val = f"{market_data.get('market_cap', 0) / 1e8:.2f}" if market_data.get("market_cap") else "N/A"
                st.markdown(f"""
                    <div class="metric-card-glass">
                        <div class="metric-card-title">总市值</div>
                        <div class="metric-card-value">{cap_val} 亿</div>
                    </div>
                """, unsafe_allow_html=True)

            # 历史分位数发光徽章卡片
            from src.services import valuation_service as _vsvc2
            hist_pct = result.get('hist_percentile', 0)
            pct_label, _pct_level = _vsvc2.percentile_badge(hist_pct)
            badge_class = {"low": "badge-green", "mid": "badge-yellow",
                           "high": "badge-red"}[_pct_level]

            st.markdown(f"""
                <div class="metric-card-glass">
                    <span style="color:#94a3b8; font-size:14px; font-weight:500;">历史估值分位数：</span>
                    <span style="color:#f8fafc; font-size:22px; font-weight:700; margin-right: 15px;">{hist_pct:.2f}%</span>
                    <span class="{badge_class}">{pct_label}</span>
                </div>
            """, unsafe_allow_html=True)

            # 目标价档位
            st.subheader("🎯 目标价档位")
            tp_df = pd.DataFrame(_vsvc2.target_price_rows(result))
            st.dataframe(tp_df, width="stretch", hide_index=True)

            # 渲染 Plotly 交互式图表
            st.subheader("📊 估值与财务分析图谱 (交互式)")
            try:
                from src.core.visualizer import Visualizer
                viz = Visualizer(result, stock_config)

                c_left, c_right = st.columns(2)
                with c_left:
                    st.plotly_chart(viz.plot_revenue_profit_plotly(), use_container_width=True)
                with c_right:
                    st.plotly_chart(viz.plot_hist_valuation_plotly(), use_container_width=True)

                st.plotly_chart(viz.plot_scenario_plotly(), use_container_width=True)
            except Exception as e:
                st.error(f"图表渲染失败: {e}")
                st.exception(e)

            # 核心指标总览数据表
            st.subheader("📋 核心指标综合总览")
            summary_data = _vsvc2.summary_rows(result, fin_df, val_type)
            st.dataframe(pd.DataFrame(summary_data), width="stretch", hide_index=True)

            # 数据原表整合进 Tab
            st.subheader("📂 原始数据总览")
            raw_tab_fin, raw_tab_hist, raw_tab_real = st.tabs(["📋 财务数据原表", "📉 历史估值原表", "💹 实时行情 Dict"])
            with raw_tab_fin:
                if fin_df is not None and not fin_df.empty:
                    st.dataframe(fin_df, width="stretch")
                else:
                    st.caption("暂无财务数据")
            with raw_tab_hist:
                if hist_val_df is not None and not hist_val_df.empty:
                    st.dataframe(hist_val_df.tail(100), width="stretch")
                else:
                    st.caption("暂无历史估值数据")
            with raw_tab_real:
                st.json(market_data)
    else:
        st.info("👈 在左侧配置后点击「📊 估值分析」生成估值报告")


# ─────────────────────────────────────────────────────────────────
# Tab 2：FCF 分析
# ─────────────────────────────────────────────────────────────────
with tab_fcf:
    st.markdown("---")
    st.subheader("💰 FCF 基本面")

    @st.cache_data(ttl=300, show_spinner=False)
    def _cached_fcf_run(market: str, code: str, is_annual: bool):
        from src.services import valuation_service as vsvc
        run = vsvc.run_fcf(market, code, is_annual=is_annual)
        return run.analyzed_df, run.score_res

    fcf_params = {"code": code.strip(), "market": market, "period": period}
    _fcf_state = load_result("fcf", code.strip(), market, fcf_params)

    if run_btn_fcf:
        if not code:
            st.warning("请输入股票代码")
        else:
            with st.spinner(f"正在拉取 {name} ({code}) 的 {period} 财务数据..."):
                try:
                    analyzed_df, score_res = _cached_fcf_run(market, code, is_annual)
                    if analyzed_df.empty:
                        st.warning("FCF 财务数据为空 - 检查代码或数据源")
                    else:
                        save_result("fcf", code.strip(), market, fcf_params, {
                            "analyzed_df": analyzed_df,
                            "score_res": score_res,
                            "name": name, "code": code,
                            "is_annual": is_annual,
                        })
                        _fcf_state = load_result("fcf", code.strip(), market, fcf_params)
                except Exception as e:
                    st.error(f"FCF 分析失败: {e}")

    stale_warning("fcf", code.strip())

    if _fcf_state:
        analyzed_df = _fcf_state["analyzed_df"]
        score_res = _fcf_state["score_res"]
        _name = _fcf_state.get("name", name)
        _code = _fcf_state.get("code", code)

        if not analyzed_df.empty:
            df_plot = analyzed_df.copy()
            for col in ("operating_cash_flow", "capex", "fcf", "revenue", "net_profit"):
                df_plot[col] = df_plot[col] / 1e8
            dates = [
                d.strftime("%Y-%m-%d") if isinstance(d, pd.Timestamp) else str(d)[:10]
                for d in df_plot.index
            ]
            st.toast(f"💰 FCF 报告：{_name} ({_code})", icon="✅")

            # 评分卡
            scores = score_res["scores"]
            summary = score_res["summary"]
            st.markdown("### 🏆 综合评分")
            col1, col2 = st.columns([1, 2])
            with col1:
                color = "#28a745" if scores["total"] >= 60 else "#dc3545"
                st.markdown(
                    f"<h1 style='text-align: center; color: {color}; font-size: 4rem;'>"
                    f"{scores['total']} <span style='font-size: 1.5rem; color: gray;'>"
                    f"/ 100</span></h1>", unsafe_allow_html=True,
                )
                st.markdown(
                    f"<h3 style='text-align: center;'>评级：{summary['rating']}</h3>",
                    unsafe_allow_html=True,
                )
            with col2:
                st.markdown(f"**💡 判断：** {summary['judgement']}")
                st.markdown(f"**⚠️ 主要风险：** {summary['main_risk']}")
                st.progress(scores["absolute"] / 20,
                            text=f"FCF 绝对水平 {scores['absolute']}/20")
                st.progress(scores["quality"] / 20,
                            text=f"FCF vs 净利润 {scores['quality']}/20")
                st.progress(scores["margin"] / 20,
                            text=f"FCF 利润率 {scores['margin']}/20")
                st.progress(scores["growth"] / 20,
                            text=f"FCF 增长 {scores['growth']}/20")
                st.progress(scores["yield"] / 20,
                            text=f"FCF Yield {scores['yield']}/20")

            st.markdown("---")
            st.markdown("### 📊 详细可视化")

            cc1, cc2 = st.columns(2)
            with cc1:
                st.markdown("#### FCF 趋势 (亿元)")
                fig1 = go.Figure()
                fig1.add_trace(go.Scatter(x=dates, y=df_plot["operating_cash_flow"],
                                          mode="lines+markers", name="经营现金流",
                                          line={"color": "#1f77b4", "width": 2}))
                fig1.add_trace(go.Scatter(x=dates, y=-df_plot["capex"],
                                          mode="lines+markers", name="资本支出(-)",
                                          line={"color": "#ff7f0e", "width": 2,
                                                "dash": "dash"}))
                fcf_colors = ["#2ca02c" if v >= 0 else "#d62728" for v in df_plot["fcf"]]
                fig1.add_trace(go.Bar(x=dates, y=df_plot["fcf"],
                                      name="FCF", marker_color=fcf_colors, opacity=0.8))
                fig1.update_layout(hovermode="x unified",
                                   margin={"t": 20, "b": 20, "l": 20, "r": 20})
                st.plotly_chart(fig1, width="stretch")

            with cc2:
                st.markdown("#### FCF vs 净利润")
                fig2 = go.Figure()
                fig2.add_trace(go.Bar(x=dates, y=df_plot["net_profit"],
                                      name="净利润", marker_color="#1f77b4"))
                npc = ["#2ca02c" if fcf > np else ("#ff7f0e" if fcf > 0 else "#d62728")
                       for fcf, np in zip(df_plot["fcf"], df_plot["net_profit"])]
                fig2.add_trace(go.Bar(x=dates, y=df_plot["fcf"],
                                      name="FCF", marker_color=npc))
                fig2.update_layout(barmode="group", hovermode="x unified",
                                   margin={"t": 20, "b": 20, "l": 20, "r": 20})
                st.plotly_chart(fig2, width="stretch")

            cc3, cc4 = st.columns(2)
            with cc3:
                st.markdown("#### FCF 利润率 (%)")
                fig3 = go.Figure()
                fig3.add_trace(go.Scatter(x=dates, y=df_plot["fcf_margin"],
                                          mode="lines+markers", name="FCF利润率",
                                          line={"color": "#9467bd", "width": 3}))
                fig3.add_hline(y=20, line_dash="dash", line_color="green",
                               annotation_text="20% (优秀)")
                fig3.add_hline(y=10, line_dash="dash", line_color="orange",
                               annotation_text="10% (一般)")
                fig3.add_hline(y=5, line_dash="dash", line_color="red",
                               annotation_text="5% (差)")
                fig3.update_layout(hovermode="x unified",
                                   margin={"t": 20, "b": 20, "l": 20, "r": 20})
                st.plotly_chart(fig3, width="stretch")

            with cc4:
                st.markdown("#### 当前 FCF Yield")
                curr_yield = summary["current_fcf_yield"]
                fig4 = go.Figure(go.Indicator(
                    mode="gauge+number", value=curr_yield,
                    title={"text": "FCF / 总市值 (%)", "font": {"size": 16}},
                    gauge={
                        "axis": {"range": [min(-5, curr_yield - 2),
                                           max(10, curr_yield + 2)]},
                        "bar": {"color": "black"},
                        "steps": [
                            {"range": [-100, 1], "color": "#ff4d4d"},
                            {"range": [1, 3], "color": "#ffa64d"},
                            {"range": [3, 5], "color": "#ffff66"},
                            {"range": [5, 100], "color": "#66ff66"},
                        ],
                    }))
                fig4.update_layout(margin={"t": 50, "b": 20, "l": 20, "r": 20})
                st.plotly_chart(fig4, width="stretch")

            with st.expander("📋 原始 FCF 数据"):
                st.dataframe(analyzed_df, width="stretch")
        else:
            st.info("FCF 分析结果为空")
    else:
        st.info("👈 在左侧配置后点击「💰 FCF 分析」生成 FCF 报告")


# ─────────────────────────────────────────────────────────────────
# Tab 3：策略回测（多策略一键对比，精细调参去「策略回测」页）
# ─────────────────────────────────────────────────────────────────
with tab_bt:
    if not code.strip():
        st.info("👈 请先在侧边栏选股")
    else:
        bt_c1, bt_c2 = st.columns([1, 3])
        with bt_c1:
            hub_bt_days = st.number_input("回测天数", 200, 5000, 1000, 100,
                                          key="hub_bt_days")
        with bt_c2:
            st.caption("跑全部已注册策略并对比 KPI。需要调参数/换策略请到「🧪 策略回测」页。")

        if st.button("▶️ 一键回测全部策略", type="primary", key="hub_bt_run"):
            from src.services import backtest_service as btsvc
            with st.spinner(f"正在回测 {name or code} ..."):
                try:
                    cmp_result, bt_df = btsvc.run_compare(
                        code.strip(), market, int(hub_bt_days))
                except Exception as e:
                    st.error(f"回测失败: {e}")
                    cmp_result = None
            if cmp_result is None:
                st.error("未能获取数据，请检查代码/网络")
            else:
                st.session_state["hub_bt_result"] = cmp_result

        _hub_cmp = st.session_state.get("hub_bt_result")
        if _hub_cmp is not None:
            from src.services import backtest_service as btsvc
            best = btsvc.best_of(_hub_cmp)
            if best is not None:
                b1, b2, b3 = st.columns(3)
                b1.metric("🏆 最佳策略", best.label)
                b2.metric("总收益", f"{(best.report or {}).get('总收益率(%)', 0):+.2f}%")
                b3.metric("夏普", f"{(best.report or {}).get('夏普比率', 0):.2f}")
            _parts = []
            for _cat in _hub_cmp.by_category():
                _d = _hub_cmp.summary_df_by_category(_cat)
                if not _d.empty:
                    _parts.append(_d)
            if _parts:
                st.dataframe(pd.concat(_parts), width="stretch", hide_index=True)
            eq = _hub_cmp.equity_curves_df()
            if eq is not None and not eq.empty:
                fig_eq = go.Figure()
                for col in eq.columns:
                    fig_eq.add_trace(go.Scatter(
                        x=eq.index, y=eq[col], mode="lines", name=col,
                        line={"width": 2 if col == "Buy & Hold" else 1.3,
                              "dash": "dash" if col == "Buy & Hold" else "solid"}))
                fig_eq.update_layout(height=420, hovermode="x unified",
                                     legend={"orientation": "h", "y": 1.05})
                st.plotly_chart(fig_eq, width="stretch")


# ─────────────────────────────────────────────────────────────────
# Tab 4：财报披露
# ─────────────────────────────────────────────────────────────────
with tab_earn:
    if not code.strip():
        st.info("👈 请先在侧边栏选股")
    else:
        if st.button("📅 查询披露计划", type="primary", key="hub_earn_run"):
            from src.services import stock_service as ssvc
            with st.spinner("查询未来 90 天披露计划..."):
                st.session_state["hub_earn"] = (
                    code.strip(),
                    ssvc.earnings_for_code(code.strip(), market, days_ahead=90),
                )
        _earn_state = st.session_state.get("hub_earn")
        if _earn_state and _earn_state[0] == code.strip():
            earn_df = _earn_state[1]
            if earn_df.empty:
                st.info("未来 90 天暂无该股披露计划（或数据源无记录）")
            else:
                st.dataframe(earn_df, width="stretch", hide_index=True)
        else:
            st.caption("查询该股未来 90 天的财报披露计划与业绩预告。全市场日历见「📅 财报披露」页。")


# ─────────────────────────────────────────────────────────────────
# Tab 5：资讯（个股新闻 + 公告）
# ─────────────────────────────────────────────────────────────────
with tab_news:
    st.markdown("---")
    st.subheader("📰 个股资讯")
    if not code.strip():
        st.caption("👈 请先在侧边栏选股")
    elif market != "a":
        st.info("个股资讯目前仅支持 A 股（数据源限制）")
    else:
        if st.button("📰 拉取最新资讯", type="primary", key="hub_news_run"):
            from src.services import stock_service as ssvc
            with st.spinner("拉取新闻与公告..."):
                st.session_state["hub_news"] = (
                    code.strip(),
                    ssvc.news_for_code(code.strip()),
                    ssvc.announcements_for_code(code.strip()),
                )
        _news_state = st.session_state.get("hub_news")
        if _news_state and _news_state[0] == code.strip():
            _, news_df, ann_df = _news_state
            nc1, nc2 = st.columns(2)
            with nc1:
                st.markdown("##### 📰 个股新闻")
                if news_df.empty:
                    st.caption("暂无新闻")
                else:
                    for _, row in news_df.head(15).iterrows():
                        _t = str(row.get("time", ""))[:16]
                        st.markdown(
                            f"- [{row.get('title', '')}]({row.get('url', '')})  \n"
                            f"  <small>{_t} · {row.get('source', '')}</small>",
                            unsafe_allow_html=True)
            with nc2:
                st.markdown("##### 📄 公司公告")
                if ann_df.empty:
                    st.caption("今日暂无公告")
                else:
                    st.dataframe(ann_df, width="stretch", hide_index=True, height=420)
        else:
            st.caption("拉取该股最新新闻与公告。批量抓取（含研报/持仓）见「🌐 资讯抓取」页。")


# ─────────────────────────────────────────────────────────────────
# Tab 6：买卖信号（买点条件扫描 + 卖出引擎判定）
# ─────────────────────────────────────────────────────────────────
with tab_sig:
    if not code.strip():
        st.info("👈 请先在侧边栏选股")
    else:
        if st.button("🚦 扫描买卖信号", type="primary", key="hub_sig_run"):
            from src.services import stock_service as ssvc
            with st.spinner("扫描买点条件 + 卖出引擎评估..."):
                st.session_state["hub_sig"] = (
                    code.strip(),
                    ssvc.scan_buy_signals(code.strip(), market),
                    ssvc.sell_verdict_for_code(code.strip(), name, market),
                )
        _sig_state = st.session_state.get("hub_sig")
        if _sig_state and _sig_state[0] == code.strip():
            _, buy_hits, evaluation = _sig_state
            sig_c1, sig_c2 = st.columns(2)
            with sig_c1:
                st.markdown("##### 🟢 买点条件")
                if not buy_hits:
                    st.caption("数据不足，无法评估")
                else:
                    n_hit = sum(1 for h in buy_hits if h.hit)
                    st.metric("命中 / 总条件", f"{n_hit} / {len(buy_hits)}")
                    for h in buy_hits:
                        icon = "✅" if h.hit else "▫️"
                        st.markdown(f"{icon} **{h.label}** `{h.period}`")
            with sig_c2:
                st.markdown("##### 🔴 卖出引擎判定")
                if evaluation is None:
                    st.caption("无法获取行情/K 线，无法评估")
                else:
                    v = evaluation.verdict
                    st.metric("综合风险分", f"{v.risk_pct:.0f}%", v.risk_level)
                    st.markdown(v.advice)
                    if v.signals:
                        for s in v.signals:
                            _pri = "🔴" if s["priority"] == 1 else "🟠" if s["priority"] == 2 else "🟡"
                            st.markdown(f"{_pri} **{s['label']}**（权重 {s['weight']}）")
                    else:
                        st.caption("无卖出信号命中")
            st.caption("💡 持仓的止损/止盈（L1）判定需在「💼 持仓监控」录入成本后查看。")
        else:
            st.caption("对该股跑一遍买点条件清单（MACD 背离/金叉/超卖/突破…）+ 卖出引擎 L2 信号。")


# ─────────────────────────────────────────────────────────────────
# Tab 7：价格预警（该股规则 + 快速添加）
# ─────────────────────────────────────────────────────────────────
with tab_alert:
    st.markdown("---")
    st.subheader("🔔 价格预警")
    if not code.strip():
        st.caption("👈 请先在侧边栏选股")
    else:
        from src.services import stock_service as ssvc
        buy_rules, sell_rules = ssvc.alert_rules_for_code(code.strip())

        st.markdown(f"##### 该股已配置规则（买入 {len(buy_rules)} / 卖出 {len(sell_rules)}）")
        for _direction, _rules in (("买入", buy_rules), ("卖出", sell_rules)):
            for r in _rules:
                _sig = (r.get("signal") or {})
                _enabled = "✅" if r.get("enabled", True) else "⏸"
                st.markdown(
                    f"- {_enabled} **[{_direction}] {r.get('name', r.get('id'))}** — "
                    f"`{_sig.get('type', '?')}` · 冷却 {r.get('cooldown_hours', 24)}h")
        if not buy_rules and not sell_rules:
            st.caption("暂无规则。下方快速添加，或到「🔔 价格预警」页做完整配置。")

        st.markdown("---")
        st.markdown("##### ⚡ 快速添加监控")
        qa_c1, qa_c2, qa_c3 = st.columns([1, 2, 1])
        with qa_c1:
            qa_dir = st.selectbox("方向", ["buy", "sell"],
                                  format_func=lambda d: "🟢 买点" if d == "buy" else "🔴 卖点",
                                  key="hub_qa_dir")
        with qa_c2:
            from src.analysis.screening.conditions import CONDITION_LABELS as _CL2
            _buy_sigs = ["weekly_macd_divergence", "daily_macd_divergence", "rsi_oversold",
                         "ma_gold_cross", "kdj_gold_cross", "box_breakout"]
            _sell_sigs = ["weekly_macd_top_divergence", "daily_macd_top_divergence",
                          "rsi_overbought", "ma_death_cross", "kdj_death_cross",
                          "break_below_ma"]
            qa_sig = st.selectbox(
                "信号", _buy_sigs if qa_dir == "buy" else _sell_sigs,
                format_func=lambda s: _CL2.get(s, s), key="hub_qa_sig")
        with qa_c3:
            st.markdown("&nbsp;")
            if st.button("➕ 添加", type="primary", key="hub_qa_add"):
                ok, msg = ssvc.add_price_alert_rule(
                    code.strip(),
                    f"{name or code} {_CL2.get(qa_sig, qa_sig)}",
                    qa_dir, qa_sig)
                (st.success if ok else st.error)(msg)
                if ok:
                    st.rerun()
        st.caption("添加后由调度器按 config/scheduler.yaml 周期自动扫描并推送。")
