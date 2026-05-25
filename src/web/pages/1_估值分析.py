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

import matplotlib.pyplot as plt  # noqa: E402
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

st.set_page_config(page_title="单股分析", page_icon="📊", layout="wide")
st.title("📊 单股深度分析")
st.caption("一次选股 → 估值 + 自由现金流 双视角")

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

    if stocks:
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
        default_code = selected["code"]
        default_name = selected["name"]
        default_val = selected.get("valuation", "pe")
        default_range = selected.get(f"{default_val}_range", [10, 20, 30])

    # focus_stock 来自其他页跳转时覆盖
    if _focus.get("code") and _focus.get("market") == market:
        default_code = _focus["code"]
        default_name = _focus.get("name", default_name)

    # 仅当用户未在选择列表里选过股时显示"手动输入"
    if not stocks or not selected_code:
        st.caption("或手动输入：")
        default_code, default_name, default_val, default_range = (
            "", "", "pe", [10, 20, 30]
        )

    code = st.text_input("股票代码", value=default_code)
    name = st.text_input("股票名称", value=default_name)

    # 同步焦点股票（让其他页继承）
    if code.strip():
        st.session_state["focus_stock"] = {
            "code": code.strip(), "name": name.strip(), "market": market,
        }

    run_btn = st.button("▶️ 开始分析", type="primary", width="stretch")

    # ───── 估值专属：估值方式 + 档位 ─────
    with st.expander("📊 估值参数（仅估值 Tab 用）", expanded=True):
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

tab_val, tab_fcf = st.tabs(["📊 估值分析", "💰 FCF 自由现金流"])


# ─────────────────────────────────────────────────────────────────
# Tab 1：估值分析
# ─────────────────────────────────────────────────────────────────
with tab_val:

    @st.cache_data(ttl=300, show_spinner=False)
    def _run_valuation_pipeline(market: str, stock_config: dict):
        from src.core.analyzer import (
            AStockAnalyzer,
            HKStockAnalyzer,
            USStockAnalyzer,
        )
        from src.core.data_fetcher import (
            AStockDataFetcher,
            HKStockDataFetcher,
            USStockDataFetcher,
        )
        mapping = {
            "a": (AStockDataFetcher, AStockAnalyzer),
            "hk": (HKStockDataFetcher, HKStockAnalyzer),
            "us": (USStockDataFetcher, USStockAnalyzer),
        }
        FetcherCls, AnalyzerCls = mapping[market]
        with FetcherCls() as fetcher:
            fin_df = fetcher.get_financial_abstract(stock_config["code"])
            v_type = stock_config.get("valuation", "pe")
            hist_val_df = fetcher.get_historical_valuation(stock_config["code"], v_type)
            market_data = fetcher.get_current_market_data(stock_config["code"])
        analyzer = AnalyzerCls(fin_df, hist_val_df, market_data, stock_config)
        result = analyzer.process()
        return result, fin_df, hist_val_df, market_data

    if run_btn:
        if not code or not name:
            st.error("请填入股票代码和名称")
            st.stop()

        stock_config = {
            "code": code.strip(), "name": name.strip(),
            "valuation": val_type,
            f"{val_type}_range": [r_low, r_mid, r_high],
            "market_name": MARKET_LABELS[market],
            "category_name": "自定义",
        }
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
            hist_pct = result.get('hist_percentile', 0)
            if hist_pct < 20:
                badge_class = "badge-green"
                pct_label = "极度低估"
            elif hist_pct < 50:
                badge_class = "badge-green"
                pct_label = "合理偏低"
            elif hist_pct < 80:
                badge_class = "badge-yellow"
                pct_label = "合理偏高"
            else:
                badge_class = "badge-red"
                pct_label = "极度高估"

            st.markdown(f"""
                <div class="metric-card-glass">
                    <span style="color:#94a3b8; font-size:14px; font-weight:500;">历史估值分位数：</span>
                    <span style="color:#f8fafc; font-size:22px; font-weight:700; margin-right: 15px;">{hist_pct:.2f}%</span>
                    <span class="{badge_class}">{pct_label}</span>
                </div>
            """, unsafe_allow_html=True)

            # 目标价档位计算 (修复原本 result 没有 target_prices 的 bug)
            st.subheader("🎯 目标价档位")
            scenarios = result.get("scenarios", [0, 0, 0])
            target_prices = {
                "保守": scenarios[0],
                "中性": scenarios[1],
                "乐观": scenarios[2],
            }
            tp_df = pd.DataFrame([
                {"档位": level, "目标价": round(price, 2),
                 "相对当前": f"{(price / result['price'] - 1) * 100:+.1f}%"
                            if result.get("price") else ""}
                for level, price in target_prices.items()
            ])
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

            # 核心指标总览数据表 (替换原来的 matplotlib 表格)
            st.subheader("📋 核心指标综合总览")
            latest_year = str(fin_df.index[0].year) if fin_df is not None and not fin_df.empty else '-'
            rev_val = result.get('ttm_revenue', 0) / 1e8
            np_val = result.get('ttm_net_profit', 0) / 1e8
            gm_val = 0.0
            if fin_df is not None and not fin_df.empty and '营业总收入' in fin_df.columns and '营业成本' in fin_df.columns:
                latest_annual = fin_df.iloc[0]
                rev_annual = latest_annual.get('营业总收入', 0)
                cost_annual = latest_annual.get('营业成本', 0)
                if rev_annual > 0:
                    gm_val = (rev_annual - cost_annual) / rev_annual * 100

            summary_data = [
                {"关键指标": "财报最新年度", "数据": latest_year, "备注解析": "最新年度数据依据"},
                {"关键指标": "营业总收入 (亿元)", "数据": f"{rev_val:.2f}", "备注解析": "年度总计 (TTM)"},
                {"关键指标": "归母净利润 (亿元)", "数据": f"{np_val:.2f}", "备注解析": "年度总计 (TTM)"},
                {"关键指标": "毛利率 (%)", "数据": f"{gm_val:.2f}%", "备注解析": "(营业收入-营业成本)/营业收入"},
                {"关键指标": "当前股价 (元)", "数据": f"{result['price']:.2f}", "备注解析": "实时动态行情"},
                {"关键指标": f"当前 {val_type.upper()} (TTM)", "数据": f"{pe_val}" if val_type == 'pe' else f"{ps_val}", "备注解析": "基于最新滚动四个季度"},
                {"关键指标": f"历史 {val_type.upper()} 分位数", "数据": f"{hist_pct:.2f}%", "备注解析": "处于过去历史排位"},
            ]
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
        st.info("👈 在左侧配置后点击「开始分析」生成估值报告")


# ─────────────────────────────────────────────────────────────────
# Tab 2：FCF 分析
# ─────────────────────────────────────────────────────────────────
with tab_fcf:

    from src.analysis.factor.fcf_analyzer import FCFAnalyzer
    from src.data.fcf_data_fetcher import FCFDataFetcher

    @st.cache_data(ttl=300, show_spinner=False)
    def _cached_fcf_fetch(market: str, code: str, is_annual: bool):
        return FCFDataFetcher.fetch(market, code, is_annual=is_annual)

    @st.cache_data(ttl=300, show_spinner=False)
    def _cached_market_cap(market: str, code: str):
        from src.core.market_registry import get_market
        try:
            spec = get_market(market)
            FetcherCls = spec.fetcher_cls()
            with FetcherCls() as fetcher:
                return fetcher.get_current_market_data(code).get("market_cap", 0.0)
        except Exception as e:
            st.warning(f"获取市值失败: {e}")
        return 0.0

    fcf_params = {"code": code.strip(), "market": market, "period": period}
    _fcf_state = load_result("fcf", code.strip(), market, fcf_params)

    if run_btn:
        if not code:
            st.warning("请输入股票代码")
        else:
            with st.spinner(f"正在拉取 {name} ({code}) 的 {period} 财务数据..."):
                try:
                    raw_df = _cached_fcf_fetch(market, code, is_annual=is_annual)
                    if raw_df.empty:
                        st.warning("FCF 财务数据为空 - 检查代码或数据源")
                    else:
                        market_cap = _cached_market_cap(market, code)
                        analyzer = FCFAnalyzer(raw_df, market_cap)
                        analyzed_df = analyzer.calculate_metrics()
                        score_res = analyzer.generate_scorecard()
                        if not analyzed_df.empty:
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
        st.info("👈 在左侧配置后点击「开始分析」生成 FCF 报告")
