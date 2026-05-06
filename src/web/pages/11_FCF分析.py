"""
pages/11_FCF分析.py — 自由现金流 (FCF) 估值分析页

功能：
  1. 下拉选择市场（A / HK / US），输入股票代码
  2. 拉取现金流及利润表数据，计算 FCF 相关指标
  3. 绘制 4 格交互式图表 (FCF趋势, FCF vs 净利润, FCF利润率, FCF Yield)
  4. 展示 100 分制综合评分卡及核心结论
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import plotly.graph_objects as go
import streamlit as st

from src.analysis.factor.fcf_analyzer import FCFAnalyzer
from src.core.data_fetcher import AStockDataFetcher, HKStockDataFetcher, USStockDataFetcher
from src.data.fcf_data_fetcher import FCFDataFetcher
from src.web.utils import (
    MARKET_LABELS,
    list_stocks_from_market_config,
)

st.set_page_config(page_title="FCF 分析", page_icon="💰", layout="wide")
st.title("💰 自由现金流 (FCF) 分析报告")
st.caption("从企业真实造血能力出发，深度评估盈利质量与投资性价比")

# ========================
# 侧边栏：输入与配置
# ========================
market = st.sidebar.selectbox(
    "市场",
    options=list(MARKET_LABELS.keys()),
    format_func=lambda k: MARKET_LABELS[k],
    index=0,
)

stocks = list_stocks_from_market_config(market)

with st.sidebar:
    st.markdown("---")
    st.markdown("**股票选择**")
    if stocks:
        options = [f"{s['name']} ({s['code']})" for s in stocks]
        idx = st.selectbox("从配置中选择", options=range(len(options)),
                           format_func=lambda i: options[i], index=0)
        selected = stocks[idx]
        default_code = selected["code"]
        default_name = selected["name"]
    else:
        default_code = "600519" if market == "a" else "0700" if market == "hk" else "AAPL"
        default_name = "贵州茅台" if market == "a" else "腾讯控股" if market == "hk" else "Apple"

    code = st.text_input("股票代码", value=default_code)
    name = st.text_input("股票名称", value=default_name)

    period = st.radio("时间区间", options=["年度", "季度"], index=0)
    is_annual = (period == "年度")

    run_btn = st.button("▶️ 开始分析", type="primary", use_container_width=True)

# ========================
# 获取市值 Helper
# ========================
def _get_market_cap(market: str, code: str) -> float:
    try:
        if market == 'a':
            with AStockDataFetcher() as fetcher:
                return fetcher.get_current_market_data(code).get("market_cap", 0.0)
        elif market == 'hk':
            with HKStockDataFetcher() as fetcher:
                return fetcher.get_current_market_data(code).get("market_cap", 0.0)
        elif market == 'us':
            with USStockDataFetcher() as fetcher:
                return fetcher.get_current_market_data(code).get("market_cap", 0.0)
    except Exception as e:
        st.warning(f"获取市值失败: {e}")
    return 0.0

# ========================
# 主逻辑
# ========================
if run_btn:
    if not code:
        st.error("请输入股票代码")
        st.stop()

    with st.spinner(f"正在拉取 {name} ({code}) 的 {period} 财务数据与市值..."):
        # 拉取 FCF 数据
        raw_df = FCFDataFetcher.fetch(market, code, is_annual=is_annual)

        if raw_df.empty:
            st.error("未获取到足够的财务数据（现金流量表或利润表为空），请检查代码或网络。")
            st.stop()

        # 拉取最新市值
        market_cap = _get_market_cap(market, code)

        # 实例化 Analyzer
        analyzer = FCFAnalyzer(raw_df, market_cap)
        analyzed_df = analyzer.calculate_metrics()
        score_res = analyzer.generate_scorecard()

    if analyzed_df.empty:
        st.error("指标计算失败。")
        st.stop()

    # --------- 格式化单位：亿元 ---------
    # 注意：yfinance 返回美股或港股可能不是人民币，展示时保持原币种，但通常数量级很大，统一除以 1亿 方便展示
    df_plot = analyzed_df.copy()
    for col in ['operating_cash_flow', 'capex', 'fcf', 'revenue', 'net_profit']:
        df_plot[col] = df_plot[col] / 1e8

    dates = [d.strftime("%Y-%m-%d") if isinstance(d, pd.Timestamp) else str(d)[:10] for d in df_plot.index]

    st.success(f"✅ 【{name} ({code})】FCF 分析报告已生成（{dates[0]} 至 {dates[-1]}）")

    # ========================
    # 核心结论与评分卡
    # ========================
    scores = score_res["scores"]
    summary = score_res["summary"]

    st.markdown("### 🏆 核心结论与综合评分")

    # 用列展示评分卡总览
    col_score1, col_score2 = st.columns([1, 2])

    with col_score1:
        st.markdown(f"<h1 style='text-align: center; color: {'#28a745' if scores['total']>=60 else '#dc3545'}; font-size: 4rem;'>{scores['total']} <span style='font-size: 1.5rem; color: gray;'>/ 100</span></h1>", unsafe_allow_html=True)
        st.markdown(f"<h3 style='text-align: center;'>评级：{summary['rating']}</h3>", unsafe_allow_html=True)

    with col_score2:
        st.markdown(f"**💡 一句话判断：** {summary['judgement']}")
        st.markdown(f"**⚠️ 主要风险点：** {summary['main_risk']}")

        st.markdown("**评分明细：**")
        st.progress(scores['absolute']/20, text=f"FCF绝对值水平: {scores['absolute']}/20")
        st.progress(scores['quality']/20, text=f"FCF vs 净利润质量: {scores['quality']}/20")
        st.progress(scores['margin']/20, text=f"FCF利润率水平: {scores['margin']}/20")
        st.progress(scores['growth']/20, text=f"FCF增长趋势: {scores['growth']}/20")
        st.progress(scores['yield']/20, text=f"FCF Yield 性价比: {scores['yield']}/20")

    st.markdown("---")

    # ========================
    # 交互式图表渲染
    # ========================
    st.markdown("### 📊 详细可视化图表")

    chart_col1, chart_col2 = st.columns(2)

    # 图表一：FCF趋势折线图
    with chart_col1:
        st.markdown("#### 图表一：FCF 趋势 (亿元)")
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=dates, y=df_plot['operating_cash_flow'], mode='lines+markers', name='经营现金流', line={"color": '#1f77b4', "width": 2}))
        fig1.add_trace(go.Scatter(x=dates, y=-df_plot['capex'], mode='lines+markers', name='资本支出(-)', line={"color": '#ff7f0e', "width": 2, "dash": 'dash'}))

        # FCF 颜色处理：正数绿色，负数红色
        fcf_colors = ['#2ca02c' if val >= 0 else '#d62728' for val in df_plot['fcf']]
        fig1.add_trace(go.Bar(x=dates, y=df_plot['fcf'], name='自由现金流(FCF)', marker_color=fcf_colors, opacity=0.8))

        fig1.update_layout(hovermode="x unified", margin={"t": 20, "b": 20, "l": 20, "r": 20}, legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1})
        st.plotly_chart(fig1, use_container_width=True)

    # 图表二：FCF vs 净利润柱状对比图
    with chart_col2:
        st.markdown("#### 图表二：FCF vs 净利润质量 (亿元)")
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=dates, y=df_plot['net_profit'], name='净利润', marker_color='#1f77b4'))

        # FCF颜色：FCF > 净利润 为绿，否则为橙色/红色
        fcf_np_colors = ['#2ca02c' if fcf > np else ('#ff7f0e' if fcf > 0 else '#d62728') for fcf, np in zip(df_plot['fcf'], df_plot['net_profit'])]
        fig2.add_trace(go.Bar(x=dates, y=df_plot['fcf'], name='FCF', marker_color=fcf_np_colors))

        fig2.update_layout(barmode='group', hovermode="x unified", margin={"t": 20, "b": 20, "l": 20, "r": 20}, legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1})
        st.plotly_chart(fig2, use_container_width=True)

    chart_col3, chart_col4 = st.columns(2)

    # 图表三：FCF利润率趋势
    with chart_col3:
        st.markdown("#### 图表三：FCF 利润率 (%)")
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(x=dates, y=df_plot['fcf_margin'], mode='lines+markers', name='FCF利润率', line={"color": '#9467bd', "width": 3}))

        # 参考线
        fig3.add_hline(y=20, line_dash="dash", line_color="green", annotation_text="20% (优秀)")
        fig3.add_hline(y=10, line_dash="dash", line_color="orange", annotation_text="10% (一般)")
        fig3.add_hline(y=5, line_dash="dash", line_color="red", annotation_text="5% (差)")

        fig3.update_layout(hovermode="x unified", margin={"t": 20, "b": 20, "l": 20, "r": 20}, yaxis_title="百分比 (%)")
        st.plotly_chart(fig3, use_container_width=True)

    # 图表四：FCF Yield 仪表盘
    with chart_col4:
        st.markdown("#### 图表四：当前 FCF Yield")
        curr_yield = summary['current_fcf_yield']

        fig4 = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = curr_yield,
            title = {'text': "FCF / 总市值 (%)", 'font': {'size': 16}},
            gauge = {
                'axis': {'range': [min(-5, curr_yield - 2), max(10, curr_yield + 2)], 'tickwidth': 1, 'tickcolor': "darkblue"},
                'bar': {'color': "black"},
                'bgcolor': "white",
                'borderwidth': 2,
                'bordercolor': "gray",
                'steps': [
                    {'range': [-100, 1], 'color': '#ff4d4d'},    # <1% Red
                    {'range': [1, 3], 'color': '#ffa64d'},       # 1-3% Orange
                    {'range': [3, 5], 'color': '#ffff66'},       # 3-5% Yellow
                    {'range': [5, 100], 'color': '#66ff66'}      # >5% Green
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': curr_yield
                }
            }
        ))
        fig4.update_layout(margin={"t": 50, "b": 20, "l": 20, "r": 20})
        st.plotly_chart(fig4, use_container_width=True)

    # --------- 数据原表 ---------
    with st.expander("📋 查看计算原始数据"):
        st.dataframe(analyzed_df, use_container_width=True)
else:
    st.info("👈 请在左侧选择目标股票，点击「开始分析」生成 FCF 报告。")
