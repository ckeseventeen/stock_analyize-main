"""
pages/7_策略回测.py — 策略回测前端页面

功能：
- 提供前端参数输入（股票代码、周期、资金、均线参数）
- 触发 BacktestRunner 进行历史回测
- 展示回测核心指标与收益图表
"""
from __future__ import annotations

# matplotlib 后端必须在任何 matplotlib / backtrader 导入前设置为 Agg
# 否则 Streamlit worker 线程调用 cerebro.plot() 会触发 MacOS GUI backend 异常
import os  # noqa: E402
os.environ["MPLBACKEND"] = "Agg"

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import matplotlib  # noqa: E402
matplotlib.use("Agg", force=True)
matplotlib.rcParams["font.sans-serif"] = [
    "Hiragino Sans GB", "PingFang HK", "Heiti TC", "STHeiti",
    "Microsoft YaHei", "SimHei", "Arial Unicode MS", "DejaVu Sans",
]
matplotlib.rcParams["axes.unicode_minus"] = False
import matplotlib.pyplot as plt  # noqa: E402

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402
import backtrader as bt  # noqa: F401, E402
import plotly.graph_objects as go  # noqa: E402

from src.screener.data_provider import ScreenerDataProvider
from src.strategy.backtest.runner import BacktestRunner
from src.strategy.backtest.ma_crossover import MACrossoverStrategy
from src.strategy.backtest.factor_strategy import FactorRebalanceStrategy

st.set_page_config(page_title="策略回测", page_icon="📈", layout="wide")
st.title("📈 策略回测 (Backtest)")
st.caption("基于 Backtrader 核心的量化策略验证页面。填入参数即可分析策略在历史数据上的表现。")

# ========================
# 侧边栏：参数配置
# ========================
st.sidebar.markdown("**基础设置 (Data & Account)**")
stock_code = st.sidebar.text_input("股票代码 (如 600519)", value="600519", help="输入A股纯数字代码")
days_back = st.sidebar.number_input("回测天数 (Trading days)", min_value=100, max_value=5000, value=1000, step=100)

initial_cash = st.sidebar.number_input("初始资金 (Initial Cash)", min_value=10000, max_value=10000000, value=100000, step=10000)
commission = st.sidebar.number_input("手续费率 (Commission)", min_value=0.0, max_value=0.01, value=0.0002, step=0.0001, format="%.4f")

st.sidebar.markdown("---")
st.sidebar.markdown("**策略选择 (Strategy Params)**")

# 提供策略选择
strategy_type = st.sidebar.selectbox("选择策略", options=["双均线交叉 (MA Crossover)", "因子再平衡 (Factor Rebalance) (Demo)"])

if strategy_type == "双均线交叉 (MA Crossover)":
    st.sidebar.caption("金叉全仓买入，死叉全仓卖出")
    fast_ma = st.sidebar.slider("短期均线 (Fast MA)", min_value=3, max_value=60, value=10, step=1)
    slow_ma = st.sidebar.slider("长期均线 (Slow MA)", min_value=10, max_value=250, value=30, step=1)
    strategy_cls = MACrossoverStrategy
    strat_params = {"fast_period": fast_ma, "slow_period": slow_ma}

else:
    st.sidebar.caption("每隔N天判断因子，低估买入，高估卖出(使用收盘价作为因子的Demo验证)")
    rebalance_days = st.sidebar.number_input("调仓周期 (天)", value=20, min_value=1)
    buy_threshold = st.sidebar.number_input("买入阈值", value=15.0)
    sell_threshold = st.sidebar.number_input("卖出阈值", value=30.0)
    strategy_cls = FactorRebalanceStrategy
    strat_params = {"rebalance_days": rebalance_days, "buy_threshold": buy_threshold, "sell_threshold": sell_threshold}

run_btn = st.sidebar.button("▶️ 开始回测", type="primary", width='stretch')


# ========================
# 运行回测逻辑
# ========================
if run_btn:
    if not stock_code:
        st.error("请输入股票代码")
        st.stop()
        
    with st.spinner("正在获取日线数据并运行回测引擎..."):
        try:
            provider = ScreenerDataProvider()
            df = provider.get_daily_ohlcv(stock_code, days_back=days_back)
            
            if df is None or df.empty:
                st.error(f"获取股票 {stock_code} 的数据失败，请检查代码是否正确或网络情况。")
                st.stop()
                
            runner = BacktestRunner(
                strategy_class=strategy_cls,
                data_df=df,
                **strat_params
            )
            report = runner.run(initial_cash=initial_cash, commission=commission)
            
        except Exception as e:
            st.error(f"回测执行失败: {e}")
            st.exception(e)
            st.stop()

    if not report:
        st.warning("回测结果为空。")
    else:
        st.success(f"✅ 回测完成：{stock_code} (使用策略: {report.get('策略')})")
        
        # --------- 核心指标面板 ---------
        st.subheader("📊 绩效指标摘要")
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("最终总资产", f"¥ {report.get('最终资产'):,.2f}", f"收益: {report.get('总收益率(%)')}%")
        c2.metric("年化收益率", f"{report.get('年化收益率(%)')}%")
        c3.metric("最大回撤", f"{report.get('最大回撤(%)')}%", delta_color="inverse")
        c4.metric("夏普比率", f"{report.get('夏普比率'):.2f}")

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("总交易次数", f"{report.get('总交易次数')} 次")
        c6.metric("胜率", f"{report.get('胜率(%)')}%")
        c7.metric("初始资金", f"¥ {report.get('初始资金'):,.2f}")
        c8.metric("手续费率", f"{commission*100}%")
        # ================================================================
        # 📈 专业 K线图 + 成交量 + 资金曲线（Plotly 交互式三面板）
        # ================================================================
        st.subheader("📈 回测可视化")

        try:
            from plotly.subplots import make_subplots

            k_df = runner._data_df.copy()
            strat = runner._results[0]

            # --- 提取买卖信号 ---
            buy_dates, buy_prices = [], []
            sell_dates, sell_prices = [], []
            for order in strat._orders:
                if hasattr(order, 'executed') and order.executed.size != 0:
                    exec_date = bt.num2date(order.executed.dt)
                    if order.executed.size > 0:
                        buy_dates.append(exec_date)
                        buy_prices.append(order.executed.price)
                    else:
                        sell_dates.append(exec_date)
                        sell_prices.append(order.executed.price)

            # --- 计算均线 ---
            fast_p = strat_params.get('fast_period', 10)
            slow_p = strat_params.get('slow_period', 30)
            ma_fast = k_df['close'].rolling(fast_p).mean()
            ma_slow = k_df['close'].rolling(slow_p).mean()

            # --- 资金曲线（买入持有 benchmark）---
            benchmark = k_df['close'] / k_df['close'].iloc[0] * initial_cash

            # --- 涨跌颜色 ---
            vol_colors = ['#FF4444' if c >= o else '#22AB94'
                          for c, o in zip(k_df['close'], k_df['open'])]

            # ======== 创建三面板子图 ========
            fig = make_subplots(
                rows=3, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.03,
                row_heights=[0.55, 0.20, 0.25],
                subplot_titles=('', '', ''),
            )

            # ---- Panel 1：K线 + 均线 + 买卖信号 ----
            fig.add_trace(go.Candlestick(
                x=k_df.index,
                open=k_df['open'], high=k_df['high'],
                low=k_df['low'], close=k_df['close'],
                increasing=dict(line=dict(color='#FF4444'), fillcolor='#FF4444'),
                decreasing=dict(line=dict(color='#22AB94'), fillcolor='#22AB94'),
                name='K线', showlegend=True,
            ), row=1, col=1)

            fig.add_trace(go.Scatter(
                x=k_df.index, y=ma_fast,
                line=dict(color='#FF9800', width=1.2),
                name=f'MA{fast_p}', opacity=0.9,
            ), row=1, col=1)

            fig.add_trace(go.Scatter(
                x=k_df.index, y=ma_slow,
                line=dict(color='#2196F3', width=1.2),
                name=f'MA{slow_p}', opacity=0.9,
            ), row=1, col=1)

            # 买入标记
            if buy_dates:
                fig.add_trace(go.Scatter(
                    x=buy_dates, y=buy_prices,
                    mode='markers',
                    marker=dict(symbol='triangle-up', size=12,
                                color='#FF4444', line=dict(color='white', width=1)),
                    name='买入 ▲',
                ), row=1, col=1)

            # 卖出标记
            if sell_dates:
                fig.add_trace(go.Scatter(
                    x=sell_dates, y=sell_prices,
                    mode='markers',
                    marker=dict(symbol='triangle-down', size=12,
                                color='#2196F3', line=dict(color='white', width=1)),
                    name='卖出 ▼',
                ), row=1, col=1)

            # ---- Panel 2：成交量 ----
            if 'volume' in k_df.columns:
                fig.add_trace(go.Bar(
                    x=k_df.index, y=k_df['volume'],
                    marker_color=vol_colors,
                    name='成交量', showlegend=False,
                    opacity=0.7,
                ), row=2, col=1)

            # ---- Panel 3：资金曲线 ----
            fig.add_trace(go.Scatter(
                x=k_df.index, y=benchmark,
                line=dict(color='#9E9E9E', width=1, dash='dot'),
                name='买入持有',
                fill='tozeroy', fillcolor='rgba(158,158,158,0.05)',
            ), row=3, col=1)

            final_v = report.get('最终资产', initial_cash)
            ret_color = '#4CAF50' if final_v >= initial_cash else '#F44336'
            fig.add_hline(y=initial_cash, line_dash='dash', line_color='#CCCCCC',
                          annotation_text=f'初始 ¥{initial_cash:,.0f}', row=3, col=1)
            fig.add_hline(y=final_v, line_dash='dash', line_color=ret_color,
                          annotation_text=f'策略 ¥{final_v:,.0f}', row=3, col=1)

            # ======== 全局样式 ========
            fig.update_layout(
                height=850,
                template='plotly_white',
                xaxis_rangeslider_visible=False,
                legend=dict(
                    orientation='h', yanchor='bottom', y=1.01,
                    xanchor='left', x=0, font=dict(size=11),
                    bgcolor='rgba(255,255,255,0.8)',
                ),
                margin=dict(l=50, r=30, t=40, b=30),
                hovermode='x unified',
                plot_bgcolor='#FAFAFA',
            )
            fig.update_yaxes(title_text='价格', row=1, col=1, gridcolor='#EEEEEE')
            fig.update_yaxes(title_text='成交量', row=2, col=1, gridcolor='#EEEEEE')
            fig.update_yaxes(title_text='资金 (¥)', row=3, col=1, gridcolor='#EEEEEE')
            fig.update_xaxes(gridcolor='#EEEEEE')

            # 隐藏非交易日缺口
            fig.update_xaxes(
                rangebreaks=[dict(bounds=['sat', 'mon'])],
                row=3, col=1,
            )

            st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.warning(f"图表生成异常: {e}")
            st.exception(e)

        # --------- 原始数据（折叠）---------
        with st.expander("📋 查看原始 K 线数据"):
            st.dataframe(df.head(200), use_container_width=True)

else:
    st.info("👈 请在左侧配置参数后，点击「开始回测」按钮执行验证。")
