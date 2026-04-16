"""
pages/7_策略回测.py — 策略回测前端页面

功能：
- 提供前端参数输入（股票代码、周期、资金、均线参数）
- 触发 BacktestRunner 进行历史回测
- 展示回测核心指标与收益图表
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# 后端画图设置
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["font.sans-serif"] = [
    "Hiragino Sans GB", "PingFang HK", "Heiti TC", "STHeiti",
    "Microsoft YaHei", "SimHei", "Arial Unicode MS", "DejaVu Sans",
]
matplotlib.rcParams["axes.unicode_minus"] = False
import matplotlib.pyplot as plt

import pandas as pd
import streamlit as st
import backtrader as bt

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

run_btn = st.sidebar.button("▶️ 开始回测", type="primary", use_container_width=True)


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

        # --------- 收益图表渲染 ---------
        st.subheader("📉 回测收益与交易图表")
        with st.spinner("正在绘制图表 (可能需要几秒钟)..."):
            try:
                # 获取 Cerebro 实例
                cerebro = runner._cerebro
                
                # matplotlib 绘制 backtrader 图表，返回 figure 列表
                # 设置风格和尺寸
                figs = cerebro.plot(style='candlestick', dpi=120, width=16, height=9)
                
                if figs and len(figs) > 0 and len(figs[0]) > 0:
                    fig = figs[0][0]
                    st.pyplot(fig, use_container_width=True)
                    plt.close(fig)
                else:
                    st.warning("图表生成失败，Backtrader 绘图未返回有效的图像对象。")
            except Exception as e:
                st.warning(f"由于 Matplotlib / Backtrader 兼容性问题，无法生成回测图表，详情: {e}")
                
        # --------- 数据清单（折叠显示）---------
        with st.expander("📋 查看回测用的起始原始 K 线数据"):
            st.dataframe(df.head(100), use_container_width=True)

else:
    st.info("👈 请在左侧配置参数后，点击「开始回测」按钮执行验证。")
