"""
pages/7_策略回测.py — 策略回测前端页面

功能：
- 支持从 config/backtest_presets.yaml 加载命名预设
- 根据策略类型动态渲染参数表单（由 STRATEGY_PARAM_SCHEMAS 驱动）
- 触发 BacktestRunner 进行历史回测
- 展示回测核心指标与收益图表
- 将当前参数另存为新预设
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

import backtrader as bt  # noqa: F401, E402
import pandas as pd  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402
import yaml as _yaml  # noqa: E402

from src.screener.data_provider import ScreenerDataProvider
from src.strategy.backtest import (
    STRATEGY_LABELS,
    STRATEGY_PARAM_SCHEMAS,
    STRATEGY_REGISTRY,
    BacktestRunner,
)
from src.visualization.gui.utils import (
    PATH_BACKTEST_PRESETS,
    list_backtest_presets,
    list_stocks_from_market_config,
    load_backtest_preset,
    quick_add_stock_widget,
    save_backtest_preset,
)

st.set_page_config(page_title="策略回测", page_icon="📈", layout="wide")
st.title("📈 策略回测 (Backtest)")
st.caption("基于 Backtrader 的量化策略验证。可从配置管理页新建预设后在此加载。")


# ========================
# 顶部：预设加载
# ========================
presets = list_backtest_presets()
col_load, col_info = st.columns([2, 3])
with col_load:
    loaded_name = st.selectbox(
        "📁 加载预设",
        options=["<不使用预设>"] + presets,
        index=0,
        help=f"预设来源：{PATH_BACKTEST_PRESETS}",
    )

loaded_preset: dict = {}
if loaded_name != "<不使用预设>":
    loaded_preset = load_backtest_preset(loaded_name)
    with col_info:
        if loaded_preset.get("description"):
            st.info(f"📝 {loaded_preset['description']}")


def _preset_default(section: str, key: str, fallback):
    """从加载的预设中取值，否则返回 fallback"""
    if not loaded_preset:
        return fallback
    return (loaded_preset.get(section) or {}).get(key, fallback)


# ========================
# 侧边栏：参数配置
# ========================
st.sidebar.markdown("**基础设置 (Data & Account)**")
market = st.sidebar.selectbox(
    "选择市场",
    options=["a", "hk", "us"],
    format_func=lambda x: {"a": "A股", "hk": "港股", "us": "美股"}.get(x, x),
    index=["a", "hk", "us"].index(_preset_default("data", "market", "a")),
)
# 预选逻辑
stocks_in_config = list_stocks_from_market_config(market)
preselect_code = ""
preselect_name = ""

if stocks_in_config:
    stock_options = ["<手动输入>"] + [f"{s.get('name')} ({s.get('code')})" for s in stocks_in_config]
    selected_stock = st.sidebar.selectbox(
        "🎯 从关注列表预选",
        options=stock_options,
        index=0,
        help="从 a_stock.yaml 等配置文件中加载已关注的股票"
    )
    if selected_stock != "<手动输入>":
        # 提取括号里的代码
        import re
        match = re.search(r"\((.*?)\)", selected_stock)
        if match:
            preselect_code = match.group(1)
            preselect_name = selected_stock.split(" (")[0]

stock_code = st.sidebar.text_input(
    "股票代码",
    value=preselect_code if preselect_code else _preset_default("data", "stock_code", "600519"),
    help="A股输入6位代码；港股5位；美股Ticker",
)

# 显示股票名称（如果是预选的或者尝试在配置中查找）
display_name = preselect_name
if not display_name and stocks_in_config:
    found = next((s for s in stocks_in_config if str(s.get("code")) == stock_code.strip()), None)
    if found:
        display_name = found.get("name")

if display_name:
    st.sidebar.info(f"📈 当前选择: **{display_name}**")
days_back = st.sidebar.number_input(
    "回测天数 (Trading days)",
    min_value=100, max_value=5000,
    value=int(_preset_default("data", "days_back", 1000)),
    step=100,
)

initial_cash = st.sidebar.number_input(
    "初始资金 (Initial Cash)",
    min_value=10000, max_value=10_000_000,
    value=int(_preset_default("account", "initial_cash", 100000)),
    step=10000,
)
commission = st.sidebar.number_input(
    "手续费率 (Commission)",
    min_value=0.0, max_value=0.01,
    value=float(_preset_default("account", "commission", 0.0002)),
    step=0.0001, format="%.4f",
)

st.sidebar.markdown("---")
st.sidebar.markdown("**回测周期 (Period)**")
freq_map = {"d": "日线 (Daily)", "w": "周线 (Weekly)", "m": "月线 (Monthly)", "y": "年线 (Yearly)"}
frequency = st.sidebar.selectbox(
    "K线周期",
    options=list(freq_map.keys()),
    format_func=lambda x: freq_map[x],
    index=list(freq_map.keys()).index(_preset_default("data", "frequency", "d")),
)

st.sidebar.markdown("---")
st.sidebar.markdown("**策略选择 (Strategy)**")

# 策略 key 来自注册表；不写死
all_strategy_keys = list(STRATEGY_REGISTRY.keys())
preset_strategy = loaded_preset.get("strategy") if loaded_preset else None
default_strategy_idx = (
    all_strategy_keys.index(preset_strategy)
    if preset_strategy in all_strategy_keys else 0
)

strategy_key = st.sidebar.selectbox(
    "选择策略",
    options=all_strategy_keys,
    format_func=lambda k: STRATEGY_LABELS.get(k, k),
    index=default_strategy_idx,
)
strategy_cls = STRATEGY_REGISTRY[strategy_key]

# 如果是筛选器桥接策略，提供一键导入功能
if strategy_key == "screener_rule":
    import yaml as _yaml
    from src.screener.config_schema import list_strategies
    from src.visualization.gui.utils import PATH_SCREEN
    
    screen_strats = list_strategies(str(PATH_SCREEN))
    if screen_strats:
        st.sidebar.markdown("---")
        st.sidebar.markdown("**🪄 快捷导入筛选策略**")
        options = ["-- 手动输入 YAML --"] + list(screen_strats.keys())
        
        def on_scheme_change():
            sel = st.session_state.get("_screener_scheme_sel")
            if sel and sel != "-- 手动输入 YAML --":
                try:
                    with open(PATH_SCREEN, encoding="utf-8") as f:
                        scfg = _yaml.safe_load(f) or {}
                    conds = scfg.get("strategies", {}).get(sel, {}).get("conditions", [])
                    
                    # 过滤掉不适合回测的基本面/排除类条件
                    # 这些条件依赖实时 spot 数据（市值、PE、名称等），历史K线中没有
                    _SPOT_ONLY_TYPES = {
                        "exclude_st", "exclude_delisting_risk",
                        "exclude_recent_unlock", "exclude_recent_large_unlock",
                        "market_cap", "pe_range", "pb_range",
                        "price_range", "turnover_rate", "roe_filter",
                    }
                    tech_conds = [c for c in conds if c.get("type") not in _SPOT_ONLY_TYPES]
                    removed = [c.get("type") for c in conds if c.get("type") in _SPOT_ONLY_TYPES]
                    
                    st.session_state[f"param_{strategy_key}_buy_conditions"] = _yaml.safe_dump(
                        tech_conds, allow_unicode=True, sort_keys=False
                    )
                    # 记录过滤掉了哪些条件，供后面展示提示
                    st.session_state["_screener_import_removed"] = removed
                except Exception:
                    pass

        st.sidebar.selectbox(
            "选择股票筛选中的方案", 
            options=options,
            format_func=lambda x: screen_strats.get(x, x),
            key="_screener_scheme_sel",
            on_change=on_scheme_change,
            help="选择后，会自动将该筛选方案的技术条件填入下方的「买入条件」输入框！基本面条件（市值/PE/ROE等）会自动过滤，因为历史K线数据中没有这些信息。"
        )
        removed = st.session_state.get("_screener_import_removed")
        if removed:
            st.sidebar.info(
                f"ℹ️ 已自动过滤 {len(removed)} 个不适合回测的基本面条件：`{'`, `'.join(removed)}`\n\n"
                "这些条件依赖实时市值/PE数据，在历史回测中无效。"
            )

# 动态渲染策略参数（由 STRATEGY_PARAM_SCHEMAS 驱动）
st.sidebar.markdown("---")
st.sidebar.markdown(f"**{STRATEGY_LABELS.get(strategy_key, strategy_key)} 参数**")
schemas = STRATEGY_PARAM_SCHEMAS.get(strategy_key, [])
strat_params: dict = {}
preset_params = (loaded_preset.get("params") or {}) if loaded_preset else {}

rule_yaml_error: str | None = None  # 规则 YAML 解析错误，run 前会阻塞

for schema in schemas:
    key = schema["key"]
    default_val = preset_params.get(key, schema["default"])
    if schema["type"] == "int":
        val = st.sidebar.number_input(
            schema["label"],
            min_value=int(schema.get("min", -10**9)),
            max_value=int(schema.get("max", 10**9)),
            value=int(default_val),
            step=int(schema.get("step", 1)),
            help=schema.get("help"),
            key=f"param_{strategy_key}_{key}",
        )
        strat_params[key] = int(val)
    elif schema["type"] == "yaml":
        # 嵌套结构（如 rule_based 的 rule_config）用 text_area 编辑
        # 默认值：若预设里带 dict，则 dump 成 YAML；否则用 schema default 的 YAML
        try:
            initial_text = _yaml.safe_dump(
                default_val, allow_unicode=True, sort_keys=False
            )
        except Exception:
            initial_text = ""
        st.sidebar.markdown(f"**{schema['label']}**")
        if schema.get("help"):
            st.sidebar.caption(schema["help"])
        yaml_text = st.sidebar.text_area(
            "YAML",
            value=initial_text,
            height=320,
            key=f"param_{strategy_key}_{key}",
            label_visibility="collapsed",
        )
        # 就地解析，UI 立即反馈语法错误
        try:
            parsed = _yaml.safe_load(yaml_text) or []
            if not isinstance(parsed, (dict, list)):
                raise ValueError("顶层必须是 dict 或 list")
            strat_params[key] = parsed
            st.sidebar.caption("✅ YAML 语法 OK")
        except Exception as e:
            rule_yaml_error = f"YAML 解析失败: {e}"
            st.sidebar.error(rule_yaml_error)
            strat_params[key] = None  # 阻止执行
    elif schema["type"] == "str":
        val = st.sidebar.text_input(
            schema["label"],
            value=str(default_val),
            help=schema.get("help"),
            key=f"param_{strategy_key}_{key}",
        )
        strat_params[key] = val
    else:
        val = st.sidebar.number_input(
            schema["label"],
            value=float(default_val),
            step=float(schema.get("step", 0.1)),
            help=schema.get("help"),
            key=f"param_{strategy_key}_{key}",
        )
        strat_params[key] = float(val)

# rule_based 专属：规则语法帮助
if strategy_key == "rule_based":
    with st.sidebar.expander("📖 规则语法帮助", expanded=False):
        st.markdown(
            "**指标 (indicators)** — key 为自定义名字，value 指定 type + 周期\n"
            "```yaml\n"
            "indicators:\n"
            "  rsi_14: {type: rsi, period: 14}\n"
            "  ma_20:  {type: sma, period: 20}\n"
            "  ema_60: {type: ema, period: 60}\n"
            "  macd_l: {type: macd_line, fast: 12, slow: 26, signal: 9}\n"
            "```\n"
            "**支持的 type**: `rsi` / `sma`(=`ma`) / `ema` / `macd_line` / `macd_signal` / `atr`\n\n"
            "**买卖规则** — 每行一个条件，格式 `左 运算符 右`（空格分隔）\n"
            "```\n"
            "rsi_14 < 30            # RSI 低于 30\n"
            "close > ma_20          # 价格站上 MA20\n"
            "ma_20 cross_up ma_60   # MA20 上穿 MA60（金叉）\n"
            "ma_20 cross_down ma_60 # MA20 下穿 MA60（死叉）\n"
            "volume > 1000000       # 成交量大于 100 万\n"
            "```\n"
            "运算符：`<` `<=` `>` `>=` `==` `!=` `cross_up` `cross_down`\n\n"
            "**买入/卖出逻辑**：\n"
            "- `buy_logic: any` — 任一条件命中即买入（OR）\n"
            "- `buy_logic: all` — 全部命中才买入（AND）"
        )

run_btn = st.sidebar.button("▶️ 开始回测", type="primary", width="stretch")

# 侧边栏底部：另存为预设
st.sidebar.markdown("---")
st.sidebar.markdown("**💾 保存当前配置为预设**")
new_preset_name = st.sidebar.text_input("预设名称", placeholder="my_strategy_v1",
                                        key="new_preset_name")
if st.sidebar.button("保存为预设", width="stretch"):
    if not new_preset_name.strip():
        st.sidebar.error("请输入预设名称")
    else:
        preset_to_save = {
            "strategy": strategy_key,
            "data": {
                "market": market,
                "stock_code": stock_code.strip(),
                "days_back": int(days_back),
                "frequency": frequency
            },
            "account": {"initial_cash": int(initial_cash),
                        "commission": float(commission)},
            "params": strat_params,
            "description": f"{STRATEGY_LABELS.get(strategy_key, strategy_key)} - {market.upper()}:{stock_code}",
        }
        if save_backtest_preset(new_preset_name.strip(), preset_to_save):
            st.sidebar.success(f"已保存 {new_preset_name}")
            st.rerun()
        else:
            st.sidebar.error("保存失败")

# 侧边栏底部：把当前回测标的加入关注列表（目前只支持 A 股纯代码）
st.sidebar.markdown("---")
with st.sidebar:
    if quick_add_stock_widget(
        key_prefix="page7_sidebar",
        default_market=market,
        default_code=stock_code.strip(),
        default_name="",
        label="➕ 把该标的加入关注列表",
    ):
        st.rerun()


# ========================
# 运行回测逻辑
# ========================
if run_btn:
    if not stock_code:
        st.error("请输入股票代码")
        st.stop()
    if rule_yaml_error:
        st.error(f"规则 YAML 有错，请先修正：{rule_yaml_error}")
        st.stop()
    # rule_based 但规则为空 dict 也不能跑
    if strategy_key == "rule_based":
        rc = strat_params.get("rule_config")
        if not isinstance(rc, dict) or not rc:
            st.error("规则策略需要有效的 rule_config（至少包含 indicators / buy_when / sell_when）")
            st.stop()

    with st.spinner("正在获取日线数据并运行回测引擎..."):
        try:
            provider = ScreenerDataProvider()
            if frequency == "d":
                df = provider.get_daily_ohlcv(stock_code, days_back=days_back, market=market)
            elif frequency == "w":
                df = provider.get_weekly_ohlcv(stock_code, days_back=days_back, market=market)
            elif frequency == "m":
                df = provider.get_monthly_ohlcv(stock_code, days_back=days_back, market=market)
            else:
                df = provider.get_yearly_ohlcv(stock_code, days_back=days_back, market=market)

            if df is None or df.empty:
                st.error(f"获取股票 {stock_code} 的数据失败，请检查代码是否正确或网络情况。")
                st.stop()

            runner = BacktestRunner(
                strategy_class=strategy_cls,
                data_df=df,
                **strat_params,
            )
            report = runner.run(initial_cash=initial_cash, commission=commission)

        except Exception as e:
            st.error(f"回测执行失败: {e}")
            st.exception(e)
            st.stop()

    if not report:
        st.warning("回测结果为空。")
    else:
        success_msg = f"✅ 回测完成：{display_name} ({stock_code})" if display_name else f"✅ 回测完成：{stock_code}"
        st.success(f"{success_msg} (使用策略: {report.get('策略')})")

        # --------- 核心指标面板 ---------
        st.subheader("📊 绩效指标摘要")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("最终总资产", f"¥ {report.get('最终资产'):,.2f}",
                  f"收益: {report.get('总收益率(%)')}%")
        c2.metric("年化收益率", f"{report.get('年化收益率(%)')}%")
        c3.metric("最大回撤", f"{report.get('最大回撤(%)')}%", delta_color="inverse")
        c4.metric("夏普比率", f"{report.get('夏普比率'):.2f}")

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("总交易次数", f"{report.get('总交易次数')} 次")
        c6.metric("胜率", f"{report.get('胜率(%)')}%")
        c7.metric("初始资金", f"¥ {report.get('初始资金'):,.2f}")
        c8.metric("手续费率", f"{commission*100}%")

        # ================================================================
        # 📈 K线 + 成交量 + 资金曲线
        # ================================================================
        st.subheader("📈 回测可视化")

        try:
            from plotly.subplots import make_subplots

            k_df = runner._data_df.copy()
            strat = runner._results[0]

            # --- 提取买卖信号与持仓区间 ---
            buy_dates, buy_prices = [], []
            sell_dates, sell_prices = [], []
            holding_intervals = []
            
            last_buy_date = None
            current_pos = 0
            
            # 按执行时间排序订单
            executed_orders = []
            for order in getattr(strat, "_orders", []):
                if hasattr(order, "executed") and order.executed.size != 0:
                    executed_orders.append(order)
            
            executed_orders.sort(key=lambda x: x.executed.dt)
            
            for order in executed_orders:
                exec_date = bt.num2date(order.executed.dt)
                if order.executed.size > 0:
                    buy_dates.append(exec_date)
                    buy_prices.append(order.executed.price)
                    if current_pos == 0:
                        last_buy_date = exec_date
                    current_pos += order.executed.size
                else:
                    sell_dates.append(exec_date)
                    sell_prices.append(order.executed.price)
                    current_pos += order.executed.size # size 是负数
                    if current_pos == 0 and last_buy_date:
                        holding_intervals.append((last_buy_date, exec_date))
                        last_buy_date = None
            
            # 如果回测结束仍持仓
            if current_pos > 0 and last_buy_date:
                holding_intervals.append((last_buy_date, k_df.index[-1]))

            # --- 均线（仅 ma_crossover 使用）---
            show_ma = strategy_key == "ma_crossover"
            if show_ma:
                fast_p = strat_params.get("fast_period", 10)
                slow_p = strat_params.get("slow_period", 30)
                ma_fast = k_df["close"].rolling(fast_p).mean()
                ma_slow = k_df["close"].rolling(slow_p).mean()
                ma_fast_name = f"MA{fast_p}"
                ma_slow_name = f"MA{slow_p}"

            # --- 买入持有基线 ---
            benchmark = k_df["close"] / k_df["close"].iloc[0] * initial_cash

            # --- 交易量颜色：涨红跌绿 (标准配色) ---
            vol_colors = ["#ef5350" if c >= o else "#26a69a"
                          for c, o in zip(k_df["close"], k_df["open"])]

            fig = make_subplots(
                rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                row_heights=[0.45, 0.15, 0.20, 0.20],
            )

            # --- MACD 指标计算与信号识别 ---
            from src.analysis.technical.indicators import TechnicalAnalyzer
            ta = TechnicalAnalyzer(k_df)
            ta.add_macd()
            m_df = ta.get_dataframe()
            
            # 金叉死叉
            m_df["prev_macd"] = m_df["macd"].shift(1)
            m_df["prev_signal"] = m_df["macd_signal"].shift(1)
            golden_cross = (m_df["macd"] > m_df["macd_signal"]) & (m_df["prev_macd"] <= m_df["prev_signal"])
            death_cross = (m_df["macd"] < m_df["macd_signal"]) & (m_df["prev_macd"] >= m_df["prev_signal"])
            
            # 背离检测 (基于 indicators.py 的简单实现)
            # 这里简单标记：如果当前是局部低点且MACD柱状图比前一个局部低点高，则标记底背离
            from scipy.signal import argrelmin, argrelmax
            
            def get_divergences(df):
                price = df["close"].values
                hist = df["macd_hist"].values
                bottom_divs = []
                top_divs = []
                
                # 寻找局部低点 (order=5)
                troughs = argrelmin(price, order=5)[0]
                for i in range(1, len(troughs)):
                    curr, prev = troughs[i], troughs[i-1]
                    if price[curr] < price[prev] and hist[curr] > hist[prev] and hist[curr] < 0 and hist[prev] < 0:
                        bottom_divs.append(df.index[curr])
                
                # 寻找局部高点
                peaks = argrelmax(price, order=5)[0]
                for i in range(1, len(peaks)):
                    curr, prev = peaks[i], peaks[i-1]
                    if price[curr] > price[prev] and hist[curr] < hist[prev] and hist[curr] > 0 and hist[prev] > 0:
                        top_divs.append(df.index[curr])
                return bottom_divs, top_divs

            bottom_div_dates, top_div_dates = get_divergences(m_df)

            fig.add_trace(go.Candlestick(
                x=k_df.index,
                open=k_df["open"], high=k_df["high"],
                low=k_df["low"], close=k_df["close"],
                increasing=dict(line=dict(color="#ef5350"), fillcolor="#ef5350"),
                decreasing=dict(line=dict(color="#26a69a"), fillcolor="#26a69a"),
                name="K线走势",
            ), row=1, col=1)

            if show_ma:
                fig.add_trace(go.Scatter(x=k_df.index, y=ma_fast,
                                         line=dict(color="#FB8C00", width=1.8),
                                         name=ma_fast_name), row=1, col=1)
                fig.add_trace(go.Scatter(x=k_df.index, y=ma_slow,
                                         line=dict(color="#1E88E5", width=1.8),
                                         name=ma_slow_name), row=1, col=1)

            # --- 绘制持仓区间高亮 (Green blocks) ---
            for start, end in holding_intervals:
                fig.add_vrect(
                    x0=start, x1=end,
                    fillcolor="rgba(76, 175, 80, 0.12)",
                    layer="below", line_width=0,
                    row=1, col=1,
                    annotation_text="持仓期", annotation_position="top left",
                    annotation_font=dict(size=10, color="rgba(76, 175, 80, 0.5)")
                )

            if buy_dates:
                fig.add_trace(go.Scatter(
                    x=buy_dates, y=buy_prices, mode="markers",
                    marker=dict(symbol="triangle-up", size=18, color="#D32F2F",
                                line=dict(color="white", width=2)),
                    name="买入信号 ▲",
                ), row=1, col=1)

            if sell_dates:
                fig.add_trace(go.Scatter(
                    x=sell_dates, y=sell_prices, mode="markers",
                    marker=dict(symbol="triangle-down", size=18, color="#1976D2",
                                line=dict(color="white", width=2)),
                    name="卖出信号 ▼",
                ), row=1, col=1)

            if "volume" in k_df.columns:
                fig.add_trace(go.Bar(x=k_df.index, y=k_df["volume"],
                                     marker_color=vol_colors, name="成交量",
                                     showlegend=False, opacity=0.7),
                              row=2, col=1)

            # --- MACD 子图 ---
            fig.add_trace(go.Scatter(x=m_df.index, y=m_df["macd"], line=dict(color="#2196F3", width=1.5), name="MACD"), row=3, col=1)
            fig.add_trace(go.Scatter(x=m_df.index, y=m_df["macd_signal"], line=dict(color="#FF9800", width=1.5), name="Signal"), row=3, col=1)
            
            # MACD 柱状图颜色
            hist_colors = ["#ef5350" if h >= 0 else "#26a69a" for h in m_df["macd_hist"]]
            fig.add_trace(go.Bar(x=m_df.index, y=m_df["macd_hist"], marker_color=hist_colors, name="Hist", showlegend=False), row=3, col=1)
            
            # 标记金叉死叉 (在 MACD 图上)
            gold_dates = m_df.index[golden_cross]
            death_dates = m_df.index[death_cross]
            
            if not gold_dates.empty:
                fig.add_trace(go.Scatter(x=gold_dates, y=m_df.loc[gold_dates, "macd"], mode="markers", 
                                         marker=dict(symbol="circle", size=8, color="#D32F2F"), name="MACD 金叉"), row=3, col=1)
            if not death_dates.empty:
                fig.add_trace(go.Scatter(x=death_dates, y=m_df.loc[death_dates, "macd"], mode="markers", 
                                         marker=dict(symbol="circle", size=8, color="#1976D2"), name="MACD 死叉"), row=3, col=1)

            # 标记背离 (在 K 线图上标记文字/图标)
            if bottom_div_dates:
                fig.add_trace(go.Scatter(x=bottom_div_dates, y=m_df.loc[bottom_div_dates, "low"] * 0.98, mode="markers+text",
                                         text="底背离", textposition="bottom center",
                                         marker=dict(symbol="star-triangle-up", size=12, color="#FF5722"), name="底背离"), row=1, col=1)
            if top_div_dates:
                fig.add_trace(go.Scatter(x=top_div_dates, y=m_df.loc[top_div_dates, "high"] * 1.02, mode="markers+text",
                                         text="顶背离", textposition="top center",
                                         marker=dict(symbol="star-triangle-down", size=12, color="#9C27B0"), name="顶背离"), row=1, col=1)

            fig.add_trace(go.Scatter(
                x=k_df.index, y=benchmark,
                line=dict(color="#616161", width=2, dash="dash"),
                name="买入持有 (Benchmark)",
                fill="tozeroy", fillcolor="rgba(0,0,0,0.05)",
            ), row=4, col=1)

            final_v = report.get("最终资产", initial_cash)
            ret_color = "#4CAF50" if final_v >= initial_cash else "#F44336"
            fig.add_hline(y=initial_cash, line_dash="dash", line_color="#CCCCCC",
                          annotation_text=f"初始 ¥{initial_cash:,.0f}", row=4, col=1)
            fig.add_hline(y=final_v, line_dash="dash", line_color=ret_color,
                          annotation_text=f"策略 ¥{final_v:,.0f}", row=4, col=1)

            fig.update_layout(
                height=950, 
                template="plotly_white",
                xaxis_rangeslider_visible=False,
                legend=dict(
                    orientation="h", 
                    yanchor="bottom", y=1.02,
                    xanchor="left", x=0,
                    font=dict(size=14, color="white"),
                    bgcolor="black",
                    bordercolor="#555555", borderwidth=1
                ),
                margin=dict(l=20, r=20, t=60, b=50),
                hovermode="x unified",
            )
            
            # 统一网格线风格
            grid_style = dict(gridcolor="#F0F0F0", zerolinecolor="#E0E0E0")
            
            fig.update_yaxes(title_text="价格", row=1, col=1, **grid_style)
            fig.update_yaxes(title_text="成交量", row=2, col=1, **grid_style)
            fig.update_yaxes(title_text="MACD", row=3, col=1, **grid_style)
            fig.update_yaxes(title_text="资金 (¥)", row=4, col=1, **grid_style)
            
            fig.update_xaxes(**grid_style)
            # 移除周末空隙（所有子图）
            fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])

            st.plotly_chart(fig, width='stretch')

        except Exception as e:
            st.warning(f"图表生成异常: {e}")
            st.exception(e)

        with st.expander("📋 查看原始 K 线数据"):
            st.dataframe(df.head(200), width='stretch')

else:
    st.info("👈 请在左侧配置参数后，点击「开始回测」按钮执行验证。可先在【⚙️ 配置管理】页创建预设。")
