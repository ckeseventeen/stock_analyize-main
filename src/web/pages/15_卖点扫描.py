"""
pages/15_卖点扫描.py — 个股卖点 / 回调风险扫描

输入一只 A 股代码，对该股票评估所有 sell 类技术条件（包括 Phase 5 新增的
顶背离 / KDJ 死叉 / BIAS / 跌破均线 / 天量天价），用卡片展示命中情况，
并给出综合风险评分。

依赖：
  - ScreenerDataProvider 拉 K 线（akshare/pytdx/Baostock 三级 fallback）
  - CONDITION_REGISTRY 已注册的 sell 类 condition
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402

from src.analysis.screening.conditions import (  # noqa: E402
    CONDITION_LABELS,
    CONDITION_REGISTRY,
    SIGNAL_DIRECTION,
    get_signal_direction,
)
from src.analysis.screening.data_provider import ScreenerDataProvider  # noqa: E402

st.set_page_config(page_title="卖点扫描", page_icon="📉", layout="wide")
st.title("📉 个股卖点 / 回调扫描")
st.caption("评估所有看跌信号 → 综合风险评分 | 持仓体检 / 高位减仓决策")


# ========================
# 卖出条件清单（带默认参数）
# ========================
# 每条 = (condition_type, kwargs, display_priority)
# priority: 1=高优(权重 2), 2=中优(权重 1), 3=辅助(权重 0.5)
_SELL_PROFILE: list[tuple[str, dict, int]] = [
    # 高优：趋势衰竭 + 极端偏离
    ("weekly_macd_top_divergence", {"lookback_bars": 60}, 1),
    ("daily_macd_top_divergence",  {"lookback_bars": 120}, 1),
    ("bias",                       {"ma_period": 20, "threshold": 8.0, "direction": "above"}, 1),
    ("volume_price_divergence",    {"lookback_bars": 30, "direction": "top"}, 1),

    # 中优：超买 + 破位
    ("rsi_overbought",             {"threshold": 75, "period": 14}, 2),
    ("kdj_death_cross",            {"j_threshold": 70}, 2),
    ("ma_death_cross",             {"fast_period": 5, "slow_period": 20}, 2),
    ("break_below_ma",             {"ma_period": 60, "lookback": 5}, 2),

    # 辅助：极端偏离 + 派发
    ("bias",                       {"ma_period": 60, "threshold": 15.0, "direction": "above"}, 3),
    ("volume_blowoff",             {"lookback_bars": 60, "vol_multiple": 3.0, "min_price_change_pct": 5.0}, 3),
    ("bollinger_breakout",         {"period": 20, "std_dev": 2.0, "direction": "upper"}, 3),
    ("volume_shrink",              {"lookback_bars": 10, "shrink_ratio": 0.6}, 3),
]

_PRIORITY_WEIGHT = {1: 2.0, 2: 1.0, 3: 0.5}
_PRIORITY_LABEL = {1: "🔴 高优", 2: "🟠 中优", 3: "🟡 辅助"}


# ========================
# 侧边栏输入
# ========================
st.sidebar.markdown("### 选股")
code_input = st.sidebar.text_input(
    "股票代码（6 位）", value="600519", help="A 股代码，如 600519 / 000001"
).strip()
days_back = st.sidebar.slider("拉取日线根数", 60, 500, 250, step=20,
                               help="日线 K 线根数，多则更准但慢")
weekly_back_days = st.sidebar.slider("周线回溯日历天数", 365, 365 * 5, 365 * 3, step=365)

run_btn = st.sidebar.button("🔍 扫描卖点", type="primary", width="stretch")


if not run_btn:
    st.info("👈 输入代码后点击「扫描卖点」")
    st.markdown(
        "**评估维度**：\n"
        "- 🔴 高优（权重 ×2）：周线顶背离 / 日线顶背离 / BIAS / 量价顶背离\n"
        "- 🟠 中优（权重 ×1）：RSI 超买 / KDJ 死叉 / 均线死叉 / 跌破 60日线\n"
        "- 🟡 辅助（权重 ×0.5）：长周期高乖离 / 天量天价 / 布林上轨 / 缩量\n"
    )
    st.stop()


# 简单代码合法性检查
if not (code_input.isdigit() and len(code_input) == 6):
    st.error(f"代码格式不合法：{code_input!r}，应为 6 位数字")
    st.stop()

# ========================
# 拉数据
# ========================
with st.spinner("拉取 K 线数据..."):
    provider = ScreenerDataProvider()
    daily_df = provider.get_daily_ohlcv(code_input, days_back=days_back)
    weekly_df = provider.get_weekly_ohlcv(code_input, days_back=weekly_back_days)

if daily_df is None or daily_df.empty:
    st.error(f"❌ 无法拉取 {code_input} 的日线数据（所有数据源都失败）")
    st.stop()

st.success(f"✅ {code_input} 数据已加载：日线 {len(daily_df)} 根 / 周线 {len(weekly_df) if weekly_df is not None else 0} 根")


# ========================
# 评估所有 sell 条件
# ========================
def _eval_condition(cond_type: str, kwargs: dict) -> tuple[bool, str]:
    """运行一个 condition 并返回 (是否命中, 错误信息)."""
    if cond_type not in CONDITION_REGISTRY:
        return False, f"未注册: {cond_type}"
    cls = CONDITION_REGISTRY[cond_type]
    try:
        cond = cls(**kwargs)
    except Exception as e:
        return False, f"构造失败: {e}"
    period = getattr(cond, "ohlcv_period", "daily")
    df = weekly_df if period == "weekly" else daily_df
    if df is None or df.empty:
        return False, f"无 {period} 数据"
    try:
        spot_row = pd.Series({"代码": code_input, "名称": code_input})
        result = cond.evaluate_full(spot_row, df)
        return bool(result), ""
    except Exception as e:
        return False, f"评估异常: {e}"


with st.spinner("评估所有看跌信号..."):
    results: list[dict] = []
    for cond_type, kwargs, priority in _SELL_PROFILE:
        hit, err = _eval_condition(cond_type, kwargs)
        direction = get_signal_direction(cond_type, kwargs)
        label = CONDITION_LABELS.get(cond_type, cond_type)
        # 区分相同 type 不同参数（比如 BIAS 20 vs 60）
        params_str = ", ".join(f"{k}={v}" for k, v in kwargs.items()
                                if k not in ("direction",))
        results.append({
            "type": cond_type,
            "label": label,
            "params": params_str,
            "priority": priority,
            "hit": hit,
            "error": err,
            "weight": _PRIORITY_WEIGHT[priority],
            "direction": direction,
        })

# ========================
# 综合风险评分
# ========================
hit_results = [r for r in results if r["hit"]]
total_weight = sum(r["weight"] for r in results if r["direction"] == "sell" or r["direction"] == "neutral")
hit_weight = sum(r["weight"] for r in hit_results)
risk_pct = (hit_weight / total_weight * 100) if total_weight > 0 else 0

# 风险等级
if risk_pct >= 60:
    risk_level = "🔴 高"
    risk_color = "red"
    advice = "**强烈建议减仓** — 多重高优信号共振，回调概率显著上升"
elif risk_pct >= 35:
    risk_level = "🟠 中"
    risk_color = "orange"
    advice = "**建议警惕** — 已有明确预警，可分批止盈或加紧密切跟踪"
elif risk_pct >= 15:
    risk_level = "🟡 低"
    risk_color = "gold"
    advice = "**偏暖警示** — 少量信号触发，正常持有但留意走势"
else:
    risk_level = "🟢 安全"
    risk_color = "green"
    advice = "**无显著看跌信号** — 当前看跌指标多数未触发"

# 顶部综合卡片
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("综合风险评分", f"{risk_pct:.0f}%", help="加权命中率")
with col2:
    st.metric("风险等级", risk_level)
with col3:
    st.metric("命中信号数", f"{len(hit_results)} / {len(results)}")
with col4:
    high_hits = sum(1 for r in hit_results if r["priority"] == 1)
    st.metric("高优信号命中", high_hits)

st.markdown(f"### 💡 操作建议：{advice}")

st.divider()


# ========================
# 信号明细
# ========================
st.subheader("📋 信号明细")

tab1, tab2, tab3 = st.tabs(["🔴 高优", "🟠 中优", "🟡 辅助"])

def _render_group(group_results):
    if not group_results:
        st.caption("（无）")
        return
    for r in group_results:
        emoji = "🔴" if r["hit"] else "⚪"
        params_part = f" `{r['params']}`" if r["params"] else ""
        if r["error"]:
            st.markdown(f"- {emoji} **{r['label']}**{params_part} — ⚠️ {r['error']}")
        else:
            status = "**命中**" if r["hit"] else "未触发"
            st.markdown(f"- {emoji} **{r['label']}**{params_part} — {status}")

with tab1:
    _render_group([r for r in results if r["priority"] == 1])
with tab2:
    _render_group([r for r in results if r["priority"] == 2])
with tab3:
    _render_group([r for r in results if r["priority"] == 3])


# ========================
# K 线 + 关键信号可视化
# ========================
st.divider()
st.subheader("📈 价格走势与均线")

if not daily_df.empty:
    date_col = "日期" if "日期" in daily_df.columns else "date"
    close_col = "收盘" if "收盘" in daily_df.columns else "close"
    df_plot = daily_df.copy()
    df_plot[date_col] = pd.to_datetime(df_plot[date_col])

    # 计算 MA20 / MA60
    df_plot["MA20"] = df_plot[close_col].rolling(20).mean()
    df_plot["MA60"] = df_plot[close_col].rolling(60).mean()
    # BIAS(20)
    df_plot["BIAS20"] = (df_plot[close_col] - df_plot["MA20"]) / df_plot["MA20"] * 100

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_plot[date_col], y=df_plot[close_col],
                              name="收盘", line=dict(color="#1f77b4", width=2)))
    fig.add_trace(go.Scatter(x=df_plot[date_col], y=df_plot["MA20"],
                              name="MA20", line=dict(color="orange", width=1)))
    fig.add_trace(go.Scatter(x=df_plot[date_col], y=df_plot["MA60"],
                              name="MA60", line=dict(color="purple", width=1, dash="dash")))
    fig.update_layout(height=420, hovermode="x unified",
                      legend=dict(orientation="h", y=1.05))
    st.plotly_chart(fig, use_container_width=True)

    # BIAS 副图
    fig_bias = go.Figure()
    fig_bias.add_trace(go.Bar(x=df_plot[date_col], y=df_plot["BIAS20"],
                               name="BIAS20", marker_color=df_plot["BIAS20"].apply(
                                   lambda x: "red" if x > 8 else "green" if x < -8 else "gray"
                               )))
    fig_bias.add_hline(y=8, line_dash="dash", line_color="red",
                       annotation_text="+8% 卖出阈值")
    fig_bias.add_hline(y=-8, line_dash="dash", line_color="green",
                       annotation_text="-8% 买入阈值")
    fig_bias.update_layout(height=240, title="BIAS(20) 乖离率",
                            hovermode="x unified")
    st.plotly_chart(fig_bias, use_container_width=True)


# ========================
# 原始数据下载
# ========================
with st.expander("💾 导出扫描结果"):
    result_df = pd.DataFrame([
        {
            "条件": r["label"],
            "参数": r["params"],
            "优先级": _PRIORITY_LABEL[r["priority"]],
            "权重": r["weight"],
            "命中": "✓" if r["hit"] else "",
            "错误": r["error"],
        }
        for r in results
    ])
    st.dataframe(result_df, use_container_width=True, hide_index=True)
    st.download_button(
        "下载 CSV",
        data=result_df.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"sell_scan_{code_input}_{pd.Timestamp.now():%Y%m%d_%H%M%S}.csv",
        mime="text/csv",
    )
