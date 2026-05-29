"""
pages/16_持仓监控.py — 持仓监控 + 卖出建议

功能：
  - 列出所有持仓 + 盈亏 / 持仓时长 / 标签
  - 每只跑 L1 (止损/移动止盈) + L2 (12 个 sell condition) 评估
  - 综合风险评分 + 操作建议
  - 持仓 CRUD（添加 / 编辑 / 删除 / 交易记录）
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

from src.analysis.screening.data_provider import ScreenerDataProvider  # noqa: E402
from src.portfolio import (  # noqa: E402
    Holding,
    PortfolioManager,
    SellEngine,
    Transaction,
)
from src.portfolio.macro_signals import analyze_macro  # noqa: E402
from src.portfolio.market_regime import MarketRegimeAnalyzer  # noqa: E402
from src.portfolio.position_sizing import (  # noqa: E402
    check_concentration,
    suggest_scale_out,
)
from src.web.components.stock_autocomplete import stock_autocomplete  # noqa: E402

st.set_page_config(page_title="持仓监控", page_icon="💼", layout="wide")
st.title("💼 持仓监控")
st.caption("L1 风控（止损/移动止盈）+ L2 信号（12 个卖出条件）综合判定")


# ========================
# 加载持仓 + 数据 provider
# ========================
mgr = PortfolioManager()
pf = mgr.load()

if not pf.holdings:
    st.info("👉 当前没有持仓。下方添加一只开始监控。")

# 侧边栏：全局默认提醒
with st.sidebar:
    st.markdown("### ⚙️ 全局默认提醒")
    g = pf.default_alerts
    new_stop = st.number_input("固定止损(%)", 0.0, 50.0, float(g.get("stop_loss_pct", 8.0)), 0.5)
    new_trail = st.number_input("移动止盈(%)", 0.0, 50.0, float(g.get("trailing_pct", 10.0)), 0.5)
    new_threshold = st.number_input("信号告警分(%)", 0.0, 100.0, float(g.get("signal_threshold_pct", 60)), 5.0)
    new_enable = st.checkbox("启用 L2 信号告警", value=bool(g.get("enable_signal_alert", True)))

    if st.button("💾 保存全局默认", width="stretch"):
        pf.default_alerts = {
            "stop_loss_pct": new_stop,
            "trailing_pct": new_trail,
            "signal_threshold_pct": new_threshold,
            "enable_signal_alert": new_enable,
        }
        if mgr.save(pf):
            st.success("已保存")
            st.rerun()
        else:
            st.error("保存失败")

    st.divider()
    auto_run = st.checkbox("打开页面自动扫描", value=True,
                            help="关闭以加速页面加载；改为手动点扫描")


# ========================
# 持仓汇总卡片
# ========================
if pf.holdings:
    # 拉最新价
    @st.cache_data(ttl=300, show_spinner=False)
    def _fetch_spot(_unused_key: str) -> pd.DataFrame:
        provider = ScreenerDataProvider()
        return provider.get_all_a_shares()

    with st.spinner("拉取实时行情..."):
        spot_df = _fetch_spot("v1")

    code_to_price: dict[str, float] = {}
    code_to_name: dict[str, str] = {}
    if spot_df is not None and not spot_df.empty:
        code_col = "代码" if "代码" in spot_df.columns else "code"
        price_col = "最新价" if "最新价" in spot_df.columns else "close"
        name_col = "名称" if "名称" in spot_df.columns else "name"
        for _, row in spot_df.iterrows():
            c = str(row.get(code_col, "")).zfill(6)
            try:
                code_to_price[c] = float(row.get(price_col, 0) or 0)
                code_to_name[c] = str(row.get(name_col, ""))
            except (ValueError, TypeError):
                pass

    total_mv = pf.total_market_value(code_to_price)
    total_cost = pf.total_cost_basis()
    total_pnl = total_mv - total_cost
    total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("持仓只数", len(pf.holdings))
    with c2:
        st.metric("总市值", f"¥{total_mv:,.0f}")
    with c3:
        st.metric("总成本", f"¥{total_cost:,.0f}")
    with c4:
        delta_color = "normal" if total_pnl_pct >= 0 else "inverse"
        st.metric("浮动盈亏", f"¥{total_pnl:+,.0f}",
                  f"{total_pnl_pct:+.2f}%", delta_color=delta_color)

    # L4 大盘环境守门员
    @st.cache_resource(ttl=600)
    def _get_regime():
        return MarketRegimeAnalyzer().analyze()

    with st.spinner("分析大盘环境..."):
        regime = _get_regime()
    regime_multiplier = regime.weight_multiplier

    regime_color = {"bull": "success", "sideways": "warning", "bear": "error"}[regime.regime]
    getattr(st, regime_color)(regime.summary)

    # L5 宏观面板（折叠展示，避免占用太多视觉空间）
    @st.cache_resource(ttl=900)
    def _get_macro():
        return analyze_macro()

    macro = _get_macro()
    if macro.is_high_risk:
        # 高风险时主动展开
        with st.expander(
            f"🌐 L5 宏观面板（**{macro.critical_count} critical / {macro.warning_count} warning**）",
            expanded=True,
        ):
            for sig in macro.signals:
                if sig.triggered:
                    icon = "⛔" if sig.severity == "critical" else "🟠"
                    st.markdown(f"- {icon} **{sig.name}**: {sig.message}")
                else:
                    st.caption(f"✅ {sig.name}: {sig.message}")
    else:
        with st.expander("🌐 L5 宏观面板（正常）", expanded=False):
            for sig in macro.signals:
                icon = "⛔" if sig.severity == "critical" and sig.triggered else (
                    "🟠" if sig.severity == "warning" and sig.triggered else "✅"
                )
                st.markdown(f"- {icon} **{sig.name}**: {sig.message}")

    # L3 集中度预警
    concentration_warnings = check_concentration(pf, code_to_price)
    if concentration_warnings:
        # 同 code 去重（行业预警会给所有该行业的持仓重复发一条，这里压成 unique reason）
        unique_reasons = list({a.reason for a in concentration_warnings})
        for reason in unique_reasons:
            st.warning(f"⚠️ {reason}")

    st.divider()


# ========================
# 单只持仓：盈亏 + 卖出评估
# ========================
def _render_holding_card(holding: Holding, regime_multiplier: float = 1.0):
    code = holding.code
    cur_price = code_to_price.get(code, 0.0)
    name_live = code_to_name.get(code) or holding.name

    # 头部摘要
    summary_cols = st.columns([2, 1, 1, 1, 1, 1])
    summary_cols[0].markdown(f"**{name_live}** `{code}` {holding.tag if holding.tag else ''}")
    summary_cols[1].metric("最新价", f"{cur_price:.2f}" if cur_price else "—")
    summary_cols[2].metric("成本", f"{holding.avg_cost:.2f}")

    if cur_price > 0 and holding.avg_cost > 0:
        pnl_pct = holding.unrealized_pnl_pct(cur_price)
        pnl = holding.unrealized_pnl(cur_price)
        summary_cols[3].metric("浮盈%", f"{pnl_pct:+.2f}%")
        summary_cols[4].metric("浮盈¥", f"{pnl:+,.0f}")
    summary_cols[5].metric("持仓天数", holding.holding_days())

    if not auto_run:
        with st.expander("🔍 点击查看本只卖出判定", expanded=False):
            run_btn = st.button(f"扫描 {code}", key=f"scan_{code}")
            if not run_btn:
                return
            _evaluate_and_show(holding, cur_price, regime_multiplier)
    else:
        _evaluate_and_show(holding, cur_price, regime_multiplier)


def _evaluate_and_show(holding: Holding, cur_price: float, regime_multiplier: float = 1.0):
    if cur_price <= 0:
        st.warning(f"⚠️ 无最新价（数据源返回空），跳过卖出评估")
        return

    provider = ScreenerDataProvider()
    with st.spinner(f"拉取 {holding.code} K 线..."):
        daily_df = provider.get_daily_ohlcv(holding.code, days_back=250)
        weekly_df = provider.get_weekly_ohlcv(holding.code, days_back=365 * 3)

    if daily_df is None or daily_df.empty:
        st.error(f"❌ 无法拉取 {holding.code} 的日线数据")
        return

    engine = SellEngine(pf.default_alerts)
    verdict = engine.evaluate(holding, cur_price, daily_df, weekly_df,
                               regime_multiplier=regime_multiplier)

    # 顶部卡片
    cols = st.columns(4)
    cols[0].metric("综合风险分", f"{verdict.risk_pct:.0f}%")
    cols[1].metric("风险等级", verdict.risk_level)
    cols[2].metric("命中信号", len(verdict.signals))
    cols[3].metric("L1 风控触发", len(verdict.l1_triggered))

    # 操作建议
    if verdict.action == "stop_loss":
        st.error(f"⛔ **操作建议：立即清仓** — {verdict.advice}")
    elif verdict.action == "reduce_all":
        st.error(f"🔴 **操作建议：建议清仓** — {verdict.advice}")
    elif verdict.action == "reduce_half":
        st.warning(f"🟠 **操作建议：减仓 50%** — {verdict.advice}")
    elif verdict.risk_pct >= 35:
        st.info(f"🟡 {verdict.advice}")
    else:
        st.success(f"🟢 {verdict.advice}")

    # L3 分批止盈建议
    scale_out = suggest_scale_out(holding, cur_price)
    if scale_out:
        st.info(f"📊 **L3 仓位建议**：{scale_out.reason}")

    # 信号明细
    if verdict.signals:
        with st.expander(f"信号明细（{len(verdict.signals)} 条命中）", expanded=False):
            for s in verdict.signals:
                params_str = ", ".join(f"{k}={v}" for k, v in s["params"].items())
                pri_emoji = "🔴" if s["priority"] == 1 else "🟠" if s["priority"] == 2 else "🟡"
                st.markdown(f"- {pri_emoji} **{s['label']}** `{params_str}` (权重 {s['weight']})")

    # K 线图 + 成本水平线 + 交易标记
    _render_kline_chart(holding, daily_df, cur_price)


def _render_kline_chart(holding: Holding, daily_df: pd.DataFrame,
                         current_price: float):
    """画 K 线 + MA20/60 + 成本水平线 + 每笔交易点 + 当前价点"""
    if daily_df is None or daily_df.empty:
        return
    date_col = "日期" if "日期" in daily_df.columns else "date"
    close_col = "收盘" if "收盘" in daily_df.columns else "close"
    high_col = "最高" if "最高" in daily_df.columns else "high"
    low_col = "最低" if "最低" in daily_df.columns else "low"
    open_col = "开盘" if "开盘" in daily_df.columns else "open"

    df = daily_df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col)
    df["MA20"] = df[close_col].rolling(20).mean()
    df["MA60"] = df[close_col].rolling(60).mean()

    fig = go.Figure()
    # 蜡烛图
    fig.add_trace(go.Candlestick(
        x=df[date_col],
        open=df[open_col], high=df[high_col],
        low=df[low_col], close=df[close_col],
        increasing_line_color="#d62728", decreasing_line_color="#2ca02c",
        name="K 线", showlegend=False,
    ))
    # MA20 / MA60
    fig.add_trace(go.Scatter(x=df[date_col], y=df["MA20"],
                              name="MA20", line=dict(color="orange", width=1.2)))
    fig.add_trace(go.Scatter(x=df[date_col], y=df["MA60"],
                              name="MA60", line=dict(color="purple", width=1.2, dash="dash")))

    # 成本水平线（绿色，醒目）
    if holding.avg_cost > 0:
        fig.add_hline(y=holding.avg_cost,
                       line_dash="solid", line_color="#1f77b4", line_width=2,
                       annotation_text=f"成本 ¥{holding.avg_cost:.2f}",
                       annotation_position="right")

    # 当前价水平线（红/绿，浮盈状态）
    if current_price > 0:
        pnl_pct = holding.unrealized_pnl_pct(current_price) if holding.avg_cost > 0 else 0
        cur_color = "#d62728" if pnl_pct >= 0 else "#2ca02c"
        fig.add_hline(y=current_price,
                       line_dash="dot", line_color=cur_color, line_width=2,
                       annotation_text=f"现价 ¥{current_price:.2f} ({pnl_pct:+.1f}%)",
                       annotation_position="left")

    # 交易标记：买入 = 绿三角向上；卖出 = 红三角向下
    if holding.transactions:
        buys_x, buys_y, buys_text = [], [], []
        sells_x, sells_y, sells_text = [], [], []
        for tx in holding.transactions:
            try:
                tx_date = pd.to_datetime(tx.date)
            except Exception:
                continue
            if tx.action == "buy":
                buys_x.append(tx_date)
                buys_y.append(tx.price)
                buys_text.append(f"买 {tx.qty}@{tx.price:.2f}")
            elif tx.action == "sell":
                sells_x.append(tx_date)
                sells_y.append(tx.price)
                sells_text.append(f"卖 {tx.qty}@{tx.price:.2f}")
        if buys_x:
            fig.add_trace(go.Scatter(
                x=buys_x, y=buys_y, mode="markers",
                marker=dict(symbol="triangle-up", color="#2ca02c", size=14,
                            line=dict(color="white", width=1.5)),
                name="买入", text=buys_text, hovertemplate="%{text}<extra></extra>",
            ))
        if sells_x:
            fig.add_trace(go.Scatter(
                x=sells_x, y=sells_y, mode="markers",
                marker=dict(symbol="triangle-down", color="#d62728", size=14,
                            line=dict(color="white", width=1.5)),
                name="卖出", text=sells_text, hovertemplate="%{text}<extra></extra>",
            ))
    elif holding.buy_date and holding.avg_cost > 0:
        # 没有 transaction 记录时，用 buy_date + avg_cost 标记
        try:
            buy_dt = pd.to_datetime(holding.buy_date)
            fig.add_trace(go.Scatter(
                x=[buy_dt], y=[holding.avg_cost], mode="markers",
                marker=dict(symbol="triangle-up", color="#2ca02c", size=14,
                            line=dict(color="white", width=1.5)),
                name="买入",
                text=[f"买入 ¥{holding.avg_cost:.2f}"],
                hovertemplate="%{text}<extra></extra>",
            ))
        except Exception:
            pass

    fig.update_layout(
        height=420,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(rangeslider=dict(visible=False)),
        legend=dict(orientation="h", y=1.05),
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)


# 渲染所有持仓
if pf.holdings:
    st.subheader("📋 持仓详情与卖出评估")
    for holding in pf.holdings:
        with st.container(border=True):
            _render_holding_card(holding, regime_multiplier)
    st.divider()


# ========================
# CRUD：添加 / 编辑 / 删除
# ========================
st.subheader("✏️ 持仓管理")

tab_add, tab_edit, tab_delete, tab_txn = st.tabs(
    ["➕ 添加", "📝 编辑", "🗑️ 删除", "📜 添加交易"]
)

with tab_add:
    st.markdown("**🔍 第一步：联想股票**")
    picked = stock_autocomplete(
        label="输入股票名称或代码",
        key="add_holding_autocomplete",
        placeholder="如：晶方 / 603005 / 鸣志",
    )

    if picked:
        st.success(
            f"✅ 已选中 **{picked['name']}** ({picked['code']}) · "
            f"{picked['industry'] or '行业未知'} · "
            f"实时价 ¥{picked['price']:.2f}"
        )

    st.markdown("**📝 第二步：填入持仓信息**")
    with st.form("add_holding_form", clear_on_submit=True):
        a1, a2, a3 = st.columns(3)
        new_code = a1.text_input(
            "代码 *", max_chars=6,
            value=picked["code"] if picked else "",
        ).strip()
        new_name = a2.text_input(
            "名称", value=picked["name"] if picked else "",
        )
        new_market = a3.selectbox(
            "市场",
            options=["a", "hk", "us"],
            index=0 if not picked else ["a", "hk", "us"].index(picked.get("market", "a")),
        )
        b1, b2, b3 = st.columns(3)
        new_qty = b1.number_input("数量", min_value=0, value=100, step=100)
        default_cost = picked["price"] if picked and picked["price"] > 0 else 10.0
        new_cost = b2.number_input(
            "成本价", min_value=0.0, value=float(default_cost),
            step=0.01, format="%.4f",
        )
        new_buy_date = b3.date_input("买入日期")
        c1, c2 = st.columns([2, 1])
        new_tag = c1.text_input(
            "标签",
            value=picked["industry"] if picked and picked.get("industry") else "",
            placeholder="科技/半导体",
        )
        new_notes = c2.text_input("备注")
        submitted = st.form_submit_button("添加", type="primary")
        if submitted:
            if not new_code.isdigit() or len(new_code) != 6:
                st.error("代码必须是 6 位数字")
            elif new_qty == 0 or new_cost <= 0:
                st.error("数量和成本价必须 > 0")
            else:
                h = Holding(
                    code=new_code,
                    name=new_name or new_code,
                    market=new_market,
                    qty=int(new_qty),
                    avg_cost=float(new_cost),
                    buy_date=new_buy_date.isoformat(),
                    notes=new_notes,
                    tag=new_tag,
                )
                ok, msg = mgr.add_holding(h)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)

with tab_edit:
    if not pf.holdings:
        st.info("无持仓可编辑")
    else:
        codes = [f"{h.code} {h.name}" for h in pf.holdings]
        sel = st.selectbox("选择持仓", options=range(len(codes)),
                           format_func=lambda i: codes[i], key="edit_sel")
        h = pf.holdings[sel]
        with st.form("edit_form"):
            e1, e2, e3 = st.columns(3)
            new_qty = e1.number_input("数量", value=h.qty, step=100)
            new_cost = e2.number_input("成本价", value=h.avg_cost, step=0.01, format="%.4f")
            new_buy_date = e3.text_input("买入日期 (YYYY-MM-DD)", value=h.buy_date)
            f1, f2 = st.columns(2)
            new_tag = f1.text_input("标签", value=h.tag)
            new_notes = f2.text_input("备注", value=h.notes)

            st.markdown("**单只覆盖提醒（留空使用全局默认）**")
            g1, g2, g3 = st.columns(3)
            cur_alerts = h.alerts or {}
            new_stop_h = g1.text_input("止损%", value=str(cur_alerts.get("stop_loss_pct", "")))
            new_trail_h = g2.text_input("移动止盈%", value=str(cur_alerts.get("trailing_pct", "")))
            new_threshold_h = g3.text_input("信号告警分%",
                                             value=str(cur_alerts.get("signal_threshold_pct", "")))

            submitted = st.form_submit_button("保存修改", type="primary")
            if submitted:
                updates = {
                    "qty": int(new_qty),
                    "avg_cost": float(new_cost),
                    "buy_date": new_buy_date.strip(),
                    "tag": new_tag,
                    "notes": new_notes,
                }
                # alerts 解析
                new_alerts: dict = {}
                for key, val_str in (("stop_loss_pct", new_stop_h),
                                      ("trailing_pct", new_trail_h),
                                      ("signal_threshold_pct", new_threshold_h)):
                    val_str = val_str.strip()
                    if val_str:
                        try:
                            new_alerts[key] = float(val_str)
                        except ValueError:
                            pass
                # 先 update 普通字段
                ok, msg = mgr.update_holding(h.code, updates)
                if ok:
                    # 再 update alerts（不在 update_holding 通用接口里）
                    pf2 = mgr.load()
                    h2 = pf2.find(h.code)
                    if h2 is not None:
                        h2.alerts = new_alerts
                        mgr.save(pf2)
                    st.success("已保存")
                    st.rerun()
                else:
                    st.error(msg)

with tab_delete:
    if not pf.holdings:
        st.info("无持仓可删除")
    else:
        codes = [f"{h.code} {h.name}" for h in pf.holdings]
        sel = st.selectbox("选择持仓", options=range(len(codes)),
                           format_func=lambda i: codes[i], key="del_sel")
        h = pf.holdings[sel]
        confirm = st.checkbox(f"我确认删除 {h.code} {h.name}", key="del_confirm")
        if st.button("🗑️ 删除", type="secondary"):
            if not confirm:
                st.warning("请先勾选确认")
            else:
                ok, msg = mgr.remove_holding(h.code)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)

with tab_txn:
    if not pf.holdings:
        st.info("无持仓可添加交易")
    else:
        codes = [f"{h.code} {h.name}" for h in pf.holdings]
        sel = st.selectbox("选择持仓", options=range(len(codes)),
                           format_func=lambda i: codes[i], key="txn_sel")
        h = pf.holdings[sel]
        with st.form("txn_form", clear_on_submit=True):
            t1, t2, t3, t4 = st.columns(4)
            tx_action = t1.selectbox("操作", ["buy", "sell"])
            tx_date = t2.date_input("日期")
            tx_qty = t3.number_input("数量", min_value=0, value=100, step=100)
            tx_price = t4.number_input("成交价", min_value=0.0, value=10.0, step=0.01, format="%.4f")
            tx_note = st.text_input("备注")
            recompute = st.checkbox("自动重算持仓数量和成本均价", value=True)
            submitted = st.form_submit_button("添加交易", type="primary")
            if submitted:
                tx = Transaction(
                    action=tx_action,
                    date=tx_date.isoformat(),
                    qty=int(tx_qty),
                    price=float(tx_price),
                    note=tx_note,
                )
                ok, msg = mgr.add_transaction(h.code, tx, recompute=recompute)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)
