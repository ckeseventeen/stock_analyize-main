"""
pages/22_交易台.py — 交易执行（模拟盘优先）

功能：
  - 账户总览（资产/现金/持仓盈亏）
  - 手动下单（市价/限价，卖出建议可从持仓体检带入）
  - 挂单撮合 / 撤单 / 订单历史
  - 券商切换（模拟盘可用；富途/老虎为适配桩）

⚠️ 所有自动化信号只生成建议，下单必须人工点击确认。
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from src.trading import (  # noqa: E402
    OrderSide,
    OrderStatus,
    available_brokers,
    create_broker,
)

st.title("💹 交易台")
st.caption("模拟盘先行验证策略 · 真实券商适配层已预留 · 下单需人工确认")

# ========================
# 券商选择
# ========================
brokers = available_brokers()
broker_name = st.sidebar.selectbox(
    "账户",
    options=list(brokers.keys()),
    format_func=lambda k: brokers[k],
    index=list(brokers.keys()).index("paper") if "paper" in brokers else 0,
)

try:
    broker = create_broker(broker_name)
    broker.connect()
except NotImplementedError as e:
    st.warning(f"该券商适配尚未启用：\n\n```\n{e}\n```\n\n请先使用模拟盘。")
    st.stop()
except Exception as e:
    st.error(f"券商连接失败: {e}")
    st.stop()

if broker.is_live:
    st.error("⚠️ 当前为**真实资金账户**，请谨慎操作！")

# ========================
# 账户总览
# ========================
if hasattr(broker, "summary"):
    s = broker.summary()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("总资产", f"{s['total_assets']:,.0f}")
    c2.metric("可用资金", f"{s['cash']:,.0f}")
    c3.metric("持仓市值", f"{s['market_value']:,.0f}")
    c4.metric("总盈亏", f"{s['total_pnl']:+,.0f}", f"{s['total_pnl_pct']:+.2f}%")
    c5.metric("持仓数", s["position_count"])

# 模拟盘管理
if broker_name == "paper":
    with st.sidebar.expander("⚙️ 模拟盘管理"):
        new_cash = st.number_input("重置后初始资金", value=1_000_000.0,
                                   min_value=10_000.0, step=100_000.0)
        if st.button("🔄 重置模拟盘", help="清空全部持仓和订单记录"):
            if st.session_state.get("_confirm_reset"):
                broker.reset(initial_cash=new_cash)
                del st.session_state["_confirm_reset"]
                st.rerun()
            else:
                st.session_state["_confirm_reset"] = True
                st.warning("再点一次确认重置（不可恢复）")

# ========================
# 下单表单
# ========================
st.markdown("---")
st.subheader("📝 下单")

# 从持仓体检带入卖出建议
_diag = st.session_state.get("_diag_result")
_suggestions = []
if _diag is not None:
    for e in _diag.holdings:
        if e.verdict and e.verdict.action in ("reduce_half", "reduce_all", "stop_loss"):
            _suggestions.append(e)
if _suggestions:
    with st.expander(f"💡 持仓体检有 {len(_suggestions)} 条卖出建议（点击带入下单表单）", expanded=True):
        for e in _suggestions:
            act_label = {"reduce_half": "减仓50%", "reduce_all": "清仓",
                         "stop_loss": "止损"}[e.verdict.action]
            if st.button(f"{e.name}({e.code}) — {act_label} · 风险分 {e.risk_pct:.0f}%",
                         key=f"suggest_{e.code}"):
                st.session_state["_order_prefill"] = {
                    "code": e.code, "name": e.name, "side": "sell",
                    "note": f"sell_engine:{e.verdict.action}",
                }
                st.rerun()

_prefill = st.session_state.get("_order_prefill", {})

with st.form("order_form"):
    f1, f2, f3, f4, f5 = st.columns([2, 2, 1, 1, 2])
    with f1:
        code = st.text_input("代码", value=_prefill.get("code", ""), placeholder="600519")
    with f2:
        name = st.text_input("名称（选填）", value=_prefill.get("name", ""))
    with f3:
        side_label = st.selectbox("方向", options=["买入", "卖出"],
                                  index=1 if _prefill.get("side") == "sell" else 0)
    with f4:
        qty = st.number_input("数量（股）", min_value=100, step=100, value=100)
    with f5:
        price = st.number_input("委托价（0 = 市价）", min_value=0.0, step=0.01, value=0.0,
                                format="%.2f")

    submitted = st.form_submit_button("✅ 确认下单", type="primary", use_container_width=True)

if submitted:
    if not code.strip():
        st.error("请输入股票代码")
    else:
        side = OrderSide.BUY if side_label == "买入" else OrderSide.SELL
        order = broker.place_order(
            code.strip(), side, int(qty), float(price),
            name=name.strip(), note=_prefill.get("note", "manual"),
        )
        st.session_state.pop("_order_prefill", None)
        if order.status == OrderStatus.FILLED:
            st.success(f"✅ 成交：{order.side.value} {order.code} × {order.qty} "
                       f"@ {order.filled_price:.2f}（费用 {order.commission:.2f}）")
        elif order.status == OrderStatus.PENDING:
            st.info(f"⏳ 已挂单：{order.code} × {order.qty} @ {order.price:.2f}，等待价格触及")
        else:
            st.error(f"❌ 下单被拒：{order.note}")

# ========================
# 持仓
# ========================
st.markdown("---")
st.subheader("📦 当前持仓")
positions = broker.get_positions()
if positions:
    df_pos = pd.DataFrame([{
        "代码": p.code, "名称": p.name, "数量": p.qty,
        "成本": round(p.avg_cost, 3), "现价": round(p.market_price, 2),
        "市值": round(p.market_value, 0), "盈亏%": round(p.pnl_pct, 2),
    } for p in positions])
    st.dataframe(df_pos, width="stretch", hide_index=True)
else:
    st.caption("暂无持仓")

# ========================
# 订单
# ========================
st.markdown("---")
head_c1, head_c2 = st.columns([3, 1])
with head_c1:
    st.subheader("📋 订单记录")
with head_c2:
    if hasattr(broker, "check_pending_fills"):
        if st.button("🔁 撮合挂单", help="按最新价检查所有限价挂单是否可成交"):
            n = broker.check_pending_fills()
            st.toast(f"本次成交 {n} 单", icon="🔁")
            if n:
                st.rerun()

orders = broker.list_orders(limit=50)
if orders:
    status_labels = {"pending": "⏳ 挂单", "filled": "✅ 成交",
                     "cancelled": "🚫 已撤", "rejected": "❌ 被拒"}
    df_ord = pd.DataFrame([{
        "时间": o.created_at, "代码": o.code, "名称": o.name,
        "方向": "买入" if o.side == OrderSide.BUY else "卖出",
        "数量": o.qty,
        "委托价": o.price if o.price > 0 else "市价",
        "成交价": o.filled_price if o.filled_price else "—",
        "费用": o.commission if o.commission else "—",
        "状态": status_labels.get(o.status.value, o.status.value),
        "备注": o.note, "订单号": o.order_id,
    } for o in orders])
    st.dataframe(df_ord, width="stretch", hide_index=True)

    pending = [o for o in orders if o.status == OrderStatus.PENDING]
    if pending:
        cancel_id = st.selectbox(
            "撤单",
            options=[o.order_id for o in pending],
            format_func=lambda oid: next(
                f"{o.code} × {o.qty} @ {o.price:.2f} ({oid})"
                for o in pending if o.order_id == oid
            ),
        )
        if st.button("🚫 撤销选中挂单"):
            if broker.cancel_order(cancel_id):
                st.toast("已撤单", icon="🚫")
                st.rerun()
            else:
                st.error("撤单失败（可能已成交）")
else:
    st.caption("暂无订单")

st.markdown("---")
st.caption("💡 模拟盘费用模型：佣金万2.5（最低5元）+ 卖出印花税0.05% + 过户费十万分之一。"
           "不模拟 T+1 / 涨跌停 / 滑点。真实券商适配见 src/trading/live_brokers.py。")
