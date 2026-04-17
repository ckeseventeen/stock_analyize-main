"""
pages/3_价格预警.py — 价格预警规则管理

功能：
  - 表格展示 config/price_alerts.yaml 中的所有规则
  - 表单新增规则（支持 price_below / price_above / pct_change_daily / ma_break 等）
  - 删除选中规则
  - 一键试跑：调用 PriceMonitor 走一遍（默认只用 console 通道）
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from src.visualization.gui.utils import (  # noqa: E402
    MARKET_LABELS,
    PATH_ALERTS,
    PATH_PRICE_ALERTS,
    load_yaml,
    quick_add_stock_widget,
    save_yaml,
)

st.set_page_config(page_title="价格预警", page_icon="🔔", layout="wide")
st.title("🔔 价格预警规则管理")
st.caption(f"规则文件：`{PATH_PRICE_ALERTS}`")


# ========================
# 加载规则
# ========================

cfg = load_yaml(PATH_PRICE_ALERTS) or {}
rules: list[dict] = cfg.get("rules", []) or []
default_cooldown = cfg.get("default_cooldown_hours", 24)


# ========================
# 上半部分：现有规则列表
# ========================

st.subheader(f"📋 现有规则 ({len(rules)} 条)")

if not rules:
    st.info("暂无规则，使用下方表单新增")
else:
    # 展平每条规则的 conditions 以便表格化
    flat_rows = []
    for i, rule in enumerate(rules):
        for cond in rule.get("conditions", []):
            flat_rows.append({
                "#": i,
                "代码": rule.get("code"),
                "名称": rule.get("name", ""),
                "市场": rule.get("market", "a").upper(),
                "条件类型": cond.get("type"),
                "参数": ", ".join(f"{k}={v}" for k, v in cond.items() if k != "type"),
                "冷却(h)": rule.get("cooldown_hours", default_cooldown),
            })

    if flat_rows:
        df_rules = pd.DataFrame(flat_rows)
        st.dataframe(df_rules, width='stretch', hide_index=True)

    # 删除规则
    with st.expander("🗑 删除规则"):
        delete_idx = st.number_input(
            "规则序号 (#)", min_value=0, max_value=max(len(rules) - 1, 0),
            value=0, step=1,
        )
        if st.button("确认删除", type="secondary"):
            removed = rules.pop(int(delete_idx))
            cfg["rules"] = rules
            if save_yaml(PATH_PRICE_ALERTS, cfg):
                st.success(f"已删除规则: {removed.get('name')} ({removed.get('code')})")
                st.rerun()
            else:
                st.error("写入 YAML 失败")


# ========================
# 下半部分：新增规则表单
# ========================

st.markdown("---")
st.subheader("➕ 新增规则")

with st.form("add_rule_form", clear_on_submit=True):
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        market = st.selectbox("市场", list(MARKET_LABELS.keys()),
                              format_func=lambda k: MARKET_LABELS[k])
    with col2:
        code = st.text_input("股票代码", placeholder="600519")
    with col3:
        name = st.text_input("股票名称", placeholder="贵州茅台")

    st.markdown("**触发条件**（任意一个满足即推送）")
    cond_type = st.selectbox(
        "类型",
        options=["price_below", "price_above", "pct_change_daily",
                 "pct_from_cost", "ma_break"],
        help="price_below/above: 绝对价阈值；pct_*: 涨跌幅；ma_break: 均线突破",
    )

    # 根据类型动态渲染参数表单
    cond_params: dict = {"type": cond_type}
    c1, c2, c3 = st.columns(3)
    if cond_type in ("price_below", "price_above"):
        with c1:
            cond_params["value"] = st.number_input("阈值价格", value=100.0, step=0.1)
    elif cond_type in ("pct_change_daily", "pct_from_cost"):
        with c1:
            cond_params["threshold"] = st.number_input(
                "阈值(%)", value=-5.0, step=0.5,
                help="正数=涨幅超，负数=跌幅超",
            )
        if cond_type == "pct_from_cost":
            with c2:
                cost = st.number_input("成本价", value=100.0, step=0.1)
                if cost > 0:
                    cond_params["cost"] = cost
    elif cond_type == "ma_break":
        with c1:
            cond_params["ma"] = st.number_input("均线周期", value=60, step=1, min_value=5)
        with c2:
            cond_params["direction"] = st.selectbox(
                "方向", options=["below", "above"],
                format_func=lambda d: "跌破" if d == "below" else "突破",
            )

    cooldown = st.number_input("冷却窗口（小时）", value=int(default_cooldown), min_value=1)

    submit = st.form_submit_button("✅ 添加规则", type="primary")

    if submit:
        if not code:
            st.error("股票代码不能为空")
        else:
            new_rule = {
                "code": code.strip(),
                "market": market,
                "name": (name or code).strip(),
                "conditions": [cond_params],
                "cooldown_hours": int(cooldown),
            }
            rules.append(new_rule)
            cfg["rules"] = rules
            if save_yaml(PATH_PRICE_ALERTS, cfg):
                st.success(f"已添加规则: {new_rule['name']} ({code})")
                # 记下来供下方"加入关注"预填
                st.session_state["_alert_last_code"] = new_rule["code"]
                st.session_state["_alert_last_name"] = new_rule["name"]
                st.session_state["_alert_last_market"] = market
                st.rerun()
            else:
                st.error("写入 YAML 失败（ruamel.yaml 未安装时注释可能丢失，但规则仍写入）")


# ========================
# 就地加入关注列表
# ========================
st.markdown("---")
st.subheader("⭐ 同步加入关注列表")
st.caption("当前规则的股票可一键加入 a_stock.yaml / hk_stock.yaml / us_stock.yaml，便于估值分析页直接选中。")

_prefill_code = st.session_state.get("_alert_last_code", "")
_prefill_name = st.session_state.get("_alert_last_name", "")
_prefill_market = st.session_state.get("_alert_last_market", "a")

if quick_add_stock_widget(
    key_prefix="page3_alerts",
    default_market=_prefill_market,
    default_code=_prefill_code,
    default_name=_prefill_name,
    expanded=bool(_prefill_code),
):
    # 清掉一次性预填，避免下次进入还带旧值
    for k in ("_alert_last_code", "_alert_last_name", "_alert_last_market"):
        st.session_state.pop(k, None)
    st.rerun()


# ========================
# 试跑按钮
# ========================

st.markdown("---")
st.subheader("🚀 试跑监控器")
st.caption("一次性执行 PriceMonitor.run()，实时拉行情+判定+推送。")

trigger = st.button("立即执行一次", type="secondary")

if trigger:
    if not rules:
        st.warning("暂无规则，先添加再试跑")
    else:
        with st.spinner("正在拉取行情并判定..."):
            try:
                from src.automation.alert import AlertStateStore, build_channels
                from src.automation.monitor.price_monitor import PriceMonitor

                alerts_cfg = load_yaml(PATH_ALERTS) or {}
                channels = build_channels(alerts_cfg)
                store = AlertStateStore()

                monitor = PriceMonitor(
                    rules=rules,
                    channels=channels,
                    state_store=store,
                    cooldown_hours=int(default_cooldown),
                )
                events = monitor.collect_events()
            except Exception as e:
                st.error(f"试跑失败: {e}")
                st.exception(e)
                st.stop()

        if not events:
            st.info("本次无触发事件（行情未满足任何规则）")
        else:
            st.success(f"触发 {len(events)} 个事件：")
            df_events = pd.DataFrame([
                {"股票": f"{e.stock_name} ({e.stock_code})",
                 "类型": e.event_type,
                 "标题": e.title,
                 "内容": e.body}
                for e in events
            ])
            st.dataframe(df_events, width='stretch', hide_index=True)
