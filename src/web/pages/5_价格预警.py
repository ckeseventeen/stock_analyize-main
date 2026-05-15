"""
pages/5_价格预警.py — 买入/卖出信号预警

设计：
  - 只保留"技术信号"作为预警触发条件（28 个 CONDITION_REGISTRY 桥接）
  - 顶部 Tab：📈 买入预警 / 📉 卖出预警
  - 每条规则可以是「单股盯盘」（填 code）或「批量扫描」（填 scopes）
  - 调度器每 15 分钟跑一次（交易时段）

去掉了原"价格触线"类规则（price_below/above/pct_change_daily/pct_from_cost/ma_break），
这些场景请用对应的技术信号代替（如：跌到目标价 → ma_break 或 rsi_oversold）。
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from src.analysis.screening.conditions import (  # noqa: E402
    CONDITION_LABELS,
    CONDITION_REGISTRY,
    SIGNAL_DIRECTION,
    get_signal_direction,
    signal_emoji,
)
from src.analysis.screening.config_schema import (  # noqa: E402
    _PARAM_MAP,
    SPOT_ONLY_TYPES,
)
from src.analysis.screening.data_provider import (  # noqa: E402
    ScreenerDataProvider as _SDP,
)
from src.web.components.confirm import confirm_action  # noqa: E402
from src.web.utils import (  # noqa: E402
    MARKET_LABELS,
    PATH_ALERTS,
    PATH_PRICE_ALERTS,
    atomic_save_yaml,
    load_yaml,
)

st.set_page_config(page_title="买卖预警", page_icon="🔔", layout="wide")
st.title("🔔 买卖信号预警")
st.caption(
    f"📈 买入信号 + 📉 卖出信号，全基于技术指标。规则文件：`{PATH_PRICE_ALERTS}`"
)

# ========================
# 加载配置
# ========================
cfg = load_yaml(PATH_PRICE_ALERTS) or {}
buy_alerts: list[dict] = cfg.get("buy_alerts", []) or []
sell_alerts: list[dict] = cfg.get("sell_alerts", []) or []
default_cooldown = int(cfg.get("default_cooldown_hours", 24))


# ========================
# 工具：渲染规则列表
# ========================

def _render_rule_list(rules: list[dict], direction: str) -> None:
    """展示 buy_alerts 或 sell_alerts 列表"""
    if not rules:
        st.info(f"还没有{'买入' if direction == 'buy' else '卖出'}预警规则，"
                f"用下方表单创建一条。")
        return
    flat = []
    for i, r in enumerate(rules):
        sig = r.get("signal", {}) or {}
        sig_type = str(sig.get("type", ""))
        sig_direction_eval = get_signal_direction(sig_type, sig.get("params") or {})
        if r.get("code"):
            scope_desc = f"单股 {r.get('code')}"
        else:
            scope_desc = ", ".join(r.get("scopes") or [])

        flat.append({
            "#": i, "ID": r.get("id", "?"),
            "名称": r.get("name", "?"),
            "范围": scope_desc,
            "信号": f"{signal_emoji(sig_direction_eval)} {sig_type}",
            "参数": ", ".join(f"{k}={v}" for k, v in (sig.get("params") or {}).items()) or "默认",
            "冷却(h)": r.get("cooldown_hours", default_cooldown),
        })
    st.dataframe(pd.DataFrame(flat), width="stretch", hide_index=True)


def _render_add_rule_form(direction: str) -> None:
    """新增规则表单 - direction: 'buy' / 'sell'"""
    list_key = "buy_alerts" if direction == "buy" else "sell_alerts"
    rules = buy_alerts if direction == "buy" else sell_alerts

    emoji = "📈" if direction == "buy" else "📉"
    dir_label = "买入" if direction == "buy" else "卖出"

    with st.form(f"add_{direction}_form", clear_on_submit=True):
        st.markdown(f"**{emoji} 新增{dir_label}预警**")

        c1, c2 = st.columns([1, 2])
        with c1:
            rule_id = st.text_input(
                "规则 ID（唯一）",
                key=f"{direction}_id",
                placeholder="gzmt_oversold",
            )
        with c2:
            rule_name = st.text_input(
                "规则名称",
                key=f"{direction}_name",
                placeholder="贵州茅台 RSI 超卖",
            )

        # 模式选择：单股 vs 批量
        mode = st.radio(
            "范围",
            options=["single", "batch"],
            format_func=lambda m: "🎯 单股盯盘" if m == "single" else "📊 批量扫描",
            horizontal=True,
            key=f"{direction}_mode",
        )

        code = ""
        market = "a"
        scopes: list[str] = []
        max_results = 20
        max_codes = 500

        if mode == "single":
            cs1, cs2, cs3 = st.columns([1, 2, 1])
            with cs1:
                market = st.selectbox(
                    "市场",
                    options=list(MARKET_LABELS.keys()),
                    format_func=lambda k: MARKET_LABELS.get(k, k),
                    key=f"{direction}_market",
                )
            with cs2:
                code = st.text_input(
                    "股票代码",
                    key=f"{direction}_code",
                    placeholder="600519",
                )
        else:
            scope_options = list(_SDP.SCOPE_DEFINITIONS.keys()) + ["关注列表"]
            scope_labels = st.multiselect(
                "股票池（多选取并集）",
                options=scope_options,
                default=["沪深300"],
                key=f"{direction}_scopes",
            )
            for lbl in scope_labels:
                if lbl == "关注列表":
                    scopes.append("watchlist")
                else:
                    scopes.append(_SDP.SCOPE_DEFINITIONS[lbl])
            cm1, cm2 = st.columns(2)
            with cm1:
                max_results = st.number_input(
                    "推送最多列出", min_value=1, max_value=200, value=20,
                    key=f"{direction}_maxresults",
                )
            with cm2:
                max_codes = st.number_input(
                    "单次扫描上限", min_value=50, max_value=5000, value=500,
                    key=f"{direction}_maxcodes",
                )

        # 信号选择（按方向过滤）
        st.markdown(f"**{emoji} 选择{dir_label}信号**")
        wanted_dirs = ("buy", "neutral") if direction == "buy" else ("sell", "neutral")
        available_signals = [
            k for k in CONDITION_REGISTRY.keys()
            if k not in SPOT_ONLY_TYPES
            and SIGNAL_DIRECTION.get(k, "unknown") in wanted_dirs
        ]
        # 优先纯粹方向信号，中性的排后面
        ordered = sorted(
            available_signals,
            key=lambda k: (0 if SIGNAL_DIRECTION.get(k) == direction else 1, k),
        )

        signal_type = st.selectbox(
            "信号类型",
            options=ordered,
            format_func=lambda k: (
                f"{CONDITION_LABELS.get(k, k)} ({k}, {SIGNAL_DIRECTION.get(k, '?')})"
            ),
            key=f"{direction}_signal",
        )

        # 信号参数
        st.markdown("**信号参数（留空 = 默认值）**")
        sig_params: dict = {}
        pm = _PARAM_MAP.get(signal_type, {})
        if pm:
            cols = st.columns(min(3, max(1, len(pm))))
            for i, (yaml_key, _ikey) in enumerate(pm.items()):
                with cols[i % len(cols)]:
                    if any(k in yaml_key for k in ("lookback", "period", "order",
                                                    "touches", "consecutive",
                                                    "ma_period", "n", "m1", "m2",
                                                    "days")):
                        v = st.number_input(yaml_key, value=0, step=1, min_value=0,
                                            key=f"{direction}_{yaml_key}",
                                            help="留 0 = 用默认值")
                        if v > 0:
                            sig_params[yaml_key] = int(v)
                    elif any(k in yaml_key for k in ("threshold", "pct", "ratio",
                                                     "multiple", "std_dev")):
                        v = st.number_input(yaml_key, value=0.0, step=0.1,
                                            key=f"{direction}_{yaml_key}",
                                            help="留 0 = 用默认值")
                        if v != 0:
                            sig_params[yaml_key] = float(v)
                    else:
                        v = st.text_input(yaml_key, value="",
                                          key=f"{direction}_{yaml_key}",
                                          help="留空 = 默认")
                        if v.strip():
                            sig_params[yaml_key] = v.strip()
        else:
            st.caption("（此信号无可配参数）")

        cooldown = st.number_input(
            "冷却（小时）",
            min_value=1, max_value=720,
            value=default_cooldown,
            key=f"{direction}_cd",
        )

        submit = st.form_submit_button(f"✅ 添加{dir_label}预警", type="primary")
        if submit:
            if not rule_id.strip():
                st.error("规则 ID 不能为空")
            elif mode == "single" and not code.strip():
                st.error("单股盯盘必须填股票代码")
            elif mode == "batch" and not scopes:
                st.error("批量扫描必须至少选一个股票池")
            elif any(r.get("id") == rule_id.strip() for r in rules):
                st.error(f"ID '{rule_id}' 已存在")
            else:
                new_rule: dict = {
                    "id": rule_id.strip(),
                    "name": rule_name.strip() or rule_id.strip(),
                    "signal": {"type": signal_type,
                              **({"params": sig_params} if sig_params else {})},
                    "cooldown_hours": int(cooldown),
                }
                if mode == "single":
                    new_rule["code"] = code.strip()
                    new_rule["market"] = market
                else:
                    new_rule["scopes"] = scopes
                    new_rule["max_results"] = int(max_results)
                    new_rule["max_codes"] = int(max_codes)

                rules.append(new_rule)
                cfg[list_key] = rules
                if atomic_save_yaml(PATH_PRICE_ALERTS, cfg):
                    st.success(f"已添加：{new_rule['name']}")
                    st.rerun()
                else:
                    st.error("写入 YAML 失败")


def _render_delete_form(direction: str) -> None:
    """删除规则表单"""
    list_key = "buy_alerts" if direction == "buy" else "sell_alerts"
    rules = buy_alerts if direction == "buy" else sell_alerts
    if not rules:
        return
    with st.expander(f"🗑 删除{'买入' if direction == 'buy' else '卖出'}预警"):
        del_idx = st.selectbox(
            "选择要删除的规则",
            options=range(len(rules)),
            format_func=lambda i: f"#{i} {rules[i].get('name', '?')}",
            key=f"{direction}_del_sel",
        )
        result = confirm_action(
            f"{direction}_del",
            f"确定删除 #{del_idx}（{rules[del_idx].get('name')}）？",
            "🗑️ 删除选中规则",
        )
        if result is True:
            rules.pop(int(del_idx))
            cfg[list_key] = rules
            if atomic_save_yaml(PATH_PRICE_ALERTS, cfg):
                st.success("已删除")
                st.rerun()
            else:
                st.error("写入失败")


# ========================
# 顶部 Tab
# ========================

tab_buy, tab_sell, tab_run = st.tabs([
    f"📈 买入预警（{len(buy_alerts)} 条）",
    f"📉 卖出预警（{len(sell_alerts)} 条）",
    "🚀 立即扫描",
])

with tab_buy:
    st.subheader(f"📈 买入预警规则 ({len(buy_alerts)} 条)")
    _render_rule_list(buy_alerts, "buy")
    _render_delete_form("buy")
    st.markdown("---")
    _render_add_rule_form("buy")

with tab_sell:
    st.subheader(f"📉 卖出预警规则 ({len(sell_alerts)} 条)")
    _render_rule_list(sell_alerts, "sell")
    _render_delete_form("sell")
    st.markdown("---")
    _render_add_rule_form("sell")

with tab_run:
    st.subheader("🚀 立即扫描全部规则")
    st.caption(
        "一次性跑一遍 buy_alerts + sell_alerts，"
        "命中后通过 alerts.yaml 配置的通道推送。"
    )
    if st.button("立即执行扫描", type="primary"):
        if not buy_alerts and not sell_alerts:
            st.warning("还没有任何预警规则")
        else:
            with st.spinner("正在扫描..."):
                try:
                    from src.automation.alert import AlertStateStore, build_channels
                    from src.automation.monitor.buy_sell_alerts import (
                        BuySellAlertMonitor,
                    )
                    alerts_cfg = load_yaml(PATH_ALERTS) or {}
                    channels = build_channels(alerts_cfg)
                    store = AlertStateStore()
                    monitor = BuySellAlertMonitor(
                        buy_alerts=buy_alerts,
                        sell_alerts=sell_alerts,
                        channels=channels,
                        state_store=store,
                        cooldown_hours=default_cooldown,
                    )
                    events = monitor.collect_events()
                except Exception as e:
                    st.error(f"扫描失败: {e}")
                    st.exception(e)
                    st.stop()
            if not events:
                st.info("本次无规则命中")
            else:
                st.success(f"命中 {len(events)} 条规则：")
                for ev in events:
                    with st.expander(ev.title, expanded=True):
                        st.code(ev.body, language="text")
