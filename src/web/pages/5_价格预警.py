"""
pages/5_价格预警.py — 买入/卖出信号预警（操作流程优化版）

UX 设计要点：
  1. **快捷预设**：4-5 个常用买卖点一键添加，不用填长表单
  2. **行内操作**：每条规则旁有 ✅启用 / ✏️编辑 / 🧪测试 / 🗑️删除
  3. **enabled 字段**：临时禁用规则不用删除
  4. **测试单条**：单条规则可独立试跑，看是否命中
  5. **空状态引导**：没规则时直接列出推荐
  6. **表单分段**：基础信息 + 范围 + 信号 + 参数，视觉层次清晰
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

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
from src.web.utils import (  # noqa: E402
    MARKET_LABELS,
    PATH_ALERTS,
    PATH_PRICE_ALERTS,
    atomic_save_yaml,
    load_yaml,
)

st.title("🔔 买卖信号预警")
st.caption(
    f"📈 买入信号 + 📉 卖出信号，全基于技术指标。规则文件：`{PATH_PRICE_ALERTS}`"
)

# ============================================================================
# 加载配置 + 工具函数
# ============================================================================

cfg = load_yaml(PATH_PRICE_ALERTS) or {}
buy_alerts: list[dict] = cfg.get("buy_alerts", []) or []
sell_alerts: list[dict] = cfg.get("sell_alerts", []) or []
default_cooldown = int(cfg.get("default_cooldown_hours", 24))


def _save_alerts(buy: list[dict], sell: list[dict]) -> bool:
    """统一保存到 yaml"""
    cfg["buy_alerts"] = buy
    cfg["sell_alerts"] = sell
    return atomic_save_yaml(PATH_PRICE_ALERTS, cfg)


def _toast(msg: str, *, success: bool = True) -> None:
    """统一 toast 反馈"""
    if success:
        st.toast(msg, icon="✅")
    else:
        st.toast(msg, icon="⚠️")


# ============================================================================
# 快捷预设
# ============================================================================

QUICK_PRESETS = {
    "buy": [
        {
            "key": "rsi_oversold",
            "label": "📈 RSI 超卖反弹",
            "desc": "沪深300 RSI(14) < 30 → 超卖买入信号",
            "rule": {
                "scopes": ["csi300"],
                "signal": {"type": "rsi_oversold",
                           "params": {"threshold": 30, "period": 14}},
            },
        },
        {
            "key": "weekly_macd_div",
            "label": "📈 周线 MACD 底背离",
            "desc": "沪深300 周线 MACD 底背离 → 中长线反转",
            "rule": {
                "scopes": ["csi300"],
                "signal": {"type": "weekly_macd_divergence",
                           "params": {"lookback_bars": 60}},
            },
        },
        {
            "key": "ma_gold_cross",
            "label": "📈 5/20 日均线金叉",
            "desc": "沪深300 短期金叉 → 短线趋势启动",
            "rule": {
                "scopes": ["csi300"],
                "signal": {"type": "ma_gold_cross",
                           "params": {"fast_period": 5, "slow_period": 20}},
            },
        },
        {
            "key": "box_breakout",
            "label": "📈 箱体突破",
            "desc": "沪深300 20 日箱体放量突破",
            "rule": {
                "scopes": ["csi300"],
                "signal": {"type": "box_breakout_volume",
                           "params": {"lookback_bars": 20, "breakout_pct": 0.02}},
            },
        },
    ],
    "sell": [
        {
            "key": "rsi_overbought",
            "label": "📉 RSI 超买",
            "desc": "关注列表 RSI(14) > 70 → 止盈信号",
            "rule": {
                "scopes": ["watchlist"],
                "signal": {"type": "rsi_overbought",
                           "params": {"threshold": 70, "period": 14}},
            },
        },
        {
            "key": "ma_death_cross",
            "label": "📉 5/20 日均线死叉",
            "desc": "关注列表均线死叉 → 趋势反转",
            "rule": {
                "scopes": ["watchlist"],
                "signal": {"type": "ma_death_cross",
                           "params": {"fast_period": 5, "slow_period": 20}},
            },
        },
        {
            "key": "trailing_stop",
            "label": "📉 移动止损（5%）",
            "desc": "关注列表收盘价从近期高点回落 5% → 止盈",
            "rule": {
                "scopes": ["watchlist"],
                "signal": {"type": "trailing_stop",
                           "params": {"callback_pct": 5.0, "lookback": 60}},
            },
        },
        {
            "key": "volume_top_div",
            "label": "📉 量价顶背离",
            "desc": "关注列表价格创新高但成交量萎缩 → 趋势衰竭",
            "rule": {
                "scopes": ["watchlist"],
                "signal": {"type": "volume_price_divergence",
                           "params": {"lookback_bars": 30, "direction": "top"}},
            },
        },
    ],
}


def _add_preset(preset: dict, direction: str) -> None:
    """加预设到 buy/sell_alerts；ID 自动生成（已存在则跳过）"""
    target = buy_alerts if direction == "buy" else sell_alerts
    auto_id = preset["key"]
    if any(r.get("id") == auto_id for r in target):
        _toast(f"规则 '{auto_id}' 已存在，未重复添加", success=False)
        return
    new_rule = {
        "id": auto_id,
        "name": preset["label"].lstrip("📈📉 "),
        "enabled": True,
        "cooldown_hours": default_cooldown,
        "max_results": 20,
        "max_codes": 500,
        **preset["rule"],
    }
    target.append(new_rule)
    if _save_alerts(buy_alerts, sell_alerts):
        _toast(f"已添加：{preset['label']}")
        st.rerun()
    else:
        _toast("写入 YAML 失败", success=False)


def _render_quick_presets(direction: str) -> None:
    """快捷预设按钮组"""
    presets = QUICK_PRESETS[direction]
    st.markdown("**⚡ 快捷添加**（默认沪深300/关注列表 + 24h 冷却）")
    cols = st.columns(len(presets))
    for i, p in enumerate(presets):
        with cols[i]:
            if st.button(p["label"], key=f"preset_{direction}_{p['key']}",
                         help=p["desc"], width="stretch"):
                _add_preset(p, direction)


# ============================================================================
# 规则列表（带行内操作）
# ============================================================================

def _toggle_enabled(direction: str, idx: int) -> None:
    target = buy_alerts if direction == "buy" else sell_alerts
    target[idx]["enabled"] = not target[idx].get("enabled", True)
    if _save_alerts(buy_alerts, sell_alerts):
        state = "启用" if target[idx]["enabled"] else "禁用"
        _toast(f"{state}：{target[idx].get('name', target[idx].get('id'))}")
        st.rerun()


def _delete_rule(direction: str, idx: int) -> None:
    target = buy_alerts if direction == "buy" else sell_alerts
    removed = target.pop(idx)
    if _save_alerts(buy_alerts, sell_alerts):
        _toast(f"已删除：{removed.get('name', removed.get('id'))}")
        st.rerun()


def _start_edit(direction: str, idx: int) -> None:
    """把规则数据放进 session_state 让表单 pre-fill"""
    target = buy_alerts if direction == "buy" else sell_alerts
    st.session_state[f"editing_{direction}"] = idx
    st.session_state[f"editing_data_{direction}"] = dict(target[idx])
    # FE-4 修复：不在此处 toast（rerun 会吞掉），改为设置 flag
    st.session_state[f"edit_toast_{direction}"] = True


def _test_rule(direction: str, idx: int) -> None:
    """单条规则试跑，结果存 session_state 给下方展示"""
    target = buy_alerts if direction == "buy" else sell_alerts
    rule = target[idx]
    with st.spinner(f"测试 '{rule.get('name')}'..."):
        try:
            from src.services import alert_service as asvc
            event = asvc.test_rule(rule, direction)
        except Exception as e:
            st.session_state[f"test_result_{direction}_{idx}"] = {
                "error": f"{type(e).__name__}: {e}",
            }
            return
    st.session_state[f"test_result_{direction}_{idx}"] = {
        "event": event,
        "rule_name": rule.get("name", rule.get("id")),
    }


def _render_rules_list(rules: list[dict], direction: str) -> None:
    """渲染规则列表 + 行内操作按钮"""
    # FE-4 修复：在 rerun 后显示编辑 toast
    toast_key = f"edit_toast_{direction}"
    if st.session_state.pop(toast_key, False):
        _toast("已加载到编辑表单（下方）")

    if not rules:
        _render_empty_state(direction)
        return

    # 表头
    header_cols = st.columns([0.6, 2.5, 1.5, 2, 0.8, 2.5])
    header_cols[0].markdown("**状态**")
    header_cols[1].markdown("**名称**")
    header_cols[2].markdown("**范围**")
    header_cols[3].markdown("**信号**")
    header_cols[4].markdown("**冷却**")
    header_cols[5].markdown("**操作**")

    for i, r in enumerate(rules):
        sig = r.get("signal", {}) or {}
        sig_type = str(sig.get("type", ""))
        sig_dir = get_signal_direction(sig_type, sig.get("params") or {})
        sig_label = f"{signal_emoji(sig_dir)} {CONDITION_LABELS.get(sig_type, sig_type)}"

        if r.get("code"):
            scope_desc = f"🎯 {r.get('code')}"
        else:
            scope_desc = "📊 " + "+".join(r.get("scopes") or [])

        enabled = r.get("enabled", True)

        cols = st.columns([0.6, 2.5, 1.5, 2, 0.8, 2.5])

        # 状态：✅/⏸
        with cols[0]:
            label = "✅" if enabled else "⏸"
            if st.button(label, key=f"toggle_{direction}_{i}",
                         help="点击切换启用/禁用"):
                _toggle_enabled(direction, i)

        # 名称
        with cols[1]:
            name_text = r.get("name", "(未命名)")
            if not enabled:
                name_text = f"~~{name_text}~~"   # 划线表示禁用
            st.markdown(name_text)
            st.caption(f"ID: `{r.get('id', '?')}`")

        # 范围
        with cols[2]:
            st.markdown(scope_desc)

        # 信号
        with cols[3]:
            st.markdown(sig_label)
            params = sig.get("params") or {}
            if params:
                st.caption(", ".join(f"{k}={v}" for k, v in params.items()))

        # 冷却
        with cols[4]:
            st.markdown(f"{r.get('cooldown_hours', default_cooldown)}h")

        # 操作
        with cols[5]:
            bc1, bc2, bc3 = st.columns(3)
            with bc1:
                if st.button("✏️", key=f"edit_{direction}_{i}", help="编辑"):
                    _start_edit(direction, i)
                    st.rerun()
            with bc2:
                if st.button("🧪", key=f"test_{direction}_{i}", help="测试单条"):
                    _test_rule(direction, i)
                    st.rerun()
            with bc3:
                # 二次确认用 session_state 状态机
                confirm_key = f"confirm_delete_{direction}_{i}"
                if st.session_state.get(confirm_key):
                    if st.button("⚠️确定", key=f"confirm_btn_{direction}_{i}"):
                        _delete_rule(direction, i)
                        st.session_state[confirm_key] = False
                else:
                    if st.button("🗑️", key=f"del_{direction}_{i}", help="删除（点 2 次）"):
                        st.session_state[confirm_key] = True
                        st.rerun()

        # 测试结果（如有）
        test_key = f"test_result_{direction}_{i}"
        if test_key in st.session_state:
            tr = st.session_state[test_key]
            with st.container():
                if tr.get("error"):
                    st.error(f"测试失败：{tr['error']}")
                elif tr.get("event") is None:
                    st.info(f"🧪 测试结果：当前不满足触发条件（{tr['rule_name']}）")
                else:
                    ev = tr["event"]
                    with st.expander(f"🧪 命中！{ev.title}", expanded=True):
                        st.code(ev.body, language="text")
                if st.button("关闭", key=f"close_test_{direction}_{i}"):
                    del st.session_state[test_key]
                    st.rerun()

        st.divider()


# ============================================================================
# 空状态引导
# ============================================================================

def _render_empty_state(direction: str) -> None:
    """没规则时的空状态：直接列推荐预设"""
    emoji = "📈" if direction == "buy" else "📉"
    tag = "买入" if direction == "buy" else "卖出"
    st.info(
        f"👋 还没有{emoji} {tag}预警规则。\n\n"
        f"可以从上方「⚡ 快捷添加」一键创建常用规则，"
        f"也可以下方「➕ 添加自定义规则」从零配置。"
    )


# ============================================================================
# 编辑/添加表单（合并：edit_mode = True/False）
# ============================================================================

def _render_form(direction: str) -> None:
    """添加或编辑规则的统一表单"""
    edit_idx = st.session_state.get(f"editing_{direction}")
    edit_data = st.session_state.get(f"editing_data_{direction}") or {}
    is_editing = edit_idx is not None

    target = buy_alerts if direction == "buy" else sell_alerts
    emoji = "📈" if direction == "buy" else "📉"
    tag = "买入" if direction == "buy" else "卖出"

    expander_label = (
        f"✏️ 编辑 #{edit_idx} - {edit_data.get('name', '?')}"
        if is_editing else
        f"➕ 添加自定义{tag}预警"
    )

    with st.expander(expander_label, expanded=is_editing):
        if is_editing:
            if st.button("❌ 取消编辑", key=f"cancel_edit_{direction}"):
                _clear_edit_state(direction)
                st.rerun()

        with st.form(f"form_{direction}", clear_on_submit=not is_editing):
            # ───── 基础信息 ─────
            st.markdown("**1️⃣ 基础信息**")
            c1, c2 = st.columns([1, 2])
            with c1:
                rule_id = st.text_input(
                    "规则 ID（唯一）",
                    value=edit_data.get("id", ""),
                    placeholder="gzmt_oversold",
                    disabled=is_editing,
                    key=f"f_id_{direction}",
                )
            with c2:
                rule_name = st.text_input(
                    "规则名称",
                    value=edit_data.get("name", ""),
                    placeholder=f"{emoji} 贵州茅台 RSI 超卖",
                    key=f"f_name_{direction}",
                )

            # ───── 范围 ─────
            st.markdown("**2️⃣ 范围**")
            existing_code = str(edit_data.get("code", ""))
            existing_scopes = edit_data.get("scopes") or []
            default_mode = "single" if existing_code else "batch"
            mode = st.radio(
                "扫描模式",
                options=["single", "batch"],
                format_func=lambda m: "🎯 单股盯盘（填一只股票）" if m == "single"
                                       else "📊 批量扫描（选股票池）",
                horizontal=True,
                key=f"f_mode_{direction}",
                index=0 if default_mode == "single" else 1,
            )

            code, market, scopes_keys, max_results, max_codes = "", "a", [], 20, 500
            if mode == "single":
                cs1, cs2 = st.columns([1, 2])
                with cs1:
                    market = st.selectbox(
                        "市场",
                        options=list(MARKET_LABELS.keys()),
                        format_func=lambda k: MARKET_LABELS.get(k, k),
                        index=list(MARKET_LABELS.keys()).index(
                            edit_data.get("market", "a")
                        ) if edit_data.get("market") in MARKET_LABELS else 0,
                        key=f"f_market_{direction}",
                    )
                with cs2:
                    code = st.text_input(
                        "股票代码", value=existing_code,
                        placeholder="600519",
                        key=f"f_code_{direction}",
                    )
            else:
                scope_options = list(_SDP.SCOPE_DEFINITIONS.keys()) + ["关注列表"]
                # 反向映射 existing_scopes (内部 key) → 显示 label
                reverse_map = {v: k for k, v in _SDP.SCOPE_DEFINITIONS.items()}
                reverse_map["watchlist"] = "关注列表"
                default_scope_labels = [
                    reverse_map.get(s, s) for s in existing_scopes
                    if s in reverse_map
                ] or ["沪深300"]
                selected_labels = st.multiselect(
                    "股票池（多选取并集）",
                    options=scope_options,
                    default=default_scope_labels,
                    key=f"f_scopes_{direction}",
                )
                for lbl in selected_labels:
                    if lbl == "关注列表":
                        scopes_keys.append("watchlist")
                    else:
                        scopes_keys.append(_SDP.SCOPE_DEFINITIONS[lbl])
                cm1, cm2 = st.columns(2)
                with cm1:
                    max_results = st.number_input(
                        "推送最多列出", min_value=1, max_value=200,
                        value=int(edit_data.get("max_results", 20)),
                        key=f"f_max_results_{direction}",
                    )
                with cm2:
                    max_codes = st.number_input(
                        "单次扫描上限", min_value=50, max_value=5000,
                        value=int(edit_data.get("max_codes", 500)),
                        key=f"f_max_codes_{direction}",
                    )

            # ───── 信号 ─────
            st.markdown(f"**3️⃣ {emoji} 选择{tag}信号**")
            wanted_dirs = ("buy", "neutral") if direction == "buy" else ("sell", "neutral")
            available_signals = [
                k for k in CONDITION_REGISTRY.keys()
                if k not in SPOT_ONLY_TYPES
                and SIGNAL_DIRECTION.get(k, "unknown") in wanted_dirs
            ]
            ordered = sorted(
                available_signals,
                key=lambda k: (0 if SIGNAL_DIRECTION.get(k) == direction else 1, k),
            )
            existing_sig = edit_data.get("signal", {}).get("type", ordered[0] if ordered else "")
            sig_index = ordered.index(existing_sig) if existing_sig in ordered else 0

            signal_type = st.selectbox(
                "信号类型",
                options=ordered,
                index=sig_index,
                format_func=lambda k: (
                    f"{CONDITION_LABELS.get(k, k)} "
                    f"({k} · {SIGNAL_DIRECTION.get(k, '?')})"
                ),
                key=f"f_sig_{direction}",
            )

            # ───── 信号参数 ─────
            st.markdown("**4️⃣ 信号参数**（留空 = 默认值）")
            sig_params: dict = {}
            existing_params = edit_data.get("signal", {}).get("params") or {}
            pm = _PARAM_MAP.get(signal_type, {})
            if pm:
                cols = st.columns(min(3, max(1, len(pm))))
                for i, (yaml_key, _ikey) in enumerate(pm.items()):
                    with cols[i % len(cols)]:
                        default_v = existing_params.get(yaml_key, 0)
                        is_int = any(k in yaml_key for k in (
                            "lookback", "period", "order", "touches",
                            "consecutive", "ma_period", "n", "m1", "m2", "days",
                        ))
                        is_float = any(k in yaml_key for k in (
                            "threshold", "pct", "ratio", "multiple", "std_dev",
                        ))
                        if is_int:
                            v = st.number_input(
                                yaml_key,
                                value=int(default_v) if default_v else 0,
                                step=1, min_value=0,
                                key=f"f_param_{direction}_{yaml_key}",
                                help="留 0 = 默认",
                            )
                            if v > 0:
                                sig_params[yaml_key] = int(v)
                        elif is_float:
                            v = st.number_input(
                                yaml_key,
                                value=float(default_v) if default_v else 0.0,
                                step=0.1,
                                key=f"f_param_{direction}_{yaml_key}",
                                help="留 0 = 默认",
                            )
                            if v != 0:
                                sig_params[yaml_key] = float(v)
                        else:
                            v = st.text_input(
                                yaml_key,
                                value=str(default_v) if default_v else "",
                                key=f"f_param_{direction}_{yaml_key}",
                                help="留空 = 默认",
                            )
                            if v.strip():
                                sig_params[yaml_key] = v.strip()
            else:
                st.caption("（此信号无可配参数）")

            # ───── 其他 ─────
            st.markdown("**5️⃣ 冷却时间**")
            cooldown = st.number_input(
                "冷却（小时）",
                min_value=1, max_value=720,
                value=int(edit_data.get("cooldown_hours", default_cooldown)),
                key=f"f_cd_{direction}",
            )
            enabled = st.checkbox(
                "立即启用",
                value=edit_data.get("enabled", True),
                key=f"f_enabled_{direction}",
            )

            # ───── 提交 ─────
            submit_label = "💾 保存修改" if is_editing else f"✅ 添加{tag}预警"
            submit = st.form_submit_button(submit_label, type="primary")

            if submit:
                # 校验
                if not rule_id.strip():
                    st.error("规则 ID 不能为空")
                elif mode == "single" and not code.strip():
                    st.error("单股盯盘必须填股票代码")
                elif mode == "batch" and not scopes_keys:
                    st.error("批量扫描必须至少选一个股票池")
                elif not is_editing and any(r.get("id") == rule_id.strip()
                                            for r in target):
                    st.error(f"ID '{rule_id}' 已存在")
                else:
                    new_rule: dict = {
                        "id": rule_id.strip(),
                        "name": rule_name.strip() or rule_id.strip(),
                        "enabled": bool(enabled),
                        "signal": {
                            "type": signal_type,
                            **({"params": sig_params} if sig_params else {}),
                        },
                        "cooldown_hours": int(cooldown),
                    }
                    if mode == "single":
                        new_rule["code"] = code.strip()
                        new_rule["market"] = market
                    else:
                        new_rule["scopes"] = scopes_keys
                        new_rule["max_results"] = int(max_results)
                        new_rule["max_codes"] = int(max_codes)

                    if is_editing:
                        target[edit_idx] = new_rule
                        _clear_edit_state(direction)
                    else:
                        target.append(new_rule)

                    if _save_alerts(buy_alerts, sell_alerts):
                        _toast(f"已{'修改' if is_editing else '添加'}：{new_rule['name']}")
                        st.rerun()
                    else:
                        st.error("写入 YAML 失败")


def _clear_edit_state(direction: str) -> None:
    """清空编辑态"""
    st.session_state.pop(f"editing_{direction}", None)
    st.session_state.pop(f"editing_data_{direction}", None)


# ============================================================================
# 顶部 Tab 渲染
# ============================================================================

tab_buy, tab_sell, tab_run = st.tabs([
    f"📈 买入预警（{len(buy_alerts)} 条）",
    f"📉 卖出预警（{len(sell_alerts)} 条）",
    "🚀 立即扫描全部",
])

with tab_buy:
    _render_quick_presets("buy")
    st.markdown("---")
    st.subheader(f"📋 已配置买入规则 ({len(buy_alerts)} 条)")
    _render_rules_list(buy_alerts, "buy")
    st.markdown("---")
    _render_form("buy")

with tab_sell:
    _render_quick_presets("sell")
    st.markdown("---")
    st.subheader(f"📋 已配置卖出规则 ({len(sell_alerts)} 条)")
    _render_rules_list(sell_alerts, "sell")
    st.markdown("---")
    _render_form("sell")

with tab_run:
    st.subheader("🚀 立即扫描全部启用规则")
    st.caption(
        f"一次性跑一遍所有 enabled=True 的 buy_alerts + sell_alerts，"
        f"通过 `{PATH_ALERTS}` 配置的告警通道推送。"
    )
    total_enabled = (
        sum(1 for r in buy_alerts if r.get("enabled", True))
        + sum(1 for r in sell_alerts if r.get("enabled", True))
    )
    st.caption(f"当前启用规则：{total_enabled} 条")

    if st.button("立即执行扫描", type="primary", disabled=total_enabled == 0):
        with st.spinner("正在扫描..."):
            try:
                from src.services import alert_service as asvc
                events = asvc.scan_all(
                    buy_alerts, sell_alerts,
                    cooldown_hours=default_cooldown,
                )
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
