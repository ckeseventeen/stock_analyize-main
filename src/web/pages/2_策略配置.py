"""
pages/2_策略配置.py — 可视化策略编辑器（薄渲染层）

业务编排全部在 src/services/screening_service.py；本页只负责：
  - 收集表单输入（基于 ParamSpec 动态渲染控件）
  - 调服务保存/试运行
  - 渲染结果
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st  # noqa: E402
import yaml as _yaml  # noqa: E402

from src.services import screening_service as svc  # noqa: E402
from src.web.components.confirm import confirm_action  # noqa: E402
from src.web.components.unsaved import mark_clean, mark_dirty, unsaved_badge  # noqa: E402

st.title("⚙️ 策略配置编辑器")
st.caption("可视化创建和编辑筛选/回测策略，无需手动编辑 YAML 文件")

unsaved_badge("strategy_editor")


# ========================
# 加载现有策略（session 暂存，保存时统一落盘）
# ========================
if "strategies_data" not in st.session_state:
    st.session_state["strategies_data"] = svc.load_all_strategies()

strategies = st.session_state["strategies_data"]

# ========================
# 侧边栏：策略列表管理
# ========================
st.sidebar.markdown("### 📋 策略列表")

strategy_ids = list(strategies.keys())

if strategy_ids:
    current_sid = st.sidebar.radio(
        "选择要编辑的策略",
        options=strategy_ids,
        format_func=lambda sid: f"{strategies[sid].get('name', sid)} ({sid})",
        key="current_strategy_id",
    )
else:
    current_sid = None
    st.sidebar.info("暂无策略，请新建一个")

st.sidebar.markdown("---")

# 新建策略
st.sidebar.markdown("**➕ 新建策略**")
new_sid = st.sidebar.text_input("策略 ID（英文）", placeholder="my_strategy", key="new_sid")
new_name = st.sidebar.text_input("策略名称（中文）", placeholder="我的策略", key="new_name")
if st.sidebar.button("创建策略", type="primary"):
    sid = new_sid.strip()
    if not sid:
        st.sidebar.error("请输入策略 ID")
    elif sid in strategies:
        st.sidebar.error(f"策略 {sid} 已存在")
    else:
        strategies[sid] = svc.default_strategy_body(new_name.strip() or sid)
        st.session_state["strategies_data"] = strategies
        mark_dirty("strategy_editor")
        st.rerun()

# 复制/删除按钮
if current_sid:
    st.sidebar.markdown("---")
    col_copy, col_del = st.sidebar.columns(2)
    with col_copy:
        if st.button("📋 复制"):
            svc.duplicate_strategy_in_memory(strategies, current_sid)
            st.session_state["strategies_data"] = strategies
            mark_dirty("strategy_editor")
            st.rerun()
    with col_del:
        del_result = confirm_action(
            "del_strategy", f"确定删除策略 {current_sid}？此操作不可撤销。", "🗑️ 删除"
        )
        if del_result is True:
            del strategies[current_sid]
            st.session_state["strategies_data"] = strategies
            mark_dirty("strategy_editor")
            st.rerun()

# 保存所有策略
st.sidebar.markdown("---")
if st.sidebar.button("💾 保存全部到文件", type="primary", use_container_width=True):
    if svc.save_all_strategies(strategies):
        st.sidebar.success("✅ 已保存到 screen_config.yaml")
        mark_clean("strategy_editor")
        st.session_state["strategies_data"] = svc.load_all_strategies()
    else:
        st.sidebar.error("保存失败")

if st.sidebar.button("🔄 从文件重新加载", use_container_width=True):
    st.session_state["strategies_data"] = svc.load_all_strategies()
    st.rerun()


# ========================
# 主区域：策略编辑器
# ========================
if not current_sid or current_sid not in strategies:
    st.info("👈 请在左侧选择或创建一个策略")
    st.stop()

s_cfg = strategies[current_sid]

title_cols = st.columns([4, 1])
with title_cols[0]:
    st.subheader(f"📝 编辑策略: {s_cfg.get('name', current_sid)}")
with title_cols[1]:
    if st.button("⚡ 试运行", help="用当前编辑中的条件跑一次筛选（A 股全集）",
                 width="stretch", type="secondary"):
        try:
            with st.spinner("试运行中..."):
                result = svc.dry_run_strategy(s_cfg, sid=current_sid)
            if result is None or result.empty:
                st.toast("试运行完成：0 只命中（条件可能太严）", icon="⚠️")
            else:
                st.toast(f"试运行完成：命中 {len(result)} 只", icon="✅")
                st.session_state["_tryrun_result"] = result
        except Exception as e:
            st.toast(f"试运行失败: {e}", icon="⚠️")

new_strategy_name = st.text_input(
    "策略名称",
    value=s_cfg.get("name", current_sid),
    key=f"name_{current_sid}",
)
if new_strategy_name != s_cfg.get("name"):
    s_cfg["name"] = new_strategy_name
    mark_dirty("strategy_editor")

# 展示上次试运行结果
_tryrun = st.session_state.get("_tryrun_result")
if _tryrun is not None and not _tryrun.empty:
    with st.expander(f"⚡ 上次试运行结果：{len(_tryrun)} 只命中", expanded=False):
        st.dataframe(_tryrun, width="stretch", hide_index=True)
        if st.button("清除试运行结果", key="clear_tryrun"):
            del st.session_state["_tryrun_result"]
            st.rerun()


# ========================
# 参数控件：基于 ParamSpec 动态渲染
# ========================
def render_param_input(spec, current_val, widget_key: str):
    """按 ParamSpec.kind 渲染对应控件并返回新值"""
    if spec.kind == "bool":
        return st.checkbox(
            spec.yaml_key,
            value=bool(current_val) if current_val is not None else bool(spec.default),
            key=widget_key,
        )
    if spec.kind == "choice":
        opts = spec.choices or []
        idx = opts.index(current_val) if current_val in opts else 0
        return st.selectbox(spec.yaml_key, options=opts, index=idx, key=widget_key)
    if spec.kind == "list":
        raw = st.text_input(
            spec.yaml_key,
            value=str(current_val if current_val is not None else spec.default or []),
            key=widget_key,
            help="用逗号分隔的列表，如: [5,10,20,60]",
        )
        try:
            parsed = _yaml.safe_load(raw)
            return parsed if isinstance(parsed, list) else current_val
        except Exception:
            return current_val
    if spec.kind == "float":
        base = current_val if current_val is not None else (spec.default or 0.0)
        return st.number_input(
            spec.yaml_key, value=float(base), step=0.01, format="%.4f", key=widget_key,
        )
    if spec.kind == "int":
        base = current_val if current_val is not None else (spec.default or 0)
        return st.number_input(spec.yaml_key, value=int(base), step=1, key=widget_key)
    # str 兜底
    return st.text_input(
        spec.yaml_key,
        value=str(current_val) if current_val is not None else str(spec.default or ""),
        key=widget_key,
    )


# ========================
# 条件编辑区
# ========================
st.markdown("---")
st.subheader("🎯 筛选条件")

conditions = s_cfg.get("conditions", [])

for i, cond in enumerate(conditions):
    cond_type = cond.get("type", "unknown")
    label = svc.condition_label(cond_type)

    with st.expander(f"**{i+1}. {label}** (`{cond_type}`)", expanded=False):
        col_params, col_actions = st.columns([4, 1])

        with col_params:
            specs = svc.condition_param_specs(cond_type)
            if specs:
                for spec in specs:
                    new_val = render_param_input(
                        spec, cond.get(spec.yaml_key),
                        widget_key=f"cond_{current_sid}_{i}_{spec.yaml_key}",
                    )
                    if new_val is not None:
                        cond[spec.yaml_key] = new_val
            else:
                st.caption("该条件无额外参数")

        with col_actions:
            st.markdown("&nbsp;")
            if st.button("🗑️", key=f"del_cond_{current_sid}_{i}", help="删除此条件"):
                conditions.pop(i)
                s_cfg["conditions"] = conditions
                mark_dirty("strategy_editor")
                st.rerun()
            if i > 0:
                if st.button("⬆️", key=f"up_cond_{current_sid}_{i}", help="上移"):
                    conditions[i], conditions[i-1] = conditions[i-1], conditions[i]
                    s_cfg["conditions"] = conditions
                    mark_dirty("strategy_editor")
                    st.rerun()
            if i < len(conditions) - 1:
                if st.button("⬇️", key=f"down_cond_{current_sid}_{i}", help="下移"):
                    conditions[i], conditions[i+1] = conditions[i+1], conditions[i]
                    s_cfg["conditions"] = conditions
                    mark_dirty("strategy_editor")
                    st.rerun()

# 添加新条件
st.markdown("---")
st.markdown("**➕ 添加新条件**")

categories = svc.condition_categories()
add_cols = st.columns(len(categories))
for idx, (cat_name, cat_types) in enumerate(categories.items()):
    with add_cols[idx % len(add_cols)]:
        st.markdown(f"**{cat_name}**")
        for ctype in cat_types:
            clabel = svc.condition_label(ctype)
            if st.button(f"+ {clabel}", key=f"add_{current_sid}_{ctype}", use_container_width=True):
                conditions.append(svc.new_condition_dict(ctype))
                s_cfg["conditions"] = conditions
                mark_dirty("strategy_editor")
                st.rerun()


# ========================
# 回测配置（可选）
# ========================
st.markdown("---")
st.subheader("📈 回测配置（可选）")
st.caption("配置后，该策略可直接在「策略回测」页面运行。技术类条件会自动作为买入信号。")

backtest_cfg = s_cfg.get("backtest", {}) or {}
enable_backtest = st.checkbox(
    "启用回测配置",
    value=bool(backtest_cfg),
    key=f"bt_enable_{current_sid}",
)

if enable_backtest:
    bt_col1, bt_col2, bt_col3, bt_col4 = st.columns(4)
    with bt_col1:
        buy_logic = st.selectbox(
            "买入逻辑", options=["all", "any"],
            index=0 if backtest_cfg.get("buy_logic", "all") == "all" else 1,
            key=f"bt_buy_logic_{current_sid}",
            help="all=所有技术条件都满足时买入; any=任一满足即买入",
        )
    with bt_col2:
        sell_logic = st.selectbox(
            "卖出逻辑", options=["any", "all"],
            index=0 if backtest_cfg.get("sell_logic", "any") == "any" else 1,
            key=f"bt_sell_logic_{current_sid}",
        )
    with bt_col3:
        position_size = st.number_input(
            "仓位比例", value=float(backtest_cfg.get("position_size", 0.95)),
            min_value=0.1, max_value=1.0, step=0.05,
            key=f"bt_pos_{current_sid}",
        )
    with bt_col4:
        days_back = st.number_input(
            "默认回测天数", value=int(backtest_cfg.get("days_back", 1000)),
            min_value=100, max_value=5000, step=100,
            key=f"bt_days_{current_sid}",
        )

    default_stock = st.text_input(
        "默认回测标的代码",
        value=backtest_cfg.get("default_stock", ""),
        placeholder="600519",
        key=f"bt_stock_{current_sid}",
    )

    # 卖出条件编辑
    st.markdown("**卖出条件**")
    sell_conditions = backtest_cfg.get("sell_conditions", [])

    for j, sc in enumerate(sell_conditions):
        sc_type = sc.get("type", "unknown")
        sc_label = svc.condition_label(sc_type)
        col_sc, col_sca = st.columns([4, 1])
        with col_sc:
            for spec in svc.condition_param_specs(sc_type):
                new_val = render_param_input(
                    spec, sc.get(spec.yaml_key),
                    widget_key=f"sell_{current_sid}_{j}_{spec.yaml_key}",
                )
                if new_val is not None:
                    sc[spec.yaml_key] = new_val
        with col_sca:
            if st.button("🗑️", key=f"del_sell_{current_sid}_{j}"):
                sell_conditions.pop(j)
                st.rerun()

    # 添加卖出条件
    sell_add_col1, sell_add_col2 = st.columns([3, 1])
    with sell_add_col1:
        sell_add_type = st.selectbox(
            "添加卖出条件",
            options=svc.sellable_condition_types(),
            format_func=svc.condition_label,
            key=f"sell_add_type_{current_sid}",
        )
    with sell_add_col2:
        st.markdown("&nbsp;")
        if st.button("➕ 添加", key=f"sell_add_btn_{current_sid}"):
            sell_conditions.append(svc.new_condition_dict(sell_add_type))
            st.rerun()

    # 写回
    s_cfg["backtest"] = {
        "sell_conditions": sell_conditions,
        "buy_logic": buy_logic,
        "sell_logic": sell_logic,
        "position_size": position_size,
        "default_stock": default_stock,
        "days_back": days_back,
    }
else:
    if "backtest" in s_cfg:
        del s_cfg["backtest"]


# ========================
# 输出配置
# ========================
st.markdown("---")
st.subheader("📊 输出设置")

output_cfg = s_cfg.get("output", {})
out_col1, out_col2, out_col3 = st.columns(3)
with out_col1:
    sort_by = st.text_input(
        "排序字段", value=output_cfg.get("sort_by", "总市值(亿)"),
        key=f"out_sort_{current_sid}",
    )
with out_col2:
    ascending = st.checkbox(
        "升序排列", value=output_cfg.get("ascending", False),
        key=f"out_asc_{current_sid}",
    )
with out_col3:
    limit = st.number_input(
        "最大返回数量", value=int(output_cfg.get("limit", 50)),
        min_value=1, max_value=500, step=10,
        key=f"out_limit_{current_sid}",
    )
s_cfg["output"] = {"sort_by": sort_by, "ascending": ascending, "limit": limit}


# ========================
# 校验 + 预览生成的 YAML
# ========================
st.markdown("---")
_errors = svc.validate_conditions(s_cfg.get("conditions", []))
if _errors:
    st.error("⚠️ 条件配置存在问题：\n" + "\n".join(f"- {e}" for e in _errors))

with st.expander("📄 预览生成的 YAML 配置", expanded=False):
    preview = {current_sid: s_cfg}
    st.code(_yaml.safe_dump(preview, allow_unicode=True, sort_keys=False), language="yaml")

# 底部统计
st.markdown("---")
tech_count = sum(1 for c in conditions if not svc.is_spot_only(c.get("type", "")))
spot_count = sum(1 for c in conditions if svc.is_spot_only(c.get("type", "")))
c1, c2, c3 = st.columns(3)
c1.metric("总条件数", len(conditions))
c2.metric("技术面条件（可回测）", tech_count)
c3.metric("基本面条件（仅筛选）", spot_count)
