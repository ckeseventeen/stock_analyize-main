"""
pages/2_策略配置.py — 可视化策略编辑器

功能：
  - 表单化编辑筛选策略（替代原始 YAML 文本框）
  - 按分类展示条件类型，动态渲染参数表单
  - 支持新增/删除/复制策略
  - 预览生成的 YAML 配置
  - 一键保存到 screen_config.yaml
  - 策略回测配置一体化（可选 backtest 段）
"""
from __future__ import annotations

import copy
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st  # noqa: E402
import yaml as _yaml  # noqa: E402

from src.analysis.screening.conditions import (  # noqa: E402
    CONDITION_CATEGORIES,
    CONDITION_LABELS,
    CONDITION_REGISTRY,
)
from src.analysis.screening.config_schema import _PARAM_MAP, SPOT_ONLY_TYPES  # noqa: E402
from src.web.utils import (  # noqa: E402
    PATH_SCREEN,
    atomic_save_yaml,
    load_yaml,
)

st.set_page_config(page_title="策略配置", page_icon="⚙️", layout="wide")
st.title("⚙️ 策略配置编辑器")
st.caption("可视化创建和编辑筛选/回测策略，无需手动编辑 YAML 文件")


# ========================
# 加载现有策略
# ========================
def _load_all_strategies() -> dict:
    cfg = load_yaml(PATH_SCREEN) or {}
    return cfg.get("strategies", {})


if "strategies_data" not in st.session_state:
    st.session_state["strategies_data"] = _load_all_strategies()

strategies = st.session_state["strategies_data"]

# ========================
# 侧边栏：策略列表管理
# ========================
st.sidebar.markdown("### 📋 策略列表")

strategy_ids = list(strategies.keys())

# 选择当前编辑的策略
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
        strategies[sid] = {
            "name": new_name.strip() or sid,
            "conditions": [
                {"type": "exclude_st"},
                {"type": "exclude_delisting_risk"},
            ],
            "output": {"sort_by": "总市值(亿)", "ascending": False, "limit": 50},
        }
        st.session_state["strategies_data"] = strategies
        st.rerun()

# 复制/删除按钮
if current_sid:
    st.sidebar.markdown("---")
    col_copy, col_del = st.sidebar.columns(2)
    with col_copy:
        if st.button("📋 复制"):
            new_key = f"{current_sid}_copy"
            i = 1
            while new_key in strategies:
                new_key = f"{current_sid}_copy{i}"
                i += 1
            strategies[new_key] = copy.deepcopy(strategies[current_sid])
            strategies[new_key]["name"] = strategies[current_sid].get("name", "") + " (副本)"
            st.session_state["strategies_data"] = strategies
            st.rerun()
    with col_del:
        if st.button("🗑️ 删除", type="secondary"):
            del strategies[current_sid]
            st.session_state["strategies_data"] = strategies
            st.rerun()

# 保存所有策略
st.sidebar.markdown("---")
if st.sidebar.button("💾 保存全部到文件", type="primary", use_container_width=True):
    cfg = load_yaml(PATH_SCREEN) or {}
    cfg["strategies"] = strategies
    if atomic_save_yaml(PATH_SCREEN, cfg):
        st.sidebar.success("✅ 已保存到 screen_config.yaml")
        # 刷新内存数据
        st.session_state["strategies_data"] = _load_all_strategies()
    else:
        st.sidebar.error("保存失败")

if st.sidebar.button("🔄 从文件重新加载", use_container_width=True):
    st.session_state["strategies_data"] = _load_all_strategies()
    st.rerun()


# ========================
# 主区域：策略编辑器
# ========================
if not current_sid or current_sid not in strategies:
    st.info("👈 请在左侧选择或创建一个策略")
    st.stop()

s_cfg = strategies[current_sid]

# 策略名称编辑
st.subheader(f"📝 编辑策略: {s_cfg.get('name', current_sid)}")
new_strategy_name = st.text_input(
    "策略名称",
    value=s_cfg.get("name", current_sid),
    key=f"name_{current_sid}",
)
if new_strategy_name != s_cfg.get("name"):
    s_cfg["name"] = new_strategy_name

# ========================
# 条件编辑区
# ========================
st.markdown("---")
st.subheader("🎯 筛选条件")

conditions = s_cfg.get("conditions", [])

# 显示现有条件
for i, cond in enumerate(conditions):
    cond_type = cond.get("type", "unknown")
    label = CONDITION_LABELS.get(cond_type, cond_type)

    with st.expander(f"**{i+1}. {label}** (`{cond_type}`)", expanded=False):
        col_params, col_actions = st.columns([4, 1])

        with col_params:
            # 动态渲染参数
            param_map = _PARAM_MAP.get(cond_type, {})
            if param_map:
                for yaml_key, _init_key in param_map.items():
                    current_val = cond.get(yaml_key)

                    # 根据类型推断输入控件
                    if isinstance(current_val, bool) or yaml_key in (
                        "zero_axis_filter", "multi_level_check",
                        "require_all_above", "require_up_trend",
                        "require_zero_near", "close_touch",
                    ):
                        new_val = st.checkbox(
                            yaml_key,
                            value=bool(current_val) if current_val is not None else False,
                            key=f"cond_{current_sid}_{i}_{yaml_key}",
                        )
                    elif isinstance(current_val, list):
                        new_val = st.text_input(
                            yaml_key,
                            value=str(current_val),
                            key=f"cond_{current_sid}_{i}_{yaml_key}",
                            help="用逗号分隔的列表，如: [5,10,20,60]",
                        )
                        try:
                            new_val = _yaml.safe_load(new_val)
                        except Exception:
                            pass
                    elif isinstance(current_val, float) or yaml_key in (
                        "breakout_pct", "consolidation_pct", "vol_multiple",
                        "shrink_ratio", "std_dev", "j_threshold",
                    ):
                        new_val = st.number_input(
                            yaml_key,
                            value=float(current_val) if current_val is not None else 0.0,
                            step=0.01,
                            format="%.4f",
                            key=f"cond_{current_sid}_{i}_{yaml_key}",
                        )
                    elif isinstance(current_val, int) or yaml_key in (
                        "min", "max", "lookback_bars", "period", "ma_period",
                        "min_touches", "threshold", "n", "m1", "m2",
                        "fast_period", "slow_period", "consecutive",
                    ):
                        new_val = st.number_input(
                            yaml_key,
                            value=int(current_val) if current_val is not None else 0,
                            step=1,
                            key=f"cond_{current_sid}_{i}_{yaml_key}",
                        )
                    elif yaml_key == "direction":
                        new_val = st.selectbox(
                            yaml_key,
                            options=["upper", "lower"],
                            index=0 if current_val != "lower" else 1,
                            key=f"cond_{current_sid}_{i}_{yaml_key}",
                        )
                    else:
                        new_val = st.text_input(
                            yaml_key,
                            value=str(current_val) if current_val is not None else "",
                            key=f"cond_{current_sid}_{i}_{yaml_key}",
                        )

                    if new_val is not None:
                        cond[yaml_key] = new_val
            else:
                st.caption("该条件无额外参数")

        with col_actions:
            st.markdown("&nbsp;")
            if st.button("🗑️", key=f"del_cond_{current_sid}_{i}", help="删除此条件"):
                conditions.pop(i)
                s_cfg["conditions"] = conditions
                st.rerun()
            if i > 0:
                if st.button("⬆️", key=f"up_cond_{current_sid}_{i}", help="上移"):
                    conditions[i], conditions[i-1] = conditions[i-1], conditions[i]
                    s_cfg["conditions"] = conditions
                    st.rerun()
            if i < len(conditions) - 1:
                if st.button("⬇️", key=f"down_cond_{current_sid}_{i}", help="下移"):
                    conditions[i], conditions[i+1] = conditions[i+1], conditions[i]
                    s_cfg["conditions"] = conditions
                    st.rerun()

# 添加新条件
st.markdown("---")
st.markdown("**➕ 添加新条件**")

# 按分类展示可选条件
add_cols = st.columns(len(CONDITION_CATEGORIES))
for idx, (cat_name, cat_types) in enumerate(CONDITION_CATEGORIES.items()):
    with add_cols[idx % len(add_cols)]:
        st.markdown(f"**{cat_name}**")
        for ctype in cat_types:
            clabel = CONDITION_LABELS.get(ctype, ctype)
            if st.button(f"+ {clabel}", key=f"add_{current_sid}_{ctype}", use_container_width=True):
                new_cond = {"type": ctype}
                # 填入默认参数
                param_map = _PARAM_MAP.get(ctype, {})
                cls = CONDITION_REGISTRY.get(ctype)
                if cls:
                    import inspect
                    sig = inspect.signature(cls.__init__)
                    for pname, param in sig.parameters.items():
                        if pname == "self":
                            continue
                        # 从 param_map 反查 yaml_key
                        yaml_key = next(
                            (yk for yk, ik in param_map.items() if ik == pname),
                            pname,
                        )
                        if param.default != inspect.Parameter.empty:
                            new_cond[yaml_key] = param.default
                conditions.append(new_cond)
                s_cfg["conditions"] = conditions
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
        sc_label = CONDITION_LABELS.get(sc_type, sc_type)
        col_sc, col_sca = st.columns([4, 1])
        with col_sc:
            param_map = _PARAM_MAP.get(sc_type, {})
            for yaml_key, _init_key in param_map.items():
                val = sc.get(yaml_key)
                if isinstance(val, (int, float)):
                    new_val = st.number_input(
                        f"[卖出] {sc_label} - {yaml_key}",
                        value=float(val) if isinstance(val, float) else int(val),
                        key=f"sell_{current_sid}_{j}_{yaml_key}",
                    )
                    sc[yaml_key] = new_val
        with col_sca:
            if st.button("🗑️", key=f"del_sell_{current_sid}_{j}"):
                sell_conditions.pop(j)
                st.rerun()

    # 添加卖出条件
    sell_type_options = [
        ct for ct in CONDITION_REGISTRY if ct not in SPOT_ONLY_TYPES
    ]
    sell_add_col1, sell_add_col2 = st.columns([3, 1])
    with sell_add_col1:
        sell_add_type = st.selectbox(
            "添加卖出条件",
            options=sell_type_options,
            format_func=lambda x: CONDITION_LABELS.get(x, x),
            key=f"sell_add_type_{current_sid}",
        )
    with sell_add_col2:
        st.markdown("&nbsp;")
        if st.button("➕ 添加", key=f"sell_add_btn_{current_sid}"):
            new_sc = {"type": sell_add_type}
            cls = CONDITION_REGISTRY.get(sell_add_type)
            if cls:
                import inspect
                param_map = _PARAM_MAP.get(sell_add_type, {})
                sig = inspect.signature(cls.__init__)
                for pname, param in sig.parameters.items():
                    if pname == "self":
                        continue
                    yaml_key = next(
                        (yk for yk, ik in param_map.items() if ik == pname),
                        pname,
                    )
                    if param.default != inspect.Parameter.empty:
                        new_sc[yaml_key] = param.default
            sell_conditions.append(new_sc)
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
# 预览生成的 YAML
# ========================
st.markdown("---")
with st.expander("📄 预览生成的 YAML 配置", expanded=False):
    preview = {current_sid: s_cfg}
    st.code(_yaml.safe_dump(preview, allow_unicode=True, sort_keys=False), language="yaml")

# 底部统计
st.markdown("---")
tech_count = sum(1 for c in conditions if c.get("type") not in SPOT_ONLY_TYPES)
spot_count = sum(1 for c in conditions if c.get("type") in SPOT_ONLY_TYPES)
c1, c2, c3 = st.columns(3)
c1.metric("总条件数", len(conditions))
c2.metric("技术面条件（可回测）", tech_count)
c3.metric("基本面条件（仅筛选）", spot_count)
