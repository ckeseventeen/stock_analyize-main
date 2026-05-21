"""
pages/14_回测预设.py — 回测预设管理（单一职责）

从 9_配置管理 拆出。在「4_策略回测」页可加载这里保存的预设。
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from src.web.utils import (  # noqa: E402
    PATH_BACKTEST_PRESETS,
    delete_backtest_preset,
    list_backtest_presets,
    load_backtest_preset,
    save_backtest_preset,
)

st.set_page_config(page_title="回测预设", page_icon="🔁", layout="wide")
st.title("🔁 回测预设")
st.caption(
    f"配置文件：`{PATH_BACKTEST_PRESETS}` · 在「📈 策略回测」页可加载"
)

from src.strategy.backtest import (  # noqa: E402
    STRATEGY_LABELS,
    STRATEGY_PARAM_SCHEMAS,
    STRATEGY_REGISTRY,
)

# ============================================================================
# 现有预设
# ============================================================================
presets = list_backtest_presets()
st.markdown(f"#### 📋 现有预设 ({len(presets)})")

if not presets:
    st.info("还没有预设。用下方表单创建一个，之后在「策略回测」页能直接加载。")
else:
    rows = []
    for name in presets:
        p = load_backtest_preset(name)
        rows.append({
            "名称": name,
            "策略": STRATEGY_LABELS.get(p.get("strategy", ""), p.get("strategy", "")),
            "股票": (p.get("data") or {}).get("stock_code", ""),
            "天数": (p.get("data") or {}).get("days_back", ""),
            "初始资金": (p.get("account") or {}).get("initial_cash", ""),
            "手续费": (p.get("account") or {}).get("commission", ""),
            "说明": p.get("description", ""),
        })
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

    with st.expander("🗑 删除预设"):
        del_name = st.selectbox("选择", options=presets, key="bt_del_sel")
        if st.button("⚠️ 确认删除", key="bt_del_btn"):
            if delete_backtest_preset(del_name):
                st.toast(f"已删除 {del_name}", icon="🗑️")
                st.rerun()
            else:
                st.toast("删除失败", icon="⚠️")

# ============================================================================
# 新建预设（动态渲染策略参数）
# ============================================================================
st.markdown("---")
st.markdown("#### ➕ 新建预设")

# 策略选择放 form 外，切换策略时能重绘表单
strategy_key = st.selectbox(
    "策略",
    options=list(STRATEGY_REGISTRY.keys()),
    format_func=lambda k: STRATEGY_LABELS.get(k, k),
    key="bt_strategy_sel",
)

with st.form("add_backtest_preset", clear_on_submit=True):
    name_input = st.text_input("预设名称（唯一）", placeholder="茅台_5_20")
    desc = st.text_input("说明（可选）", placeholder="贵州茅台 快5慢20")

    c1, c2 = st.columns(2)
    with c1:
        stock_code = st.text_input("股票代码", value="600519")
        days_back = st.number_input("回测天数",
                                    value=1000, min_value=100, max_value=5000, step=100)
    with c2:
        initial_cash = st.number_input("初始资金",
                                       value=100000, min_value=10000,
                                       max_value=10_000_000, step=10000)
        commission = st.number_input("手续费率",
                                     value=0.0002, min_value=0.0, max_value=0.01,
                                     step=0.0001, format="%.4f")

    st.markdown(f"##### 📐 策略参数：{STRATEGY_LABELS.get(strategy_key, strategy_key)}")
    schemas = STRATEGY_PARAM_SCHEMAS.get(strategy_key, [])
    strat_params = {}
    for s in schemas:
        key = s["key"]
        if s["type"] == "int":
            if s.get("widget") == "slider" and "min" in s and "max" in s:
                val = st.slider(
                    s["label"],
                    min_value=int(s["min"]), max_value=int(s["max"]),
                    value=int(s["default"]),
                    step=int(s.get("step", 1)),
                    help=s.get("help"),
                    key=f"bt_{key}",
                )
            else:
                val = st.number_input(
                    s["label"], value=int(s["default"]),
                    min_value=int(s.get("min", -10**9)),
                    max_value=int(s.get("max", 10**9)),
                    step=int(s.get("step", 1)),
                    help=s.get("help"),
                    key=f"bt_{key}",
                )
            strat_params[key] = int(val)
        elif s["type"] == "yaml":
            import yaml
            default_str = yaml.dump(s["default"], allow_unicode=True, sort_keys=False)
            val_str = st.text_area(
                s["label"], value=default_str, height=200,
                help=s.get("help"), key=f"bt_{key}",
            )
            try:
                strat_params[key] = yaml.safe_load(val_str)
            except Exception as e:
                st.error(f"YAML 解析失败: {e}")
                strat_params[key] = s["default"]
        elif s["type"] == "bool":
            val = st.checkbox(
                s["label"], value=bool(s["default"]),
                help=s.get("help"), key=f"bt_{key}",
            )
            strat_params[key] = bool(val)
        else:   # float
            if s.get("widget") == "slider" and "min" in s and "max" in s:
                val = st.slider(
                    s["label"],
                    min_value=float(s["min"]), max_value=float(s["max"]),
                    value=float(s["default"]),
                    step=float(s.get("step", 0.05)),
                    help=s.get("help"),
                    key=f"bt_{key}",
                )
            else:
                val = st.number_input(
                    s["label"], value=float(s["default"]),
                    step=float(s.get("step", 0.1)),
                    help=s.get("help"),
                    key=f"bt_{key}",
                )
            strat_params[key] = float(val)

    submit = st.form_submit_button("💾 保存预设", type="primary")
    if submit:
        if not name_input.strip():
            st.error("预设名称不能为空")
        else:
            preset = {
                "strategy": strategy_key,
                "data": {"stock_code": stock_code.strip(), "days_back": int(days_back)},
                "account": {"initial_cash": int(initial_cash),
                            "commission": float(commission)},
                "params": strat_params,
                "description": desc.strip(),
            }
            if save_backtest_preset(name_input.strip(), preset):
                st.toast(f"已保存预设 {name_input}", icon="✅")
                st.rerun()
            else:
                st.error("写入失败")

# 跳转
st.markdown("---")
nav = st.columns(3)
with nav[0]:
    st.page_link("pages/9_指标参数.py", label="📐 指标参数", icon="📐")
with nav[1]:
    st.page_link("pages/13_因子库.py", label="🎯 因子库", icon="🎯")
with nav[2]:
    st.page_link("pages/4_策略回测.py", label="📈 去回测", icon="📈")
