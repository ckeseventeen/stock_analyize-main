"""
pages/8_配置管理.py — 统一配置管理

四个 tab：
  📌 关注标的 — 增删股票 + 板块分类（写入 a/hk/us_stock.yaml）
  📐 技术指标 — 多 profile 的 MACD/RSI/KDJ/BOLL/MA 参数
  🎯 因子库   — 从 FACTOR_REGISTRY 增删因子并配置参数
  🔁 回测预设 — 从 STRATEGY_REGISTRY 创建命名预设供"7_策略回测"加载

所有写入使用 atomic_save_yaml；不会破坏既有注释和结构。
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
    PATH_A_STOCK,
    PATH_BACKTEST_PRESETS,
    PATH_FACTORS,
    PATH_HK_STOCK,
    PATH_INDICATORS,
    PATH_US_STOCK,
    add_category_to_market,
    add_stock_to_market,
    delete_backtest_preset,
    delete_factor_profile,
    delete_indicator_profile,
    list_backtest_presets,
    list_factor_profiles,
    list_indicator_profiles,
    list_market_categories,
    list_stocks_from_market_config,
    load_backtest_preset,
    load_factor_profile,
    load_indicator_profile,
    load_yaml,
    remove_category_from_market,
    remove_stock_from_market,
    save_backtest_preset,
    save_factor_profile,
    save_indicator_profile,
    set_active_factor_profile,
    set_active_indicator_profile,
)

st.set_page_config(page_title="配置管理", page_icon="⚙️", layout="wide")
st.title("⚙️ 配置管理")
st.caption("所有标的 / 指标 / 因子 / 回测参数均存于 YAML，前端增删即改文件，重启生效。")

tab_stocks, tab_indicators, tab_factors, tab_backtest = st.tabs([
    "📌 关注标的", "📐 技术指标", "🎯 因子库", "🔁 回测预设",
])


# ==============================================================
# Tab 1: 关注标的
# ==============================================================
with tab_stocks:
    st.subheader("关注标的管理")
    market_paths = {"a": PATH_A_STOCK, "hk": PATH_HK_STOCK, "us": PATH_US_STOCK}

    market = st.selectbox(
        "市场",
        options=list(MARKET_LABELS.keys()),
        format_func=lambda k: MARKET_LABELS[k],
        key="cfg_market",
    )
    st.caption(f"配置文件：`{market_paths[market]}`")

    # --- 现有股票列表 ---
    stocks = list_stocks_from_market_config(market)
    st.markdown(f"**现有股票 ({len(stocks)} 只)**")
    if stocks:
        df = pd.DataFrame([{
            "分类": s.get("category", ""),
            "代码": s.get("code", ""),
            "名称": s.get("name", ""),
            "估值方式": s.get("valuation", ""),
            "PE 档位": s.get("pe_range", ""),
            "PS 档位": s.get("ps_range", ""),
        } for s in stocks])
        st.dataframe(df, width="stretch", hide_index=True)
    else:
        st.info("该市场暂无股票，使用下方表单新增")

    # --- 新增股票 ---
    st.markdown("---")
    st.markdown("**➕ 新增股票**")
    cats = list_market_categories(market)
    if not cats:
        st.warning("该市场尚无分类板块，请先到下方【新增板块】创建")
    else:
        with st.form(f"add_stock_{market}", clear_on_submit=True):
            col1, col2, col3 = st.columns([1, 1, 1])
            with col1:
                cat_key = st.selectbox(
                    "板块",
                    options=[k for k, _ in cats],
                    format_func=lambda k: dict(cats).get(k, k),
                )
            with col2:
                code = st.text_input("代码", placeholder="600519")
            with col3:
                name = st.text_input("名称", placeholder="贵州茅台")

            col4, col5, col6 = st.columns([1, 2, 2])
            with col4:
                val_type = st.selectbox("估值方式", options=["pe", "ps"])
            with col5:
                pe_str = st.text_input("PE 档位（低,中,高）", value="10,20,30")
            with col6:
                ps_str = st.text_input("PS 档位（低,中,高）", value="1,2,3")

            submit = st.form_submit_button("✅ 添加", type="primary")
            if submit:
                try:
                    pe_range = [float(x.strip()) for x in pe_str.split(",") if x.strip()]
                    ps_range = [float(x.strip()) for x in ps_str.split(",") if x.strip()]
                except ValueError:
                    st.error("档位必须为数字，用逗号分隔")
                else:
                    if len(pe_range) != 3 or len(ps_range) != 3:
                        st.error("档位必须是 3 个数字（低/中/高）")
                    elif not code.strip() or not name.strip():
                        st.error("代码和名称不能为空")
                    else:
                        new_stock = {
                            "name": name.strip(),
                            "code": code.strip(),
                            "valuation": val_type,
                            "pe_range": pe_range,
                            "ps_range": ps_range,
                        }
                        ok, msg = add_stock_to_market(market, cat_key, new_stock)
                        if ok:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)

    # --- 删除股票 ---
    if stocks:
        with st.expander("🗑 删除股票"):
            code_to_del = st.selectbox(
                "选择要删除的代码",
                options=[s["code"] for s in stocks],
                format_func=lambda c: f"{c} - {next((s['name'] for s in stocks if s['code'] == c), '')}",
                key=f"del_code_{market}",
            )
            if st.button("确认删除", type="secondary", key=f"del_btn_{market}"):
                ok, msg = remove_stock_from_market(market, code_to_del)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)

    # --- 板块分类 CRUD ---
    st.markdown("---")
    st.markdown("**📂 板块分类**")
    c1, c2 = st.columns(2)
    with c1:
        with st.form(f"add_cat_{market}", clear_on_submit=True):
            new_key = st.text_input("分类 key（英文）", placeholder="finance")
            new_name = st.text_input("分类名称", placeholder="金融")
            if st.form_submit_button("➕ 新增板块"):
                if not new_key.strip():
                    st.error("分类 key 不能为空")
                else:
                    ok, msg = add_category_to_market(market, new_key.strip(), new_name.strip())
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
    with c2:
        if cats:
            del_cat_key = st.selectbox(
                "删除板块",
                options=[k for k, _ in cats],
                format_func=lambda k: f"{dict(cats)[k]} ({k})",
                key=f"del_cat_sel_{market}",
            )
            if st.button("🗑 删除板块（及其下全部股票）", key=f"del_cat_btn_{market}"):
                ok, msg = remove_category_from_market(market, del_cat_key)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)


# ==============================================================
# Tab 2: 技术指标
# ==============================================================
with tab_indicators:
    st.subheader("技术指标参数配置")
    st.caption(f"配置文件：`{PATH_INDICATORS}` — 代码端通过 `TechnicalAnalyzer.add_all_from_config()` 读取")

    # Profile 选择/切换
    profiles = list_indicator_profiles() or ["default"]
    cfg_raw = load_yaml(PATH_INDICATORS) or {}
    active = cfg_raw.get("active_profile", "default")

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        picked = st.selectbox(
            "选择 profile（可编辑/另存为新）",
            options=profiles,
            index=profiles.index(active) if active in profiles else 0,
            key="ind_profile",
        )
    with c2:
        st.markdown(f"当前激活：`{active}`")
        if picked != active and st.button("激活此 profile", key="set_active_ind"):
            if set_active_indicator_profile(picked):
                st.success(f"已激活 {picked}")
                st.rerun()
            else:
                st.error("激活失败")
    with c3:
        if picked != "default" and st.button("🗑 删除 profile", key="del_ind"):
            if delete_indicator_profile(picked):
                st.success(f"已删除 {picked}")
                st.rerun()

    params = load_indicator_profile(picked)

    st.markdown("---")
    st.markdown(f"**编辑 `{picked}` 参数**")

    with st.form("edit_ind_profile"):
        st.markdown("**MACD**")
        cm1, cm2, cm3 = st.columns(3)
        macd_fast = cm1.number_input("fast", value=int(params["macd"]["fast"]), min_value=2)
        macd_slow = cm2.number_input("slow", value=int(params["macd"]["slow"]), min_value=5)
        macd_signal = cm3.number_input("signal", value=int(params["macd"]["signal"]), min_value=2)

        st.markdown("**RSI**")
        rsi_period = st.number_input("period", value=int(params["rsi"]["period"]), min_value=2,
                                     key="rsi_period_input")

        st.markdown("**KDJ**")
        ck1, ck2, ck3 = st.columns(3)
        kdj_n = ck1.number_input("n", value=int(params["kdj"]["n"]), min_value=2)
        kdj_m1 = ck2.number_input("m1", value=int(params["kdj"]["m1"]), min_value=1)
        kdj_m2 = ck3.number_input("m2", value=int(params["kdj"]["m2"]), min_value=1)

        st.markdown("**Bollinger Bands**")
        cb1, cb2 = st.columns(2)
        bb_period = cb1.number_input("period", value=int(params["bollinger"]["period"]),
                                     min_value=5, key="bb_period_input")
        bb_std = cb2.number_input("std_dev", value=float(params["bollinger"]["std_dev"]),
                                  min_value=0.5, step=0.1, key="bb_std_input")

        st.markdown("**Moving Averages**")
        ma_periods_str = st.text_input(
            "均线周期（逗号分隔）",
            value=",".join(str(p) for p in (params["moving_averages"].get("periods") or [])),
        )

        st.markdown("---")
        save_as_name = st.text_input("另存为 profile（留空=覆盖当前）", value="")
        submitted = st.form_submit_button("💾 保存", type="primary")

        if submitted:
            try:
                ma_periods = [int(x.strip()) for x in ma_periods_str.split(",") if x.strip()]
            except ValueError:
                st.error("均线周期必须为整数")
            else:
                new_params = {
                    "macd": {"fast": int(macd_fast), "slow": int(macd_slow),
                             "signal": int(macd_signal)},
                    "rsi": {"period": int(rsi_period)},
                    "kdj": {"n": int(kdj_n), "m1": int(kdj_m1), "m2": int(kdj_m2)},
                    "bollinger": {"period": int(bb_period), "std_dev": float(bb_std)},
                    "moving_averages": {"periods": ma_periods},
                }
                target = save_as_name.strip() or picked
                if save_indicator_profile(target, new_params):
                    st.success(f"已保存到 profile `{target}`")
                    st.rerun()
                else:
                    st.error("写入失败")


# ==============================================================
# Tab 3: 因子库
# ==============================================================
with tab_factors:
    from src.analysis.factor import FACTOR_REGISTRY  # 触发注册

    st.subheader("因子配置")
    st.caption(f"配置文件：`{PATH_FACTORS}` — 代码端通过 `build_engine_from_config()` 读取")

    profiles = list_factor_profiles() or ["default"]
    cfg_raw = load_yaml(PATH_FACTORS) or {}
    active = cfg_raw.get("active_profile", "default")

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        picked = st.selectbox(
            "选择 profile",
            options=profiles,
            index=profiles.index(active) if active in profiles else 0,
            key="fac_profile",
        )
    with c2:
        st.markdown(f"当前激活：`{active}`")
        if picked != active and st.button("激活此 profile", key="set_active_fac"):
            if set_active_factor_profile(picked):
                st.success(f"已激活 {picked}")
                st.rerun()
    with c3:
        if picked != "default" and st.button("🗑 删除 profile", key="del_fac"):
            if delete_factor_profile(picked):
                st.success(f"已删除 {picked}")
                st.rerun()

    factors = load_factor_profile(picked)

    st.markdown(f"**`{picked}` profile 包含的因子 ({len(factors)} 个)**")
    if factors:
        df = pd.DataFrame([{
            "#": i,
            "type": f.get("type", ""),
            "enabled": f.get("enabled", True),
            "params": f.get("params", {}),
        } for i, f in enumerate(factors)])
        st.dataframe(df, width="stretch", hide_index=True)
    else:
        st.info("该 profile 暂无因子")

    # --- 切换 enable / 删除 ---
    if factors:
        st.markdown("**🎚 切换启用 / 删除**")
        col_a, col_b, col_c = st.columns([3, 1, 1])
        with col_a:
            row_idx = st.number_input("因子序号 #", min_value=0,
                                      max_value=max(len(factors) - 1, 0),
                                      value=0, step=1, key="fac_row_idx")
        with col_b:
            if st.button("切换启用", key="toggle_enabled"):
                factors[int(row_idx)]["enabled"] = not factors[int(row_idx)].get("enabled", True)
                save_factor_profile(picked, factors)
                st.rerun()
        with col_c:
            if st.button("🗑 删除该行", key="del_factor_row"):
                factors.pop(int(row_idx))
                save_factor_profile(picked, factors)
                st.rerun()

    # --- 新增因子 ---
    st.markdown("---")
    st.markdown("**➕ 新增因子**")
    all_types = sorted(FACTOR_REGISTRY.keys())
    with st.form("add_factor_form", clear_on_submit=True):
        col1, col2 = st.columns([2, 1])
        with col1:
            type_key = st.selectbox("因子类型", options=all_types)
        with col2:
            enabled = st.checkbox("启用", value=True)

        # 简单的 params 输入：用 key=value 格式，解析为 dict
        params_str = st.text_input(
            "参数 (key=value, 逗号分隔；留空=无参数)",
            placeholder="period=14, lookback_bars=60",
        )

        save_as_name = st.text_input("保存到 profile（留空=当前 profile）", value="")
        submit = st.form_submit_button("✅ 添加", type="primary")

        if submit:
            params_dict = {}
            if params_str.strip():
                try:
                    for pair in params_str.split(","):
                        if "=" not in pair:
                            continue
                        k, v = pair.split("=", 1)
                        k = k.strip()
                        v = v.strip()
                        # 尝试转 int / float
                        if v.lstrip("-").isdigit():
                            params_dict[k] = int(v)
                        else:
                            try:
                                params_dict[k] = float(v)
                            except ValueError:
                                params_dict[k] = v
                except Exception as e:
                    st.error(f"参数解析失败: {e}")
                    params_dict = None

            if params_dict is not None:
                entry = {"type": type_key, "enabled": enabled}
                if params_dict:
                    entry["params"] = params_dict
                target = save_as_name.strip() or picked
                current = load_factor_profile(target) if target != picked else list(factors)
                current.append(entry)
                if save_factor_profile(target, current):
                    st.success(f"已添加因子 {type_key} 到 {target}")
                    st.rerun()
                else:
                    st.error("写入失败")

    # --- 可用因子一览 ---
    with st.expander("📚 全部可用因子 type（FACTOR_REGISTRY）"):
        st.json({k: cls.__name__ for k, cls in sorted(FACTOR_REGISTRY.items())})


# ==============================================================
# Tab 4: 回测预设
# ==============================================================
with tab_backtest:
    from src.strategy.backtest import (
        STRATEGY_LABELS,
        STRATEGY_PARAM_SCHEMAS,
        STRATEGY_REGISTRY,
    )

    st.subheader("回测预设")
    st.caption(f"配置文件：`{PATH_BACKTEST_PRESETS}` — `7_策略回测` 页可加载/另存为预设")

    presets = list_backtest_presets()
    st.markdown(f"**现有预设 ({len(presets)})**")
    if presets:
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
                "策略参数": p.get("params", {}),
                "说明": p.get("description", ""),
            })
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
    else:
        st.info("暂无预设，使用下方表单创建")

    # --- 删除预设 ---
    if presets:
        with st.expander("🗑 删除预设"):
            del_name = st.selectbox("选择", options=presets, key="bt_del_sel")
            if st.button("确认删除", key="bt_del_btn"):
                if delete_backtest_preset(del_name):
                    st.success(f"已删除 {del_name}")
                    st.rerun()

    # --- 新建预设（动态渲染策略参数）---
    st.markdown("---")
    st.markdown("**➕ 新建预设**")

    # 策略选择（form 外，让切换策略能重绘表单）
    strategy_key = st.selectbox(
        "策略",
        options=list(STRATEGY_REGISTRY.keys()),
        format_func=lambda k: STRATEGY_LABELS.get(k, k),
        key="bt_strategy_sel",
    )

    with st.form("add_backtest_preset", clear_on_submit=True):
        name_input = st.text_input("预设名称（唯一）", placeholder="茅台_5_20")
        desc = st.text_input("说明（可选）", placeholder="贵州茅台 快5慢20")

        col1, col2 = st.columns(2)
        with col1:
            stock_code = st.text_input("股票代码", value="600519")
            days_back = st.number_input("回测天数", value=1000, min_value=100, max_value=5000,
                                        step=100)
        with col2:
            initial_cash = st.number_input("初始资金", value=100000, min_value=10000,
                                           max_value=10_000_000, step=10000)
            commission = st.number_input("手续费率", value=0.0002, min_value=0.0, max_value=0.01,
                                         step=0.0001, format="%.4f")

        st.markdown(f"**策略参数：{STRATEGY_LABELS.get(strategy_key, strategy_key)}**")
        schemas = STRATEGY_PARAM_SCHEMAS.get(strategy_key, [])
        strat_params = {}
        for s in schemas:
            key = s["key"]
            if s["type"] == "int":
                val = st.number_input(
                    s["label"], value=int(s["default"]),
                    min_value=int(s.get("min", -10**9)),
                    max_value=int(s.get("max", 10**9)),
                    step=int(s.get("step", 1)),
                    help=s.get("help"),
                    key=f"bt_{key}",
                )
                strat_params[key] = int(val)
            else:
                val = st.number_input(
                    s["label"], value=float(s["default"]),
                    step=float(s.get("step", 0.1)),
                    help=s.get("help"),
                    key=f"bt_{key}",
                )
                strat_params[key] = float(val)

        submit = st.form_submit_button("✅ 保存预设", type="primary")
        if submit:
            if not name_input.strip():
                st.error("预设名称不能为空")
            else:
                preset = {
                    "strategy": strategy_key,
                    "data": {"stock_code": stock_code.strip(),
                             "days_back": int(days_back)},
                    "account": {"initial_cash": int(initial_cash),
                                "commission": float(commission)},
                    "params": strat_params,
                    "description": desc.strip(),
                }
                if save_backtest_preset(name_input.strip(), preset):
                    st.success(f"已保存预设 {name_input}")
                    st.rerun()
                else:
                    st.error("写入失败")
