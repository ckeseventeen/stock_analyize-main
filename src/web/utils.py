"""
src/web/utils.py — Streamlit 前端共享工具（向后兼容聚合层）

本文件原为 1050 行超大模块，现已拆分为：
  - config_ops.py   : YAML 读写、原子写入、路径常量、通用工具
  - watchlist_ops.py : 关注标的 CRUD、profile 管理、财报关注

本文件作为向后兼容层，从子模块统一 re-export 所有公开 API，
确保现有的 `from src.web.utils import ...` 语句无需修改。
"""
from __future__ import annotations

# ========================
# 本文件保留的轻量工具（不值得单独拆模块）
# ========================
import pandas as pd

# ========================
# 从 config_ops 导入（路径常量 + YAML 读写 + 通用工具）
# ========================
from src.web.config_ops import (  # noqa: F401
    ALERT_LOG_PATH,
    ALERT_STATE_PATH,
    CACHE_DIR,
    CONFIG_DIR,
    LOGS_DIR,
    MARKET_CONFIG_PATHS,
    MARKET_LABELS,
    OUTPUT_DIR,
    PATH_A_STOCK,
    PATH_ALERTS,
    PATH_BACKTEST_PRESETS,
    PATH_EARNINGS,
    PATH_FACTORS,
    PATH_HK_STOCK,
    PATH_INDICATORS,
    PATH_PRICE_ALERTS,
    PATH_SCRAPER,
    PATH_SCREEN,
    PATH_US_STOCK,
    PROJECT_ROOT,
    _resolve_dotted,
    add_to_yaml_list,
    atomic_save_yaml,
    ensure_project_dirs,
    list_yaml_list,
    load_yaml,
    remove_from_yaml_list,
    safe_import,
    save_yaml,
    setup_matplotlib_chinese,
)

# ========================
# 从 watchlist_ops 导入（关注标的 CRUD + profile 管理）
# ========================
from src.web.watchlist_ops import (  # noqa: F401
    add_category_to_market,
    add_code_to_earnings_watchlist,
    add_stock_to_market,
    delete_backtest_preset,
    delete_factor_profile,
    delete_indicator_profile,
    get_active_factor_config,
    get_active_indicator_profile,
    list_backtest_presets,
    list_earnings_watchlist,
    list_factor_profiles,
    list_indicator_profiles,
    list_market_categories,
    load_backtest_preset,
    load_factor_profile,
    load_indicator_profile,
    move_stock_category,
    remove_category_from_market,
    remove_code_from_earnings_watchlist,
    remove_stock_from_market,
    save_backtest_preset,
    save_factor_profile,
    save_indicator_profile,
    set_active_factor_profile,
    set_active_indicator_profile,
    update_stock_in_market,
)


def list_stocks_from_market_config(market: str, ttl: int = 300) -> list[dict]:
    """
    从市场 YAML 提取所有股票（展平 categories）供下拉选择。

    Args:
        market: 市场标识 ("a", "hk", "us")
        ttl: 缓存秒数（默认 5 分钟），避免每次 rerun 重新读取 YAML

    Returns:
        [{"code": "...", "name": "...", "category": "...", "valuation": "pe", ...}, ...]
    """
    cfg_path = MARKET_CONFIG_PATHS.get(market)
    if not cfg_path:
        return []
    cfg = load_yaml(cfg_path, ttl=ttl)
    if not cfg:
        return []

    stocks: list[dict] = []
    for cat_key, cat_data in (cfg.get("categories") or {}).items():
        if not cat_data:
            continue
        cat_name = cat_data.get("name", cat_key)
        for stock in cat_data.get("stocks", []) or []:
            entry = dict(stock)
            entry["category"] = cat_name
            stocks.append(entry)
    return stocks


# ========================
# DataFrame 工具
# ========================

def format_numeric_columns(df: pd.DataFrame, decimals: int = 2) -> pd.DataFrame:
    """
    对 DataFrame 的数值列做保留小数格式化（返回副本，不改原数据）。
    用于前端表格展示，避免科学计数法/过多小数位。
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    for col in out.select_dtypes(include=["float", "float64", "float32"]).columns:
        out[col] = out[col].round(decimals)
    return out


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """DataFrame 转 UTF-8-SIG 编码的 CSV bytes，用于 st.download_button"""
    return df.to_csv(index=False).encode("utf-8-sig")


# ========================
# 可复用 UI 组件：紧凑式"加入关注"表单
# 供 1_估值分析 / 2_股票筛选 / 3_价格预警 / 7_策略回测 五页就地嵌入
# ========================

def quick_add_stock_widget(
    key_prefix: str,
    default_market: str = "a",
    default_code: str = "",
    default_name: str = "",
    default_valuation: str = "pe",
    default_range: list | None = None,
    expanded: bool = False,
    label: str = "➕ 加入关注列表",
) -> bool:
    """
    嵌入页面的紧凑式 "加入关注列表" 表单（st.expander 折叠）。

    用法::
        quick_add_stock_widget("page1", default_market=market,
                                default_code=code, default_name=name)

    每个 key_prefix 必须唯一，避免多组件在同页冲突。
    Returns:
        True 表示本次提交触发了写入（调用方可决定是否 st.rerun()）
    """
    import streamlit as st

    default_range = default_range or [10, 20, 30]
    markets = list(MARKET_LABELS.keys())
    try:
        default_market_idx = markets.index(default_market)
    except ValueError:
        default_market_idx = 0

    triggered = False
    with st.expander(label, expanded=expanded):
        st.caption("直接写入 a_stock.yaml / hk_stock.yaml / us_stock.yaml，无需手动编辑配置。")

        col_mkt, col_cat = st.columns([1, 2])
        with col_mkt:
            market = st.selectbox(
                "市场",
                options=markets,
                format_func=lambda k: MARKET_LABELS[k],
                index=default_market_idx,
                key=f"{key_prefix}_qa_market",
            )
        cats = list_market_categories(market)
        with col_cat:
            cat_opt_labels = ["<新建分类>"] + [f"{k} ({n})" for k, n in cats]
            cat_sel = st.selectbox("分类板块", options=cat_opt_labels,
                                   index=min(1, len(cat_opt_labels) - 1),
                                   key=f"{key_prefix}_qa_cat_sel")

        # 新建分类的补充字段
        if cat_sel == "<新建分类>":
            ck_col, cn_col = st.columns([1, 1])
            with ck_col:
                cat_key_input = st.text_input(
                    "新分类 key（英文）", placeholder="tech",
                    key=f"{key_prefix}_qa_new_cat_key",
                )
            with cn_col:
                cat_name_input = st.text_input(
                    "新分类 名称", placeholder="科技板块",
                    key=f"{key_prefix}_qa_new_cat_name",
                )
        else:
            cat_key_input, cat_name_input = None, None

        c1, c2 = st.columns([1, 1])
        with c1:
            code = st.text_input("股票代码", value=default_code,
                                 key=f"{key_prefix}_qa_code")
        with c2:
            name = st.text_input("股票名称", value=default_name,
                                 key=f"{key_prefix}_qa_name")

        c3 = st.columns([1, 1, 1, 1])
        with c3[0]:
            val_type = st.radio(
                "估值方式", options=["pe", "ps"],
                index=0 if default_valuation == "pe" else 1,
                horizontal=True, key=f"{key_prefix}_qa_val",
            )
        with c3[1]:
            r_low = st.number_input("低档位", value=float(default_range[0]),
                                    step=0.5, key=f"{key_prefix}_qa_low")
        with c3[2]:
            r_mid = st.number_input("中档位", value=float(default_range[1]),
                                    step=0.5, key=f"{key_prefix}_qa_mid")
        with c3[3]:
            r_high = st.number_input("高档位", value=float(default_range[2]),
                                     step=0.5, key=f"{key_prefix}_qa_high")

        if st.button("✅ 确认加入", key=f"{key_prefix}_qa_submit", type="primary"):
            code_s = code.strip()
            if not code_s:
                st.error("股票代码不能为空")
                return False

            # 确定 category_key：新建时先创建
            if cat_sel == "<新建分类>":
                ck = (cat_key_input or "").strip()
                cn = (cat_name_input or "").strip() or ck
                if not ck:
                    st.error("新分类 key 不能为空")
                    return False
                ok_cat, msg_cat = add_category_to_market(market, ck, cn)
                # "已存在" 时允许继续往该分类追加
                if not ok_cat and "已存在" not in msg_cat:
                    st.error(f"新建分类失败: {msg_cat}")
                    return False
                cat_key_final = ck
            else:
                # 已选已有分类，去掉 " (xxx)" 取 key
                cat_key_final = cats[cat_opt_labels.index(cat_sel) - 1][0]

            stock_entry = {
                "code": code_s,
                "name": (name or code_s).strip(),
                "valuation": val_type,
                f"{val_type}_range": [float(r_low), float(r_mid), float(r_high)],
            }
            ok, msg = add_stock_to_market(market, cat_key_final, stock_entry)
            if ok:
                st.success(
                    f"✅ 已添加 {stock_entry['name']} ({code_s}) → "
                    f"{MARKET_LABELS[market]} / {cat_key_final}"
                )
                triggered = True
            else:
                st.error(f"添加失败: {msg}")
    return triggered


def quick_add_earnings_widget(
    key_prefix: str,
    default_market: str = "a",
    default_code: str = "",
    expanded: bool = False,
    label: str = "➕ 加入财报关注列表",
) -> bool:
    """
    紧凑式 "加入财报关注" 表单 —— 写入 earnings_monitor.yaml 的 watchlist[market]。

    与 quick_add_stock_widget 分开：财报 watchlist 是简单 list[code] 而不是 categories。
    """
    import streamlit as st

    triggered = False
    markets = ["a", "hk", "us"]
    try:
        default_market_idx = markets.index(default_market)
    except ValueError:
        default_market_idx = 0

    with st.expander(label, expanded=expanded):
        st.caption(f"写入 `{PATH_EARNINGS}` 的 watchlist 段。")
        cols = st.columns([1, 2, 1])
        with cols[0]:
            market = st.selectbox(
                "市场", options=markets,
                format_func=lambda k: MARKET_LABELS.get(k, k),
                index=default_market_idx,
                key=f"{key_prefix}_ea_market",
            )
        with cols[1]:
            code = st.text_input(
                "股票代码", value=default_code,
                placeholder="A股6位/港股5位/美股ticker",
                key=f"{key_prefix}_ea_code",
            )
        with cols[2]:
            st.markdown("&nbsp;")  # 对齐
            if st.button("✅ 添加", key=f"{key_prefix}_ea_submit", type="primary"):
                ok, msg = add_code_to_earnings_watchlist(market, code)
                if ok:
                    st.success(f"{msg}: {code}")
                    triggered = True
                else:
                    st.error(msg)

        # 顺便展示该市场当前关注（供用户确认无重复）
        current = list_earnings_watchlist(market)
        if current:
            st.caption(f"当前 {MARKET_LABELS.get(market, market)} 关注: {', '.join(current)}")
    return triggered


def quick_edit_list_widget(
    *,
    path,
    dotted_key: str,
    key_prefix: str,
    label: str = "✏️ 管理此列表",
    placeholder: str = "",
    expanded: bool = False,
    help_text: str = "",
) -> bool:
    """
    折叠式的"列表增删"小组件，适用于 scraper.yaml 这类"每段 = 一个字符串列表"配置。

    Args:
        path: YAML 文件路径
        dotted_key: 点号路径（如 "news.keywords" / "announcements.watchlist"）
        key_prefix: Streamlit 组件 key 前缀（必须在当前页面唯一）
        label: expander 标题
        placeholder: 输入框占位文本
        expanded: 是否默认展开
        help_text: expander 顶部说明（可留空）

    Returns:
        True 表示本次发生了写入（调用方决定是否 st.rerun()）
    """
    import streamlit as st

    triggered = False
    current = list_yaml_list(path, dotted_key)

    with st.expander(label, expanded=expanded):
        if help_text:
            st.caption(help_text)

        # 显示当前项（如果较多则做成 caption，避免撑开太多）
        if current:
            st.caption(f"当前 {len(current)} 项: {', '.join(current)}")
        else:
            st.caption("（当前为空）")

        # 添加
        c_add1, c_add2 = st.columns([3, 1])
        with c_add1:
            new_val = st.text_input(
                "新增项",
                value="",
                placeholder=placeholder,
                key=f"{key_prefix}_qel_add",
                label_visibility="collapsed",
            )
        with c_add2:
            if st.button("➕ 添加", key=f"{key_prefix}_qel_add_btn",
                         width='stretch'):
                ok, msg = add_to_yaml_list(path, dotted_key, new_val)
                if ok:
                    st.success(f"{msg}: {new_val}")
                    triggered = True
                else:
                    st.error(msg)

        # 移除
        if current:
            c_rm1, c_rm2 = st.columns([3, 1])
            with c_rm1:
                rm_val = st.selectbox(
                    "删除项",
                    options=current,
                    key=f"{key_prefix}_qel_rm",
                    label_visibility="collapsed",
                )
            with c_rm2:
                if st.button("🗑 删除", key=f"{key_prefix}_qel_rm_btn",
                             width='stretch'):
                    ok, msg = remove_from_yaml_list(path, dotted_key, rm_val)
                    if ok:
                        st.success(f"{msg}: {rm_val}")
                        triggered = True
                    else:
                        st.error(msg)

    return triggered
