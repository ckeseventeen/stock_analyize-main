"""
pages/12_关注标的.py — 关注标的增删改查

功能：
  - 多市场（A 股 / 港股 / 美股）关注列表的完整 CRUD
  - 板块分类管理（新增 / 删除 / 移动股票到其他板块）
  - 在线编辑单只股票的名称、估值方式、档位参数
  - 搜索与筛选
  - 批量删除
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
    MARKET_LABELS,
    MARKET_CONFIG_PATHS,
    PATH_A_STOCK,
    PATH_HK_STOCK,
    PATH_US_STOCK,
    add_category_to_market,
    add_stock_to_market,
    list_market_categories,
    list_stocks_from_market_config,
    load_yaml,
    move_stock_category,
    remove_category_from_market,
    remove_stock_from_market,
    update_stock_in_market,
)

# ========================
# 行业分类常量
# ========================
INDUSTRY_OPTIONS = [
    "（未分类）",
    "半导体",
    "消费电子",
    "软件服务",
    "互联网",
    "人工智能",
    "云计算",
    "通信设备",
    "新能源",
    "光伏",
    "锂电池",
    "汽车",
    "医药生物",
    "医疗器械",
    "食品饮料",
    "白酒",
    "家电",
    "纺织服装",
    "零售",
    "银行",
    "保险",
    "证券",
    "房地产",
    "建筑建材",
    "钢铁",
    "有色金属",
    "化工",
    "机械设备",
    "电力设备",
    "军工",
    "交通运输",
    "农林牧渔",
    "传媒",
    "教育",
    "旅游酒店",
    "环保",
    "公用事业",
    "其他",
]

st.set_page_config(page_title="关注标的管理", page_icon="📌", layout="wide")

# ========================
# 自定义样式
# ========================
st.markdown("""
<style>
/* 页面标题渐变 */
div[data-testid="stAppViewBlockContainer"] > div:first-child h1 {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-weight: 800;
}

/* 指标卡片 */
div[data-testid="stMetric"] {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    border: 1px solid rgba(102, 126, 234, 0.3);
    border-radius: 12px;
    padding: 16px 20px;
    box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
}
div[data-testid="stMetric"] label {
    color: #a0aec0 !important;
    font-size: 0.85rem !important;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #e2e8f0 !important;
    font-weight: 700 !important;
}

/* 数据表格 */
div[data-testid="stDataFrame"] {
    border-radius: 10px;
    overflow: hidden;
}

/* 表单容器 */
div[data-testid="stForm"] {
    background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 100%);
    border: 1px solid rgba(102, 126, 234, 0.2);
    border-radius: 12px;
    padding: 20px;
}

/* Expander */
details[data-testid="stExpander"] {
    border: 1px solid rgba(102, 126, 234, 0.2) !important;
    border-radius: 10px !important;
    background: rgba(15, 15, 26, 0.6) !important;
}

/* 选中 Tab 高亮 */
button[data-baseweb="tab"][aria-selected="true"] {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
    border-radius: 8px 8px 0 0 !important;
    color: white !important;
}

/* 成功/错误消息 */
div[data-testid="stAlert"] {
    border-radius: 8px;
}
</style>
""", unsafe_allow_html=True)

# ========================
# 页面标题与概览
# ========================
st.title("📌 关注标的管理")
st.caption("统一管理 A 股 · 港股 · 美股关注列表，支持增删改查、板块分类、搜索筛选。")

# --- 统计卡片 ---
counts = {}
for mkt in MARKET_LABELS:
    counts[mkt] = len(list_stocks_from_market_config(mkt))
total = sum(counts.values())

col_total, col_a, col_hk, col_us = st.columns(4)
with col_total:
    st.metric("📊 总关注数", total)
with col_a:
    st.metric("🇨🇳 A 股", counts.get("a", 0))
with col_hk:
    st.metric("🇭🇰 港股", counts.get("hk", 0))
with col_us:
    st.metric("🇺🇸 美股", counts.get("us", 0))

st.markdown("---")

# ========================
# 市场选择
# ========================
market = st.selectbox(
    "选择市场",
    options=list(MARKET_LABELS.keys()),
    format_func=lambda k: f"{MARKET_LABELS[k]}  ({counts.get(k, 0)} 只)",
    key="wl_market",
)

cfg_path = MARKET_CONFIG_PATHS[market]
st.caption(f"配置文件：`{cfg_path}`")

# ========================
# 主 Tab
# ========================
tab_list, tab_add, tab_edit, tab_category = st.tabs([
    "📋 标的列表", "➕ 新增标的", "✏️ 编辑标的", "📂 板块管理",
])


# ==============================================================
# Tab 1: 标的列表（查、删、搜索、批量）
# ==============================================================
with tab_list:
    stocks = list_stocks_from_market_config(market)

    # --- 搜索 & 筛选 ---
    col_search, col_filter, col_ind_filter = st.columns([2, 1, 1])
    with col_search:
        search_q = st.text_input(
            "🔍 搜索（代码 / 名称 / 备注）",
            value="",
            placeholder="输入代码、名称或备注关键字...",
            key="wl_search",
        )
    with col_filter:
        cats = list_market_categories(market)
        cat_options = ["全部"] + [f"{n} ({k})" for k, n in cats]
        cat_filter = st.selectbox("筛选板块", options=cat_options, key="wl_cat_filter")
    with col_ind_filter:
        ind_filter_options = ["全部行业"] + INDUSTRY_OPTIONS[1:]  # 去掉"（未分类）"用原值
        ind_filter = st.selectbox("筛选行业", options=ind_filter_options, key="wl_ind_filter")

    # 应用筛选
    filtered = stocks
    if search_q.strip():
        q = search_q.strip().lower()
        filtered = [
            s for s in filtered
            if q in str(s.get("code", "")).lower()
            or q in str(s.get("name", "")).lower()
            or q in str(s.get("notes", "")).lower()
        ]
    if cat_filter != "全部":
        # 提取板块 name
        cat_name_selected = cat_filter.split(" (")[0]
        filtered = [s for s in filtered if s.get("category", "") == cat_name_selected]
    if ind_filter != "全部行业":
        filtered = [s for s in filtered if s.get("industry", "") == ind_filter]

    st.markdown(f"**共 {len(filtered)} 只标的**" + (f"（搜索/筛选后）" if len(filtered) != len(stocks) else ""))

    if filtered:
        df = pd.DataFrame([{
            "板块": s.get("category", ""),
            "代码": s.get("code", ""),
            "名称": s.get("name", ""),
            "行业": s.get("industry", "") or "—",
            "估值": s.get("valuation", "").upper(),
            "PE 档位": " / ".join(str(x) for x in s.get("pe_range", [])) if s.get("pe_range") else "—",
            "PS 档位": " / ".join(str(x) for x in s.get("ps_range", [])) if s.get("ps_range") else "—",
            "备注": s.get("notes", "") or "",
        } for s in filtered])

        # 使用 column_config 美化
        st.dataframe(
            df,
            width="stretch",
            hide_index=True,
            column_config={
                "板块": st.column_config.TextColumn("板块", width="small"),
                "代码": st.column_config.TextColumn("代码", width="small"),
                "名称": st.column_config.TextColumn("名称", width="small"),
                "行业": st.column_config.TextColumn("行业", width="small"),
                "估值": st.column_config.TextColumn("估值", width="small"),
                "PE 档位": st.column_config.TextColumn("PE 档位", width="small"),
                "PS 档位": st.column_config.TextColumn("PS 档位", width="small"),
                "备注": st.column_config.TextColumn("备注", width="medium"),
            },
        )
    else:
        st.info("🈳 暂无标的。请到 **➕ 新增标的** 添加。")

    # --- 单条删除 ---
    if filtered:
        st.markdown("---")
        st.markdown("**🗑️ 删除标的**")
        col_del, col_btn = st.columns([3, 1])
        with col_del:
            del_code = st.selectbox(
                "选择要删除的标的",
                options=[s["code"] for s in filtered],
                format_func=lambda c: f"{c} — {next((s['name'] for s in filtered if s['code'] == c), '')}",
                key="wl_del_sel",
            )
        with col_btn:
            st.markdown("&nbsp;")  # 对齐
            if st.button("🗑️ 确认删除", key="wl_del_btn", type="secondary", width="stretch"):
                ok, msg = remove_stock_from_market(market, del_code)
                if ok:
                    st.success(f"✅ 已删除 {del_code}")
                    st.rerun()
                else:
                    st.error(f"❌ {msg}")

    # --- 批量删除 ---
    if len(filtered) > 1:
        with st.expander("⚠️ 批量删除", expanded=False):
            st.warning("请谨慎操作！批量删除不可撤销。")
            batch_codes = st.multiselect(
                "选择要删除的标的",
                options=[s["code"] for s in filtered],
                format_func=lambda c: f"{c} — {next((s['name'] for s in filtered if s['code'] == c), '')}",
                key="wl_batch_del",
            )
            if batch_codes:
                if st.button(f"🗑️ 确认批量删除 ({len(batch_codes)} 只)", type="secondary",
                             key="wl_batch_del_btn"):
                    success_count = 0
                    for c in batch_codes:
                        ok, _ = remove_stock_from_market(market, c)
                        if ok:
                            success_count += 1
                    st.success(f"✅ 成功删除 {success_count} / {len(batch_codes)} 只")
                    st.rerun()


# ==============================================================
# Tab 2: 新增标的
# ==============================================================
with tab_add:
    st.subheader("➕ 新增关注标的")

    cats = list_market_categories(market)
    if not cats:
        st.warning("⚠️ 该市场尚无板块分类，请先到 **📂 板块管理** 创建。")
    else:
        with st.form("wl_add_form", clear_on_submit=True):
            st.markdown("**基本信息**")
            col1, col2, col3 = st.columns([1, 1, 1])
            with col1:
                cat_key = st.selectbox(
                    "所属板块",
                    options=[k for k, _ in cats],
                    format_func=lambda k: dict(cats).get(k, k),
                    key="wl_add_cat",
                )
            with col2:
                code = st.text_input("股票代码", placeholder="600519", key="wl_add_code")
            with col3:
                name = st.text_input("股票名称", placeholder="贵州茅台", key="wl_add_name")

            col_ind, col_notes = st.columns([1, 2])
            with col_ind:
                industry = st.selectbox(
                    "所属行业",
                    options=INDUSTRY_OPTIONS,
                    index=0,
                    key="wl_add_industry",
                )
            with col_notes:
                notes = st.text_input(
                    "备注",
                    placeholder="投资逻辑、关注理由等...",
                    key="wl_add_notes",
                )

            st.markdown("**估值参数**")
            col4, col5, col6 = st.columns([1, 2, 2])
            with col4:
                val_type = st.selectbox("估值方式", options=["pe", "ps"], key="wl_add_val")
            with col5:
                pe_str = st.text_input(
                    "PE 档位（低, 中, 高）", value="10, 20, 30",
                    help="三个数字用逗号分隔，分别代表低估/合理/高估的 PE 倍数",
                    key="wl_add_pe",
                )
            with col6:
                ps_str = st.text_input(
                    "PS 档位（低, 中, 高）", value="1, 2, 3",
                    help="三个数字用逗号分隔，分别代表低估/合理/高估的 PS 倍数",
                    key="wl_add_ps",
                )

            submit = st.form_submit_button("✅ 确认添加", type="primary")

            if submit:
                code_s = code.strip()
                name_s = name.strip()
                if not code_s:
                    st.error("❌ 股票代码不能为空")
                elif not name_s:
                    st.error("❌ 股票名称不能为空")
                else:
                    try:
                        pe_range = [float(x.strip()) for x in pe_str.split(",") if x.strip()]
                        ps_range = [float(x.strip()) for x in ps_str.split(",") if x.strip()]
                    except ValueError:
                        st.error("❌ 档位必须为数字，用逗号分隔")
                    else:
                        if len(pe_range) != 3 or len(ps_range) != 3:
                            st.error("❌ 每个档位必须恰好 3 个数字（低/中/高）")
                        else:
                            new_stock = {
                                "name": name_s,
                                "code": code_s,
                                "valuation": val_type,
                                "pe_range": pe_range,
                                "ps_range": ps_range,
                            }
                            # 行业 & 备注（非空才写入，保持 YAML 简洁）
                            if industry and industry != "（未分类）":
                                new_stock["industry"] = industry
                            if notes.strip():
                                new_stock["notes"] = notes.strip()
                            ok, msg = add_stock_to_market(market, cat_key, new_stock)
                            if ok:
                                st.success(f"✅ 已添加 {name_s}（{code_s}）→ {dict(cats).get(cat_key, cat_key)}")
                                st.rerun()
                            else:
                                st.error(f"❌ {msg}")

    # --- 快速添加：仅代码和名称 ---
    st.markdown("---")
    with st.expander("⚡ 快速添加（仅填代码名称，使用默认参数）"):
        if not cats:
            st.warning("请先创建板块分类")
        else:
            qcol1, qcol2, qcol3, qcol4 = st.columns([1.5, 1.5, 1, 1])
            with qcol1:
                q_code = st.text_input("代码", placeholder="000001", key="wl_quick_code")
            with qcol2:
                q_name = st.text_input("名称", placeholder="平安银行", key="wl_quick_name")
            with qcol3:
                q_cat = st.selectbox(
                    "板块",
                    options=[k for k, _ in cats],
                    format_func=lambda k: dict(cats).get(k, k),
                    key="wl_quick_cat",
                )
            with qcol4:
                st.markdown("&nbsp;")
                if st.button("⚡ 添加", key="wl_quick_btn", type="primary", width="stretch"):
                    if not q_code.strip():
                        st.error("代码不能为空")
                    else:
                        quick_stock = {
                            "name": (q_name or q_code).strip(),
                            "code": q_code.strip(),
                            "valuation": "pe",
                            "pe_range": [10.0, 20.0, 30.0],
                            "ps_range": [1.0, 2.0, 3.0],
                        }
                        ok, msg = add_stock_to_market(market, q_cat, quick_stock)
                        if ok:
                            st.success(f"✅ 快速添加成功: {q_code.strip()}")
                            st.rerun()
                        else:
                            st.error(f"❌ {msg}")


# ==============================================================
# Tab 3: 编辑标的（改）
# ==============================================================
with tab_edit:
    st.subheader("✏️ 编辑关注标的")

    stocks = list_stocks_from_market_config(market)
    if not stocks:
        st.info("🈳 该市场暂无标的，请先新增。")
    else:
        # 选择要编辑的股票
        edit_code = st.selectbox(
            "选择要编辑的标的",
            options=[s["code"] for s in stocks],
            format_func=lambda c: f"{c} — {next((s['name'] for s in stocks if s['code'] == c), '')}  [{next((s['category'] for s in stocks if s['code'] == c), '')}]",
            key="wl_edit_sel",
        )

        # 找到当前数据
        current = next((s for s in stocks if s["code"] == edit_code), None)
        if current:
            cur_industry = current.get("industry", "")
            cur_notes = current.get("notes", "")
            st.markdown(f"**当前信息**：{current.get('name', '')} / "
                        f"板块: {current.get('category', '')} / "
                        f"行业: {cur_industry or '未设置'} / "
                        f"估值: {current.get('valuation', '').upper()}")
            if cur_notes:
                st.caption(f"📝 备注：{cur_notes}")

            with st.form("wl_edit_form"):
                st.markdown("**编辑字段**")

                ecol1, ecol2 = st.columns(2)
                with ecol1:
                    edit_name = st.text_input(
                        "股票名称",
                        value=current.get("name", ""),
                        key="wl_edit_name",
                    )
                with ecol2:
                    edit_val = st.selectbox(
                        "估值方式",
                        options=["pe", "ps"],
                        index=0 if current.get("valuation", "pe") == "pe" else 1,
                        key="wl_edit_val",
                    )

                ecol_ind, ecol_notes = st.columns([1, 2])
                with ecol_ind:
                    # 确定当前行业在列表中的索引
                    try:
                        ind_idx = INDUSTRY_OPTIONS.index(cur_industry) if cur_industry in INDUSTRY_OPTIONS else 0
                    except ValueError:
                        ind_idx = 0
                    edit_industry = st.selectbox(
                        "所属行业",
                        options=INDUSTRY_OPTIONS,
                        index=ind_idx,
                        key="wl_edit_industry",
                    )
                with ecol_notes:
                    edit_notes = st.text_input(
                        "备注",
                        value=cur_notes,
                        placeholder="投资逻辑、关注理由等...",
                        key="wl_edit_notes",
                    )

                ecol3, ecol4 = st.columns(2)
                cur_pe = current.get("pe_range", [])
                cur_ps = current.get("ps_range", [])
                with ecol3:
                    edit_pe_str = st.text_input(
                        "PE 档位（低, 中, 高）",
                        value=", ".join(str(x) for x in cur_pe) if cur_pe else "",
                        key="wl_edit_pe",
                    )
                with ecol4:
                    edit_ps_str = st.text_input(
                        "PS 档位（低, 中, 高）",
                        value=", ".join(str(x) for x in cur_ps) if cur_ps else "",
                        key="wl_edit_ps",
                    )

                submit_edit = st.form_submit_button("💾 保存修改", type="primary")

                if submit_edit:
                    updates = {}
                    if edit_name.strip():
                        updates["name"] = edit_name.strip()
                    updates["valuation"] = edit_val

                    # 行业 & 备注
                    if edit_industry and edit_industry != "（未分类）":
                        updates["industry"] = edit_industry
                    else:
                        updates["industry"] = ""
                    updates["notes"] = edit_notes.strip()

                    # 解析 PE 档位
                    if edit_pe_str.strip():
                        try:
                            pe_vals = [float(x.strip()) for x in edit_pe_str.split(",") if x.strip()]
                            if len(pe_vals) == 3:
                                updates["pe_range"] = pe_vals
                            else:
                                st.error("❌ PE 档位必须是 3 个数字")
                                st.stop()
                        except ValueError:
                            st.error("❌ PE 档位格式错误")
                            st.stop()

                    # 解析 PS 档位
                    if edit_ps_str.strip():
                        try:
                            ps_vals = [float(x.strip()) for x in edit_ps_str.split(",") if x.strip()]
                            if len(ps_vals) == 3:
                                updates["ps_range"] = ps_vals
                            else:
                                st.error("❌ PS 档位必须是 3 个数字")
                                st.stop()
                        except ValueError:
                            st.error("❌ PS 档位格式错误")
                            st.stop()

                    if updates:
                        ok, msg = update_stock_in_market(market, edit_code, updates)
                        if ok:
                            st.success(f"✅ {msg}")
                            st.rerun()
                        else:
                            st.error(f"❌ {msg}")

            # --- 移动板块 ---
            st.markdown("---")
            st.markdown("**📦 移动到其他板块**")
            cats = list_market_categories(market)
            if len(cats) <= 1:
                st.caption("当前市场只有一个板块，无法移动。")
            else:
                mcol1, mcol2 = st.columns([3, 1])
                with mcol1:
                    move_target = st.selectbox(
                        "目标板块",
                        options=[k for k, _ in cats],
                        format_func=lambda k: dict(cats).get(k, k),
                        key="wl_move_cat",
                    )
                with mcol2:
                    st.markdown("&nbsp;")
                    if st.button("📦 移动", key="wl_move_btn", type="secondary", width="stretch"):
                        ok, msg = move_stock_category(market, edit_code, move_target)
                        if ok:
                            st.success(f"✅ 已将 {edit_code} 移动到 {dict(cats).get(move_target, move_target)}")
                            st.rerun()
                        else:
                            st.error(f"❌ {msg}")


# ==============================================================
# Tab 4: 板块管理
# ==============================================================
with tab_category:
    st.subheader("📂 板块分类管理")

    cats = list_market_categories(market)

    # --- 现有板块 ---
    if cats:
        st.markdown(f"**现有板块 ({len(cats)})**")
        cat_data = []
        for k, n in cats:
            cat_stocks = [s for s in list_stocks_from_market_config(market) if s.get("category") == n]
            cat_data.append({
                "Key": k,
                "名称": n,
                "标的数": len(cat_stocks),
                "标的代码": ", ".join(s.get("code", "") for s in cat_stocks[:8])
                           + ("..." if len(cat_stocks) > 8 else ""),
            })
        st.dataframe(
            pd.DataFrame(cat_data),
            width="stretch",
            hide_index=True,
            column_config={
                "Key": st.column_config.TextColumn("Key", width="small"),
                "名称": st.column_config.TextColumn("名称", width="small"),
                "标的数": st.column_config.NumberColumn("标的数", width="small"),
                "标的代码": st.column_config.TextColumn("标的代码", width="large"),
            },
        )
    else:
        st.info("🈳 该市场暂无板块分类。")

    # --- 新增板块 ---
    st.markdown("---")
    st.markdown("**➕ 新增板块**")
    with st.form("wl_add_cat_form", clear_on_submit=True):
        cc1, cc2 = st.columns(2)
        with cc1:
            new_cat_key = st.text_input(
                "板块 Key（英文，唯一标识）",
                placeholder="finance",
                key="wl_new_cat_key",
            )
        with cc2:
            new_cat_name = st.text_input(
                "板块名称（中文显示名）",
                placeholder="金融",
                key="wl_new_cat_name",
            )
        if st.form_submit_button("➕ 创建板块", type="primary"):
            k = new_cat_key.strip()
            n = new_cat_name.strip() or k
            if not k:
                st.error("❌ 板块 Key 不能为空")
            else:
                ok, msg = add_category_to_market(market, k, n)
                if ok:
                    st.success(f"✅ 已创建板块: {n} ({k})")
                    st.rerun()
                else:
                    st.error(f"❌ {msg}")

    # --- 删除板块 ---
    if cats:
        st.markdown("---")
        st.markdown("**🗑️ 删除板块**")
        st.warning("⚠️ 删除板块会同时删除其下所有标的，请谨慎操作！")
        dcol1, dcol2 = st.columns([3, 1])
        with dcol1:
            del_cat = st.selectbox(
                "选择要删除的板块",
                options=[k for k, _ in cats],
                format_func=lambda k: f"{dict(cats).get(k, k)} ({k})",
                key="wl_del_cat_sel",
            )
        with dcol2:
            st.markdown("&nbsp;")
            if st.button("🗑️ 确认删除板块", key="wl_del_cat_btn", type="secondary",
                         width="stretch"):
                ok, msg = remove_category_from_market(market, del_cat)
                if ok:
                    st.success(f"✅ {msg}")
                    st.rerun()
                else:
                    st.error(f"❌ {msg}")

# ========================
# 页脚
# ========================
st.markdown("---")
st.caption("💡 所有修改自动写入对应的 YAML 配置文件（a_stock.yaml / hk_stock.yaml / us_stock.yaml），Streamlit 刷新即生效。")
