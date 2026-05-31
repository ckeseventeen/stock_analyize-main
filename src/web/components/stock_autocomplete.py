"""
src/web/components/stock_autocomplete.py — 全市场股票名称/代码自动联想

数据源：ScreenerDataProvider.get_all_a_shares()，含 所属行业 字段（来自 pywencai）。

用法：
    from src.web.components.stock_autocomplete import stock_autocomplete

    picked = stock_autocomplete(key="add_holding")
    # picked = {"code": "603005", "name": "晶方科技",
    #           "industry": "半导体", "market": "a", "price": 28.5} or None
"""
from __future__ import annotations

from typing import Optional

import streamlit as st


# 用 cache_resource 而非 cache_data：cache_resource 是 process-wide 共享，
# 多 tab/多页面打开不会重复拉 4951 只全市场数据
@st.cache_resource(ttl=3600, show_spinner=False)
def _load_universe() -> list[dict]:
    """加载全市场股票（A 股，含名称+行业）；TTL 1 小时全进程共享"""
    try:
        from src.analysis.screening.data_provider import ScreenerDataProvider
        provider = ScreenerDataProvider()
        df = provider.get_all_a_shares()
    except Exception:
        return []
    if df is None or df.empty:
        return []

    code_col = "代码" if "代码" in df.columns else "code"
    name_col = "名称" if "名称" in df.columns else "name"
    price_col = "最新价" if "最新价" in df.columns else "close"
    ind_col = "所属行业" if "所属行业" in df.columns else None

    universe = []
    for _, row in df.iterrows():
        try:
            code = str(row.get(code_col, "")).zfill(6)
            name = str(row.get(name_col, "")).strip()
            if not code or not name:
                continue
            entry = {
                "code": code,
                "name": name,
                "market": "a",
                "price": float(row.get(price_col, 0) or 0),
                "industry": str(row.get(ind_col, "")) if ind_col else "",
            }
            universe.append(entry)
        except (ValueError, TypeError):
            continue
    return universe


def stock_autocomplete(
    label: str = "股票名称/代码",
    key: str = "stock_autocomplete",
    placeholder: str = "输入名称（如 晶方）或代码（如 603005）",
    max_suggestions: int = 10,
) -> Optional[dict]:
    """
    全市场股票联想选择器。

    Returns:
        选中的股票 dict 或 None
        dict 字段: code / name / market / price / industry
    """
    universe = _load_universe()
    if not universe:
        st.warning("⚠️ 联想数据未加载（首次使用需 ~90 秒加载全市场，请稍后重试）")
        # 退化为手动输入
        col1, col2 = st.columns(2)
        manual_code = col1.text_input(f"{label} - 代码", key=f"{key}_manual_code")
        manual_name = col2.text_input(f"{label} - 名称", key=f"{key}_manual_name")
        if manual_code.strip():
            return {"code": manual_code.strip().zfill(6), "name": manual_name.strip(),
                    "market": "a", "price": 0.0, "industry": ""}
        return None

    query = st.text_input(label, key=f"{key}_q", placeholder=placeholder)

    if not query.strip():
        st.caption(f"💡 全市场 {len(universe)} 只 A 股可联想")
        return None

    q = query.strip().lower()
    # 模糊匹配：代码前缀 > 代码包含 > 名称包含
    code_prefix = []
    code_contain = []
    name_contain = []
    for stk in universe:
        code = stk["code"].lower()
        name = stk["name"].lower()
        if code.startswith(q):
            code_prefix.append(stk)
        elif q in code:
            code_contain.append(stk)
        elif q in name:
            name_contain.append(stk)

    matched = (code_prefix + code_contain + name_contain)[:max_suggestions]
    if not matched:
        st.warning(f"无匹配股票：{query}")
        return None

    options = [
        f"{m['name']} ({m['code']}) · {m['industry'] or '—'} · ¥{m['price']:.2f}"
        for m in matched
    ]
    idx = st.selectbox(
        f"候选（{len(matched)} 只）",
        options=range(len(options)),
        format_func=lambda i: options[i],
        key=f"{key}_pick",
    )
    return matched[idx]
