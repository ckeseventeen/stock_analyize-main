"""
src/web/components/market_stock_selector.py — 市场+股票选择器公共组件

解决的核心问题：
  - 1_估值分析.py、11_FCF分析.py 等页面存在重复的市场+股票选择代码
  - 模式：市场选择 → YAML加载股票 → searchable_select → 手动覆盖

用法：
    from src.web.components.market_stock_selector import market_stock_selector

    result = market_stock_selector(
        key_prefix="valuation",
        default_market="a",
    )
    # result = {
    #     "market": "a",
    #     "code": "600519",
    #     "name": "贵州茅台",
    #     "valuation": "pe",
    #     "pe_range": [10, 20, 30],
    #     "ps_range": [1, 2, 3],
    # }
"""
from __future__ import annotations

import streamlit as st

from src.web.components.search import searchable_select
from src.web.utils import (
    MARKET_LABELS,
    list_stocks_from_market_config,
)


def market_stock_selector(
    key_prefix: str,
    default_market: str = "a",
    show_market: bool = True,
    show_manual_override: bool = True,
    show_valuation_params: bool = True,
    sidebar: "st.sidebar" = None,  # type hint only
) -> dict:
    """
    统一的市场+股票选择器组件。

    Args:
        key_prefix: 组件唯一标识前缀（用于 session_state key）
        default_market: 默认市场
        show_market: 是否显示市场选择器（ False=由调用方控制市场）
        show_manual_override: 是否显示"手动覆盖"区块
        show_valuation_params: 是否显示估值方式和档位参数
        sidebar: 可传入 st.sidebar 或其他容器，默认 st.sidebar

    Returns:
        dict: {
            "market": str,       # 市场标识 "a"/"hk"/"us"
            "code": str,         # 股票代码
            "name": str,         # 股票名称
            "valuation": str,    # "pe" 或 "ps"
            "pe_range": list,    # [低, 中, 高]
            "ps_range": list,    # [低, 中, 高]
        }
    """
    container = st.sidebar if sidebar is None else sidebar

    # --- 市场选择 ---
    if show_market:
        markets = list(MARKET_LABELS.keys())
        try:
            default_idx = markets.index(default_market)
        except ValueError:
            default_idx = 0
        market = container.selectbox(
            "市场",
            options=markets,
            format_func=lambda k: MARKET_LABELS[k],
            index=default_idx,
            key=f"{key_prefix}_market",
        )
    else:
        market = default_market

    # --- 加载YAML股票列表 ---
    stocks = list_stocks_from_market_config(market)

    with container:
        container.markdown("---")
        container.markdown("**股票选择**")

        if stocks:
            selected_code = searchable_select(
                "选择股票",
                options=stocks,
                key=f"{key_prefix}_stock",
                id_field="code",
                name_field="name",
            )
            if selected_code:
                selected = next(
                    (s for s in stocks if s["code"] == selected_code), stocks[0]
                )
            else:
                selected = stocks[0]
            default_code = selected["code"]
            default_name = selected["name"]
            default_val = selected.get("valuation", "pe")
            default_range = selected.get(f"{default_val}_range", [10, 20, 30])
        else:
            container.caption("该市场 YAML 中暂无股票，请手动输入")
            default_code, default_name, default_val, default_range = "", "", "pe", [10, 20, 30]

        # --- 手动覆盖区块 ---
        if show_manual_override:
            container.markdown("---")
            container.markdown("**或手动覆盖**")

        code = container.text_input(
            "股票代码",
            value=default_code,
            key=f"{key_prefix}_code",
        )
        name = container.text_input(
            "股票名称",
            value=default_name,
            key=f"{key_prefix}_name",
        )

        # --- 估值方式和档位 ---
        if show_valuation_params:
            val_type = container.radio(
                "估值方式",
                options=["pe", "ps"],
                index=0 if default_val == "pe" else 1,
                horizontal=True,
                key=f"{key_prefix}_val_type",
            )
            container.caption("估值档位（低 / 合理 / 高）")
            c1, c2, c3 = container.columns(3)
            with c1:
                r_low = container.number_input(
                    "低", value=float(default_range[0]), step=0.5,
                    key=f"{key_prefix}_r_low",
                )
            with c2:
                r_mid = container.number_input(
                    "中", value=float(default_range[1]), step=0.5,
                    key=f"{key_prefix}_r_mid",
                )
            with c3:
                r_high = container.number_input(
                    "高", value=float(default_range[2]), step=0.5,
                    key=f"{key_prefix}_r_high",
                )
        else:
            val_type = default_val
            r_low, r_mid, r_high = default_range[0], default_range[1], default_range[2]

        # 档位默认值（如果显示估值参数则用实际值，否则用默认值）
        if not show_valuation_params:
            pe_range = [10, 20, 30]
            ps_range = [1, 2, 3]
        else:
            pe_range = [r_low, r_mid, r_high] if val_type == "pe" else [10, 20, 30]
            ps_range = [r_low, r_mid, r_high] if val_type == "ps" else [1, 2, 3]

    return {
        "market": market,
        "code": code,
        "name": name,
        "valuation": val_type,
        "pe_range": pe_range,
        "ps_range": ps_range,
    }
