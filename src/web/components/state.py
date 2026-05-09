"""
src/web/components/state.py — 统一分析状态管理

解决的核心问题：
  - 分析结果在 sidebar 参数变化后消失
  - 切换股票后仍显示旧结果（stale state）
  - 各页面自行管理 session_state，逻辑不一致

用法：
  from src.web.components.state import save_result, load_result, is_stale, stale_warning

  # 保存分析结果
  save_result("valuation", stock_code, market, params_dict, result_data)

  # 加载（自动检测 stale）
  result = load_result("valuation", stock_code, market, params_dict)

  # 在 UI 中显示 stale 警告
  if is_stale("valuation", stock_code, market, params_dict):
      stale_warning(stock_code)
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

import streamlit as st


def _params_hash(params: dict) -> str:
    """参数指纹，用于检测参数是否变化"""
    return hashlib.md5(
        json.dumps(params, sort_keys=True, default=str).encode()
    ).hexdigest()[:8]


def save_result(
    key: str,
    stock_code: str,
    market: str,
    params: dict,
    result: Any,
) -> None:
    """
    保存分析结果到 session_state。

    Args:
        key: 结果标识（如 "valuation", "fcf", "backtest"）
        stock_code: 股票代码
        market: 市场标识
        params: 参数字典（用于 stale 检测）
        result: 分析结果数据
    """
    st.session_state[f"_state_{key}"] = {
        "stock_code": stock_code,
        "market": market,
        "params_hash": _params_hash(params),
        "result": result,
    }


def load_result(
    key: str,
    stock_code: str,
    market: str,
    params: dict,
) -> Any | None:
    """
    加载分析结果。自动检测 stale：
    - 股票代码变了 → None
    - 市场变了 → None
    - 参数变了 → None
    - 都没变 → 返回结果

    Returns:
        分析结果，或 None（无结果/stale）
    """
    state = st.session_state.get(f"_state_{key}")
    if state is None:
        return None
    if state["stock_code"] != stock_code:
        return None
    if state["market"] != market:
        return None
    if state["params_hash"] != _params_hash(params):
        return None
    return state["result"]


def is_stale(
    key: str,
    stock_code: str,
    market: str,
    params: dict,
) -> bool:
    """
    检查当前显示的结果是否与当前参数不匹配。

    Returns:
        True = 结果是 stale 的（股票/参数已变但结果未刷新）
    """
    state = st.session_state.get(f"_state_{key}")
    if state is None:
        return False  # 没有结果，不算 stale
    return (
        state["stock_code"] != stock_code
        or state["market"] != market
        or state["params_hash"] != _params_hash(params)
    )


def get_stale_stock(key: str) -> str | None:
    """获取 stale 结果对应的旧股票代码（用于警告消息）"""
    state = st.session_state.get(f"_state_{key}")
    if state is None:
        return None
    return state.get("stock_code")


def stale_warning(key: str, current_code: str) -> None:
    """
    显示 stale 警告横幅。当用户切换了股票但还没点击"分析"时，
    提示当前显示的是旧股票的结果。

    Args:
        key: 结果标识
        current_code: 当前选中的股票代码
    """
    old_code = get_stale_stock(key)
    if old_code and old_code != current_code:
        st.warning(
            f"⚠️ 当前显示的是 **{old_code}** 的分析结果，"
            f"与您选择的 **{current_code}** 不匹配。"
            f"请点击「开始分析」刷新结果。"
        )
    elif is_stale(key, current_code, "", {}):
        st.warning(
            "⚠️ 分析参数已变更，当前结果可能不是最新的。"
            "请点击「开始分析」刷新。"
        )
