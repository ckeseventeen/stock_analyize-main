"""
src/web/widgets/stats_cards.py — 多指标卡片一行展示

取代 12 个页面里散落的 4 列 st.metric 样板。
"""
from __future__ import annotations

from typing import Any


def stats_row(items: list[tuple[str, Any, str | None]] | list[dict]) -> None:
    """
    渲染一行 metric 卡片。

    items 支持两种格式：
      - [(label, value, delta), ...]  最简
      - [{"label":..., "value":..., "delta":..., "help":...}, ...]  全功能

    用法：
        stats_row([
            ("当前价", "100.5", "+1.5%"),
            ("PE", "28.5", None),
            ("市值(亿)", "1023", None),
        ])
    """
    import streamlit as st

    if not items:
        return
    cols = st.columns(len(items))
    for col, item in zip(cols, items):
        with col:
            if isinstance(item, dict):
                st.metric(
                    label=item.get("label", ""),
                    value=item.get("value", ""),
                    delta=item.get("delta"),
                    help=item.get("help"),
                )
            else:
                # tuple 形式
                label = item[0] if len(item) > 0 else ""
                value = item[1] if len(item) > 1 else ""
                delta = item[2] if len(item) > 2 else None
                st.metric(label=label, value=value, delta=delta)


def kv_block(items: dict[str, Any], cols: int = 2) -> None:
    """
    把一组 key-value 信息以多列布局展示（取代散落的 st.write 拼接）。
    """
    import streamlit as st

    keys = list(items.keys())
    if not keys:
        return
    columns = st.columns(cols)
    for i, k in enumerate(keys):
        with columns[i % cols]:
            st.markdown(f"**{k}**")
            st.write(items[k])
