"""
src/web/components/pagination.py — 带分页的 DataFrame 展示

解决的核心问题：
  - 所有 DataFrame 全量渲染，大数据集卡顿
  - 长表格无法快速定位

用法：
  from src.web.components.pagination import paginated_dataframe

  paginated_dataframe(df, page_size=50, key="alert_history")
"""
from __future__ import annotations

import pandas as pd
import streamlit as st


def paginated_dataframe(
    df: pd.DataFrame,
    page_size: int = 50,
    key: str = "pager",
    **kwargs,
) -> None:
    """
    带分页的 DataFrame 展示。

    Args:
        df: 要展示的 DataFrame
        page_size: 每页条数
        key: 组件唯一标识
        **kwargs: 传递给 st.dataframe 的额外参数
    """
    total = len(df)
    if total == 0:
        st.info("暂无数据")
        return

    total_pages = max(1, (total - 1) // page_size + 1)

    # 分页控件
    nav_col, info_col = st.columns([1, 3])
    with nav_col:
        page = st.number_input(
            "页码",
            min_value=1,
            max_value=total_pages,
            value=1,
            key=f"{key}_page",
            step=1,
        )
    with info_col:
        start = (page - 1) * page_size
        end = min(start + page_size, total)
        st.caption(
            f"显示 {start + 1}-{end} / 共 {total} 条 · 第 {page}/{total_pages} 页"
        )

    # 渲染当前页
    page_df = df.iloc[start:end]
    st.dataframe(page_df, use_container_width=True, **kwargs)
