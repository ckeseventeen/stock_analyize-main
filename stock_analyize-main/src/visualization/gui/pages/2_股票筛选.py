"""
pages/2_股票筛选.py — 股票筛选页

功能：
  - 加载 screen_config.yaml 的条件配置（只读展示，方便用户查看）
  - 触发 StockScreener.run_from_config()
  - 表格展示结果 + 下载 CSV
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
    PATH_SCREEN,
    df_to_csv_bytes,
    load_yaml,
)

st.set_page_config(page_title="股票筛选", page_icon="🔍", layout="wide")
st.title("🔍 股票筛选")
st.caption("基于 screen_config.yaml 配置的条件组合进行 A 股筛选")


# ========================
# 侧边栏：配置文件选择
# ========================

st.sidebar.markdown("**配置文件**")
config_path_input = st.sidebar.text_input(
    "YAML 路径",
    value=str(PATH_SCREEN),
    help="默认使用 config/screen_config.yaml",
)
config_path = Path(config_path_input)

run_btn = st.sidebar.button("▶️ 开始筛选", type="primary", use_container_width=True)


# ========================
# 当前配置预览
# ========================

st.subheader("📋 当前筛选配置")

if not config_path.exists():
    st.error(f"配置文件不存在: {config_path}")
    st.stop()

cfg = load_yaml(config_path)
if not cfg:
    st.error("配置加载失败或为空")
    st.stop()

# 用 YAML 源码展示（保留用户视角）
try:
    with open(config_path, encoding="utf-8") as f:
        raw = f.read()
    with st.expander("🔧 查看完整 YAML", expanded=False):
        st.code(raw, language="yaml")
except Exception:
    pass

# 关键信息摘要
col1, col2, col3 = st.columns(3)
with col1:
    conditions = cfg.get("conditions", []) or []
    st.metric("启用的条件数", len([c for c in conditions if c.get("enable", True)]))
with col2:
    filters = cfg.get("filters", []) or []
    st.metric("启用的过滤器数", len([f for f in filters if f.get("enable", True)]))
with col3:
    st.metric("最多返回股票", cfg.get("limit", "无限制"))


# ========================
# 运行筛选
# ========================

if run_btn:
    with st.spinner("正在执行筛选（可能需要 1-3 分钟，取决于全市场数据规模）..."):
        try:
            from src.screener import StockScreener
            screener = StockScreener()
            results = screener.run_from_config(str(config_path))
        except Exception as e:
            st.error(f"筛选执行失败: {e}")
            st.exception(e)
            st.stop()

    if results is None or results.empty:
        st.warning("筛选结果为空，未找到符合条件的股票")
    else:
        st.success(f"✅ 筛选完成：共 {len(results)} 只股票符合条件")

        # 结果表格（支持排序/筛选）
        st.dataframe(results, use_container_width=True, hide_index=True)

        # 下载按钮
        st.download_button(
            label="💾 下载 CSV",
            data=df_to_csv_bytes(results),
            file_name=f"screen_result_{pd.Timestamp.now():%Y%m%d_%H%M%S}.csv",
            mime="text/csv",
            type="primary",
        )

else:
    st.info("👈 点击侧边栏的「开始筛选」按钮执行")
