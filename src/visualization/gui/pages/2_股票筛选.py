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
    quick_add_stock_widget,
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

st.sidebar.markdown("---")
st.sidebar.markdown("**⚡ 性能参数**")
max_workers = st.sidebar.slider(
    "并行 worker 数",
    min_value=1, max_value=16, value=8, step=1,
    help="akshare 批量并发 HTTP 预取 K 线 + Phase B 线程池评估。"
         "建议 6-10；过高可能触发东方财富限频",
)
request_delay = st.sidebar.number_input(
    "每股请求间隔 (秒)",
    min_value=0.0, max_value=2.0, value=0.0, step=0.05,
    help="Phase A 并发预取完成后几乎不用，默认 0",
)
clear_cache = st.sidebar.checkbox(
    "忽略缓存（强制重新拉K线）", value=False,
    help="默认使用 .cache/screener 中的 4 小时缓存。勾选后会删除缓存重新拉。",
)
st.sidebar.info(
    "💡 **速度说明**\n\n"
    "新版本改用 akshare 并发批量预取 K 线，"
    "从 20+ 分钟优化到约 2-3 分钟。\n\n"
    "⚠️ **首次改动代码后须重启** `streamlit run ...`，否则 Streamlit "
    "仍会用旧的串行逻辑（Streamlit 只热重载 pages/，不重载 src/）。"
)

run_btn = st.sidebar.button("▶️ 开始筛选", type="primary", width='stretch')


# ========================
# 当前配置预览
# ========================

st.subheader("📋 当前筛选配置")

if not config_path.exists():
    st.error(f"配置文件不存在: {config_path}")
    st.stop()

# 策略选择
from src.screener.config_schema import list_strategies
available_strategies = list_strategies(str(config_path))

if not available_strategies:
    st.error("配置加载失败或未定义策略 (strategies 键)")
    st.stop()

st.subheader("🎯 选择筛选方案")
selected_ids = st.multiselect(
    "可以勾选多个方案进行组合（取交集）",
    options=list(available_strategies.keys()),
    default=[list(available_strategies.keys())[0]],
    format_func=lambda sid: available_strategies[sid],
    help="勾选多个方案时，系统会合并所有方案的筛选条件（AND 逻辑）"
)

if not selected_ids:
    st.warning("请至少选择一个方案")
    st.stop()

# 加载预览用的配置
from src.screener.config_schema import parse_screen_config
conditions, output_cfg = parse_screen_config(str(config_path), strategy_ids=selected_ids)

# 关键信息摘要
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("方案数", len(selected_ids))
with col2:
    st.metric("总条件数", len(conditions))
with col3:
    st.metric("结果上限", output_cfg.get("limit", "无限制"))

# 运行筛选
if run_btn:
    with st.spinner(f"正在执行筛选（方案: {selected_ids}）..."):
        try:
            from src.screener import ScreenerDataProvider, StockScreener
            provider = ScreenerDataProvider()
            if clear_cache:
                n = provider._cache.clear_all()
                st.info(f"已清除缓存 {n} 个文件")
            screener = StockScreener(
                data_provider=provider,
                request_delay=float(request_delay),
                max_workers=int(max_workers),
            )
            t0 = pd.Timestamp.now()
            results = screener.run_from_config(str(config_path), strategy_ids=selected_ids)
            elapsed = (pd.Timestamp.now() - t0).total_seconds()
            st.caption(f"⏱ 本次筛选总耗时 **{elapsed:.1f}s**")
        except Exception as e:
            st.error(f"筛选执行失败: {e}")
            st.exception(e)
            st.stop()

    if results is None or results.empty:
        st.warning("筛选结果为空，未找到符合条件的股票")
    else:
        st.success(f"✅ 筛选完成：共 {len(results)} 只股票符合条件")

        # 结果表格（支持排序/筛选）
        st.dataframe(results, width='stretch', hide_index=True)

        # 下载按钮
        st.download_button(
            label="💾 下载 CSV",
            data=df_to_csv_bytes(results),
            file_name=f"screen_result_{pd.Timestamp.now():%Y%m%d_%H%M%S}.csv",
            mime="text/csv",
            type="primary",
        )

        # 缓存结果给下方"加入关注"组件使用
        st.session_state["_screen_last_results"] = results

# ========================
# 策略管理：直接编辑 YAML
# ========================
st.markdown("---")
st.subheader("🛠️ 策略方案管理")
st.caption("你可以在这里直接修改 YAML 配置，添加新方案或修改参数。修改后点击“保存并应用”。")

with st.expander("📝 编辑 screen_config.yaml", expanded=False):
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            yaml_content = f.read()
        
        new_yaml = st.text_area(
            "YAML 内容",
            value=yaml_content,
            height=400,
            key="yaml_editor"
        )
        
        c1, c2 = st.columns([1, 4])
        if c1.button("💾 保存并应用", type="primary"):
            try:
                # 校验语法
                import yaml
                test_cfg = yaml.safe_load(new_yaml)
                if not isinstance(test_cfg, dict) or "strategies" not in test_cfg:
                    st.error("格式错误：必须包含 strategies 顶级键")
                else:
                    with open(config_path, "w", encoding="utf-8") as f:
                        f.write(new_yaml)
                    st.success("✅ 配置已更新")
                    st.rerun()
            except Exception as e:
                st.error(f"保存失败：{e}")
    except Exception as e:
        st.error(f"读取配置失败: {e}")

# ========================
# 从筛选结果加入关注列表（任何时候都可用）
# ========================
st.markdown("---")
st.subheader("⭐ 把结果加入关注列表")

cached_results = st.session_state.get("_screen_last_results")
prefill_code, prefill_name = "", ""

if cached_results is not None and not cached_results.empty:
    # 尝试定位 code/name 列（不同条件组合可能列名略有差异）
    code_col = next((c for c in ("代码", "code") if c in cached_results.columns), None)
    name_col = next((c for c in ("名称", "name") if c in cached_results.columns), None)

    if code_col:
        options = [
            f"{row[name_col] if name_col else ''} ({row[code_col]})"
            for _, row in cached_results.iterrows()
        ]
        picked_idx = st.selectbox(
            "从上方筛选结果中选一只（或下方手动输入）",
            options=range(len(options)),
            format_func=lambda i: options[i],
            key="screen_pick_row",
        )
        picked_row = cached_results.iloc[picked_idx]
        prefill_code = str(picked_row[code_col])
        prefill_name = str(picked_row[name_col]) if name_col else ""
else:
    st.caption("提示：运行一次筛选后，可从结果中一键选股加入关注。")

if quick_add_stock_widget(
    key_prefix="page2_screener",
    default_market="a",
    default_code=prefill_code,
    default_name=prefill_name,
    expanded=bool(prefill_code),
):
    st.rerun()

if not run_btn:
    st.info("👈 点击侧边栏的「开始筛选」按钮执行")
