"""
pages/3_股票筛选.py — 股票筛选执行页

功能：
  - 加载 screen_config.yaml 的条件配置（只读展示）
  - 触发 StockScreener.run_from_config()
  - 表格展示结果 + 下载 CSV
  - 策略编辑请前往「策略配置」页
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
    PATH_SCREEN,
    df_to_csv_bytes,
    load_yaml,
    quick_add_stock_widget,
)

st.set_page_config(page_title="股票筛选", page_icon="🔍", layout="wide")
st.title("🔍 股票筛选")
st.caption("执行筛选策略并查看结果  |  编辑策略请前往「⚙️ 策略配置」页")


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

# ========================
# 侧边栏：板块/指数范围
# ========================
st.sidebar.markdown("---")
st.sidebar.markdown("**🏷️ 筛选范围（板块/指数）**")
st.sidebar.caption('可多选，取并集。不选或选「全部A股」表示不限制范围。')

from src.analysis.screening.data_provider import ScreenerDataProvider as _SDP  # noqa: E402

_SCOPE_OPTIONS = _SDP.SCOPE_DEFINITIONS  # {"全部A股": "all", "沪市主板": "sh_main", ...}

selected_scope_labels = st.sidebar.multiselect(
    "选择板块/指数",
    options=list(_SCOPE_OPTIONS.keys()),
    default=["全部A股"],
    help='多选时取并集：如同时选「沪市主板」+「创业板」，则两个板块的股票都纳入筛选范围',
)

# 将中文标签转为内部 key
selected_scope_keys = [_SCOPE_OPTIONS[label] for label in selected_scope_labels]

# 显示当前范围信息
if not selected_scope_keys or "all" in selected_scope_keys:
    st.sidebar.success("📊 范围：全部A股")
else:
    st.sidebar.info(f"📊 范围：{' + '.join(selected_scope_labels)}")

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
from src.analysis.screening.config_schema import list_strategies
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
from src.analysis.screening.config_schema import parse_screen_config
conditions, output_cfg = parse_screen_config(str(config_path), strategy_ids=selected_ids)

# 关键信息摘要
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("方案数", len(selected_ids))
with col2:
    st.metric("总条件数", len(conditions))
with col3:
    st.metric("结果上限", output_cfg.get("limit", "无限制"))
with col4:
    scope_text = "全部A股" if (not selected_scope_keys or "all" in selected_scope_keys) \
        else " + ".join(selected_scope_labels)
    st.metric("筛选范围", scope_text)

# 运行筛选
if run_btn:
    with st.spinner(f"正在执行筛选（方案: {selected_ids}）..."):
        try:
            from src.analysis.screening import ScreenerDataProvider, StockScreener
            provider = ScreenerDataProvider()
            if clear_cache:
                n = provider._cache.clear_all()
                st.info(f"已清除缓存 {n} 个文件")

            # 获取板块/指数范围
            stock_scope = None
            if selected_scope_keys and "all" not in selected_scope_keys:
                with st.spinner("正在获取板块/指数成分股..."):
                    stock_scope = provider.get_scope_codes(selected_scope_keys)
                    if stock_scope:
                        st.info(f"📊 板块/指数范围限定：{len(stock_scope)} 只股票")
                    else:
                        st.warning("板块/指数成分股获取失败，将使用全部A股")

            screener = StockScreener(
                data_provider=provider,
                request_delay=float(request_delay),
                max_workers=int(max_workers),
            )
            t0 = pd.Timestamp.now()
            results = screener.run_from_config(
                str(config_path),
                strategy_ids=selected_ids,
                stock_scope=stock_scope,
            )
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
# 策略条件详情预览（只读）
# ========================
st.markdown("---")
st.subheader("📋 当前选中策略的条件详情")
st.caption("编辑策略请前往侧边栏「⚙️ 策略配置」页面。")

from src.analysis.screening.conditions import CONDITION_LABELS as _CL  # noqa: E402

for cond in conditions:
    ctype = getattr(cond, "name", type(cond).__name__)
    clabel = _CL.get(ctype, ctype)
    attrs = {k: v for k, v in vars(cond).items() if not k.startswith("_")}
    attrs_str = ", ".join(f"{k}={v}" for k, v in attrs.items()) if attrs else "（无参数）"
    st.markdown(f"- **{clabel}** `{ctype}` — {attrs_str}")

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
