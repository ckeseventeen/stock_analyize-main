# -*- coding: utf-8 -*-
"""
pages/18_荐股逆向.py — 荐股服务逆向工程分析页

功能：
  - 管理推荐记录列表（增删改 PICKS）
  - 管理空仓日列表
  - 一键执行特征提取 + 全市场横截面分位
  - 交互式结果表格（支持排序/筛选/下载）
  - 可视化特征分布（柱状图 + 雷达图）
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from src.web.utils import df_to_csv_bytes  # noqa: E402

st.set_page_config(page_title="荐股逆向工程", page_icon="🔬", layout="wide")
st.title("🔬 荐股逆向工程")
st.caption("对推荐标的提取触发日量价/形态/题材特征，并与全市场做横截面对照")

# ========================
# Session State 初始化
# ========================
if "re_picks" not in st.session_state:
    from src.analysis.reverse_engineer import DEFAULT_PICKS, DEFAULT_EMPTY_DAYS
    st.session_state["re_picks"] = list(DEFAULT_PICKS)
    st.session_state["re_empty_days"] = list(DEFAULT_EMPTY_DAYS)

if "re_feats_df" not in st.session_state:
    st.session_state["re_feats_df"] = None
    st.session_state["re_ctx_df"] = None


# ========================
# 侧边栏：推荐记录管理
# ========================
st.sidebar.markdown("## 📝 推荐记录管理")

# ---- 添加新记录 ----
with st.sidebar.expander("➕ 添加推荐记录", expanded=False):
    new_date = st.text_input("推荐日 (YYYYMMDD)", placeholder="20260528", key="re_new_date")
    new_code = st.text_input("股票代码 (6位)", placeholder="600378", key="re_new_code")
    new_name = st.text_input("股票名称", placeholder="昊华科技", key="re_new_name")
    new_theme = st.text_input("题材标签", placeholder="化学制品", key="re_new_theme")
    new_px = st.number_input("触发价", min_value=0.0, step=0.01, key="re_new_px")

    if st.button("✅ 添加", key="re_add_btn", type="primary"):
        if new_date and new_code and new_name:
            st.session_state["re_picks"].append(
                (new_date.strip(), new_code.strip(), new_name.strip(),
                 new_theme.strip(), float(new_px))
            )
            st.toast(f"✅ 已添加 {new_name} ({new_code})", icon="✅")
            st.rerun()
        else:
            st.error("推荐日、代码、名称不能为空")

# ---- 空仓日管理 ----
with st.sidebar.expander("📅 空仓日管理", expanded=False):
    empty_days_str = ", ".join(st.session_state["re_empty_days"])
    st.caption(f"当前空仓日: {empty_days_str or '无'}")

    new_empty = st.text_input("新增空仓日 (YYYYMMDD)", placeholder="20260529", key="re_new_empty")
    col_add, col_clear = st.columns(2)
    with col_add:
        if st.button("➕ 添加", key="re_add_empty"):
            if new_empty.strip() and len(new_empty.strip()) == 8:
                if new_empty.strip() not in st.session_state["re_empty_days"]:
                    st.session_state["re_empty_days"].append(new_empty.strip())
                    st.toast(f"✅ 已添加空仓日 {new_empty.strip()}", icon="📅")
                    st.rerun()
                else:
                    st.warning("该日期已存在")
            else:
                st.error("日期格式错误")
    with col_clear:
        if st.button("🗑 清空", key="re_clear_empty"):
            st.session_state["re_empty_days"] = []
            st.rerun()

st.sidebar.markdown("---")

# ---- 执行按钮 ----
run_btn = st.sidebar.button("🚀 开始分析", type="primary", use_container_width=True)

st.sidebar.markdown("---")

# 数据源状态：tushare 历史截面 vs 今日近似
from src.analysis.reverse_engineer import _get_tushare_pro  # noqa: E402

if _get_tushare_pro() is not None:
    st.sidebar.success(
        "✅ TUSHARE_TOKEN 已配置\n\n横截面分位采用**推荐日当天**的历史行情（精确）"
    )
else:
    st.sidebar.warning(
        "⚠️ 未配置 TUSHARE_TOKEN\n\n"
        "横截面分位将用**今日**行情近似——推荐日越久误差越大，"
        "空仓日环境对比无效。\n\n"
        "在 .env 加 `TUSHARE_TOKEN=...` 可获得历史精确截面（tushare.pro 免费注册）"
    )

st.sidebar.caption(
    "💡 个股日线: Baostock（不复权）\n\n"
    "结果输出到 `output/reverse_engineer/`"
)


# ========================
# 主区域：当前推荐记录预览
# ========================
st.subheader("📋 当前推荐记录")

picks = st.session_state["re_picks"]
if picks:
    picks_preview = pd.DataFrame(picks, columns=["推荐日", "代码", "名称", "题材", "触发价"])

    # 指标卡
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("推荐标的数", len(picks))
    with col2:
        st.metric("覆盖日期", picks_preview["推荐日"].nunique())
    with col3:
        st.metric("题材类别", picks_preview["题材"].nunique())
    with col4:
        st.metric("空仓日数", len(st.session_state["re_empty_days"]))

    # 可编辑表格
    edited_df = st.data_editor(
        picks_preview,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        key="re_picks_editor",
        column_config={
            "推荐日": st.column_config.TextColumn("推荐日", help="格式 YYYYMMDD", width="small"),
            "代码": st.column_config.TextColumn("代码", help="6位股票代码", width="small"),
            "名称": st.column_config.TextColumn("名称", width="medium"),
            "题材": st.column_config.TextColumn("题材", width="medium"),
            "触发价": st.column_config.NumberColumn("触发价", format="%.2f", width="small"),
        },
    )

    # 同步编辑结果回 session state
    if edited_df is not None:
        new_picks = []
        for _, row in edited_df.iterrows():
            if pd.notna(row["推荐日"]) and pd.notna(row["代码"]) and pd.notna(row["名称"]):
                new_picks.append((
                    str(row["推荐日"]).strip(),
                    str(row["代码"]).strip(),
                    str(row["名称"]).strip(),
                    str(row.get("题材", "")).strip(),
                    float(row.get("触发价", 0) or 0),
                ))
        st.session_state["re_picks"] = new_picks

else:
    st.info("暂无推荐记录，请在侧边栏添加。")


# ========================
# 执行分析
# ========================
if run_btn:
    current_picks = st.session_state["re_picks"]
    current_empty = st.session_state["re_empty_days"]

    if not current_picks:
        st.error("推荐记录为空，无法执行分析")
        st.stop()

    with st.spinner(f"正在分析 {len(current_picks)} 只推荐标的..."):
        try:
            from src.analysis.reverse_engineer import PicksReverseEngineer

            engine = PicksReverseEngineer(
                picks=current_picks,
                empty_days=current_empty,
                output_dir="./output/reverse_engineer",
            )
            feats_df, ctx_df = engine.run()

            st.session_state["re_feats_df"] = feats_df
            st.session_state["re_ctx_df"] = ctx_df
            st.toast(f"✅ 分析完成：{len(feats_df)} 条推荐记录", icon="✅")

        except Exception as e:
            st.error(f"分析执行失败: {e}")
            st.exception(e)
            st.stop()


# ========================
# 结果展示
# ========================
feats_df = st.session_state.get("re_feats_df")
ctx_df = st.session_state.get("re_ctx_df")

if feats_df is not None and not feats_df.empty:
    st.markdown("---")
    st.subheader("📊 特征提取结果")

    # 错误/正常记录统计
    error_mask = feats_df.get("error", pd.Series(dtype=str)).notna()
    n_ok = int((~error_mask).sum())
    n_err = int(error_mask.sum())

    col_s1, col_s2, col_s3, col_s4 = st.columns(4)
    with col_s1:
        st.metric("成功提取", n_ok)
    with col_s2:
        st.metric("数据异常", n_err)
    with col_s3:
        if "pct_chg_T" in feats_df.columns:
            avg_chg = feats_df["pct_chg_T"].mean()
            st.metric("平均涨跌幅", f"{avg_chg:.2f}%")
    with col_s4:
        if "vol_ratio_5d" in feats_df.columns:
            avg_vr = feats_df["vol_ratio_5d"].mean()
            st.metric("平均量比(5D)", f"{avg_vr:.2f}")

    # 特征分组展示
    tab_overview, tab_price, tab_vol, tab_ma, tab_fwd, tab_mkt, tab_raw = st.tabs([
        "📋 总览", "💰 量价特征", "📊 量能特征", "📈 均线形态",
        "🔮 前瞻收益", "🌐 市场分位", "🗂 原始数据"
    ])

    with tab_overview:
        overview_cols = ["trade_date", "name", "ts_code", "theme", "trigger_px", "close_T",
                         "pct_chg_T", "vol_ratio_5d", "close_vs_ma20_pct", "turnover_rate"]
        available = [c for c in overview_cols if c in feats_df.columns]
        display_df = feats_df[available].copy()
        # 格式化数值列
        for c in display_df.select_dtypes(include=["float"]).columns:
            display_df[c] = display_df[c].round(2)
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    with tab_price:
        price_cols = ["name", "pct_chg_T", "amplitude_T", "close_pos_in_range",
                      "gap_open_pct", "is_limit_up_T", "near_limit_T", "close_vs_trigger_pct"]
        available = [c for c in price_cols if c in feats_df.columns]
        if available:
            df_p = feats_df[available].copy()
            for c in df_p.select_dtypes(include=["float"]).columns:
                df_p[c] = df_p[c].round(2)
            st.dataframe(df_p, use_container_width=True, hide_index=True)

            # 涨跌幅柱状图
            if "pct_chg_T" in feats_df.columns and "name" in feats_df.columns:
                st.markdown("**触发日涨跌幅分布**")
                chart_data = feats_df[["name", "pct_chg_T"]].dropna().set_index("name")
                st.bar_chart(chart_data)

    with tab_vol:
        vol_cols = ["name", "vol_ratio_5d", "vol_ratio_20d", "amount_yi",
                    "turnover_rate", "pe_ttm", "pb_mrq"]
        available = [c for c in vol_cols if c in feats_df.columns]
        if available:
            df_v = feats_df[available].copy()
            for c in df_v.select_dtypes(include=["float"]).columns:
                df_v[c] = df_v[c].round(2)
            st.dataframe(df_v, use_container_width=True, hide_index=True)

            # 量比柱状图
            if "vol_ratio_5d" in feats_df.columns and "name" in feats_df.columns:
                st.markdown("**5日量比分布**")
                chart_data = feats_df[["name", "vol_ratio_5d"]].dropna().set_index("name")
                st.bar_chart(chart_data)

    with tab_ma:
        ma_cols = ["name", "close_vs_ma5_pct", "close_vs_ma10_pct",
                   "close_vs_ma20_pct", "close_vs_ma60_pct",
                   "breakout_20d_high", "breakout_60d_high", "dist_to_60d_high_pct",
                   "volatility_20d_pct"]
        available = [c for c in ma_cols if c in feats_df.columns]
        if available:
            df_m = feats_df[available].copy()
            for c in df_m.select_dtypes(include=["float"]).columns:
                df_m[c] = df_m[c].round(2)
            st.dataframe(df_m, use_container_width=True, hide_index=True)

            # 均线偏离柱状图
            ma_chart_cols = [c for c in ["close_vs_ma5_pct", "close_vs_ma10_pct",
                                          "close_vs_ma20_pct", "close_vs_ma60_pct"]
                             if c in feats_df.columns]
            if ma_chart_cols and "name" in feats_df.columns:
                st.markdown("**均线偏离度对比**")
                chart_data = feats_df[["name"] + ma_chart_cols].dropna().set_index("name")
                st.bar_chart(chart_data)

    with tab_fwd:
        fwd_cols = ["name", "next_open_ret_pct", "hold_T1_close_ret_pct",
                    "hold_T3_close_ret_pct", "hold_T5_close_ret_pct",
                    "max_high_5d_vs_trigger_pct", "max_high_5d_vs_buy_pct",
                    "max_dd_5d_vs_buy_pct"]
        available = [c for c in fwd_cols if c in feats_df.columns]
        if available:
            df_f = feats_df[available].copy()
            for c in df_f.select_dtypes(include=["float"]).columns:
                df_f[c] = df_f[c].round(2)
            st.dataframe(df_f, use_container_width=True, hide_index=True)

            # 持有收益对比
            ret_chart_cols = [c for c in ["hold_T1_close_ret_pct", "hold_T3_close_ret_pct",
                                           "hold_T5_close_ret_pct"]
                              if c in feats_df.columns]
            if ret_chart_cols and "name" in feats_df.columns:
                st.markdown("**次日开盘买入后持有收益**")
                chart_data = feats_df[["name"] + ret_chart_cols].dropna().set_index("name")
                st.bar_chart(chart_data)

    with tab_mkt:
        mkt_cols = ["name", "mkt_pctile_pct_chg", "mkt_pctile_turnover", "mkt_pctile_amount"]
        available = [c for c in mkt_cols if c in feats_df.columns]
        if available:
            df_mkt = feats_df[available].copy()
            for c in df_mkt.select_dtypes(include=["float"]).columns:
                df_mkt[c] = df_mkt[c].round(1)
            st.dataframe(df_mkt, use_container_width=True, hide_index=True)

            if len(available) > 1:
                st.markdown("**全市场百分位排名（越高 = 越强于市场）**")
                pctile_cols = [c for c in available if c != "name"]
                if pctile_cols and "name" in feats_df.columns:
                    chart_data = feats_df[["name"] + pctile_cols].dropna().set_index("name")
                    st.bar_chart(chart_data)
        else:
            st.info("暂无市场分位数据，请确保分析时全市场行情可用。")

    with tab_raw:
        st.dataframe(feats_df, use_container_width=True, hide_index=True)

    # 下载
    st.markdown("---")
    dl_col1, dl_col2 = st.columns(2)
    with dl_col1:
        st.download_button(
            label="💾 下载特征表 CSV",
            data=df_to_csv_bytes(feats_df),
            file_name=f"picks_features_{pd.Timestamp.now():%Y%m%d_%H%M%S}.csv",
            mime="text/csv",
            type="primary",
            use_container_width=True,
        )
    with dl_col2:
        if ctx_df is not None and not ctx_df.empty:
            st.download_button(
                label="💾 下载市场环境 CSV",
                data=df_to_csv_bytes(ctx_df),
                file_name=f"market_context_{pd.Timestamp.now():%Y%m%d_%H%M%S}.csv",
                mime="text/csv",
                use_container_width=True,
            )


# ========================
# 市场环境快照
# ========================
if ctx_df is not None and not ctx_df.empty:
    st.markdown("---")
    st.subheader("🌐 市场环境快照")

    # 按推荐日/空仓日分色标注
    ctx_display = ctx_df.copy()
    for c in ctx_display.select_dtypes(include=["float"]).columns:
        ctx_display[c] = ctx_display[c].round(2)

    st.dataframe(ctx_display, use_container_width=True, hide_index=True)

    # 上涨占比 & 涨停数对比图
    if "pct_up" in ctx_df.columns and "trade_date" in ctx_df.columns:
        st.markdown("**各日上涨占比 & 涨停数**")
        chart_cols = []
        if "pct_up" in ctx_df.columns:
            chart_cols.append("pct_up")
        if "n_limit_up_approx" in ctx_df.columns:
            chart_cols.append("n_limit_up_approx")
        if chart_cols:
            chart_data = ctx_df[["trade_date"] + chart_cols].set_index("trade_date")
            st.bar_chart(chart_data)


# ========================
# 动量特征详情
# ========================
if feats_df is not None and not feats_df.empty:
    st.markdown("---")
    st.subheader("🔥 动量与涨停特征")

    mom_cols = ["name", "ret_3d_pct", "ret_5d_pct", "ret_10d_pct", "ret_20d_pct",
                "consec_up_days", "limit_ups_prev10"]
    available = [c for c in mom_cols if c in feats_df.columns]
    if available:
        df_mom = feats_df[available].copy()
        for c in df_mom.select_dtypes(include=["float"]).columns:
            df_mom[c] = df_mom[c].round(2)
        st.dataframe(df_mom, use_container_width=True, hide_index=True)

        # 近期收益率对比
        ret_cols = [c for c in ["ret_3d_pct", "ret_5d_pct", "ret_10d_pct", "ret_20d_pct"]
                    if c in feats_df.columns]
        if ret_cols and "name" in feats_df.columns:
            st.markdown("**近期收益率对比**")
            chart_data = feats_df[["name"] + ret_cols].dropna().set_index("name")
            st.bar_chart(chart_data)


# ========================
# 底部提示
# ========================
if feats_df is None:
    st.markdown("---")
    st.info("👈 点击侧边栏的「🚀 开始分析」按钮执行特征提取")
