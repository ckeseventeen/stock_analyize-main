"""
pages/4_财报披露.py — 财报披露日历

功能：
  - 加载 earnings_monitor.yaml 的 watchlist
  - 拉取未来 N 天披露日历（A/港/美）
  - 按市场分 Tab 展示表格 + 高亮即将披露（3 天内）
  - 一键导出 CSV
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from datetime import datetime, timedelta  # noqa: E402

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from src.visualization.gui.utils import (  # noqa: E402
    PATH_EARNINGS,
    df_to_csv_bytes,
    load_yaml,
)

st.set_page_config(page_title="财报披露", page_icon="📅", layout="wide")
st.title("📅 财报披露日历")
st.caption(f"配置文件：`{PATH_EARNINGS}`")


cfg = load_yaml(PATH_EARNINGS) or {}
watchlist = cfg.get("watchlist", {}) or {}
default_days_ahead = int(cfg.get("days_ahead", 30))
remind_days_ahead = int(cfg.get("remind_days_ahead", 3))


# ========================
# 侧边栏控制
# ========================

with st.sidebar:
    days_ahead = st.slider("查看未来天数", min_value=7, max_value=90,
                           value=default_days_ahead, step=1)
    st.markdown("---")
    track_forecasts = st.checkbox("包含业绩预告",
                                  value=cfg.get("track_forecasts", True))
    refresh = st.button("🔄 刷新数据", type="primary", width='stretch')


# ========================
# 数据拉取（带缓存 12h）
# ========================

@st.cache_data(ttl=43200, show_spinner=False)
def _fetch_calendar(market: str, codes: tuple, days: int, track_fc: bool) -> pd.DataFrame:
    """拉取单市场披露日历，tuple 做缓存 key"""
    from src.data.fetcher.earnings_fetcher import EarningsFetcher
    fetcher = EarningsFetcher()
    try:
        if market == "a":
            df = fetcher.get_a_share_upcoming(days_ahead=days)
            if df is not None and not df.empty and codes:
                df = df[df["code"].astype(str).isin(codes)]
            if not track_fc and df is not None and not df.empty:
                df = df[df["event_type"] != "yjyg"]
            return df
        if market == "hk":
            return fetcher.get_hk_upcoming(list(codes), days_ahead=days)
        if market == "us":
            return fetcher.get_us_upcoming(list(codes), days_ahead=days)
    except Exception as e:
        st.warning(f"[{market}] 拉取失败: {e}")
    return pd.DataFrame()


# ========================
# Tab 展示
# ========================

if refresh:
    _fetch_calendar.clear()

tab_a, tab_hk, tab_us, tab_all = st.tabs(["🇨🇳 A 股", "🇭🇰 港股", "🇺🇸 美股", "📊 汇总"])

all_frames: list[pd.DataFrame] = []

for market, tab in [("a", tab_a), ("hk", tab_hk), ("us", tab_us)]:
    codes = tuple(watchlist.get(market, []) or [])
    with tab:
        if not codes:
            st.info(f"{market.upper()} 市场 watchlist 为空，请在 earnings_monitor.yaml 添加")
            continue

        st.caption(f"关注：{', '.join(codes)}")
        with st.spinner(f"正在拉取 {market.upper()} 披露日历..."):
            df = _fetch_calendar(market, codes, days_ahead, track_forecasts)

        if df is None or df.empty:
            st.warning("暂无披露事件")
            continue

        # 高亮 3 天内的披露
        soon_cutoff = datetime.now() + timedelta(days=remind_days_ahead)
        df = df.copy()
        if "disclose_date" in df.columns:
            df["disclose_date"] = pd.to_datetime(df["disclose_date"], errors="coerce")
            df = df.sort_values("disclose_date").reset_index(drop=True)
            # 用默认参数把 soon_cutoff 绑定到 lambda 闭包，避免 B023 循环变量捕获
            df["即将披露"] = df["disclose_date"].apply(
                lambda d, cutoff=soon_cutoff:
                    "🔴 即将披露" if pd.notna(d) and d <= cutoff else ""
            )

        # 展示
        st.dataframe(df, width='stretch', hide_index=True)
        st.download_button(
            label="💾 下载 CSV",
            data=df_to_csv_bytes(df),
            file_name=f"earnings_{market}_{pd.Timestamp.now():%Y%m%d}.csv",
            mime="text/csv",
            key=f"dl_{market}",
        )

        all_frames.append(df.assign(市场=market.upper()))


# 汇总 Tab
with tab_all:
    if not all_frames:
        st.info("暂无任何市场数据，请先在各 Tab 点击刷新")
    else:
        merged = pd.concat(all_frames, ignore_index=True)
        if "disclose_date" in merged.columns:
            merged = merged.sort_values("disclose_date").reset_index(drop=True)
        st.dataframe(merged, width='stretch', hide_index=True)

        # 按日期聚合统计
        if "disclose_date" in merged.columns:
            merged["disclose_date"] = pd.to_datetime(
                merged["disclose_date"], errors="coerce"
            )
            stat = merged.groupby(merged["disclose_date"].dt.date).size().reset_index(name="披露数")
            stat.columns = ["披露日期", "披露数"]
            st.subheader("按日期分布")
            st.bar_chart(stat.set_index("披露日期"))

        st.download_button(
            label="💾 下载总表 CSV",
            data=df_to_csv_bytes(merged),
            file_name=f"earnings_calendar_{pd.Timestamp.now():%Y%m%d}.csv",
            mime="text/csv",
            key="dl_all",
        )


st.markdown("---")
st.caption("⏱ 数据缓存 12 小时。下次自动刷新前可点击侧边栏「🔄 刷新数据」。")
