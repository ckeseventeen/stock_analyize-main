"""
pages/5_资讯抓取.py — 财经资讯抓取

四个 Tab 分别对应 scraper.yaml 的 4 类抓取器：
  - 财经新闻 (NewsScraper)
  - 公司公告 (AnnouncementScraper)
  - 股东持仓 (HoldingsScraper)
  - 研报评级 (ResearchScraper)

点击按钮即执行抓取并展示结果。
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
    PATH_SCRAPER,
    df_to_csv_bytes,
    load_yaml,
    quick_edit_list_widget,
)

st.set_page_config(page_title="资讯抓取", page_icon="🌐", layout="wide")
st.title("🌐 财经资讯抓取")
st.caption(f"配置文件：`{PATH_SCRAPER}`")


cfg = load_yaml(PATH_SCRAPER) or {}


# ========================
# 缓存的抓取封装
# ========================

@st.cache_data(ttl=900, show_spinner=False)
def _fetch_news(keywords: tuple, watchlist: tuple) -> pd.DataFrame:
    """拉取新闻（缓存 15 分钟）"""
    from src.data.scraper import NewsScraper
    s = NewsScraper(keywords=list(keywords), watchlist_codes=list(watchlist))
    return s.fetch()


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_announcements(watchlist: tuple, types_: tuple) -> pd.DataFrame:
    from src.data.scraper import AnnouncementScraper
    s = AnnouncementScraper(watchlist=list(watchlist), types=list(types_))
    return s.fetch()


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_holdings(watchlist: tuple) -> pd.DataFrame:
    from src.data.scraper import HoldingsScraper
    s = HoldingsScraper(watchlist=list(watchlist))
    return s.fetch()


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_research(watchlist: tuple, rating_filter: tuple) -> pd.DataFrame:
    from src.data.scraper import ResearchScraper
    s = ResearchScraper(watchlist=list(watchlist),
                        rating_filter=list(rating_filter) if rating_filter else [])
    return s.fetch()


def _show_table(df: pd.DataFrame, download_prefix: str):
    """展示 + 下载"""
    if df is None or df.empty:
        st.info("暂无数据")
        return
    st.dataframe(df, width='stretch', hide_index=True)
    st.download_button(
        label="💾 下载 CSV",
        data=df_to_csv_bytes(df),
        file_name=f"{download_prefix}_{pd.Timestamp.now():%Y%m%d_%H%M%S}.csv",
        mime="text/csv",
        key=f"dl_{download_prefix}",
    )


# ========================
# 4 Tab
# ========================

tab1, tab2, tab3, tab4 = st.tabs(["📰 财经新闻", "📢 公司公告", "👥 股东持仓", "📑 研报评级"])

# --- 新闻 ---
with tab1:
    news_cfg = cfg.get("news", {}) or {}
    st.caption(f"关键词过滤：{news_cfg.get('keywords', [])}")
    st.caption(f"个股列表：{news_cfg.get('watchlist_codes', []) or '无'}")

    # ---- 就地 CRUD（无需手动编辑 scraper.yaml）----
    _rerun = False
    if quick_edit_list_widget(
        path=PATH_SCRAPER, dotted_key="news.keywords",
        key_prefix="news_kw",
        label="✏️ 管理关键词过滤",
        placeholder="如：降息、关税",
        help_text="命中任一关键词即保留（空列表=不过滤）",
    ):
        _rerun = True
    if quick_edit_list_widget(
        path=PATH_SCRAPER, dotted_key="news.watchlist_codes",
        key_prefix="news_wl",
        label="✏️ 管理个股列表",
        placeholder="6 位 A 股代码，如 600519",
        help_text="只针对这些个股抓新闻（为空则按关键词泛抓）",
    ):
        _rerun = True
    if _rerun:
        st.rerun()

    if st.button("🔄 抓取最新新闻", key="run_news", type="primary"):
        with st.spinner("拉取中..."):
            _fetch_news.clear()
            try:
                df = _fetch_news(
                    tuple(news_cfg.get("keywords", []) or []),
                    tuple(news_cfg.get("watchlist_codes", []) or []),
                )
                _show_table(df, "news")
            except Exception as e:
                st.error(f"抓取失败: {e}")


# --- 公告 ---
with tab2:
    ann_cfg = cfg.get("announcements", {}) or {}
    st.caption(f"关注股票：{ann_cfg.get('watchlist', [])}")
    st.caption(f"类型过滤：{ann_cfg.get('types', [])}")

    _rerun = False
    if quick_edit_list_widget(
        path=PATH_SCRAPER, dotted_key="announcements.watchlist",
        key_prefix="ann_wl",
        label="✏️ 管理关注股票",
        placeholder="6 位 A 股代码，如 600519",
    ):
        _rerun = True
    if quick_edit_list_widget(
        path=PATH_SCRAPER, dotted_key="announcements.types",
        key_prefix="ann_types",
        label="✏️ 管理公告类型过滤",
        placeholder="如：财务报告、重大事项",
        help_text="模糊子串匹配；空 = 不过滤",
    ):
        _rerun = True
    if _rerun:
        st.rerun()

    if st.button("🔄 抓取最新公告", key="run_ann", type="primary"):
        with st.spinner("拉取中..."):
            _fetch_announcements.clear()
            try:
                df = _fetch_announcements(
                    tuple(ann_cfg.get("watchlist", []) or []),
                    tuple(ann_cfg.get("types", []) or []),
                )
                _show_table(df, "announcements")
            except Exception as e:
                st.error(f"抓取失败: {e}")


# --- 持仓 ---
with tab3:
    hold_cfg = cfg.get("holdings", {}) or {}
    st.caption(f"关注股票：{hold_cfg.get('watchlist', [])}")
    st.caption(f"变动阈值：±{hold_cfg.get('delta_pct_threshold', 5)}%")

    if quick_edit_list_widget(
        path=PATH_SCRAPER, dotted_key="holdings.watchlist",
        key_prefix="hold_wl",
        label="✏️ 管理关注股票",
        placeholder="6 位 A 股代码，如 600519",
    ):
        st.rerun()

    if st.button("🔄 抓取股东/北向持仓", key="run_hold", type="primary"):
        with st.spinner("拉取中..."):
            _fetch_holdings.clear()
            try:
                df = _fetch_holdings(tuple(hold_cfg.get("watchlist", []) or []))
                _show_table(df, "holdings")
            except Exception as e:
                st.error(f"抓取失败: {e}")


# --- 研报 ---
with tab4:
    res_cfg = cfg.get("research", {}) or {}
    st.caption(f"关注股票：{res_cfg.get('watchlist', [])}")
    st.caption(f"评级过滤：{res_cfg.get('rating_filter', []) or '不过滤'}")

    _rerun = False
    if quick_edit_list_widget(
        path=PATH_SCRAPER, dotted_key="research.watchlist",
        key_prefix="res_wl",
        label="✏️ 管理关注股票",
        placeholder="6 位 A 股代码，如 600519",
    ):
        _rerun = True
    if quick_edit_list_widget(
        path=PATH_SCRAPER, dotted_key="research.rating_filter",
        key_prefix="res_rating",
        label="✏️ 管理评级过滤",
        placeholder="如：买入、增持",
        help_text="模糊子串匹配；空 = 不过滤",
    ):
        _rerun = True
    if _rerun:
        st.rerun()

    if st.button("🔄 抓取最新研报", key="run_res", type="primary"):
        with st.spinner("拉取中..."):
            _fetch_research.clear()
            try:
                df = _fetch_research(
                    tuple(res_cfg.get("watchlist", []) or []),
                    tuple(res_cfg.get("rating_filter", []) or []),
                )
                _show_table(df, "research")
            except Exception as e:
                st.error(f"抓取失败: {e}")


st.markdown("---")
st.caption("⏱ 新闻缓存 15 分钟；公告/持仓/研报缓存 1 小时。")
