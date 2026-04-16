"""
pages/1_估值分析.py — 单只股票四格估值分析页

功能：
  1. 下拉选择市场（A / HK / US）
  2. 从对应 YAML 中列出已配置股票（也允许手动输入代码）
  3. 调用对应 DataFetcher + Analyzer，渲染 4 格 matplotlib 图
  4. 展示关键估值指标（当前价、PE、PS、目标价档）
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# 图形后端要在 pyplot import 前设置
import matplotlib  # noqa: E402

matplotlib.use("Agg")
matplotlib.rcParams["font.sans-serif"] = [
    "Hiragino Sans GB", "PingFang HK", "Heiti TC", "STHeiti",
    "Microsoft YaHei", "SimHei", "Arial Unicode MS", "DejaVu Sans",
]
matplotlib.rcParams["axes.unicode_minus"] = False

import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from src.visualization.gui.utils import (  # noqa: E402
    MARKET_LABELS,
    list_stocks_from_market_config,
)

st.set_page_config(page_title="估值分析", page_icon="📈", layout="wide")
st.title("📈 四格估值分析")
st.caption("选择市场与股票 → 拉取最新财务/行情数据 → 生成 4 格估值图")


# ========================
# Fetcher / Analyzer 动态导入
# ========================

def _get_fetcher_analyzer(market: str):
    """按市场返回 (FetcherClass, AnalyzerClass)"""
    from analyzer import AStockAnalyzer, HKStockAnalyzer, USStockAnalyzer
    from data_fetcher import (
        AStockDataFetcher,
        HKStockDataFetcher,
        USStockDataFetcher,
    )
    mapping = {
        "a": (AStockDataFetcher, AStockAnalyzer),
        "hk": (HKStockDataFetcher, HKStockAnalyzer),
        "us": (USStockDataFetcher, USStockAnalyzer),
    }
    return mapping[market]


# ========================
# 交互：市场 + 股票选择
# ========================

market = st.sidebar.selectbox(
    "市场",
    options=list(MARKET_LABELS.keys()),
    format_func=lambda k: MARKET_LABELS[k],
    index=0,
)

stocks = list_stocks_from_market_config(market)

with st.sidebar:
    st.markdown("---")
    st.markdown("**股票选择**")
    if stocks:
        # 下拉选配置中的股票
        options = [f"{s['name']} ({s['code']}) · {s.get('category', '')}" for s in stocks]
        idx = st.selectbox("从配置中选择", options=range(len(options)),
                           format_func=lambda i: options[i], index=0)
        selected = stocks[idx]
        default_code = selected["code"]
        default_name = selected["name"]
        default_val = selected.get("valuation", "pe")
        default_range = selected.get(f"{default_val}_range", [10, 20, 30])
    else:
        st.caption("该市场 YAML 中暂无股票，请手动输入")
        default_code, default_name, default_val, default_range = "", "", "pe", [10, 20, 30]

    st.markdown("---")
    st.markdown("**或手动覆盖**")
    code = st.text_input("股票代码", value=default_code)
    name = st.text_input("股票名称", value=default_name)
    val_type = st.radio("估值方式", options=["pe", "ps"],
                        index=0 if default_val == "pe" else 1, horizontal=True)
    st.caption("估值档位（低 / 合理 / 高）")
    c1, c2, c3 = st.columns(3)
    with c1:
        r_low = st.number_input("低", value=float(default_range[0]), step=0.5)
    with c2:
        r_mid = st.number_input("中", value=float(default_range[1]), step=0.5)
    with c3:
        r_high = st.number_input("高", value=float(default_range[2]), step=0.5)

    run_btn = st.button("▶️ 开始分析", type="primary", use_container_width=True)


# ========================
# 数据拉取 + 分析（带缓存，5 分钟）
# ========================

@st.cache_data(ttl=300, show_spinner=False)
def _run_pipeline(market: str, stock_config: dict):
    """
    完整的"拉取→清洗→分析"管道。
    返回 (analysis_result, fin_df, hist_val_df, market_data)。
    """
    FetcherCls, AnalyzerCls = _get_fetcher_analyzer(market)
    with FetcherCls() as fetcher:
        fin_df = fetcher.get_financial_abstract(stock_config["code"])
        val_type = stock_config.get("valuation", "pe")
        hist_val_df = fetcher.get_historical_valuation(stock_config["code"], val_type)
        market_data = fetcher.get_current_market_data(stock_config["code"])

    analyzer = AnalyzerCls(fin_df, hist_val_df, market_data, stock_config)
    result = analyzer.process()
    return result, fin_df, hist_val_df, market_data


if run_btn:
    if not code or not name:
        st.error("请填入股票代码和名称")
        st.stop()

    # 构造 stock_config（analyzer 需要的格式）
    stock_config = {
        "code": code.strip(),
        "name": name.strip(),
        "valuation": val_type,
        f"{val_type}_range": [r_low, r_mid, r_high],
        "market_name": MARKET_LABELS[market],
        "category_name": "自定义",
    }

    with st.spinner(f"正在拉取 {name} ({code}) 数据..."):
        try:
            result, fin_df, hist_val_df, market_data = _run_pipeline(market, stock_config)
        except Exception as e:
            st.error(f"数据拉取或分析异常: {e}")
            st.exception(e)
            st.stop()

    if not result:
        st.warning("分析结果为空，可能数据缺失或 price=0。请检查代码是否正确。")
        st.stop()

    # --------- 关键指标汇总 ---------
    st.success(f"✅ 分析完成: {name} ({code})")

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("当前价", f"{result.get('price', 0):.2f}")
    with m2:
        pe_ttm = result.get("current_pe")
        st.metric("PE (TTM)", f"{pe_ttm:.2f}" if pe_ttm and pe_ttm > 0 else "N/A")
    with m3:
        ps_ttm = result.get("current_ps")
        st.metric("PS (TTM)", f"{ps_ttm:.2f}" if ps_ttm and ps_ttm > 0 else "N/A")
    with m4:
        st.metric("市值(亿)", f"{market_data.get('market_cap', 0) / 1e8:.2f}"
                  if market_data.get("market_cap") else "N/A")

    # --------- 目标价档位 ---------
    st.subheader("🎯 目标价档位")
    target_prices = result.get("target_prices") or {}
    if target_prices:
        tp_df = pd.DataFrame([
            {"档位": level, "目标价": round(price, 2),
             "相对当前": f"{(price / result['price'] - 1) * 100:+.1f}%"
                       if result.get("price") else ""}
            for level, price in target_prices.items()
        ])
        st.dataframe(tp_df, use_container_width=True, hide_index=True)

    # --------- 4 格估值图（matplotlib）---------
    st.subheader("📊 4 格估值图")
    try:
        from visualizer import Visualizer
        viz = Visualizer(result, stock_config)
        fig = viz.plot()
        st.pyplot(fig, use_container_width=True)
        plt.close(fig)
    except Exception as e:
        st.error(f"图表渲染失败: {e}")
        st.exception(e)

    # --------- 原始数据展示（可折叠）---------
    with st.expander("📋 财务数据原表"):
        if fin_df is not None and not fin_df.empty:
            st.dataframe(fin_df, use_container_width=True)
        else:
            st.caption("暂无财务数据")

    with st.expander("📉 历史估值原表"):
        if hist_val_df is not None and not hist_val_df.empty:
            st.dataframe(hist_val_df.tail(100), use_container_width=True)
        else:
            st.caption("暂无历史估值数据")

    with st.expander("💹 实时行情 Dict"):
        st.json(market_data)

else:
    st.info("👈 在左侧配置后点击「开始分析」")
