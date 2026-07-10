"""
src/services/valuation_service.py — 估值 / FCF 分析的无头服务层

原 pages/1_估值分析.py 内嵌的编排逻辑下沉至此：
  - run_valuation(): 拉数 → 分析器 → 结果四元组（改用 market_registry，
    消灭页面里手写的 market → Fetcher/Analyzer 映射）
  - run_fcf(): FCF 数据 + 评分卡
  - 展示辅助计算（分位徽章 / 目标价档位 / 核心指标行）也在这里，
    页面只做控件与图表渲染
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.core.market_registry import get_market
from src.utils.logger import get_logger

logger = get_logger("valuation_service")


# ============================================================================
# 估值分析
# ============================================================================

@dataclass
class ValuationRun:
    """一次估值分析的完整输出"""
    result: dict
    fin_df: pd.DataFrame
    hist_val_df: pd.DataFrame
    market_data: dict
    stock_config: dict = field(default_factory=dict)


def run_valuation(market: str, stock_config: dict) -> ValuationRun:
    """
    执行单股估值分析。

    Args:
        market: 市场 key（a / hk / us，见 market_registry）
        stock_config: {code, name, valuation, {pe|ps}_range, ...}
    """
    spec = get_market(market)
    FetcherCls = spec.fetcher_cls
    AnalyzerCls = spec.analyzer_cls

    with FetcherCls() as fetcher:
        fin_df = fetcher.get_financial_abstract(stock_config["code"])
        v_type = stock_config.get("valuation", "pe")
        hist_val_df = fetcher.get_historical_valuation(stock_config["code"], v_type)
        market_data = fetcher.get_current_market_data(stock_config["code"])

    analyzer = AnalyzerCls(fin_df, hist_val_df, market_data, stock_config)
    result = analyzer.process()
    return ValuationRun(
        result=result or {},
        fin_df=fin_df,
        hist_val_df=hist_val_df,
        market_data=market_data or {},
        stock_config=dict(stock_config),
    )


def build_stock_config(code: str, name: str, market: str, val_type: str,
                       price_range: list[float]) -> dict:
    """页面表单 → 分析器需要的 stock_config"""
    spec = get_market(market)
    return {
        "code": code.strip(),
        "name": name.strip(),
        "valuation": val_type,
        f"{val_type}_range": list(price_range),
        "market_name": spec.label,
        "category_name": "自定义",
    }


# ── 展示辅助计算 ──

def percentile_badge(hist_pct: float) -> tuple[str, str]:
    """
    历史分位数 → (评价标签, 等级)。

    等级 ∈ {"low", "mid", "high"}，页面据此选样式。
    """
    if hist_pct < 20:
        return "极度低估", "low"
    if hist_pct < 50:
        return "合理偏低", "low"
    if hist_pct < 80:
        return "合理偏高", "mid"
    return "极度高估", "high"


def target_price_rows(result: dict) -> list[dict]:
    """目标价三档 → 表格行"""
    scenarios = result.get("scenarios", [0, 0, 0])
    price = result.get("price") or 0
    rows = []
    for level, target in zip(("保守", "中性", "乐观"), scenarios):
        rows.append({
            "档位": level,
            "目标价": round(target, 2),
            "相对当前": f"{(target / price - 1) * 100:+.1f}%" if price else "",
        })
    return rows


def summary_rows(result: dict, fin_df: pd.DataFrame | None, val_type: str) -> list[dict]:
    """核心指标综合总览表格行（原页面内嵌的计算逻辑）"""
    annual_df = result.get("annual_df")
    if annual_df is not None and not annual_df.empty and hasattr(annual_df.index, "year"):
        latest_year = str(annual_df.index[-1].year)
    else:
        latest_year = "-"

    rev_val = result.get("ttm_revenue", 0) / 1e8
    np_val = result.get("ttm_net_profit", 0) / 1e8

    gm_val = 0.0
    if (fin_df is not None and not fin_df.empty
            and "营业总收入" in fin_df.columns and "营业成本" in fin_df.columns):
        latest_annual = fin_df.iloc[0]
        rev_annual = latest_annual.get("营业总收入", 0)
        cost_annual = latest_annual.get("营业成本", 0)
        if rev_annual > 0:
            gm_val = (rev_annual - cost_annual) / rev_annual * 100

    current_ratio = result.get(f"current_{val_type}")
    ratio_str = f"{current_ratio:.2f}" if current_ratio and current_ratio > 0 else "N/A"
    hist_pct = result.get("hist_percentile", 0)

    return [
        {"关键指标": "财报最新年度", "数据": latest_year, "备注解析": "最新年度数据依据"},
        {"关键指标": "营业总收入 (亿元)", "数据": f"{rev_val:.2f}", "备注解析": "年度总计 (TTM)"},
        {"关键指标": "归母净利润 (亿元)", "数据": f"{np_val:.2f}", "备注解析": "年度总计 (TTM)"},
        {"关键指标": "毛利率 (%)", "数据": f"{gm_val:.2f}%", "备注解析": "(营业收入-营业成本)/营业收入"},
        {"关键指标": "当前股价 (元)", "数据": f"{result.get('price', 0):.2f}", "备注解析": "实时动态行情"},
        {"关键指标": f"当前 {val_type.upper()} (TTM)", "数据": ratio_str, "备注解析": "基于最新滚动四个季度"},
        {"关键指标": f"历史 {val_type.upper()} 分位数", "数据": f"{hist_pct:.2f}%", "备注解析": "处于过去历史排位"},
    ]


# ============================================================================
# FCF 分析
# ============================================================================

@dataclass
class FCFRun:
    """一次 FCF 分析的完整输出"""
    analyzed_df: pd.DataFrame
    score_res: dict
    market_cap: float


def fetch_market_cap(market: str, code: str) -> float:
    """经 market_registry 取当前总市值；失败返回 0"""
    try:
        spec = get_market(market)
        with spec.fetcher_cls() as fetcher:
            return float(fetcher.get_current_market_data(code).get("market_cap", 0) or 0)
    except Exception as e:
        logger.warning(f"获取市值失败 {market}:{code}: {e}")
        return 0.0


def run_fcf(market: str, code: str, *, is_annual: bool = True) -> FCFRun:
    """
    执行 FCF 分析：数据拉取 → 指标计算 → 评分卡。

    数据为空时返回空 DataFrame（页面自行提示）。
    """
    from src.analysis.factor.fcf_analyzer import FCFAnalyzer
    from src.data.fcf_data_fetcher import FCFDataFetcher

    raw_df = FCFDataFetcher.fetch(market, code, is_annual=is_annual)
    if raw_df is None or raw_df.empty:
        return FCFRun(analyzed_df=pd.DataFrame(), score_res={}, market_cap=0.0)

    market_cap = fetch_market_cap(market, code)
    analyzer = FCFAnalyzer(raw_df, market_cap)
    analyzed_df = analyzer.calculate_metrics()
    score_res = analyzer.generate_scorecard()
    return FCFRun(analyzed_df=analyzed_df, score_res=score_res, market_cap=market_cap)
