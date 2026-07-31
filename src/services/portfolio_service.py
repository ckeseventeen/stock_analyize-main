"""
src/services/portfolio_service.py — 持仓监控的无头服务层

原 pages/16_持仓监控.py 内嵌的编排逻辑下沉至此：
  - build_price_maps(): A 股全市场 spot + 港/美股按 K 线尾根补价
  - pnl_rows() / industry_exposure(): 汇总视图的数据计算
  - evaluate_holding(): 动态 K 线长度 + SellEngine 综合判定
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd

from src.portfolio.models import Holding, Portfolio
from src.portfolio.sell_engine import SellEngine, SellVerdict
from src.utils.logger import get_logger

logger = get_logger("portfolio_service")


# ============================================================================
# 行情
# ============================================================================

def build_a_share_maps() -> tuple[dict[str, float], dict[str, str]]:
    """全市场 A 股 spot → ({code: 最新价}, {code: 名称})；失败返回空 dict"""
    from src.analysis.screening.data_provider import ScreenerDataProvider

    code_to_price: dict[str, float] = {}
    code_to_name: dict[str, str] = {}
    try:
        spot_df = ScreenerDataProvider().get_all_a_shares()
    except Exception as e:
        logger.warning(f"全市场行情获取失败: {e}")
        return code_to_price, code_to_name

    if spot_df is None or spot_df.empty:
        return code_to_price, code_to_name

    code_col = "代码" if "代码" in spot_df.columns else "code"
    price_col = "最新价" if "最新价" in spot_df.columns else "close"
    name_col = "名称" if "名称" in spot_df.columns else "name"
    for _, row in spot_df.iterrows():
        c = str(row.get(code_col, "")).zfill(6)
        try:
            code_to_price[c] = float(row.get(price_col, 0) or 0)
            code_to_name[c] = str(row.get(name_col, ""))
        except (ValueError, TypeError):
            continue
    return code_to_price, code_to_name


def fetch_intl_price(code: str, market: str) -> float:
    """港/美股最新价：K 线尾根收盘价；失败返回 0"""
    from src.analysis.screening.data_provider import ScreenerDataProvider

    try:
        df = ScreenerDataProvider().get_daily_ohlcv(code, days_back=10, market=market)
        if df is None or df.empty:
            return 0.0
        close_col = "收盘" if "收盘" in df.columns else "close"
        return float(pd.to_numeric(df[close_col], errors="coerce").dropna().iloc[-1])
    except Exception:
        return 0.0


def fill_intl_prices(pf: Portfolio, code_to_price: dict[str, float]) -> dict[str, float]:
    """给港/美股持仓补价（A 股 spot 不含它们）；原地更新并返回"""
    for h in pf.holdings:
        if h.market in ("hk", "us") and h.code not in code_to_price:
            code_to_price[h.code] = fetch_intl_price(h.code, h.market)
    return code_to_price


# ============================================================================
# 汇总视图数据
# ============================================================================

def pnl_rows(pf: Portfolio, price_map: dict[str, float]) -> list[dict]:
    """单只持仓盈亏排行数据（升序留给页面自己排）"""
    rows = []
    for h in pf.holdings:
        price = price_map.get(h.code, 0.0)
        if price > 0 and h.avg_cost > 0:
            rows.append({
                "代码": h.code,
                "名称": h.name,
                "浮盈%": h.unrealized_pnl_pct(price),
                "市值": h.market_value(price),
            })
    return rows


def industry_exposure(pf: Portfolio, price_map: dict[str, float]) -> dict[str, float]:
    """行业（tag 首段）→ 市值"""
    out: dict[str, float] = {}
    for h in pf.holdings:
        price = price_map.get(h.code, 0.0)
        if price <= 0:
            continue
        ind = h.tag.split("-")[0] if h.tag else "未分类"
        out[ind] = out.get(ind, 0.0) + h.market_value(price)
    return out


# ============================================================================
# 单只持仓评估
# ============================================================================

def kline_days_needed(holding: Holding, *, base_days: int = 250,
                      margin_days: int = 30) -> int:
    """
    动态日线长度：长期持仓要拉到 buy_date 起 + 余量，
    否则 K 线买入标记会落在图外。
    """
    days = base_days
    if holding.buy_date:
        try:
            buy_dt = datetime.strptime(holding.buy_date, "%Y-%m-%d").date()
            days = max(days, (date.today() - buy_dt).days + margin_days)
        except ValueError:
            pass
    return days


def prefetch_klines(holdings: list[Holding], *, max_workers: int = 8) -> None:
    """
    并行预热持仓 K 线缓存（每只 ~1s 串行 → 并发后大幅缩短），
    后续 evaluate_holding 全命中本地缓存。失败静默（评估时会再报）。
    """
    from concurrent.futures import ThreadPoolExecutor

    from src.analysis.screening.data_provider import ScreenerDataProvider

    def _one(h: Holding) -> None:
        try:
            provider = ScreenerDataProvider()
            provider.get_daily_ohlcv(h.code, days_back=kline_days_needed(h),
                                     market=h.market)
            provider.get_weekly_ohlcv(h.code, days_back=365 * 3, market=h.market)
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=min(max_workers, max(1, len(holdings)))) as pool:
        list(pool.map(_one, holdings))


@dataclass
class HoldingEvaluation:
    """单只持仓的评估输出（含画图所需的日线）"""
    verdict: SellVerdict
    daily_df: pd.DataFrame
    weekly_df: pd.DataFrame


def evaluate_holding(
    holding: Holding,
    current_price: float,
    default_alerts: dict | None = None,
    *,
    regime_multiplier: float = 1.0,
) -> HoldingEvaluation | None:
    """
    单只持仓完整评估：拉 K 线（动态长度）→ SellEngine L1+L2 判定。

    Returns:
        HoldingEvaluation；日线拉不到时返回 None
    """
    from src.analysis.screening.data_provider import ScreenerDataProvider

    provider = ScreenerDataProvider()
    daily_df = provider.get_daily_ohlcv(
        holding.code, days_back=kline_days_needed(holding), market=holding.market,
    )
    weekly_df = provider.get_weekly_ohlcv(
        holding.code, days_back=365 * 3, market=holding.market,
    )
    if daily_df is None or daily_df.empty:
        return None

    engine = SellEngine(default_alerts)
    verdict = engine.evaluate(
        holding, current_price, daily_df, weekly_df,
        regime_multiplier=regime_multiplier,
    )
    return HoldingEvaluation(
        verdict=verdict,
        daily_df=daily_df,
        weekly_df=weekly_df if weekly_df is not None else pd.DataFrame(),
    )


# ============================================================================
# 持仓体检（服务层入口）
# ============================================================================

def run_portfolio_diagnosis() -> dict:
    """
    四维持仓体检（卖出信号/集中度/盈亏结构/大盘环境）→ 可 JSON 化的报告。

    服务层入口：API 不再直接 import src.portfolio.diagnostics。
    """
    from src.portfolio.diagnostics import run_diagnosis

    return run_diagnosis().to_report_dict()
