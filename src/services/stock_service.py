"""
src/services/stock_service.py — 个股中心的无头服务层

围绕「一只股票」聚合各引擎的单股视图：
  - resolve_name(): 代码 → 名称（关注列表 → 全市场 spot → resolver 多级解析）
  - earnings_for_code(): 财报披露（A/HK/US）
  - news_for_code() / announcements_for_code(): 个股资讯
  - scan_buy_signals() / sell_verdict(): 买点条件扫描 + 卖出引擎判定
  - alert_rules_for_code() / add_price_alert_rule(): 该股的预警规则
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.core.config_io import PATH_PRICE_ALERTS, atomic_save_yaml, load_yaml
from src.utils.logger import get_logger

logger = get_logger("stock_service")


# ============================================================================
# 名称解析（用户只输代码）
# ============================================================================

def resolve_name(code: str, market: str = "a") -> str:
    """
    代码 → 名称，多级解析：关注列表 → resolver（配置聚合）→ A 股全市场 spot。
    全部失败返回空串（页面提示手动确认）。
    """
    code = str(code).strip()
    if not code:
        return ""

    # 1) 关注列表
    try:
        from src.web.utils import list_stocks_from_market_config
        for s in list_stocks_from_market_config(market) or []:
            if str(s.get("code", "")).strip() == code:
                return str(s.get("name", ""))
    except Exception:
        pass

    # 2) 配置聚合 resolver
    try:
        from src.utils.name_resolver import StockNameResolver
        name = StockNameResolver().get_name(code, market)
        if name and name != code:
            return name
    except Exception:
        pass

    # 3) A 股全市场 spot（有 5000+ 名称）
    if market == "a":
        try:
            from src.services.portfolio_service import build_a_share_maps
            _, code_to_name = build_a_share_maps()
            return code_to_name.get(code.zfill(6), "")
        except Exception:
            pass
    return ""


# A 股全市场名称表的进程级缓存（搜索联想每敲一个键都会查，不能每次拉全市场）
_A_NAME_CACHE: dict = {"ts": 0.0, "map": {}}
_A_NAME_TTL = 600.0


def _a_share_names() -> dict[str, str]:
    """{code: name}，10 分钟缓存"""
    import time
    if time.time() - _A_NAME_CACHE["ts"] > _A_NAME_TTL or not _A_NAME_CACHE["map"]:
        try:
            from src.services.portfolio_service import build_a_share_maps
            _, names = build_a_share_maps()
            if names:
                _A_NAME_CACHE.update(ts=time.time(), map=names)
        except Exception as e:
            logger.warning(f"全市场名称表获取失败: {e}")
    return _A_NAME_CACHE["map"]


def search_stocks(query: str, market: str = "a", limit: int = 10) -> list[dict]:
    """
    按代码前缀或名称子串搜索。

    A 股搜全市场（spot 名称表）；港/美股搜关注列表。
    代码前缀命中排前，名称命中排后。
    """
    q = str(query).strip()
    if not q:
        return []

    if market == "a":
        # 全市场 spot 名称表 + 关注列表兜底合并
        # （spot 源抖动降级时可能只有部分股票，关注股必须始终可搜）
        merged = dict(_a_share_names())
        try:
            from src.web.utils import list_stocks_from_market_config
            for s in list_stocks_from_market_config("a") or []:
                c = str(s.get("code", "")).strip()
                if c:
                    merged.setdefault(c, str(s.get("name", "")))
        except Exception:
            pass
        universe = merged.items()
    else:
        try:
            from src.web.utils import list_stocks_from_market_config
            universe = [(str(s.get("code", "")), str(s.get("name", "")))
                        for s in (list_stocks_from_market_config(market) or [])]
        except Exception:
            universe = []

    q_upper = q.upper()
    by_code, by_name = [], []
    for code, name in universe:
        if code.upper().startswith(q_upper):
            by_code.append({"code": code, "name": name})
        elif q in name:
            by_name.append({"code": code, "name": name})
        if len(by_code) >= limit:
            break
    return (by_code + by_name)[:limit]


def current_price(code: str, market: str = "a") -> float:
    """最新价：日线尾根收盘（全市场通用）；失败返回 0"""
    from src.services.portfolio_service import fetch_intl_price
    return fetch_intl_price(code, market)


# ============================================================================
# 财报披露（单股）
# ============================================================================

def earnings_for_code(code: str, market: str = "a", days_ahead: int = 90) -> pd.DataFrame:
    """该股未来 days_ahead 天的披露计划；无数据返回空 df"""
    from src.data.providers.earnings_fetcher import EarningsFetcher

    fetcher = EarningsFetcher()
    code = str(code).strip()
    try:
        if market == "a":
            df = fetcher.get_a_share_upcoming(days_ahead=days_ahead)
            if df is None or df.empty:
                return pd.DataFrame()
            code_col = next((c for c in ("代码", "code", "股票代码") if c in df.columns), None)
            if code_col is None:
                return pd.DataFrame()
            return df[df[code_col].astype(str).str.zfill(6) == code.zfill(6)].copy()
        if market == "hk":
            return fetcher.get_hk_upcoming([code], days_ahead=days_ahead)
        return fetcher.get_us_upcoming([code], days_ahead=days_ahead)
    except Exception as e:
        logger.warning(f"财报披露获取失败 {market}:{code}: {e}")
        return pd.DataFrame()


# ============================================================================
# 个股资讯（新闻 + 公告）
# ============================================================================

def news_for_code(code: str, limit: int = 30) -> pd.DataFrame:
    """个股新闻（东财源，带小时级缓存）"""
    try:
        from src.data.scrapers.news_scraper import NewsScraper
        df = NewsScraper()._fetch_stock_news(str(code).strip())
        if df is None or df.empty:
            return pd.DataFrame()
        return df.sort_values("time", ascending=False).head(limit).reset_index(drop=True)
    except Exception as e:
        logger.warning(f"个股新闻获取失败 {code}: {e}")
        return pd.DataFrame()


def announcements_for_code(code: str, limit: int = 30) -> pd.DataFrame:
    """公司公告（当日为主，带缓存）"""
    try:
        from src.data.scrapers.announcement_scraper import AnnouncementScraper
        scraper = AnnouncementScraper(watchlist=[str(code).strip()])
        df = scraper.fetch()
        if df is None or df.empty:
            return pd.DataFrame()
        return df.head(limit).reset_index(drop=True)
    except Exception as e:
        logger.warning(f"公告获取失败 {code}: {e}")
        return pd.DataFrame()


# ============================================================================
# 买点 / 卖点信号
# ============================================================================

# 买点条件清单（条件自身的 ohlcv_period 决定用日线还是周线）
_BUY_PROFILE: list[tuple[str, dict]] = [
    ("weekly_macd_divergence", {}),
    ("daily_macd_divergence", {}),
    ("weekly_macd_gold_cross", {}),
    ("rsi_oversold", {"threshold": 30}),
    ("ma_gold_cross", {"fast_period": 5, "slow_period": 20}),
    ("kdj_gold_cross", {}),
    ("box_breakout", {}),
    ("volume_break", {}),
    ("multi_ma_bull", {}),
]


@dataclass
class SignalHit:
    """单个条件的评估结果"""
    cond_type: str
    label: str
    period: str          # daily / weekly
    hit: bool


def scan_buy_signals(code: str, market: str = "a") -> list[SignalHit]:
    """
    对该股跑一遍买点条件清单。

    Returns:
        每个条件的命中情况（含未命中，页面全量展示）
    """
    from src.analysis.screening.conditions import CONDITION_LABELS, CONDITION_REGISTRY
    from src.analysis.screening.data_provider import ScreenerDataProvider

    provider = ScreenerDataProvider()
    daily_df = provider.get_daily_ohlcv(code, days_back=500, market=market)
    weekly_df = provider.get_weekly_ohlcv(code, days_back=365 * 3, market=market)

    spot_row = pd.Series({"代码": code, "名称": ""})
    out: list[SignalHit] = []
    for cond_type, kwargs in _BUY_PROFILE:
        cls = CONDITION_REGISTRY.get(cond_type)
        if cls is None:
            continue
        try:
            cond = cls(**kwargs)
        except Exception:
            continue
        period = getattr(cond, "ohlcv_period", "daily")
        df = weekly_df if period == "weekly" else daily_df
        if df is None or df.empty or len(df) < 25:
            continue
        try:
            hit = bool(cond.evaluate_full(spot_row, df))
        except Exception as e:
            logger.debug(f"{code} 买点条件 {cond_type} 异常: {e}")
            continue
        out.append(SignalHit(
            cond_type=cond_type,
            label=CONDITION_LABELS.get(cond_type, cond_type),
            period=period,
            hit=hit,
        ))
    return out


def scan_strategy_signals(code: str, market: str = "a",
                          config_path=None) -> list[dict]:
    """
    对单只股票逐策略扫描买入/卖出两侧信号。

    每个策略：
      买入侧 = 技术筛选条件（按 backtest.buy_logic 组合，默认 all）
      卖出侧 = backtest.sell_conditions（按 sell_logic 组合，默认 any）

    Returns:
        [{sid, name, buy_hit, sell_hit,
          buy_conditions: [{type, label, hit}], sell_conditions: [...]}]
    """
    from src.analysis.screening.conditions import CONDITION_LABELS
    from src.analysis.screening.config_schema import _build_conditions
    from src.analysis.screening.data_provider import ScreenerDataProvider
    from src.services import screening_service as svc

    strategies = svc.load_all_strategies(config_path)
    if not strategies:
        return []

    provider = ScreenerDataProvider()
    daily_df = provider.get_daily_ohlcv(code, days_back=500, market=market)
    weekly_df = provider.get_weekly_ohlcv(code, days_back=365 * 3, market=market)
    if daily_df is None or daily_df.empty:
        return []

    spot_row = pd.Series({"代码": code, "名称": ""})

    def _eval_side(cond_dicts: list[dict]) -> list[dict]:
        out = []
        for cond in _build_conditions(cond_dicts, strict=False):
            if not getattr(cond, "requires_ohlcv", True):
                continue  # 纯 Spot 基本面条件无法对单股逐日判定，跳过
            df = weekly_df if getattr(cond, "ohlcv_period", "daily") == "weekly" else daily_df
            if df is None or df.empty or len(df) < 25:
                continue
            try:
                hit = bool(cond.evaluate_full(spot_row, df))
            except Exception as e:
                logger.debug(f"{code} 策略信号 {cond.name} 异常: {e}")
                continue
            out.append({"type": cond.name,
                        "label": CONDITION_LABELS.get(cond.name, cond.name),
                        "hit": hit})
        return out

    results = []
    for sid, body in strategies.items():
        bt = body.get("backtest") or {}
        buy = _eval_side(body.get("conditions", []))
        sell = _eval_side(bt.get("sell_conditions", []))

        buy_logic = str(bt.get("buy_logic", "all")).lower()
        sell_logic = str(bt.get("sell_logic", "any")).lower()
        buy_hit = bool(buy) and (all(c["hit"] for c in buy) if buy_logic == "all"
                                 else any(c["hit"] for c in buy))
        sell_hit = bool(sell) and (all(c["hit"] for c in sell) if sell_logic == "all"
                                   else any(c["hit"] for c in sell))
        results.append({
            "sid": sid,
            "name": body.get("name", sid),
            "buy_hit": buy_hit,
            "sell_hit": sell_hit,
            "buy_logic": buy_logic,
            "sell_logic": sell_logic,
            "buy_conditions": buy,
            "sell_conditions": sell,
        })
    return results


def sell_verdict_for_code(code: str, name: str = "", market: str = "a",
                          avg_cost: float = 0.0):
    """
    卖出引擎判定（无持仓也可用：avg_cost=0 时只跑 L2 信号，不触发止损）。

    Returns:
        HoldingEvaluation | None（K 线拉取失败）
    """
    from src.portfolio.models import Holding
    from src.services.portfolio_service import evaluate_holding

    price = current_price(code, market)
    if price <= 0:
        return None
    pseudo = Holding(code=str(code).strip(), name=name or code, market=market,
                     qty=0, avg_cost=float(avg_cost), buy_date="")
    return evaluate_holding(pseudo, price, None)


# ============================================================================
# 价格预警（单股规则）
# ============================================================================

def alert_rules_for_code(code: str) -> tuple[list[dict], list[dict]]:
    """该股关联的 (买入规则, 卖出规则)"""
    cfg = load_yaml(PATH_PRICE_ALERTS) or {}
    code = str(code).strip()

    def _mine(rules: list) -> list[dict]:
        return [r for r in (rules or [])
                if str(r.get("code", "")).strip() == code]

    return _mine(cfg.get("buy_alerts")), _mine(cfg.get("sell_alerts"))


def add_price_alert_rule(
    code: str,
    name: str,
    direction: str,
    signal_type: str,
    params: dict | None = None,
    cooldown_hours: int = 24,
) -> tuple[bool, str]:
    """
    给该股加一条预警规则（direction ∈ buy/sell）。

    规则 schema 与 pages/5_价格预警 一致，调度器直接消费。
    """
    from src.analysis.screening.conditions import CONDITION_REGISTRY

    code = str(code).strip()
    if signal_type not in CONDITION_REGISTRY:
        return False, f"未知信号类型: {signal_type}"
    if direction not in ("buy", "sell"):
        return False, f"方向必须是 buy/sell: {direction}"

    cfg = load_yaml(PATH_PRICE_ALERTS) or {}
    key = f"{direction}_alerts"
    rules = cfg.get(key) or []

    rule_id = f"hub_{code}_{signal_type}"
    if any(r.get("id") == rule_id for r in rules):
        return False, "该股已存在同信号规则"

    rules.append({
        "id": rule_id,
        "name": name or f"{code} {signal_type}",
        "code": code,
        "enabled": True,
        "signal": {"type": signal_type, "params": dict(params or {})},
        "cooldown_hours": int(cooldown_hours),
        "max_results": 5,
    })
    cfg[key] = rules
    ok = atomic_save_yaml(PATH_PRICE_ALERTS, cfg)
    return ok, ("已添加，调度器下个周期生效" if ok else "写入失败")
