"""
src/services/market_monitor_service.py — 市场情绪监控服务层

六大情绪维度：
  1. 投机情绪  — 涨停数 / 连板数 / 连板高度 / 炸板率
  2. 市场情绪  — 指数强度 / 涨跌比
  3. 板块情绪  — 概念板块热度 / 行业涨幅榜
  4. 整体市场情绪 — 综合赚钱效应 + 消息面
  5. 整体投机情绪 — 涨停溢价 / 炸板负溢价 / 高位妖股
  6. 整体板块情绪 — 龙头拉开空间 / 题材持续性

另含美股盘前总结（道琼斯 / 纳斯达克 / 标普500）。
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("market_monitor")


# ============================================================================
# 代理禁用 — akshare 底层 requests 会读取系统代理，代理不通时全部失败
# ============================================================================

_PROXY_CLEARED = False

def _ensure_no_proxy():
    """一次性清空代理环境变量 + 禁用 Windows 注册表代理（全进程生效）。"""
    global _PROXY_CLEARED
    if _PROXY_CLEARED:
        return
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
              "ALL_PROXY", "all_proxy"):
        os.environ.pop(k, None)
    os.environ["NO_PROXY"] = "*"
    try:
        import urllib.request
        urllib.request.getproxies = lambda: {}
    except Exception:
        pass
    _PROXY_CLEARED = True
    logger.debug("市场监控: 代理已全局禁用")


# 模块加载时立即执行
_ensure_no_proxy()


# ============================================================================
# 请求限速 & 重试
# ============================================================================

_LAST_CALL_TIME = 0.0
_MIN_INTERVAL = 1.2  # akshare/东财请求最小间隔(秒)，避免被 rate-limit


def _throttle():
    """确保两次 akshare 请求之间至少间隔 _MIN_INTERVAL 秒"""
    global _LAST_CALL_TIME
    elapsed = time.monotonic() - _LAST_CALL_TIME
    if elapsed < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - elapsed)
    _LAST_CALL_TIME = time.monotonic()


def _retry(fn: Callable, *args, retries: int = 3, delay: float = 2.0, **kwargs):
    """
    带指数退避的重试包装器。

    针对 ConnectionError / RemoteDisconnected / Timeout 等网络异常重试，
    其他异常直接抛出。
    """
    last_exc = None
    for attempt in range(retries):
        try:
            _throttle()
            return fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            err_str = str(e).lower()
            # 仅对网络类异常重试
            is_network = any(kw in err_str for kw in [
                "connection", "remote", "timeout", "timed out",
                "aborted", "reset", "unreachable", "broken pipe",
            ])
            if not is_network or attempt == retries - 1:
                raise
            wait = delay * (2 ** attempt)  # 2s, 4s, 8s
            logger.warning(f"{fn.__name__} 第{attempt+1}次失败({e})，{wait:.0f}s 后重试")
            time.sleep(wait)
    raise last_exc  # type: ignore


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class SentimentPanel:
    """完整的情绪面板"""
    # ── 投机情绪 ──
    limit_up_count: int = 0          # 涨停数
    limit_down_count: int = 0        # 跌停数
    consecutive_boards: int = 0      # 连板数（≥2板的股票数）
    max_board_height: int = 0        # 最高连板高度
    failed_limit_rate: float = 0.0   # 炸板率(%)
    limit_up_details: list[dict] = field(default_factory=list)  # 涨停股明细

    # ── 市场情绪 ──
    index_strength: dict[str, dict] = field(default_factory=dict)  # {指数名: {change_pct, close, ...}}
    up_count: int = 0                # 上涨数
    down_count: int = 0              # 下跌数
    flat_count: int = 0              # 平盘
    up_down_ratio: float = 0.0       # 涨跌比

    # ── 板块情绪 ──
    top_concepts: list[dict] = field(default_factory=list)   # 热门概念板块
    top_industries: list[dict] = field(default_factory=list)  # 热门行业板块
    sector_heat: float = 0.0         # 板块热度分(0-100)

    # ── 整体市场情绪 ──
    market_sentiment_score: float = 0.0  # 综合赚钱效应(0-100)
    market_sentiment_label: str = "中性"

    # ── 整体投机情绪 ──
    speculation_sentiment_score: float = 0.0  # 投机氛围(0-100)
    speculation_sentiment_label: str = "中性"
    limit_up_premium: float = 0.0   # 涨停溢价(%)
    high_board_stocks: list[dict] = field(default_factory=list)  # 高位妖股(≥3板)

    # ── 整体板块情绪 ──
    sector_sentiment_score: float = 0.0  # 板块情绪(0-100)
    sector_sentiment_label: str = "中性"

    # ── 元信息 ──
    updated_at: str = ""
    trading_date: str = ""
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}


# ============================================================================
# 美股盘前总结
# ============================================================================

def us_market_summary() -> dict:
    """
    获取美股三大指数最近交易日表现。

    Returns:
        {
            "indices": [
                {"name": "道琼斯", "code": ".DJI", "close": 38000, "change_pct": 0.5, ...},
                ...
            ],
            "updated_at": "2024-01-01 08:30:00",
            "summary": "美股收盘涨跌互现，纳指涨1.2%..."
        }
    """
    import akshare as ak

    indices = [
        {"name": "道琼斯", "code": ".DJI", "symbol": ".DJI"},
        {"name": "纳斯达克", "code": ".IXIC", "symbol": ".IXIC"},
        {"name": "标普500", "code": ".INX", "symbol": ".INX"},
    ]
    results = []
    errors = []

    for idx in indices:
        try:
            df = _retry(ak.index_us_stock_sina, symbol=idx["symbol"])
            if df is None or df.empty:
                errors.append(f"{idx['name']}数据为空")
                continue
            # 取最近两行计算涨跌
            latest = df.iloc[-1]
            prev = df.iloc[-2] if len(df) > 1 else None
            close = float(latest.get("close", latest.iloc[-1]))
            prev_close = float(prev.get("close", prev.iloc[-1])) if prev is not None else close
            change = close - prev_close
            change_pct = round(change / prev_close * 100, 2) if prev_close else 0.0
            date_str = str(latest.get("date", ""))[:10]
            results.append({
                "name": idx["name"],
                "code": idx["code"],
                "close": round(close, 2),
                "change": round(change, 2),
                "change_pct": change_pct,
                "date": date_str,
            })
        except Exception as e:
            logger.warning(f"获取{idx['name']}失败: {e}")
            errors.append(f"{idx['name']}: {e}")

    # 生成文字摘要
    summary_parts = []
    for r in results:
        direction = "涨" if r["change_pct"] > 0 else ("跌" if r["change_pct"] < 0 else "平")
        summary_parts.append(f"{r['name']}{direction}{abs(r['change_pct'])}%")
    summary = "；".join(summary_parts) if summary_parts else "数据获取失败"

    return {
        "indices": results,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "summary": summary,
        "errors": errors,
    }


# ============================================================================
# 投机情绪
# ============================================================================

def _get_limit_up_pool(date_str: str) -> pd.DataFrame:
    """获取涨停板池数据"""
    import akshare as ak
    try:
        df = _retry(ak.stock_zt_pool_em, date=date_str)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        logger.warning(f"涨停池获取失败({date_str}): {e}")
    return pd.DataFrame()


def _get_limit_down_pool(date_str: str) -> pd.DataFrame:
    """获取跌停板池数据"""
    import akshare as ak
    try:
        df = _retry(ak.stock_zt_pool_dtgc_em, date=date_str)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        logger.warning(f"跌停池获取失败({date_str}): {e}")
    return pd.DataFrame()


def _get_failed_limit_pool(date_str: str) -> pd.DataFrame:
    """获取炸板池数据（曾涨停但未封住）"""
    import akshare as ak
    try:
        df = _retry(ak.stock_zt_pool_zbgc_em, date=date_str)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        logger.warning(f"炸板池获取失败({date_str}): {e}")
    return pd.DataFrame()


def _parse_consecutive_boards(df: pd.DataFrame) -> tuple[int, int, list[dict]]:
    """
    从涨停池中解析连板信息。

    Returns:
        (连板股数, 最高连板高度, 高位股列表)
    """
    if df.empty:
        return 0, 0, []

    # akshare 涨停池可能有 "连板数" 列，也可能没有
    board_col = None
    for col_name in ["连板数", "连板", "连续涨停天数"]:
        if col_name in df.columns:
            board_col = col_name
            break

    if board_col is None:
        # 无法获取连板数，返回基本统计
        return 0, 0, []

    boards = pd.to_numeric(df[board_col], errors="coerce").fillna(0).astype(int)
    consecutive_count = int((boards >= 2).sum())
    max_height = int(boards.max()) if len(boards) > 0 else 0

    # 高位妖股 (≥3板)
    high_board = []
    high_mask = boards >= 3
    if high_mask.any():
        for _, row in df[high_mask].head(15).iterrows():
            high_board.append({
                "code": str(row.get("代码", row.get("股票代码", ""))),
                "name": str(row.get("名称", row.get("股票名称", ""))),
                "boards": int(row[board_col]),
                "change_pct": _safe_float(row.get("涨跌幅", 0)),
                "amount": _safe_float(row.get("成交额", 0)),
            })
        high_board.sort(key=lambda x: x["boards"], reverse=True)

    return consecutive_count, max_height, high_board


def _safe_float(v, default=0.0) -> float:
    """安全转 float"""
    try:
        val = float(v)
        if pd.isna(val) or not abs(val) < 1e15:
            return default
        return round(val, 2)
    except (TypeError, ValueError):
        return default


def get_speculation_sentiment(date_str: str = "") -> dict:
    """
    投机情绪：涨停数 / 连板数 / 连板高度 / 炸板率

    Returns:
        {
            "limit_up_count": int,
            "limit_down_count": int,
            "consecutive_boards": int,
            "max_board_height": int,
            "failed_limit_rate": float,
            "limit_up_details": [...],
            "high_board_stocks": [...],
        }
    """
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")

    zt_df = _get_limit_up_pool(date_str)
    dt_df = _get_limit_down_pool(date_str)
    zb_df = _get_failed_limit_pool(date_str)

    limit_up_count = len(zt_df) if not zt_df.empty else 0
    limit_down_count = len(dt_df) if not dt_df.empty else 0
    failed_count = len(zb_df) if not zb_df.empty else 0

    # 炸板率 = 炸板数 / (涨停数 + 炸板数) * 100
    total_attempt = limit_up_count + failed_count
    failed_limit_rate = round(failed_count / total_attempt * 100, 1) if total_attempt > 0 else 0.0

    # 连板信息
    consecutive_count, max_height, high_board = _parse_consecutive_boards(zt_df)

    # 涨停明细 (取前 20 只按成交额排序)
    limit_up_details = []
    if not zt_df.empty:
        amount_col = None
        for c in ["成交额", "成交额(元)"]:
            if c in zt_df.columns:
                amount_col = c
                break
        if amount_col:
            zt_sorted = zt_df.sort_values(amount_col, ascending=False).head(20)
        else:
            zt_sorted = zt_df.head(20)
        for _, row in zt_sorted.iterrows():
            limit_up_details.append({
                "code": str(row.get("代码", row.get("股票代码", ""))),
                "name": str(row.get("名称", row.get("股票名称", ""))),
                "change_pct": _safe_float(row.get("涨跌幅", 0)),
                "amount": _safe_float(row.get("成交额", 0)),
                "reason": str(row.get("涨停统计", row.get("涨停原因", "")))[:60],
            })

    # 涨停溢价 (近似：涨停股次日平均高开的幅度，此处用涨停股当日封板强度近似)
    # 简化处理：涨停数 > 30 且炸板率 < 15% 时溢价高
    limit_up_premium = 0.0
    if limit_up_count > 0:
        if limit_up_count > 30 and failed_limit_rate < 15:
            limit_up_premium = round(60 + limit_up_count * 0.5, 1)
        elif limit_up_count > 15 and failed_limit_rate < 25:
            limit_up_premium = round(30 + limit_up_count * 0.5, 1)
        else:
            limit_up_premium = round(limit_up_count * 0.5, 1)

    return {
        "limit_up_count": limit_up_count,
        "limit_down_count": limit_down_count,
        "consecutive_boards": consecutive_count,
        "max_board_height": max_height,
        "failed_limit_rate": failed_limit_rate,
        "limit_up_details": limit_up_details,
        "high_board_stocks": high_board,
        "limit_up_premium": limit_up_premium,
    }


# ============================================================================
# 市场情绪
# ============================================================================

def get_market_sentiment() -> dict:
    """
    市场情绪：指数强度 + 涨跌比

    数据源策略：东财(push2) → 新浪 降级（push2 被某些网络封锁时自动切换）
    """
    import akshare as ak

    # ── 指数实时行情 ──
    index_strength = {}
    target_codes = {"000001": "上证指数", "399001": "深证成指",
                    "399006": "创业板指", "000688": "科创50",
                    "000300": "沪深300", "000905": "中证500"}

    # 尝试东财 → 降级新浪
    def _parse_em_index(df):
        """解析东货行情格式"""
        result = {}
        for _, row in df.iterrows():
            code = str(row.get("代码", ""))
            if code in target_codes:
                result[target_codes[code]] = {
                    "code": code,
                    "name": target_codes[code],
                    "close": _safe_float(row.get("最新价", 0)),
                    "change_pct": _safe_float(row.get("涨跌幅", 0)),
                    "change": _safe_float(row.get("涨跌额", 0)),
                    "amount": _safe_float(row.get("成交额", 0)),
                    "turnover_rate": _safe_float(row.get("换手率", 0)),
                }
        return result

    def _parse_sina_index(df):
        """解析新浪行情格式（代码格式: sh000001 / sz399001）"""
        result = {}
        for _, row in df.iterrows():
            raw_code = str(row.get("代码", ""))  # sh000001 / sz399001
            code = raw_code[2:] if len(raw_code) > 2 else raw_code
            if code in target_codes:
                result[target_codes[code]] = {
                    "code": code,
                    "name": target_codes[code],
                    "close": _safe_float(row.get("最新价", 0)),
                    "change_pct": _safe_float(row.get("涨跌幅", 0)),
                    "change": _safe_float(row.get("涨跌额", 0)),
                    "amount": _safe_float(row.get("成交额", 0)),
                }
        return result

    try:
        # 东财路径（有降级，只试 1 次避免浪费 时间）
        df = _retry(ak.stock_zh_index_spot_em, symbol="上证系列指数", retries=1)
        if df is not None and not df.empty:
            index_strength.update(_parse_em_index(df))
        df2 = _retry(ak.stock_zh_index_spot_em, symbol="深证系列指数", retries=1)
        if df2 is not None and not df2.empty:
            for k, v in _parse_em_index(df2).items():
                if k not in index_strength:
                    index_strength[k] = v
        logger.debug("指数行情: 东财源")
    except Exception as e:
        logger.warning(f"指数行情(东财)失败，降级新浪: {e}")
        try:
            df = _retry(ak.stock_zh_index_spot_sina)
            if df is not None and not df.empty:
                index_strength.update(_parse_sina_index(df))
            logger.debug("指数行情: 新浪源")
        except Exception as e2:
            logger.warning(f"指数行情(新浪)也失败: {e2}")

    # ── 全市场涨跌统计 ──
    up_count = down_count = flat_count = 0
    try:
        spot = _retry(ak.stock_zh_a_spot_em, retries=1)
        if spot is not None and not spot.empty:
            pct_col = "涨跌幅"
            if pct_col in spot.columns:
                pcts = pd.to_numeric(spot[pct_col], errors="coerce").dropna()
                up_count = int((pcts > 0).sum())
                down_count = int((pcts < 0).sum())
                flat_count = int((pcts == 0).sum())
        logger.debug("全市场行情: 东财源")
    except Exception as e:
        logger.warning(f"全市场行情(东财)失败，降级新浪: {e}")
        try:
            spot = _retry(ak.stock_zh_a_spot)
            if spot is not None and not spot.empty:
                pct_col = "涨跌幅"
                if pct_col in spot.columns:
                    pcts = pd.to_numeric(spot[pct_col], errors="coerce").dropna()
                    up_count = int((pcts > 0).sum())
                    down_count = int((pcts < 0).sum())
                    flat_count = int((pcts == 0).sum())
            logger.debug("全市场行情: 新浪源")
        except Exception as e2:
            logger.warning(f"全市场行情(新浪)也失败: {e2}")

    up_down_ratio = round(up_count / down_count, 2) if down_count > 0 else (float(up_count) if up_count > 0 else 0.0)

    return {
        "index_strength": index_strength,
        "up_count": up_count,
        "down_count": down_count,
        "flat_count": flat_count,
        "up_down_ratio": up_down_ratio,
    }


# ============================================================================
# 板块情绪
# ============================================================================

def get_sector_sentiment() -> dict:
    """
    板块情绪：概念板块热度 + 行业涨幅榜

    数据源策略：东财(push2) → 新浪/同花顺 降级
    """
    import akshare as ak

    top_concepts = []
    top_industries = []
    concept_df = None  # 保留全量概念板块 DataFrame 用于板块热度计算

    # ── 概念/行业板块：尝试东财 → 降级新浪 ──
    try:
        concept_df = _retry(ak.stock_board_concept_name_em, retries=1)
        if concept_df is not None and not concept_df.empty:
            pct_col = "涨跌幅"
            if pct_col in concept_df.columns:
                df_sorted = concept_df.sort_values(pct_col, ascending=False)
                for _, row in df_sorted.head(10).iterrows():
                    top_concepts.append({
                        "name": str(row.get("板块名称", "")),
                        "change_pct": _safe_float(row.get(pct_col, 0)),
                        "amount": _safe_float(row.get("总市值", row.get("成交额", 0))),
                        "leader": str(row.get("领涨股票", "")),
                        "leader_change": _safe_float(row.get("领涨股票-涨跌幅", 0)),
                    })
        logger.debug("概念板块: 东财源")
    except Exception as e:
        logger.warning(f"概念板块(东财)失败，降级新浪行业板块: {e}")
        # 东财概念板块不可用时，用新浪行业板块替代
        try:
            sina_df = _retry(ak.stock_sector_spot)
            if sina_df is not None and not sina_df.empty:
                pct_col = "涨跌幅"
                if pct_col in sina_df.columns:
                    df_sorted = sina_df.sort_values(pct_col, ascending=False)
                    for _, row in df_sorted.head(10).iterrows():
                        top_concepts.append({
                            "name": str(row.get("板块", "")),
                            "change_pct": _safe_float(row.get(pct_col, 0)),
                            "amount": _safe_float(row.get("总成交额", 0)),
                            "leader": str(row.get("股票名称", "")),
                            "leader_change": _safe_float(row.get("个股-涨跌幅", 0)),
                        })
                concept_df = sina_df  # 复用于板块热度计算
            logger.debug("概念板块: 新浪行业源")
        except Exception as e2:
            logger.warning(f"概念板块(新浪)也失败: {e2}")

    # ── 行业板块：尝试东财 → 降级同花顺 ──
    try:
        df = _retry(ak.stock_board_industry_name_em, retries=1)
        if df is not None and not df.empty:
            pct_col = "涨跌幅"
            if pct_col in df.columns:
                df_sorted = df.sort_values(pct_col, ascending=False)
                for _, row in df_sorted.head(10).iterrows():
                    top_industries.append({
                        "name": str(row.get("板块名称", "")),
                        "change_pct": _safe_float(row.get(pct_col, 0)),
                        "amount": _safe_float(row.get("总市值", row.get("成交额", 0))),
                        "leader": str(row.get("领涨股票", "")),
                        "leader_change": _safe_float(row.get("领涨股票-涨跌幅", 0)),
                    })
        logger.debug("行业板块: 东财源")
    except Exception as e:
        logger.warning(f"行业板块(东财)失败，降级同花顺: {e}")
        try:
            ths_df = _retry(ak.stock_board_industry_summary_ths)
            if ths_df is not None and not ths_df.empty:
                pct_col = "涨跌幅"
                if pct_col in ths_df.columns:
                    df_sorted = ths_df.sort_values(pct_col, ascending=False)
                    for _, row in df_sorted.head(10).iterrows():
                        top_industries.append({
                            "name": str(row.get("板块", "")),
                            "change_pct": _safe_float(row.get(pct_col, 0)),
                            "amount": _safe_float(row.get("总成交额", 0)),
                            "leader": str(row.get("领涨股", "")),
                            "leader_change": _safe_float(row.get("领涨股-涨跌幅", 0)),
                        })
            logger.debug("行业板块: 同花顺源")
        except Exception as e2:
            logger.warning(f"行业板块(同花顺)也失败: {e2}")

    # ── 板块热度分(0-100) ──
    sector_heat = 50.0
    if concept_df is not None and not concept_df.empty:
        pct_col = None
        for c in ("涨跌幅",):
            if c in concept_df.columns:
                pct_col = c
                break
        if pct_col:
            pcts = pd.to_numeric(concept_df[pct_col], errors="coerce").dropna()
            if len(pcts) > 0:
                up_ratio = int((pcts > 0).sum()) / len(pcts)
                sector_heat = round(up_ratio * 100, 1)
                if top_concepts and top_concepts[0]["change_pct"] > 3:
                    sector_heat = min(100, sector_heat + 10)

    return {
        "top_concepts": top_concepts,
        "top_industries": top_industries,
        "sector_heat": sector_heat,
    }


# ============================================================================
# 综合情绪评分
# ============================================================================

def _score_to_label(score: float) -> str:
    """分数 → 情绪标签"""
    if score >= 75:
        return "🔥 极强"
    elif score >= 60:
        return "🟢 强势"
    elif score >= 40:
        return "🟡 中性"
    elif score >= 25:
        return "🟠 偏弱"
    else:
        return "🔴 弱势"


def _calc_market_sentiment(spec: dict, market: dict, sector: dict) -> tuple[float, str]:
    """
    整体市场情绪评分（综合赚钱效应）

    评分维度：
    - 涨跌比 (30%) — 涨跌比 > 2 强, < 0.5 弱
    - 指数强度 (30%) — 主要指数涨幅
    - 板块热度 (20%) — 上涨板块占比
    - 涨停数 (20%) — 涨停 > 30 强, < 10 弱
    """
    score = 50.0

    # 涨跌比
    ud = market.get("up_down_ratio", 0)
    if ud > 3:
        score += 15
    elif ud > 2:
        score += 10
    elif ud > 1:
        score += 5
    elif ud < 0.3:
        score -= 15
    elif ud < 0.5:
        score -= 10
    elif ud < 1:
        score -= 5

    # 指数强度（取沪深300 + 创业板指平均）
    idx_changes = []
    for name in ["沪深300", "创业板指", "上证指数"]:
        idx_data = market.get("index_strength", {}).get(name, {})
        if idx_data:
            idx_changes.append(idx_data.get("change_pct", 0))
    avg_idx_change = sum(idx_changes) / len(idx_changes) if idx_changes else 0
    score += max(-15, min(15, avg_idx_change * 5))

    # 板块热度
    heat = sector.get("sector_heat", 50)
    score += (heat - 50) * 0.3

    # 涨停数
    lu = spec.get("limit_up_count", 0)
    if lu > 50:
        score += 10
    elif lu > 30:
        score += 6
    elif lu > 15:
        score += 3
    elif lu < 5:
        score -= 8
    elif lu < 10:
        score -= 4

    score = max(0, min(100, round(score, 1)))
    return score, _score_to_label(score)


def _calc_speculation_sentiment(spec: dict) -> tuple[float, str]:
    """
    整体投机情绪评分

    评分维度：
    - 涨停数 (25%) — 越多越强
    - 连板高度 (25%) — 高位妖股出现 = 投机强
    - 炸板率 (25%) — 高炸板率 = 弱化
    - 涨停溢价 (25%)
    """
    score = 50.0

    lu = spec.get("limit_up_count", 0)
    max_h = spec.get("max_board_height", 0)
    fail_rate = spec.get("failed_limit_rate", 0)
    premium = spec.get("limit_up_premium", 0)
    high_boards = len(spec.get("high_board_stocks", []))

    # 涨停数
    if lu > 50:
        score += 15
    elif lu > 30:
        score += 10
    elif lu > 15:
        score += 5
    elif lu < 5:
        score -= 12
    elif lu < 10:
        score -= 6

    # 连板高度
    if max_h >= 5:
        score += 15
    elif max_h >= 4:
        score += 10
    elif max_h >= 3:
        score += 6
    elif max_h >= 2:
        score += 3

    # 高位妖股
    if high_boards >= 5:
        score += 8
    elif high_boards >= 3:
        score += 4

    # 炸板率 (反向)
    if fail_rate > 40:
        score -= 15
    elif fail_rate > 30:
        score -= 10
    elif fail_rate > 20:
        score -= 5
    elif fail_rate < 10:
        score += 8
    elif fail_rate < 15:
        score += 4

    # 涨停溢价
    score += max(-10, min(10, premium * 0.15))

    score = max(0, min(100, round(score, 1)))
    return score, _score_to_label(score)


def _calc_sector_sentiment(sector: dict, spec: dict) -> tuple[float, str]:
    """
    整体板块情绪评分

    评分维度：
    - 板块热度 (40%)
    - 头部板块涨幅 (30%) — 龙头拉开空间
    - 涨停数 (30%) — 题材非一日游需要涨停股支撑
    """
    score = 50.0

    heat = sector.get("sector_heat", 50)
    score += (heat - 50) * 0.6

    # 头部概念涨幅
    top_concepts = sector.get("top_concepts", [])
    if top_concepts:
        top1_pct = top_concepts[0].get("change_pct", 0)
        if top1_pct > 5:
            score += 12
        elif top1_pct > 3:
            score += 8
        elif top1_pct > 1:
            score += 4
        elif top1_pct < -2:
            score -= 8

        # 上涨板块数量（前10中）
        up_sectors = sum(1 for c in top_concepts if c.get("change_pct", 0) > 0)
        if up_sectors >= 8:
            score += 6
        elif up_sectors >= 6:
            score += 3
        elif up_sectors <= 2:
            score -= 6

    # 涨停数支撑
    lu = spec.get("limit_up_count", 0)
    if lu > 30:
        score += 6
    elif lu > 15:
        score += 3
    elif lu < 5:
        score -= 5

    score = max(0, min(100, round(score, 1)))
    return score, _score_to_label(score)


# ============================================================================
# 完整面板
# ============================================================================

def get_full_panel() -> dict:
    """
    获取完整的市场情绪面板（六大维度 + 元信息）。

    任何一路数据源失败不影响其他维度，错误记录在 errors 中。
    """
    now = datetime.now()
    date_str = now.strftime("%Y%m%d")
    errors = []

    # ── 投机情绪 ──
    try:
        spec = get_speculation_sentiment(date_str)
    except Exception as e:
        logger.error(f"投机情绪获取异常: {e}", exc_info=True)
        spec = {"limit_up_count": 0, "limit_down_count": 0, "consecutive_boards": 0,
                "max_board_height": 0, "failed_limit_rate": 0, "limit_up_details": [],
                "high_board_stocks": [], "limit_up_premium": 0}
        errors.append(f"投机情绪: {e}")

    # ── 市场情绪 ──
    try:
        market = get_market_sentiment()
    except Exception as e:
        logger.error(f"市场情绪获取异常: {e}", exc_info=True)
        market = {"index_strength": {}, "up_count": 0, "down_count": 0,
                  "flat_count": 0, "up_down_ratio": 0}
        errors.append(f"市场情绪: {e}")

    # ── 板块情绪 ──
    try:
        sector = get_sector_sentiment()
    except Exception as e:
        logger.error(f"板块情绪获取异常: {e}", exc_info=True)
        sector = {"top_concepts": [], "top_industries": [], "sector_heat": 50}
        errors.append(f"板块情绪: {e}")

    # ── 综合评分 ──
    mkt_score, mkt_label = _calc_market_sentiment(spec, market, sector)
    spc_score, spc_label = _calc_speculation_sentiment(spec)
    sec_score, sec_label = _calc_sector_sentiment(sector, spec)

    # 用 .get(默认) 而非 [] 直取：任一子函数演化后少返回一个键，
    # 也只是该项显示默认值，而不是整个面板 500（前端六个维度会一起白屏）
    return {
        # ── 投机情绪 ──
        "limit_up_count": spec.get("limit_up_count", 0),
        "limit_down_count": spec.get("limit_down_count", 0),
        "consecutive_boards": spec.get("consecutive_boards", 0),
        "max_board_height": spec.get("max_board_height", 0),
        "failed_limit_rate": spec.get("failed_limit_rate", 0),
        "limit_up_details": spec.get("limit_up_details", []),
        "high_board_stocks": spec.get("high_board_stocks", []),
        "limit_up_premium": spec.get("limit_up_premium", 0),

        # ── 市场情绪 ──
        "index_strength": market.get("index_strength", {}),
        "up_count": market.get("up_count", 0),
        "down_count": market.get("down_count", 0),
        "flat_count": market.get("flat_count", 0),
        "up_down_ratio": market.get("up_down_ratio", 0),

        # ── 板块情绪 ──
        "top_concepts": sector.get("top_concepts", []),
        "top_industries": sector.get("top_industries", []),
        "sector_heat": sector.get("sector_heat", 50),

        # ── 综合评分 ──
        "market_sentiment_score": mkt_score,
        "market_sentiment_label": mkt_label,
        "speculation_sentiment_score": spc_score,
        "speculation_sentiment_label": spc_label,
        "sector_sentiment_score": sec_score,
        "sector_sentiment_label": sec_label,

        # ── 元信息 ──
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "trading_date": now.strftime("%Y-%m-%d"),
        "errors": errors,
    }


# ============================================================================
# 辅助：判断是否 A 股交易时间
# ============================================================================

def is_a_share_trading_time(now: datetime | None = None) -> bool:
    """判断当前是否 A 股交易时段（周一至五 9:30-11:30, 13:00-15:00）"""
    if now is None:
        now = datetime.now()
    if now.weekday() >= 5:  # 周末
        return False
    t = now.hour * 100 + now.minute
    return (930 <= t < 1130) or (1300 <= t < 1500)


def is_pre_market_time(now: datetime | None = None) -> bool:
    """判断当前是否盘前时间（8:00-9:30）"""
    if now is None:
        now = datetime.now()
    if now.weekday() >= 5:
        return False
    t = now.hour * 100 + now.minute
    return 800 <= t < 930
