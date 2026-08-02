"""
src/data/providers/yfinance_client.py — yfinance 取数的统一入口（带重试 + 健康度）

## 为什么需要这一层

港美股的财务数据（估值的三大报表、FCF 的现金流/利润表）全部来自 yfinance。
而 Yahoo **限流非常激进**：同一分钟内连打几次就返回空表，且不抛异常——
`tk.cashflow` 直接给你一个 empty DataFrame。

原先各处直接 `yf.Ticker(...).cashflow`，拿到空表就当"这只股票没有财务数据"，
于是页面报"新股/数据源无记录"、"分析结果为空，请检查代码"——**把数据源限流
说成了用户输错代码**，用户完全无从判断该重试还是该换标的。

线上实测：同一个 RBLX，第一次请求全空（页面报错），隔几秒重试就 4 年数据齐全。

## 这一层做两件事

1. **重试**：空表和异常都重试（指数退避），把偶发限流吃掉
2. **区分失败与真空**：返回 DataResult——重试耗尽是 FAILED（数据源故障，
   建议稍后重试），真的查不到才是 EMPTY（新股/退市/代码不存在）

上层据此给出准确文案，而不是一律甩给用户"请检查代码"。
"""
from __future__ import annotations

import time

import pandas as pd

from src.core.data_health import DataResult
from src.utils.logger import get_logger

logger = get_logger("yfinance_client")

_MAX_ATTEMPTS = 3
_BACKOFF_S = (0.0, 1.5, 4.0)   # 第 n 次尝试前等待

# yfinance 报表在"限流"和"真无数据"两种情况下都返回空表，无法从返回值区分。
# 但**同时**拿到现金流与利润表都为空，限流的可能性远高于两张表恰好都无记录，
# 故重试；重试到底仍空才判 EMPTY。
_STATEMENT_ATTRS = {
    True:  ("cashflow", "financials"),              # 年报
    False: ("quarterly_cashflow", "quarterly_financials"),
}


def fetch_statements(symbol: str, is_annual: bool = True) -> DataResult:
    """
    拉取 yfinance 现金流表 + 利润表（已转置为 index=日期）。

    Returns:
        DataResult，data 为 (cashflow_T, financials_T) 二元组：
          - ok       两张表都有数据
          - empty    重试到底仍为空（多半是新股/退市/代码不存在）
          - failed   持续抛异常（网络不通/Yahoo 不可达），建议稍后重试
    """
    cf_attr, inc_attr = _STATEMENT_ATTRS[bool(is_annual)]
    last_err: BaseException | None = None

    for attempt in range(_MAX_ATTEMPTS):
        if _BACKOFF_S[attempt]:
            time.sleep(_BACKOFF_S[attempt])
        try:
            import yfinance as yf

            tk = yf.Ticker(symbol)
            cf = getattr(tk, cf_attr)
            inc = getattr(tk, inc_attr)
        except Exception as e:      # 网络/解析异常
            last_err = e
            logger.warning(
                f"{symbol} yfinance 第 {attempt + 1}/{_MAX_ATTEMPTS} 次取数异常: "
                f"{type(e).__name__}: {e}")
            continue

        if cf is not None and not cf.empty and inc is not None and not inc.empty:
            if attempt:
                logger.info(f"{symbol} yfinance 第 {attempt + 1} 次重试成功（前几次疑似限流）")
            return DataResult.ok((cf.T, inc.T), source=f"yfinance:{symbol}")

        logger.debug(
            f"{symbol} yfinance 第 {attempt + 1}/{_MAX_ATTEMPTS} 次返回空表"
            f"（现金流空={cf is None or cf.empty}, 利润表空={inc is None or inc.empty}）")

    if last_err is not None:
        return DataResult.failed(
            f"yfinance 连续 {_MAX_ATTEMPTS} 次取数失败（{type(last_err).__name__}），"
            f"多半是 Yahoo 不可达或被限流，请稍后重试",
            source=f"yfinance:{symbol}", error=last_err)

    # 空表在"被限流"和"真无记录"下**完全同形**，重试到底也无法区分。
    # 故这里不武断归因（对 RBLX 这类大票说"可能是新股"是明显错的），
    # 两种可能都讲清楚，并给出可执行的下一步。
    return DataResult.empty(
        f"{symbol} 连续 {_MAX_ATTEMPTS} 次取到空报表：可能是 Yahoo 限流"
        f"（稍等一两分钟重试即可），也可能该标的确实无财务记录（新股/已退市/代码有误）",
        source=f"yfinance:{symbol}")


def statements_or_empty(symbol: str, is_annual: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    """老调用方的兼容入口：只要 DataFrame，拿不到就给空表"""
    res = fetch_statements(symbol, is_annual)
    if not res.is_ok:
        return pd.DataFrame(), pd.DataFrame()
    return res.data
