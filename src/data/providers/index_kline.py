"""
src/data/providers/index_kline.py — 指数日线统一拉取（带三级 fallback）

由 ml/dataset_builder.py 提取，供 portfolio/market_regime.py、
portfolio/macro_signals.py、ml/dataset_builder.py 共用。

三级 fallback：
  1. akshare 新浪 stock_zh_index_daily（symbol 需要 sh/sz 前缀）
  2. pytdx 直连（指数 market: 000xxx=sh=1, 399xxx=sz=0）
  3. Baostock K-data（指数代码格式 sh.000300 / sz.399006）

为什么不能用 ak.stock_zh_index_daily_em：东方财富的 endpoint
（push2.eastmoney.com）在很多机器上被反爬封 IP。
"""
from __future__ import annotations

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("index_kline")


# 指数代码常量（避免 magic string 散落）
CSI300 = "000300"        # 沪深 300
SSE50 = "000016"         # 上证 50
CSI500 = "000905"        # 中证 500
CSI1000 = "000852"       # 中证 1000
CYBZ = "399006"          # 创业板指
STAR50 = "000688"        # 科创 50
SSE = "000001"           # 上证综指 — 注意与平安银行 000001 撞代码，按上下文区分


def _is_shenzhen_index(code: str) -> bool:
    """深市指数代码（创业板/中小板）以 399 开头；其余按沪市处理。"""
    return code.startswith("399")


def fetch_index_kline(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    拉指数日线（不走缓存层，调用方自己缓存）

    Args:
        code: 6 位指数代码（不带 sh/sz 前缀），如 "000300"
        start_date / end_date: "YYYY-MM-DD" 字符串

    Returns:
        DataFrame，至少含 [日期, 收盘] 列。三级都失败返回空 DataFrame。
    """
    # 1. akshare 新浪
    try:
        import akshare as ak
        symbol = ("sz" if _is_shenzhen_index(code) else "sh") + code
        df = ak.stock_zh_index_daily(symbol=symbol)
        if df is not None and not df.empty:
            df = df.rename(columns={"date": "日期", "close": "收盘",
                                      "open": "开盘", "high": "最高",
                                      "low": "最低", "volume": "成交量"})
            df["日期"] = pd.to_datetime(df["日期"])
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            df = df[(df["日期"] >= start) & (df["日期"] <= end)]
            if not df.empty:
                return df.reset_index(drop=True)
    except Exception as e:
        logger.debug(f"指数 {code} akshare 新浪失败: {e}")

    # 2. pytdx 直连（指数 market 修正）
    try:
        from pytdx.hq import TdxHq_API
        from src.data.providers.pytdx_provider import get_global_pytdx
        pytdx = get_global_pytdx()
        if pytdx.is_available() and pytdx._working_servers:
            ip, port = pytdx._working_servers[0]
            api = TdxHq_API()
            if api.connect(ip, port, time_out=3):
                # 深市指数 market=0，沪市指数 market=1
                market = 0 if _is_shenzhen_index(code) else 1
                bars = api.get_index_bars(category=9, market=market, code=code,
                                             start=0, count=800)
                try:
                    api.disconnect()
                except Exception:
                    pass
                if bars:
                    df = pd.DataFrame(bars)
                    df = df.rename(columns={"datetime": "日期", "close": "收盘",
                                              "open": "开盘", "high": "最高",
                                              "low": "最低", "vol": "成交量"})
                    df["日期"] = pd.to_datetime(df["日期"]).dt.normalize()
                    df = df.sort_values("日期")
                    start = pd.to_datetime(start_date)
                    end = pd.to_datetime(end_date)
                    df = df[(df["日期"] >= start) & (df["日期"] <= end)]
                    if not df.empty:
                        return df.reset_index(drop=True)
    except Exception as e:
        logger.debug(f"指数 {code} pytdx 失败: {e}")

    # 3. Baostock 兜底
    try:
        from src.data.providers.baostock_provider import BaostockProvider
        bs_code = ("sz." if _is_shenzhen_index(code) else "sh.") + code
        with BaostockProvider() as bp:
            df = bp.get_k_data(bs_code, days_back=2000, frequency="d",
                                 fields="date,open,high,low,close,volume")
        if df is not None and not df.empty:
            df = df.rename(columns={"date": "日期", "close": "收盘",
                                      "open": "开盘", "high": "最高",
                                      "low": "最低", "volume": "成交量"})
            df["日期"] = pd.to_datetime(df["日期"])
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            df = df[(df["日期"] >= start) & (df["日期"] <= end)]
            if not df.empty:
                return df.reset_index(drop=True)
    except Exception as e:
        logger.debug(f"指数 {code} Baostock 失败: {e}")

    return pd.DataFrame()
