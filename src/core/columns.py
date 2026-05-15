"""
src/core/columns.py — 列名常量与中英文映射

跨层数据契约的单点真相（Single Source of Truth）：
  - akshare 返回的 Spot 行情列（中文）
  - akshare 返回的 OHLCV 列（中文 + 英文别名）
  - 财务摘要列
  - 估值/技术指标衍生列

为什么：原代码在 14 个文件、99 处硬编码 "代码"/"名称"/"市盈率-动态" 等字面量，
任何 akshare 列名变更（或新增市场使用英文列）都得 grep 全项目。

用法：
    from src.core.columns import Col

    pe = spot_row.get(Col.PE_DYN)            # 而不是 spot_row.get("市盈率-动态")
    df.rename(columns=Col.OHLCV_CN_TO_EN, inplace=True)

向后兼容：常量"值"仍是中文字面量（如 "代码"），完全不破坏现存 yaml/csv/akshare 数据格式。
"""
from __future__ import annotations

from typing import Final


class Col:
    """akshare / 项目自有的列名常量集合"""

    # ---------------- Spot 行情（akshare stock_zh_a_spot_em 等）----------------
    CODE: Final[str] = "代码"
    NAME: Final[str] = "名称"
    PRICE: Final[str] = "最新价"
    CHANGE_PCT: Final[str] = "涨跌幅"
    CHANGE_AMT: Final[str] = "涨跌额"
    VOLUME: Final[str] = "成交量"
    TURNOVER: Final[str] = "换手率"
    AMOUNT: Final[str] = "成交额"
    AMPLITUDE: Final[str] = "振幅"
    HIGH_TODAY: Final[str] = "最高"
    LOW_TODAY: Final[str] = "最低"
    OPEN_TODAY: Final[str] = "今开"
    PREV_CLOSE: Final[str] = "昨收"

    # ---------------- 估值 ----------------
    PE_DYN: Final[str] = "市盈率-动态"
    PE_TTM: Final[str] = "市盈率-TTM"
    PB: Final[str] = "市净率"
    PS_TTM: Final[str] = "市销率-TTM"
    MARKET_CAP: Final[str] = "总市值"
    FLOAT_MARKET_CAP: Final[str] = "流通市值"

    # ---------------- 财务 ----------------
    NET_PROFIT: Final[str] = "归母净利润"
    REVENUE: Final[str] = "营业总收入"
    COST: Final[str] = "营业成本"
    ROE: Final[str] = "ROE"
    ROE_CN: Final[str] = "净资产收益率"
    GROSS_MARGIN: Final[str] = "毛利率"

    # ---------------- OHLCV（akshare 日线/周线/月线）----------------
    DATE: Final[str] = "日期"
    OPEN: Final[str] = "开盘"
    HIGH: Final[str] = "最高"
    LOW: Final[str] = "最低"
    CLOSE: Final[str] = "收盘"

    # ---------------- 英文标准列（OHLCV + spot 通用化映射的目标）----------------
    EN_OPEN: Final[str] = "open"
    EN_HIGH: Final[str] = "high"
    EN_LOW: Final[str] = "low"
    EN_CLOSE: Final[str] = "close"
    EN_VOLUME: Final[str] = "volume"
    EN_DATE: Final[str] = "date"
    EN_AMOUNT: Final[str] = "amount"
    EN_TURNOVER: Final[str] = "turnover"

    # ---------------- 财报披露日历（自有约定）----------------
    EVENT_TYPE: Final[str] = "event_type"
    DISCLOSE_DATE: Final[str] = "disclose_date"
    REPORT_PERIOD: Final[str] = "report_period"


# ============================================================================
# 双向映射：方便 DataFrame.rename(columns=...)
# ============================================================================

# OHLCV: 中文 → 英文（用于 TechnicalAnalyzer / Backtrader 等需英文列的下游）
OHLCV_CN_TO_EN: Final[dict[str, str]] = {
    Col.DATE: Col.EN_DATE,
    Col.OPEN: Col.EN_OPEN,
    Col.HIGH: Col.EN_HIGH,
    Col.LOW: Col.EN_LOW,
    Col.CLOSE: Col.EN_CLOSE,
    Col.VOLUME: Col.EN_VOLUME,
    Col.AMOUNT: Col.EN_AMOUNT,
    Col.TURNOVER: Col.EN_TURNOVER,
}

OHLCV_EN_TO_CN: Final[dict[str, str]] = {v: k for k, v in OHLCV_CN_TO_EN.items()}


def get_close_col(df_columns) -> str | None:
    """
    从 DataFrame 列名中智能识别"收盘价"列。
    返回找到的列名，找不到返回 None。

    替代散落在各处的 `for col in ("close", "收盘"): if col in df.columns: ...` 样板。
    """
    cols = set(df_columns)
    for candidate in (Col.EN_CLOSE, Col.CLOSE):
        if candidate in cols:
            return candidate
    return None


def get_volume_col(df_columns) -> str | None:
    """从 DataFrame 列名中智能识别"成交量"列"""
    cols = set(df_columns)
    for candidate in (Col.EN_VOLUME, Col.VOLUME):
        if candidate in cols:
            return candidate
    return None


def normalize_ohlcv_columns(df):
    """
    把 OHLCV DataFrame 的中文列名归一化为英文（in-place 友好版本）。
    返回 rename 后的 DataFrame。
    """
    rename_map = {k: v for k, v in OHLCV_CN_TO_EN.items() if k in df.columns}
    if rename_map:
        return df.rename(columns=rename_map)
    return df
