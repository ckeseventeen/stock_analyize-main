"""
src/data/fetcher/earnings_fetcher.py — 财报披露日历数据获取器

为 EarningsMonitor 提供统一接口：
  - get_a_share_upcoming(days_ahead)  : A 股业绩预告/快报/预约披露
  - get_hk_upcoming(codes, days_ahead): 港股（从财报历史推导下次披露窗口）
  - get_us_upcoming(codes, days_ahead): 美股（yfinance 日历接口）

输出 DataFrame 统一列：
  code / name / market / event_type / disclose_date / report_period / extra
  其中 event_type ∈ {yjyg业绩预告, yjkb业绩快报, yjbb业绩报告, yysj预约披露, inferred推导}
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable, Optional

import pandas as pd

from src.data.fetcher.cache_manager import CacheManager
from src.utils.exception_handler import retry
from src.utils.logger import get_logger

logger = get_logger("earnings")


# 统一 DataFrame 列
COLUMNS = ["code", "name", "market", "event_type", "disclose_date", "report_period", "extra"]


class EarningsFetcher:
    """
    财报披露数据获取器（A/港/美三市场统一接口）。

    用法：
        fetcher = EarningsFetcher()
        a_df = fetcher.get_a_share_upcoming(days_ahead=30)
        hk_df = fetcher.get_hk_upcoming(["00700", "09988"], days_ahead=30)
        us_df = fetcher.get_us_upcoming(["AAPL", "TSLA"], days_ahead=30)
    """

    def __init__(self, cache_dir: str = ".cache/earnings", ttl_hours: int = 12):
        # 披露日历变动不频繁，TTL 默认 12 小时
        self._cache = CacheManager(cache_dir=cache_dir, ttl_hours=ttl_hours)

    # ------------------------------------------------------------------ #
    # A 股
    # ------------------------------------------------------------------ #
    def get_a_share_upcoming(self, days_ahead: int = 30) -> pd.DataFrame:
        """
        合并 A 股四类披露事件：
          业绩预告 stock_yjyg_em / 业绩快报 stock_yjkb_em / 业绩报告 stock_yjbb_em / 预约披露 stock_yysj_em

        所有方法按"当前季度 + 上季度"拉取，再按 disclose_date 过滤到未来 N 天。
        """
        today = datetime.now().date()
        cutoff = today + timedelta(days=days_ahead)

        cache_key = f"a_share_upcoming_{days_ahead}_{today.isoformat()}"

        def _fetch() -> pd.DataFrame:
            frames: list[pd.DataFrame] = []
            periods = _recent_quarter_codes(2)  # 最近两个季度

            for period in periods:
                for src_name, loader, etype, date_col in (
                    ("yjyg", self._safe_yjyg, "业绩预告", "预告日期"),
                    ("yjkb", self._safe_yjkb, "业绩快报", "公告日期"),
                    ("yjbb", self._safe_yjbb, "业绩报告", "最新公告日期"),
                    ("yysj", self._safe_yysj, "预约披露", "预约披露日期"),
                ):
                    df = loader(period)
                    if df is None or df.empty:
                        continue
                    parsed = _parse_ashare_df(df, etype, date_col, period)
                    if not parsed.empty:
                        frames.append(parsed)

            if not frames:
                logger.warning("A 股披露日历为空")
                return pd.DataFrame(columns=COLUMNS)

            merged = pd.concat(frames, ignore_index=True)
            # 过滤：未来 N 天内
            merged["disclose_date"] = pd.to_datetime(merged["disclose_date"], errors="coerce")
            merged = merged.dropna(subset=["disclose_date"])
            mask = (merged["disclose_date"].dt.date >= today) & (merged["disclose_date"].dt.date <= cutoff)
            merged = merged[mask].sort_values("disclose_date").reset_index(drop=True)
            logger.info(f"A 股披露日历：未来 {days_ahead} 天共 {len(merged)} 条事件")
            return merged

        return self._cache.get_or_fetch(cache_key, _fetch)

    # --- 薄封装：akshare 调用易报错（网络/参数），独立封装便于容错 ---
    @retry(max_attempts=2, delay=2.0)
    def _safe_yjyg(self, period: str) -> Optional[pd.DataFrame]:
        try:
            import akshare as ak
            return ak.stock_yjyg_em(date=period)
        except Exception as e:
            logger.debug(f"stock_yjyg_em({period}) 失败: {e}")
            return None

    @retry(max_attempts=2, delay=2.0)
    def _safe_yjkb(self, period: str) -> Optional[pd.DataFrame]:
        try:
            import akshare as ak
            return ak.stock_yjkb_em(date=period)
        except Exception as e:
            logger.debug(f"stock_yjkb_em({period}) 失败: {e}")
            return None

    @retry(max_attempts=2, delay=2.0)
    def _safe_yjbb(self, period: str) -> Optional[pd.DataFrame]:
        try:
            import akshare as ak
            return ak.stock_yjbb_em(date=period)
        except Exception as e:
            logger.debug(f"stock_yjbb_em({period}) 失败: {e}")
            return None

    @retry(max_attempts=2, delay=2.0)
    def _safe_yysj(self, period: str) -> Optional[pd.DataFrame]:
        try:
            import akshare as ak
            return ak.stock_yysj_em(date=period)
        except Exception as e:
            logger.debug(f"stock_yysj_em({period}) 失败: {e}")
            return None

    # ------------------------------------------------------------------ #
    # 港股 —— 通过财报历史推导下次披露时间窗
    # ------------------------------------------------------------------ #
    def get_hk_upcoming(self, codes: Iterable[str], days_ahead: int = 30) -> pd.DataFrame:
        """
        港股官方披露日历 akshare 未直接提供，
        这里用策略：读取最近年报/中报，推导下一个披露窗口。
        港股披露规则（简化）：
          - 中报（12 月财年）：次年 3 月底前发年报；9 月底前发中报
          - 实际以公司历史平均间隔估算
        """
        today = datetime.now().date()
        cutoff = today + timedelta(days=days_ahead)
        records: list[dict] = []

        for code in codes:
            cache_key = f"hk_hist_fin_{code}"

            def _fetch_hist(c=code):
                try:
                    import akshare as ak
                    return ak.stock_financial_hk_analysis_indicator_em(
                        symbol=c, indicator="年度"
                    )
                except Exception as e:
                    logger.debug(f"HK 财报历史获取失败 {c}: {e}")
                    return pd.DataFrame()

            hist = self._cache.get_or_fetch(cache_key, _fetch_hist)
            if hist is None or hist.empty:
                continue

            # 取最近报告期 + 6 个月作为下次披露窗口估计
            date_cols = [c for c in hist.columns if "报告" in str(c) or "日期" in str(c)]
            if not date_cols:
                continue
            dates = pd.to_datetime(hist[date_cols[0]], errors="coerce").dropna()
            if dates.empty:
                continue
            last_report = dates.max()
            # 港股通常每半年发一次财报
            estimated = last_report + pd.DateOffset(months=6)
            est_date = estimated.date()

            if today <= est_date <= cutoff:
                records.append({
                    "code": code,
                    "name": code,
                    "market": "hk",
                    "event_type": "推导披露",
                    "disclose_date": pd.Timestamp(est_date),
                    "report_period": last_report.strftime("%Y-%m-%d"),
                    "extra": "基于历史报告期 + 6 个月估算",
                })

        df = pd.DataFrame(records, columns=COLUMNS)
        logger.info(f"港股披露日历：未来 {days_ahead} 天共 {len(df)} 条事件")
        return df

    # ------------------------------------------------------------------ #
    # 美股 —— yfinance.Ticker.calendar
    # ------------------------------------------------------------------ #
    def get_us_upcoming(self, codes: Iterable[str], days_ahead: int = 30) -> pd.DataFrame:
        """通过 yfinance 读取 Earnings Date（可能返回区间，取起始）"""
        today = datetime.now().date()
        cutoff = today + timedelta(days=days_ahead)
        records: list[dict] = []

        try:
            import yfinance as yf
        except ImportError:
            logger.warning("未安装 yfinance，跳过美股披露日历")
            return pd.DataFrame(columns=COLUMNS)

        for code in codes:
            cache_key = f"us_calendar_{code}"

            def _fetch_cal(c=code):
                try:
                    tk = yf.Ticker(c)
                    cal = tk.calendar
                    # calendar 可能为 dict 或 DataFrame（新旧版本差异）
                    if isinstance(cal, dict):
                        return cal
                    if isinstance(cal, pd.DataFrame) and not cal.empty:
                        return cal.to_dict()
                    return None
                except Exception as e:
                    logger.debug(f"yfinance calendar {c} 失败: {e}")
                    return None

            cal = self._cache.get_or_fetch(cache_key, _fetch_cal)
            if not cal:
                continue

            # 尝试提取 Earnings Date
            earnings_date = None
            try:
                dates = cal.get("Earnings Date") if isinstance(cal, dict) else None
                if isinstance(dates, (list, tuple)) and dates:
                    earnings_date = pd.to_datetime(dates[0])
                elif dates is not None:
                    earnings_date = pd.to_datetime(dates)
            except Exception:
                earnings_date = None

            if earnings_date is None:
                continue
            ed = earnings_date.date() if hasattr(earnings_date, "date") else earnings_date

            if today <= ed <= cutoff:
                records.append({
                    "code": code,
                    "name": code,
                    "market": "us",
                    "event_type": "财报日",
                    "disclose_date": pd.Timestamp(ed),
                    "report_period": "",
                    "extra": "via yfinance",
                })

        df = pd.DataFrame(records, columns=COLUMNS)
        logger.info(f"美股披露日历：未来 {days_ahead} 天共 {len(df)} 条事件")
        return df


# ========================
# 内部工具
# ========================

def _recent_quarter_codes(n: int = 2) -> list[str]:
    """
    生成最近 n 个季度的字符串代码，格式 'YYYYMMDD'（季末日）。
    akshare 相关接口参数是季末日：0331 / 0630 / 0930 / 1231。
    """
    today = datetime.now()
    quarters: list[str] = []
    y, m = today.year, today.month
    # 当前季度索引
    cur_q = (m - 1) // 3 + 1
    # 往前回溯 n 个季度
    for i in range(n):
        q = cur_q - i
        yy = y
        while q <= 0:
            q += 4
            yy -= 1
        q_end_map = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}
        quarters.append(f"{yy}{q_end_map[q]}")
    return quarters


def _parse_ashare_df(
    df: pd.DataFrame, event_type: str, date_col: str, period: str
) -> pd.DataFrame:
    """
    把 akshare 某类披露数据规范到统一列。

    akshare 字段名因接口略有差异，此函数做宽松匹配。
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=COLUMNS)

    def _pick(columns_upper: str) -> str:
        """从 df.columns 里找包含 columns_upper 关键字的列"""
        for c in df.columns:
            if columns_upper in str(c):
                return c
        return ""

    code_col = _pick("代码")
    name_col = _pick("名称") or _pick("简称")
    actual_date_col = date_col if date_col in df.columns else _pick(date_col) or _pick("日期")

    if not code_col or not actual_date_col:
        logger.debug(f"[{event_type}] 缺失关键列，跳过：{df.columns.tolist()}")
        return pd.DataFrame(columns=COLUMNS)

    out = pd.DataFrame({
        "code": df[code_col].astype(str).str.zfill(6),
        "name": df[name_col].astype(str) if name_col else "",
        "market": "a",
        "event_type": event_type,
        "disclose_date": pd.to_datetime(df[actual_date_col], errors="coerce"),
        "report_period": period,
        "extra": "",
    })

    # 业绩预告附加：预告类型 + 变动幅度
    if event_type == "业绩预告":
        type_col = _pick("预告类型")
        change_col = _pick("预告净利润变动幅度") or _pick("变动幅度")
        parts = []
        if type_col:
            parts.append(df[type_col].astype(str))
        if change_col:
            parts.append("幅度: " + df[change_col].astype(str))
        if parts:
            out["extra"] = [" | ".join(x) for x in zip(*parts)] if len(parts) > 1 else parts[0].tolist()

    return out.dropna(subset=["disclose_date"])
