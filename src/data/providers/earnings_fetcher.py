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

from src.data.providers.cache_manager import CacheManager
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
        self._name_map: dict[str, str] = {}
        self._config_loaded = False

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
                    ("yysj", self._safe_yysj, "预约披露", "时间"),
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
        """通过 yfinance 读取港股 Earnings Date（优于之前的推导法）"""
        today = datetime.now().date()
        cutoff = today + timedelta(days=days_ahead)
        records: list[dict] = []

        try:
            import yfinance as yf
        except ImportError:
            return pd.DataFrame(columns=COLUMNS)

        for code in codes:
            yf_code = self._to_yf_symbol(code, "hk")
            cache_key = f"hk_calendar_{yf_code}"

            def _fetch_cal(c=yf_code):
                try:
                    tk = yf.Ticker(c)
                    cal = tk.calendar
                    return cal if cal else None
                except Exception:
                    return None

            cal = self._cache.get_or_fetch(cache_key, _fetch_cal)
            if not cal:
                continue

            # 提取日期逻辑与美股一致
            ed = None
            try:
                dates = cal.get("Earnings Date") if isinstance(cal, dict) else None
                if isinstance(dates, (list, tuple)) and dates:
                    dt_val = pd.to_datetime(dates[0])
                elif dates is not None:
                    dt_val = pd.to_datetime(dates)
                else:
                    continue

                # 稳健获取 date 对象
                if hasattr(dt_val, "date"):
                    if isinstance(dt_val, pd.DatetimeIndex):
                        ed = dt_val[0].date()
                    else:
                        ed = dt_val.date()
                else:
                    ed = dt_val
            except Exception:
                continue

            if ed is None:
                continue

            if today <= ed <= cutoff:
                records.append({
                    "code": code,
                    "name": self._get_friendly_name(code, "hk", yf_code=yf_code),
                    "market": "hk",
                    "event_type": "财报日",
                    "disclose_date": pd.Timestamp(ed),
                    "report_period": "",
                    "extra": "via yfinance",
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
            yf_code = self._to_yf_symbol(code, "us")
            cache_key = f"us_calendar_{yf_code}"

            def _fetch_cal(c=yf_code):
                try:
                    tk = yf.Ticker(c)
                    cal = tk.calendar
                    if isinstance(cal, dict):
                        return cal
                    if isinstance(cal, pd.DataFrame) and not cal.empty:
                        return cal.to_dict()
                    return None
                except Exception:
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
                    "name": self._get_friendly_name(code, "us", yf_code=yf_code),
                    "market": "us",
                    "event_type": "财报日",
                    "disclose_date": pd.Timestamp(ed),
                    "report_period": "",
                    "extra": "via yfinance",
                })

        df = pd.DataFrame(records, columns=COLUMNS)
        logger.info(f"美股披露日历：未来 {days_ahead} 天共 {len(df)} 条事件")
        return df

    def _to_yf_symbol(self, code: str, market: str) -> str:
        """统一转换为 yfinance 符号"""
        code_str = str(code).strip()
        if market == "hk":
            # 00700 -> 0700.HK
            pure = code_str.split(".")[0].lstrip("0")
            return f"{pure.zfill(4)}.HK"
        if market == "a":
            # 600519 -> 600519.SS / 000001 -> 000001.SZ
            pure = code_str.split(".")[0]
            suffix = ".SS" if pure.startswith("6") else ".SZ"
            return f"{pure}{suffix}"
        return code_str  # 美股直接用原始代码

    def _get_friendly_name(self, code: str, market: str, yf_code: str = None) -> str:
        """获取可读名称：优先本地配置 -> 其次 yfinance -> 最后返回 code"""
        code_str = str(code).strip()
        cache_key = f"{market}:{code_str}"
        
        if cache_key in self._name_map:
            return self._name_map[cache_key]

        # 1. 尝试从本地配置加载 (仅加载一次)
        if not self._config_loaded:
            self._load_local_stock_configs()
            self._config_loaded = True
            if cache_key in self._name_map:
                return self._name_map[cache_key]

        # 2. 如果提供了 yf_code 且本地没找到，尝试从 yf 获取
        if yf_code:
            try:
                import yfinance as yf
                tk = yf.Ticker(yf_code)
                name = tk.info.get("shortName") or tk.info.get("longName")
                if name:
                    self._name_map[cache_key] = name
                    return name
            except Exception:
                pass

        return code_str

    def _load_local_stock_configs(self):
        """将 a_stock, hk_stock, us_stock 中的名称存入内存映射"""
        import yaml
        from pathlib import Path
        
        # 寻找 config 目录 (假设在项目根目录)
        # EarningsFetcher 在 src/data/fetcher/
        root = Path(__file__).resolve().parents[3]
        config_dir = root / "config"
        
        for m, filename in [("a", "a_stock.yaml"), ("hk", "hk_stock.yaml"), ("us", "us_stock.yaml")]:
            p = config_dir / filename
            if not p.exists():
                continue
            try:
                with open(p, encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                    if data and "categories" in data:
                        for cat in data["categories"].values():
                            for stock in cat.get("stocks", []):
                                c = str(stock.get("code", "")).strip()
                                n = str(stock.get("name", "")).strip()
                                if c and n:
                                    self._name_map[f"{m}:{c}"] = n
            except Exception as e:
                logger.debug(f"加载 {filename} 映射失败: {e}")


# ========================
# 内部工具
# ========================

def _recent_quarter_codes(n: int = 4) -> list[str]:
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

    def _pick(keywords: list[str]) -> str:
        """从 df.columns 里按优先级找包含关键字的列"""
        for k in keywords:
            for c in df.columns:
                if k in str(c):
                    return c
        return ""

    code_col = _pick(["代码"])
    name_col = _pick(["名称", "简称"])
    # 优先匹配传入的 date_col，其次是通俗的日期/时间列
    actual_date_col = _pick([date_col, "实际披露时间", "日期", "时间"])

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
