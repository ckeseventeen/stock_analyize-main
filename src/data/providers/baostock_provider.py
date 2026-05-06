"""
src/data/fetcher/baostock_provider.py — Baostock 统一封装层

所有需要 Baostock 数据的模块统一通过本模块调用，
避免到处写 login/logout/行转列/代码格式转换逻辑。

使用示例:
    from src.data.providers.baostock_provider import BaostockProvider

    with BaostockProvider() as bp:
        df = bp.get_k_data("600519", days_back=120, frequency="d")
        all_stocks = bp.get_all_stocks()
        val_df = bp.get_valuation_history("600519", days_back=365*5)
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta

import baostock as bs
import pandas as pd

logger = logging.getLogger("stock_analyzer")

# Baostock 库内部使用单一全局 TCP socket，非线程安全。
# 所有 bs.* 调用（login / logout / query_*）必须在此锁保护下串行执行，
# 否则多线程并发会导致 socket 数据交叉读取、Bad file descriptor 等致命错误。
_bs_lock = threading.Lock()


# ========================
# 代码格式转换
# ========================

def to_bs_code(code: str) -> str:
    """
    将纯数字股票代码转换为 Baostock 格式。

    '600519'  → 'sh.600519'
    '000001'  → 'sz.000001'
    '300750'  → 'sz.300750'
    'sh.600519' → 'sh.600519'  (已有前缀则不变)
    """
    code = str(code).strip()
    if "." in code:
        # 已有前缀 (sh.xxx / sz.xxx) → 直接返回
        return code.lower()
    if code.startswith("6"):
        return f"sh.{code}"
    # 0xx / 3xx / 8xx（北交所）等均走深圳
    return f"sz.{code}"


def from_bs_code(bs_code: str) -> str:
    """Baostock 格式转纯数字: 'sh.600519' → '600519'"""
    return bs_code.split(".")[-1] if "." in bs_code else bs_code


# ========================
# 查询结果转 DataFrame
# ========================

def _rs_to_df(rs) -> pd.DataFrame:
    """将 Baostock ResultData 对象转换为 pandas DataFrame。

    注意：本函数通常在 _bs_lock 持有期间调用（rs 对象内部仍会走 socket 读取）。
    """
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=rs.fields)


# ========================
# 各频率支持的字段常量
# ========================
# 日线支持估值指标 (peTTM/pbMRQ/psTTM/isST)
_DAILY_FIELDS = "date,open,high,low,close,volume,amount,turn,peTTM,pbMRQ,psTTM,isST"
# 周线/月线仅支持基础 OHLCV + turn + pctChg
_WEEKLY_MONTHLY_FIELDS = "date,open,high,low,close,volume,amount,turn,pctChg"


# ========================
# Baostock 会话管理
# ========================

class BaostockProvider:
    """
    Baostock 数据提供器（支持上下文管理器自动 login/logout）。

    用法:
        with BaostockProvider() as bp:
            df = bp.get_k_data("600519")
    """

    def __init__(self):
        self._logged_in = False

    def login(self) -> BaostockProvider:
        """登录 Baostock 服务器（线程安全）。"""
        if not self._logged_in:
            with _bs_lock:
                lg = bs.login()
            if lg.error_code != "0":
                logger.warning(f"Baostock 登录失败: {lg.error_msg}")
            else:
                self._logged_in = True
                logger.debug("Baostock 登录成功")
        return self

    def logout(self) -> None:
        """登出 Baostock 服务器（线程安全）。"""
        if self._logged_in:
            with _bs_lock:
                bs.logout()
            self._logged_in = False
            logger.debug("Baostock 登出成功")

    def __enter__(self) -> BaostockProvider:
        return self.login()

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.logout()
        return False

    # ========================
    # K 线数据
    # ========================

    def get_k_data(
        self,
        code: str,
        days_back: int = 120,
        frequency: str = "d",
        adjust: str = "2",
        fields: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        获取 K 线数据（含估值指标）。

        Args:
            code: 股票代码（纯数字，如 '600519'）
            days_back: 向前获取的天数
            frequency: 'd'=日K, 'w'=周K, 'm'=月K, '5'/'15'/'30'/'60'=分钟K
            adjust: '1'=后复权, '2'=前复权, '3'=不复权
            fields: 查询字段（Baostock 字段名）。None 时自动根据 frequency 选取，
                    周线/月线 **不支持** peTTM/pbMRQ/psTTM/isST。
            start_date: 起始日期 (YYYY-MM-DD)，优先于 days_back
            end_date: 结束日期 (YYYY-MM-DD)，默认今天

        Returns:
            DataFrame，数值列已转为 float
        """
        bs_code = to_bs_code(code)
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        # 自动选择合法字段：周线/月线不支持 peTTM/pbMRQ/psTTM/isST
        if fields is None:
            fields = _DAILY_FIELDS if frequency == "d" else _WEEKLY_MONTHLY_FIELDS

        try:
            with _bs_lock:
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    fields=fields,
                    start_date=start_date,
                    end_date=end_date,
                    frequency=frequency,
                    adjustflag=adjust,
                )
                df = _rs_to_df(rs)
            if df.empty:
                logger.debug(f"Baostock K线数据为空: {code} ({start_date} ~ {end_date})")
                return df

            # 数值列转 float
            numeric_cols = [c for c in df.columns if c not in ("date", "code", "isST")]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            logger.debug(f"Baostock K线获取成功: {code} {frequency} 共 {len(df)} 条")
            return df

        except Exception as e:
            logger.warning(f"Baostock K线查询失败 [{code}]: {e}")
            return pd.DataFrame()

    # ========================
    # 全市场股票列表
    # ========================

    def get_all_stocks(self, date: str | None = None) -> pd.DataFrame:
        """
        获取指定日期全市场股票列表。

        Args:
            date: 日期 (YYYY-MM-DD)，默认最近交易日

        Returns:
            DataFrame 含 code, tradeStatus, code_name 列
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        try:
            with _bs_lock:
                rs = bs.query_all_stock(day=date)
                df = _rs_to_df(rs)
            if df.empty:
                # 可能是非交易日，往前推 7 天再试
                for delta in range(1, 8):
                    alt_date = (datetime.now() - timedelta(days=delta)).strftime("%Y-%m-%d")
                    with _bs_lock:
                        rs = bs.query_all_stock(day=alt_date)
                        df = _rs_to_df(rs)
                    if not df.empty:
                        break

            if not df.empty:
                logger.debug(f"Baostock 全市场股票列表: {len(df)} 只")
            return df

        except Exception as e:
            logger.warning(f"Baostock 全市场列表查询失败: {e}")
            return pd.DataFrame()

    # ========================
    # 估值历史 (PE/PB/PS 时间序列)
    # ========================

    def get_valuation_history(
        self,
        code: str,
        days_back: int = 365 * 5,
        val_type: str = "pe",
    ) -> pd.DataFrame:
        """
        获取历史估值数据（从日K线提取 peTTM/pbMRQ/psTTM）。

        Args:
            code: 股票代码
            days_back: 向前天数（默认 5 年）
            val_type: 'pe' / 'pb' / 'ps'

        Returns:
            DataFrame 含 trade_date + pe_ttm/ps_ttm 列（兼容下游 analyzer）
        """
        field_map = {
            "pe": "peTTM",
            "pb": "pbMRQ",
            "ps": "psTTM",
        }
        bs_field = field_map.get(val_type, "peTTM")

        df = self.get_k_data(
            code,
            days_back=days_back,
            frequency="d",
            fields=f"date,{bs_field}",
            adjust="2",
        )
        if df.empty:
            return pd.DataFrame()

        # 统一列名以兼容下游 analyzer
        col_map = {
            "peTTM": "pe_ttm",
            "pbMRQ": "pb_mrq",
            "psTTM": "ps_ttm",
        }
        val_col = col_map.get(bs_field, "pe_ttm")
        df = df.rename(columns={"date": "trade_date", bs_field: val_col})

        # 过滤掉空值和 0 值
        df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
        df = df.dropna(subset=[val_col])
        df = df[df[val_col] != 0.0]

        return df

    # ========================
    # 盈利能力 (季度)
    # ========================

    def get_profit_data(self, code: str, year: int = 0, quarter: int = 0) -> pd.DataFrame:
        """
        获取季度盈利能力数据（营业收入、净利润、总股本等）。

        Args:
            code: 股票代码
            year: 年份，0=自动推算最新年份
            quarter: 季度 (1-4)，0=自动推算最新季度

        Returns:
            DataFrame 含 statDate, netProfit, epsTTM, totalShare, liqaShare, ...
        """
        bs_code = to_bs_code(code)

        # 自动推算最近的年/季度（财报有滞后，回退到上一个季度）
        if year == 0 or quarter == 0:
            now = datetime.now()
            # 当前月份对应的上一个已披露季度
            month = now.month
            if month <= 4:
                # 1-4月：上年Q3可能是最新的（Q4年报4月底才出）
                year = now.year - 1
                quarter = 4
            elif month <= 7:
                year = now.year
                quarter = 1
            elif month <= 10:
                year = now.year
                quarter = 2
            else:
                year = now.year
                quarter = 3

        # 尝试当前季度，失败则向前回退
        for _attempt in range(4):
            try:
                with _bs_lock:
                    rs = bs.query_profit_data(code=bs_code, year=year, quarter=quarter)
                    df = _rs_to_df(rs)
                if not df.empty:
                    logger.debug(f"Baostock 盈利数据获取成功: {code} {year}Q{quarter} 共 {len(df)} 期")
                    return df
            except Exception as e:
                logger.debug(f"Baostock 盈利数据 {year}Q{quarter} 失败: {e}")

            # 回退到上一个季度
            quarter -= 1
            if quarter <= 0:
                quarter = 4
                year -= 1

        logger.warning(f"Baostock 盈利数据查询失败 [{code}]: 连续4个季度均无数据")
        return pd.DataFrame()

    def get_profit_history(self, code: str, num_years: int = 5) -> pd.DataFrame:
        """
        批量获取近 N 年的年度盈利数据 + 当年最新季度数据，拼为一张 DataFrame。
        供 AStockAnalyzer 做 TTM 计算使用。

        Args:
            code: 股票代码
            num_years: 年份数（默认 5 年，覆盖 annual_df.head(5) 需求）

        Returns:
            DataFrame，每行一个报告期，列包含:
              statDate, netProfit, MBRevenue, gpMargin, totalShare, epsTTM 等
            按 statDate 降序（最新在最前）
        """
        bs_code = to_bs_code(code)
        now = datetime.now()
        year = now.year
        month = now.month

        # 本年度已披露的最新季度（用于 TTM 计算）
        if month <= 4:
            current_year, current_quarter = year - 1, 4
        elif month <= 7:
            current_year, current_quarter = year, 1
        elif month <= 10:
            current_year, current_quarter = year, 2
        else:
            current_year, current_quarter = year, 3

        rows: list[list[str]] = []
        fields: list[str] = []

        # 1. 当年最新已披露季度（非 Q4）
        if current_quarter != 4:
            for _attempt in range(3):
                try:
                    with _bs_lock:
                        rs = bs.query_profit_data(code=bs_code, year=current_year, quarter=current_quarter)
                        df_cur = _rs_to_df(rs)
                    if not df_cur.empty:
                        fields = list(df_cur.columns)
                        rows.extend(df_cur.values.tolist())
                        break
                except Exception as e:
                    logger.debug(f"Baostock profit {current_year}Q{current_quarter} 失败: {e}")
                current_quarter -= 1
                if current_quarter <= 0:
                    current_quarter = 4
                    current_year -= 1

        # 2. 近 num_years 年的年报 (Q4)
        for delta in range(num_years):
            y = year - 1 - delta
            try:
                with _bs_lock:
                    rs = bs.query_profit_data(code=bs_code, year=y, quarter=4)
                    df_y = _rs_to_df(rs)
                if not df_y.empty:
                    if not fields:
                        fields = list(df_y.columns)
                    rows.extend(df_y.values.tolist())
            except Exception as e:
                logger.debug(f"Baostock profit {y}Q4 失败: {e}")

        if not rows or not fields:
            logger.warning(f"Baostock profit_history [{code}] 无数据")
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=fields)
        # 数值列转换
        numeric_cols = [c for c in df.columns if c not in ("code", "pubDate", "statDate")]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # 按 statDate 降序并去重
        df = df.drop_duplicates(subset=["statDate"], keep="first")
        df = df.sort_values("statDate", ascending=False).reset_index(drop=True)
        logger.debug(f"Baostock profit_history [{code}] 共 {len(df)} 期")
        return df

    # ========================
    # 成长能力 (季度)
    # ========================

    def get_growth_data(self, code: str, year: int = 0, quarter: int = 0) -> pd.DataFrame:
        """获取季度成长能力数据 (营收增长率、净利润增长率等)"""
        bs_code = to_bs_code(code)
        try:
            with _bs_lock:
                rs = bs.query_growth_data(code=bs_code, year=year, quarter=quarter)
                df = _rs_to_df(rs)
            return df
        except Exception as e:
            logger.warning(f"Baostock 成长数据查询失败 [{code}]: {e}")
            return pd.DataFrame()

    # ========================
    # 最新收盘行情 (单只股票)
    # ========================

    def get_latest_price(self, code: str) -> dict:
        """
        获取单只股票最新收盘行情。

        Returns:
            {"price": float, "peTTM": float, "pbMRQ": float, "psTTM": float,
             "turn": float, "volume": float, "amount": float}
            失败返回 {"price": 0.0}
        """
        df = self.get_k_data(
            code,
            days_back=10,
            frequency="d",
            fields="date,close,volume,amount,turn,peTTM,pbMRQ,psTTM",
        )
        if df.empty:
            return {"price": 0.0}

        latest = df.iloc[-1]
        return {
            "price": float(latest.get("close", 0) or 0),
            "peTTM": float(latest.get("peTTM", 0) or 0),
            "pbMRQ": float(latest.get("pbMRQ", 0) or 0),
            "psTTM": float(latest.get("psTTM", 0) or 0),
            "turn": float(latest.get("turn", 0) or 0),
            "volume": float(latest.get("volume", 0) or 0),
            "amount": float(latest.get("amount", 0) or 0),
        }
