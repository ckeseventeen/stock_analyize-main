"""
src/screener/data_provider.py — 筛选器数据提供层

批量获取A股实时行情和历史K线数据，供筛选器使用。
使用 CacheManager 缓存，减少重复API请求。

数据源优先级：akshare (主，含总市值) → Baostock (fallback，无总市值)
"""
import akshare as ak
import pandas as pd

from src.data.fetcher.baostock_provider import BaostockProvider
from src.data.fetcher.cache_manager import CacheManager
from src.utils.exception_handler import retry
from src.utils.logger import get_logger

logger = get_logger("screener_data")


class ScreenerDataProvider:
    """
    筛选器数据提供层

    使用示例:
        provider = ScreenerDataProvider()
        all_stocks = provider.get_all_a_shares()      # 全A实时行情
        weekly_df = provider.get_weekly_ohlcv("600519")  # 单只周线
    """

    def __init__(self, cache_dir: str = ".cache/screener", spot_ttl_hours: int = 1, ohlcv_ttl_hours: int = 4):
        self._cache = CacheManager(cache_dir=cache_dir, ttl_hours=ohlcv_ttl_hours)
        self._spot_ttl = spot_ttl_hours
        self._ohlcv_ttl = ohlcv_ttl_hours

    def get_all_a_shares(self) -> pd.DataFrame:
        """
        获取全A股行情数据（约5000只）

        优先级：akshare (一次拉完，含 总市值/PE/PB) → Baostock (逐只拉 K 线，无总市值)
        返回列包含：代码, 名称, 最新价, 总市值, 市盈率-动态, 市净率, 换手率, 涨跌幅 等。
        """

        def _fetch_baostock():
            """通过 Baostock 获取全A行情数据"""
            logger.info("正在通过 Baostock 拉取全A股行情数据...")
            with BaostockProvider() as bp:
                # 1. 获取全市场股票列表
                all_stocks = bp.get_all_stocks()
                if all_stocks.empty:
                    return pd.DataFrame()

                # 过滤只保留 A股（sh.6xxx, sz.0xxx, sz.3xxx），排除指数和B股
                a_codes = all_stocks[
                    all_stocks["code"].str.match(r"^(sh\.6|sz\.0|sz\.3)")
                ]["code"].tolist()
                logger.info(f"Baostock 全市场 {len(all_stocks)} 只，A股过滤后 {len(a_codes)} 只")

                # 2. 批量获取最新一天的K线含估值指标
                today = pd.Timestamp.now().strftime("%Y-%m-%d")
                week_ago = (pd.Timestamp.now() - pd.Timedelta(days=7)).strftime("%Y-%m-%d")

                rows = []
                for bs_code in a_codes:
                    try:
                        df_k = bp.get_k_data(
                            bs_code,
                            start_date=week_ago,
                            end_date=today,
                            frequency="d",
                            fields="date,close,volume,amount,turn,peTTM,pbMRQ,psTTM,isST",
                        )
                        if df_k is not None and not df_k.empty:
                            latest = df_k.iloc[-1]
                            code_name_row = all_stocks[all_stocks["code"] == bs_code]
                            name = code_name_row["code_name"].values[0] if not code_name_row.empty and "code_name" in code_name_row.columns else ""
                            pure_code = bs_code.split(".")[-1]

                            rows.append({
                                "代码": pure_code,
                                "名称": name,
                                "最新价": latest.get("close", 0),
                                "市盈率-动态": latest.get("peTTM", 0),
                                "市净率": latest.get("pbMRQ", 0),
                                "换手率": latest.get("turn", 0),
                                "成交量": latest.get("volume", 0),
                                "成交额": latest.get("amount", 0),
                            })
                    except Exception:
                        continue

                if not rows:
                    return pd.DataFrame()

                result = pd.DataFrame(rows)
                # 数值列转换
                for col in ("最新价", "市盈率-动态", "市净率", "换手率", "成交量", "成交额"):
                    if col in result.columns:
                        result[col] = pd.to_numeric(result[col], errors="coerce")

                # 过滤 ST
                result = result[~result["名称"].str.contains("ST|退市", na=False)]
                logger.info(f"Baostock 全A数据拉取完成，共 {len(result)} 只（过滤ST后）")
                return result

        def _fetch_akshare():
            """akshare fallback"""
            logger.info("正在通过 akshare 拉取全A股实时行情数据...")
            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                logger.info(f"akshare 全A股实时行情拉取成功，共 {len(df)} 只股票")
                df = df[~df["名称"].str.contains("ST|退市", na=False)]
                for col in ("总市值", "流通市值", "市盈率-动态", "市净率", "换手率", "最新价", "涨跌幅", "振幅"):
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                logger.info(f"过滤ST/退市后剩余 {len(df)} 只")
                return df
            logger.warning("akshare 全A股实时行情数据为空")
            return pd.DataFrame()

        def _fetch():
            # 优先 akshare（一次调用含总市值/PE/PB/换手率，完全覆盖 Spot 条件）
            try:
                df = _fetch_akshare()
                if df is not None and not df.empty:
                    return df
            except Exception as e:
                logger.warning(f"akshare 全A数据拉取失败: {e}")

            # fallback 到 Baostock（逐只拉 K 线，无总市值；仅支持 PE/PB/换手率条件）
            try:
                logger.warning("akshare 失败，降级到 Baostock（不含总市值列，market_cap 条件将被跳过）")
                return _fetch_baostock()
            except Exception as e:
                logger.error(f"Baostock fallback 也失败: {e}")
                return pd.DataFrame()

        return self._cache.get_or_fetch("all_a_shares_spot", _fetch, ttl_hours=self._spot_ttl)

    def get_weekly_ohlcv(self, code: str, days_back: int = 365) -> pd.DataFrame:
        """
        获取单只股票周线OHLCV数据

        优先级：Baostock → akshare fallback

        Args:
            code: 股票代码（如 "600519"）
            days_back: 向前获取的天数

        Returns:
            包含 日期, 开盘, 最高, 最低, 收盘, 成交量 等列的 DataFrame
        """
        cache_key = f"weekly_{code}_{days_back}"

        def _fetch():
            # 1. Baostock
            try:
                with BaostockProvider() as bp:
                    df = bp.get_k_data(code, days_back=days_back, frequency="w")
                    if df is not None and not df.empty:
                        # 统一列名与 akshare 兼容
                        col_map = {"date": "日期", "open": "开盘", "high": "最高",
                                   "low": "最低", "close": "收盘", "volume": "成交量"}
                        df = df.rename(columns=col_map)
                        logger.debug(f"{code} Baostock周线获取成功，共 {len(df)} 条")
                        return df
            except Exception as e:
                logger.debug(f"{code} Baostock周线失败: {e}")

            # 2. akshare fallback
            end_date = pd.Timestamp.now().strftime("%Y%m%d")
            start_date = (pd.Timestamp.now() - pd.Timedelta(days=days_back)).strftime("%Y%m%d")
            try:
                df = ak.stock_zh_a_hist(symbol=code, period="weekly", start_date=start_date, end_date=end_date, adjust="qfq")
                if df is not None and not df.empty:
                    logger.debug(f"{code} akshare周线数据获取成功，共 {len(df)} 条")
                    return df
            except Exception as e:
                logger.debug(f"{code} akshare周线数据获取失败: {e}")
            return pd.DataFrame()

        return self._cache.get_or_fetch(cache_key, _fetch, ttl_hours=self._ohlcv_ttl)

    def get_daily_ohlcv(self, code: str, days_back: int = 120) -> pd.DataFrame:
        """
        获取单只股票日线OHLCV数据

        优先级：Baostock → akshare fallback

        Args:
            code: 股票代码
            days_back: 向前获取的天数
        """
        cache_key = f"daily_{code}_{days_back}"

        def _fetch():
            # 1. Baostock
            try:
                with BaostockProvider() as bp:
                    df = bp.get_k_data(code, days_back=days_back, frequency="d")
                    if df is not None and not df.empty:
                        col_map = {"date": "日期", "open": "开盘", "high": "最高",
                                   "low": "最低", "close": "收盘", "volume": "成交量"}
                        df = df.rename(columns=col_map)
                        logger.debug(f"{code} Baostock日线获取成功，共 {len(df)} 条")
                        return df
            except Exception as e:
                logger.debug(f"{code} Baostock日线失败: {e}")

            # 2. akshare fallback
            end_date = pd.Timestamp.now().strftime("%Y%m%d")
            start_date = (pd.Timestamp.now() - pd.Timedelta(days=days_back)).strftime("%Y%m%d")
            try:
                df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
                if df is not None and not df.empty:
                    logger.debug(f"{code} akshare日线数据获取成功，共 {len(df)} 条")
                    return df
            except Exception as e:
                logger.debug(f"{code} akshare日线数据获取失败: {e}")
            return pd.DataFrame()

        return self._cache.get_or_fetch(cache_key, _fetch, ttl_hours=self._ohlcv_ttl)

