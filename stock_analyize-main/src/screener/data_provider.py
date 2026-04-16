"""
src/screener/data_provider.py — 筛选器数据提供层

批量获取A股实时行情和历史K线数据，供筛选器使用。
使用 CacheManager 缓存，减少重复API请求。
"""
import akshare as ak
import pandas as pd

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

    @retry(max_attempts=3, delay=2.0)
    def get_all_a_shares(self) -> pd.DataFrame:
        """
        获取全A股实时行情数据（约5000只）

        使用 ak.stock_zh_a_spot_em()，一次API调用获取全部。
        返回列包含：代码, 名称, 最新价, 总市值, 流通市值, 市盈率-动态, 市净率, 换手率, 涨跌幅 等。
        """

        def _fetch():
            logger.info("正在拉取全A股实时行情数据...")
            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                logger.info(f"全A股实时行情拉取成功，共 {len(df)} 只股票")
                # 过滤掉 ST 和退市股
                df = df[~df["名称"].str.contains("ST|退市", na=False)]
                # 数值列转换
                for col in ("总市值", "流通市值", "市盈率-动态", "市净率", "换手率", "最新价", "涨跌幅", "振幅"):
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                logger.info(f"过滤ST/退市后剩余 {len(df)} 只")
                return df
            logger.warning("全A股实时行情数据为空")
            return pd.DataFrame()

        return self._cache.get_or_fetch("all_a_shares_spot", _fetch, ttl_hours=self._spot_ttl)

    @retry(max_attempts=2, delay=1.0)
    def get_weekly_ohlcv(self, code: str, days_back: int = 365) -> pd.DataFrame:
        """
        获取单只股票周线OHLCV数据

        Args:
            code: 股票代码（如 "600519"）
            days_back: 向前获取的天数

        Returns:
            包含 日期, 开盘, 最高, 最低, 收盘, 成交量 等列的 DataFrame
        """
        cache_key = f"weekly_{code}_{days_back}"

        def _fetch():
            end_date = pd.Timestamp.now().strftime("%Y%m%d")
            start_date = (pd.Timestamp.now() - pd.Timedelta(days=days_back)).strftime("%Y%m%d")
            try:
                df = ak.stock_zh_a_hist(symbol=code, period="weekly", start_date=start_date, end_date=end_date, adjust="qfq")
                if df is not None and not df.empty:
                    logger.debug(f"{code} 周线数据获取成功，共 {len(df)} 条")
                    return df
            except Exception as e:
                logger.debug(f"{code} 周线数据获取失败: {e}")
            return pd.DataFrame()

        return self._cache.get_or_fetch(cache_key, _fetch, ttl_hours=self._ohlcv_ttl)

    @retry(max_attempts=2, delay=1.0)
    def get_daily_ohlcv(self, code: str, days_back: int = 120) -> pd.DataFrame:
        """
        获取单只股票日线OHLCV数据

        Args:
            code: 股票代码
            days_back: 向前获取的天数
        """
        cache_key = f"daily_{code}_{days_back}"

        def _fetch():
            end_date = pd.Timestamp.now().strftime("%Y%m%d")
            start_date = (pd.Timestamp.now() - pd.Timedelta(days=days_back)).strftime("%Y%m%d")
            try:
                df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
                if df is not None and not df.empty:
                    logger.debug(f"{code} 日线数据获取成功，共 {len(df)} 条")
                    return df
            except Exception as e:
                logger.debug(f"{code} 日线数据获取失败: {e}")
            return pd.DataFrame()

        return self._cache.get_or_fetch(cache_key, _fetch, ttl_hours=self._ohlcv_ttl)
