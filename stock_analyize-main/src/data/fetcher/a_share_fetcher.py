"""
src/data/fetcher/a_share_fetcher.py — A股专属数据获取器

数据源：
  - 通达信 (pytdx)：实时行情、总股本
  - akshare：财务摘要、历史估值（百度接口）

迁移自：根目录 data_fetcher.py 的 AStockDataFetcher 类
修复：移除残留的 yfinance import
"""
import akshare as ak
import pandas as pd
from pytdx.hq import TdxHq_API

from src.utils.exception_handler import NetworkError, retry
from src.utils.logger import get_logger

logger = get_logger()


class AStockDataFetcher:
    """
    A股数据获取器（pytdx + akshare）

    接口规范（与 HK/US Fetcher 保持一致）：
      - get_current_market_data(code) -> {"price": float, "market_cap": float}
      - get_financial_abstract(code) -> pd.DataFrame
      - get_historical_valuation(code, val_type) -> pd.DataFrame
    """

    SERVERS = [
        ('119.147.212.81', 7709),
        ('114.80.80.222', 7709),
        ('180.153.18.170', 7709),
    ]

    def __init__(self):
        self.market_name = "A股"
        self.api = TdxHq_API()
        self._connect_tdx()
        logger.info(f"初始化【{self.market_name}】数据获取器")

    def _connect_tdx(self) -> None:
        """连接通达信行情服务器（按优先级遍历直到成功）"""
        for ip, port in self.SERVERS:
            if self.api.connect(ip, port):
                logger.info(f"通达信行情服务器连接成功: {ip}:{port}")
                return
            logger.debug(f"通达信服务器连接失败: {ip}:{port}，尝试下一个...")
        raise NetworkError("所有通达信服务器连接失败，请检查网络或 IP 可用性")

    def _get_tdx_market_code(self, code: str) -> int:
        """
        获取通达信市场代码。
        - 1: 上海（600, 688 开头）
        - 0: 深圳（000, 002, 300 开头）
        """
        return 1 if str(code).startswith('6') else 0

    @retry(max_attempts=3, delay=1.0)
    def get_current_market_data(self, code: str) -> dict:
        """
        获取 A股实时行情与总市值。

        Returns:
            {"price": float, "market_cap": float}，失败返回零值
        """
        market = self._get_tdx_market_code(code)
        try:
            quotes = self.api.get_security_quotes([(market, code)])
            if not quotes:
                logger.warning(f"无法获取 {code} 的通达信实时报价")
                return {"price": 0.0, "market_cap": 0.0}

            price = quotes[0]['price']

            finance_info = self.api.get_finance_info(market, code)
            total_shares = finance_info.get('zongguben', 0) if finance_info else 0
            market_cap = price * total_shares

            logger.debug(f"A股 {code} 实时行情: 股价={price}, 总市值={market_cap:.0f}")
            return {"price": float(price), "market_cap": float(market_cap)}
        except Exception as e:
            logger.error(f"通达信获取 {code} 实时行情失败: {e}")
            return {"price": 0.0, "market_cap": 0.0}

    @retry(max_attempts=3, delay=2.0)
    def get_financial_abstract(self, code: str) -> pd.DataFrame:
        """获取 A股财务摘要（akshare）"""
        try:
            df = ak.stock_financial_abstract(symbol=code)
            logger.debug(f"A股 {code} 财务摘要获取成功，shape={df.shape}")
            return df
        except Exception as e:
            logger.error(f"获取 A股 {code} 财务数据失败: {e}")
            return pd.DataFrame()

    @retry(max_attempts=3, delay=2.0)
    def get_historical_valuation(self, code: str, val_type: str = 'pe') -> pd.DataFrame:
        """
        获取 A股历史估值走势（akshare 百度接口）。

        Args:
            val_type: 'pe'（市盈率）或 'ps'（市净率替代）

        Returns:
            包含 trade_date 和 pe_ttm/ps_ttm 列的 DataFrame
        """
        indicator_map = {'pe': '市盈率(TTM)', 'ps': '市净率'}
        indicator = indicator_map.get(val_type, '市盈率(TTM)')
        try:
            df = ak.stock_zh_valuation_baidu(symbol=code, indicator=indicator, period="近五年")
            if df is not None and not df.empty:
                val_col = 'pe_ttm' if val_type == 'pe' else 'ps_ttm'
                df = df.rename(columns={'date': 'trade_date', 'value': val_col})
                logger.debug(f"A股 {code} 历史估值获取成功 ({indicator})，共 {len(df)} 条")
                return df
            logger.warning(f"A股 {code} 历史估值数据为空 ({indicator})")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"获取 A股 {code} 历史估值失败: {e}")
            return pd.DataFrame()

    def __del__(self):
        """安全断开通达信连接"""
        try:
            self.api.disconnect()
        except Exception:
            pass
