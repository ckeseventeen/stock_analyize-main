"""
src/data/fetcher/hk_share_fetcher.py — 港股专属数据获取器

数据源：100% akshare（东方财富 + 百度），零 yfinance 依赖
"""
import akshare as ak
import pandas as pd

from src.utils.exception_handler import retry
from src.utils.logger import get_logger

logger = get_logger()


class HKStockDataFetcher:
    """
    港股数据获取器（100% akshare）

    接口规范（与 A股/US Fetcher 保持一致）：
      - get_current_market_data(code) -> {"price": float, "market_cap": float}
      - get_financial_abstract(code) -> pd.DataFrame
      - get_historical_valuation(code, val_type) -> pd.DataFrame
    """

    def __init__(self):
        self.market_name = "港股"
        self._shares_cache: dict[str, float] = {}
        logger.info(f"初始化【{self.market_name}】数据获取器")

    def _pure_code(self, code: str) -> str:
        """提取纯净股票代码：'00700.HK' -> '00700'"""
        return code.split('.')[0]

    def _get_total_shares(self, code: str) -> float:
        """从财务数据推算总股本（归母净利润 / 每股收益）"""
        if code in self._shares_cache:
            return self._shares_cache[code]
        symbol = self._pure_code(code)
        try:
            df = ak.stock_financial_hk_analysis_indicator_em(symbol=symbol)
            if df is not None and not df.empty:
                row = df.iloc[0]
                net_profit = float(row.get('HOLDER_PROFIT', 0) or 0)
                eps = float(row.get('BASIC_EPS', 0) or 0)
                if eps != 0:
                    shares = abs(net_profit / eps)
                    self._shares_cache[code] = shares
                    return shares
        except Exception as e:
            logger.debug(f"港股 {code} 总股本推算失败: {e}")
        return 0.0

    @retry(max_attempts=3, delay=2.0)
    def get_financial_abstract(self, code: str) -> pd.DataFrame:
        """获取港股财务指标（akshare 东方财富）"""
        symbol = self._pure_code(code)
        try:
            df = ak.stock_financial_hk_analysis_indicator_em(symbol=symbol)
            if df is None or df.empty:
                logger.warning(f"港股 {code} 财务数据为空")
                return pd.DataFrame()

            # 缓存总股本
            row = df.iloc[0]
            eps = float(row.get('BASIC_EPS', 0) or 0)
            net_profit = float(row.get('HOLDER_PROFIT', 0) or 0)
            if eps != 0:
                self._shares_cache[code] = abs(net_profit / eps)

            # 转换为下游 analyzer 兼容格式
            result = pd.DataFrame(index=pd.to_datetime(df['REPORT_DATE']))
            result['Total Revenue'] = pd.to_numeric(df['OPERATE_INCOME'].values, errors='coerce')
            result['Net Income Common Stockholders'] = pd.to_numeric(df['HOLDER_PROFIT'].values, errors='coerce')
            gross_profit = pd.to_numeric(df['GROSS_PROFIT'].values, errors='coerce')
            result['Cost Of Revenue'] = result['Total Revenue'].values - gross_profit
            result.sort_index(ascending=False, inplace=True)

            logger.debug(f"港股 {code} 财务摘要获取成功，shape={result.shape}")
            return result
        except Exception as e:
            logger.error(f"获取港股 {code} 财务数据失败: {e}")
            return pd.DataFrame()

    @retry(max_attempts=3, delay=2.0)
    def get_historical_valuation(self, code: str, val_type: str = 'pe') -> pd.DataFrame:
        """获取港股历史估值走势（akshare 百度接口）"""
        symbol = self._pure_code(code)
        indicator_map = {'pe': '市盈率(TTM)', 'ps': '市净率'}
        indicator = indicator_map.get(val_type, '市盈率(TTM)')
        try:
            df = ak.stock_hk_valuation_baidu(symbol=symbol, indicator=indicator, period="近五年")
            if df is None or df.empty:
                logger.warning(f"港股 {code} 历史估值数据为空")
                return pd.DataFrame()
            val_col = 'pe_ttm' if val_type == 'pe' else 'ps_ttm'
            df = df.rename(columns={'date': 'trade_date', 'value': val_col})
            logger.debug(f"港股 {code} 历史估值获取成功 ({indicator})，共 {len(df)} 条")
            return df
        except Exception as e:
            logger.error(f"获取港股 {code} 历史估值失败: {e}")
            return pd.DataFrame()

    @retry(max_attempts=3, delay=2.0)
    def get_current_market_data(self, code: str) -> dict:
        """获取港股实时行情（价格 + 总市值）"""
        symbol = self._pure_code(code)
        price = 0.0

        # 1. akshare 日线最新收盘价
        try:
            end_date = pd.Timestamp.now().strftime('%Y%m%d')
            start_date = (pd.Timestamp.now() - pd.Timedelta(days=10)).strftime('%Y%m%d')
            df = ak.stock_hk_hist(symbol=symbol, period='daily',
                                  start_date=start_date, end_date=end_date, adjust='qfq')
            if df is not None and not df.empty:
                price = float(df.iloc[-1]['收盘'])
        except Exception as e:
            logger.debug(f"港股 {code} akshare日线获取失败: {e}")

        # 2. 兜底：PE * EPS 反推
        if price == 0.0:
            try:
                val_df = ak.stock_hk_valuation_baidu(symbol=symbol, indicator='市盈率(TTM)', period="近一年")
                if val_df is not None and not val_df.empty:
                    latest_pe = float(val_df.iloc[-1]['value'])
                    fin_df = ak.stock_financial_hk_analysis_indicator_em(symbol=symbol)
                    eps = float(fin_df.iloc[0].get('BASIC_EPS', 0) or 0)
                    if eps > 0 and latest_pe > 0:
                        price = latest_pe * eps
            except Exception as e:
                logger.debug(f"港股 {code} 价格兜底失败: {e}")

        # 3. 总市值
        total_shares = self._get_total_shares(code)
        market_cap = price * total_shares if price > 0 and total_shares > 0 else 0.0

        logger.debug(f"港股 {code} 实时行情: 股价={price:.2f}, 总市值={market_cap:.0f}")
        return {"price": price, "market_cap": market_cap}
