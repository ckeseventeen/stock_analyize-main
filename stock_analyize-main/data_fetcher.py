import akshare as ak
import pandas as pd
from abc import ABC, abstractmethod
from pytdx.hq import TdxHq_API
from logger import setup_logger

# 全局日志初始化
logger = setup_logger()



# ---------------------- 抽象基类：定义统一接口规范 ----------------------
class BaseDataFetcher(ABC):
    """
    数据获取器抽象基类
    所有市场子类必须实现以下3个核心方法，保证接口完全统一
    下游Analyzer无需区分市场，直接调用统一方法即可
    """

    def __init__(self, market_name: str):
        self.market_name = market_name
        logger.info(f"初始化【{self.market_name}】数据获取器")

    # ---- 上下文管理器协议：支持 with 语句安全释放资源 ----
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """默认无操作，子类可覆盖以释放连接等资源"""
        return False

    @abstractmethod
    def get_current_market_data(self, code: str) -> dict:
        """
        抽象方法：获取股票实时行情+总市值
        统一返回格式：{"price": 实时股价(float), "market_cap": 总市值(float)}
        失败返回 {"price": 0.0, "market_cap": 0.0}
        """
        pass

    @abstractmethod
    def get_financial_abstract(self, code: str) -> pd.DataFrame:
        """
        抽象方法：获取股票财务摘要
        成功返回DataFrame，失败返回空DataFrame
        """
        pass

    @abstractmethod
    def get_historical_valuation(self, code: str, val_type: str = 'pe') -> pd.DataFrame:
        """
        抽象方法：获取股票历史估值数据
        统一列名：trade_date(日期)、pe_ttm/ps_ttm(估值指标)
        成功返回DataFrame，失败返回空DataFrame
        """
        pass


# ---------------------- A股数据子类：通达信实时行情 + akshare 财务/估值 ----------------------
class AStockDataFetcher(BaseDataFetcher):
    """A股数据获取器：实时行情走 pytdx（通达信），财务和历史估值走 akshare"""

    def __init__(self):
        super().__init__("A股")
        # 通达信行情服务器地址池，按优先级尝试连接
        self.servers = [
            ('119.147.212.81', 7709),
            ('114.80.80.222', 7709),
            ('180.153.18.170', 7709),
        ]
        self.api = TdxHq_API()
        self._connect_tdx()

    def _connect_tdx(self):
        """遍历服务器池，连接第一个可用的通达信行情服务器"""
        connected = False
        for ip, port in self.servers:
            if self.api.connect(ip, port):
                logger.info(f"通达信行情服务器连接成功: {ip}:{port}")
                connected = True
                break
            else:
                logger.debug(f"通达信服务器连接失败: {ip}:{port}，尝试下一个...")
        if not connected:
            raise Exception("所有通达信服务器连接失败，请检查网络或 IP 可用性")

    def _get_tdx_market_code(self, code):
        """通达信市场代码映射：0=深圳(000/002/300)，1=上海(600/688)"""
        if str(code).startswith('6'):
            return 1
        return 0

    def get_current_market_data(self, code: str) -> dict:
        """[pytdx] 获取A股实时股价及总市值"""
        market = self._get_tdx_market_code(code)

        try:
            # 1. 获取盘口实时报价
            quotes = self.api.get_security_quotes([(market, code)])
            if not quotes:
                logger.warning(f"无法获取 {code} 的通达信实时报价")
                return {"price": 0.0, "market_cap": 0.0}

            price = quotes[0]['price']

            # 2. 获取最新财务基本面 (取 zongguben 总股本)
            finance_info = self.api.get_finance_info(market, code)
            if finance_info and 'zongguben' in finance_info:
                # pytdx 返回的 zongguben 单位是 "股"
                total_shares = finance_info['zongguben']
            else:
                total_shares = 0

            market_cap = price * total_shares

            logger.debug(f"{code} 实时行情: 股价={price}, 总市值={market_cap:.0f}")
            return {
                "price": float(price),
                "market_cap": float(market_cap)
            }
        except Exception as e:
            logger.error(f"通达信获取 {code} 实时行情数据失败: {e}")
            return {"price": 0.0, "market_cap": 0.0}

    def get_financial_abstract(self, code: str) -> pd.DataFrame:
        """[akshare] 获取A股财务摘要"""
        try:
            df = ak.stock_financial_abstract(symbol=code)
            logger.debug(f"{code} 财务摘要获取成功，shape={df.shape}")
            return df
        except Exception as e:
            logger.error(f"获取 {code} 财务数据失败: {e}")
            return pd.DataFrame()

    def get_historical_valuation(self, code: str, val_type: str = 'pe') -> pd.DataFrame:
        """[akshare] 获取A股历史估值走势（百度财经数据源）"""
        # 映射估值类型到百度接口的 indicator 参数
        # 注意：百度接口无市销率(PS)，配置 ps 时实际拉取市净率(PB) 作为替代
        indicator_map = {
            'pe': '市盈率(TTM)',
            'ps': '市净率',  # 百度接口无市销率，使用市净率(PB)替代
        }
        indicator = indicator_map.get(val_type, '市盈率(TTM)')

        if val_type == 'ps':
            logger.warning(f"{code} 百度接口不支持市销率(PS)，历史估值数据使用市净率(PB)替代")

        try:
            df = ak.stock_zh_valuation_baidu(symbol=code, indicator=indicator, period="近五年")
            if df is not None and not df.empty:
                # pe 对应 pe_ttm；ps 配置时实际数据为 PB，但列名仍用 ps_ttm 保持下游兼容
                val_col = 'pe_ttm' if val_type == 'pe' else 'ps_ttm'
                df = df.rename(columns={'date': 'trade_date', 'value': val_col})
                logger.debug(f"{code} 历史估值获取成功 ({indicator})，共 {len(df)} 条")
                return df
            else:
                logger.warning(f"{code} 历史估值数据为空 ({indicator})")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"获取 {code} 历史估值失败: {e}")
            return pd.DataFrame()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """通过上下文管理器安全断开通达信连接"""
        try:
            self.api.disconnect()
            logger.debug("通达信连接已安全断开")
        except Exception:
            pass
        return False

    def __del__(self):
        """析构兜底：若未使用 with 语句，尝试断开连接"""
        try:
            self.api.disconnect()
        except Exception:
            pass


# ---------------------- 港股/美股通用基类：抽取 akshare 数据获取共性逻辑 ----------------------
class AkshareDataFetcher(BaseDataFetcher):
    """
    港股/美股数据获取器通用基类，使用 akshare（东方财富+百度）数据源。
    子类只需实现以下钩子方法，指定各市场对应的 akshare API 函数：
      - _fetch_financial_api(symbol) -> DataFrame  获取财务指标
      - _fetch_valuation_api(symbol, indicator, period) -> DataFrame  获取历史估值
      - _fetch_price_df(symbol) -> DataFrame  获取日线行情
      - _get_net_profit_col(df) -> str  获取归母净利润列名
      - _normalize_code(code) -> str  代码格式化（如去掉 .HK 后缀）
    """

    def __init__(self, market_name: str):
        super().__init__(market_name)
        # 缓存总股本，避免同一股票重复请求财务接口
        self._shares_cache = {}

    def _normalize_code(self, code: str) -> str:
        """默认不做处理，港股子类覆盖去除 .HK 后缀"""
        return code

    def _get_net_profit_col(self, df: pd.DataFrame) -> str:
        """获取归母净利润对应的列名，美股可能是 PARENT_HOLDER_NETPROFIT"""
        return 'HOLDER_PROFIT'

    def _get_total_shares(self, code: str) -> float:
        """从财务数据推算总股本：总股本 = 归母净利润 / 每股收益（EPS）"""
        if code in self._shares_cache:
            return self._shares_cache[code]
        symbol = self._normalize_code(code)
        try:
            df = self._fetch_financial_api(symbol)
            if df is not None and not df.empty:
                row = df.iloc[0]
                net_profit_col = self._get_net_profit_col(df)
                net_profit = float(row.get(net_profit_col, 0) or 0)
                eps = float(row.get('BASIC_EPS', 0) or 0)
                if eps != 0:
                    shares = abs(net_profit / eps)
                    self._shares_cache[code] = shares
                    return shares
        except Exception as e:
            logger.debug(f"{self.market_name} {code} 总股本推算失败: {e}")
        return 0.0

    def get_financial_abstract(self, code: str) -> pd.DataFrame:
        """获取财务指标并转换为 analyzer 兼容格式"""
        symbol = self._normalize_code(code)
        try:
            df = self._fetch_financial_api(symbol)
            if df is None or df.empty:
                logger.warning(f"{self.market_name} {code} 财务数据为空")
                return pd.DataFrame()

            # 缓存总股本（从最新一期数据推算）
            row = df.iloc[0]
            net_profit_col = self._get_net_profit_col(df)
            eps = float(row.get('BASIC_EPS', 0) or 0)
            net_profit = float(row.get(net_profit_col, 0) or 0)
            if eps != 0:
                self._shares_cache[code] = abs(net_profit / eps)

            # 转换为与 analyzer 兼容的统一 DataFrame 格式
            result = pd.DataFrame(index=pd.to_datetime(df['REPORT_DATE']))
            result['Total Revenue'] = pd.to_numeric(df['OPERATE_INCOME'].values, errors='coerce')
            result['Net Income Common Stockholders'] = pd.to_numeric(df[net_profit_col].values, errors='coerce')
            gross_profit = pd.to_numeric(df['GROSS_PROFIT'].values, errors='coerce')
            result['Cost Of Revenue'] = result['Total Revenue'].values - gross_profit
            result.sort_index(ascending=False, inplace=True)

            logger.debug(f"{self.market_name} {code} 财务摘要获取成功，shape={result.shape}")
            return result
        except Exception as e:
            logger.error(f"获取{self.market_name} {code} 财务数据失败: {e}")
            return pd.DataFrame()

    def get_historical_valuation(self, code: str, val_type: str = 'pe') -> pd.DataFrame:
        """获取历史估值走势（百度接口）
        注意：百度接口无市销率(PS)，ps 配置时拉取市净率(PB)替代
        """
        symbol = self._normalize_code(code)
        # 百度接口无 PS 指标，ps 时使用 PB 替代
        indicator_map = {'pe': '市盈率(TTM)', 'ps': '市净率'}
        indicator = indicator_map.get(val_type, '市盈率(TTM)')

        try:
            df = self._fetch_valuation_api(symbol, indicator, "近五年")
            if df is None or df.empty:
                logger.warning(f"{self.market_name} {code} 历史估值数据为空")
                return pd.DataFrame()

            val_col = 'pe_ttm' if val_type == 'pe' else 'ps_ttm'
            df = df.rename(columns={'date': 'trade_date', 'value': val_col})
            logger.debug(f"{self.market_name} {code} 历史估值获取成功 ({indicator})，共 {len(df)} 条")
            return df
        except Exception as e:
            logger.error(f"获取{self.market_name} {code} 历史估值失败: {e}")
            return pd.DataFrame()

    def get_current_market_data(self, code: str) -> dict:
        """获取实时行情：价格取日线收盘价，市值从财务数据推算"""
        symbol = self._normalize_code(code)
        price = 0.0
        market_cap = 0.0

        # 1. 价格：akshare 日线最新收盘价
        try:
            df = self._fetch_price_df(symbol)
            if df is not None and not df.empty:
                # 港股日线列名为中文"收盘"，美股为英文"close"
                price_col = '收盘' if '收盘' in df.columns else 'close'
                price = float(df.iloc[-1][price_col])
        except Exception as e:
            logger.debug(f"{self.market_name} {code} akshare 日线获取失败: {e}")

        # 2. 价格兜底：通过 PE * EPS 反推
        if price == 0.0:
            try:
                val_df = self._fetch_valuation_api(symbol, '市盈率(TTM)', "近一年")
                if val_df is not None and not val_df.empty:
                    latest_pe = float(val_df.iloc[-1]['value'])
                    fin_df = self._fetch_financial_api(symbol)
                    if fin_df is not None and not fin_df.empty:
                        eps = float(fin_df.iloc[0].get('BASIC_EPS', 0) or 0)
                        if eps > 0 and latest_pe > 0:
                            price = latest_pe * eps
                            logger.info(f"{self.market_name} {code} 主行情获取失败，使用 PE*EPS 反推价格: {price:.2f}")
            except Exception as e:
                logger.debug(f"{self.market_name} {code} 价格兜底失败: {e}")

        # 3. 市值 = 股价 × 总股本
        total_shares = self._get_total_shares(code)
        if price > 0 and total_shares > 0:
            market_cap = price * total_shares

        logger.debug(f"{self.market_name} {code} 实时行情: 股价={price:.2f}, 总市值={market_cap:.0f}")
        return {"price": price, "market_cap": market_cap}

    # ---- 以下为子类必须实现的钩子方法 ----

    def _fetch_financial_api(self, symbol: str) -> pd.DataFrame:
        """子类实现：调用对应市场的 akshare 财务接口"""
        raise NotImplementedError

    def _fetch_valuation_api(self, symbol: str, indicator: str, period: str) -> pd.DataFrame:
        """子类实现：调用对应市场的 akshare 百度估值接口"""
        raise NotImplementedError

    def _fetch_price_df(self, symbol: str) -> pd.DataFrame:
        """子类实现：调用对应市场的 akshare 日线行情接口"""
        raise NotImplementedError


# ---------------------- 港股数据子类 ----------------------
class HKStockDataFetcher(AkshareDataFetcher):
    """港股数据获取器，使用 akshare（东方财富+百度）数据源"""

    def __init__(self):
        super().__init__("港股")

    def _normalize_code(self, code: str) -> str:
        """去除港股代码后缀：00700.HK -> 00700"""
        return code.split('.')[0]

    def _fetch_financial_api(self, symbol: str) -> pd.DataFrame:
        """调用东方财富港股财务分析指标接口"""
        return ak.stock_financial_hk_analysis_indicator_em(symbol=symbol)

    def _fetch_valuation_api(self, symbol: str, indicator: str, period: str) -> pd.DataFrame:
        """调用百度港股历史估值接口"""
        return ak.stock_hk_valuation_baidu(symbol=symbol, indicator=indicator, period=period)

    def _fetch_price_df(self, symbol: str) -> pd.DataFrame:
        """获取港股近10日日线行情（前复权）"""
        end_date = pd.Timestamp.now().strftime('%Y%m%d')
        start_date = (pd.Timestamp.now() - pd.Timedelta(days=10)).strftime('%Y%m%d')
        return ak.stock_hk_hist(symbol=symbol, period='daily',
                                start_date=start_date, end_date=end_date, adjust='qfq')


# ---------------------- 美股数据子类 ----------------------
class USStockDataFetcher(AkshareDataFetcher):
    """美股数据获取器，使用 akshare（东方财富+百度）数据源"""

    def __init__(self):
        super().__init__("美股")

    def _get_net_profit_col(self, df: pd.DataFrame) -> str:
        """美股归母净利润列名可能为 PARENT_HOLDER_NETPROFIT 或 HOLDER_PROFIT"""
        return 'PARENT_HOLDER_NETPROFIT' if 'PARENT_HOLDER_NETPROFIT' in df.columns else 'HOLDER_PROFIT'

    def _fetch_financial_api(self, symbol: str) -> pd.DataFrame:
        """调用东方财富美股财务分析指标接口"""
        return ak.stock_financial_us_analysis_indicator_em(symbol=symbol)

    def _fetch_valuation_api(self, symbol: str, indicator: str, period: str) -> pd.DataFrame:
        """调用百度美股历史估值接口"""
        return ak.stock_us_valuation_baidu(symbol=symbol, indicator=indicator, period=period)

    def _fetch_price_df(self, symbol: str) -> pd.DataFrame:
        """获取美股全量日线行情（前复权）"""
        return ak.stock_us_daily(symbol=symbol, adjust='qfq')