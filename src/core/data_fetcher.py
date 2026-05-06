from abc import ABC, abstractmethod

import akshare as ak
import pandas as pd

from src.data.providers.baostock_provider import BaostockProvider
from src.utils.logger import setup_logger

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


# ---------------------- A股数据子类：Baostock 为主 + 通达信 fallback ----------------------
class AStockDataFetcher(BaseDataFetcher):
    """
    A股数据获取器

    数据源优先级（不使用 akshare：eastmoney/雪球 线路经常不可达）：
      - 行情: Baostock 最新收盘 (主) → 通达信 pytdx 实时 (备用)
      - 财务摘要: Baostock 盈利数据 (主) → 通达信 finance_info 推导 (备用)
      - 历史估值: Baostock K线含 peTTM/pbMRQ/psTTM (唯一可靠源)
    """

    # 通达信行情服务器地址池（备用数据源）
    TDX_SERVERS = [
        ('119.147.212.81', 7709),
        ('114.80.80.222', 7709),
        ('180.153.18.170', 7709),
    ]

    def __init__(self):
        super().__init__("A股")
        # Baostock 按需登录，无需在 __init__ 建立持久连接
        # 通达信仅作为末端备用，按需延迟连接
        self._tdx_api = None

    def get_current_market_data(self, code: str) -> dict:
        """
        获取A股最新行情与市值。

        优先级：Baostock 最新收盘价 + 总股本 → 通达信 pytdx 实时行情+市值
        """
        # 1. Baostock：最新收盘行情 + Baostock 盈利数据取总股本
        try:
            with BaostockProvider() as bp:
                data = bp.get_latest_price(code)
                price = data.get("price", 0.0)
                if price > 0:
                    market_cap = 0.0
                    profit_df = bp.get_profit_data(code)
                    if profit_df is not None and not profit_df.empty:
                        total_share = pd.to_numeric(
                            profit_df.iloc[0].get("totalShare", 0), errors="coerce"
                        ) or 0.0
                        if total_share > 0:
                            market_cap = price * total_share
                    logger.debug(f"A股 {code} Baostock行情: 价格={price:.2f}, 市值={market_cap:.0f}")
                    if market_cap > 0:
                        return {"price": price, "market_cap": market_cap}
                    # 市值推算失败，继续走 TDX 备用
        except Exception as e:
            logger.debug(f"A股 {code} Baostock行情失败: {e}")

        # 2. 通达信 pytdx 末端备用（实时行情+总市值）
        try:
            price, market_cap = self._try_tdx(code)
            if price > 0:
                return {"price": price, "market_cap": market_cap}
        except Exception as e:
            logger.debug(f"A股 {code} 通达信备用也失败: {e}")

        logger.warning(f"A股 {code} 所有行情源均失败")
        return {"price": 0.0, "market_cap": 0.0}

    def _try_tdx(self, code: str) -> tuple:
        """
        通达信 pytdx 备用行情获取。
        延迟连接：首次调用时才尝试连接通达信服务器。

        Returns:
            (price, market_cap) 元组
        """
        from pytdx.hq import TdxHq_API

        if self._tdx_api is None:
            self._tdx_api = TdxHq_API()
            connected = False
            for ip, port in self.TDX_SERVERS:
                if self._tdx_api.connect(ip, port):
                    logger.info(f"通达信备用连接成功: {ip}:{port}")
                    connected = True
                    break
            if not connected:
                self._tdx_api = None
                raise ConnectionError("所有通达信服务器连接失败")

        market = 1 if str(code).startswith('6') else 0
        quotes = self._tdx_api.get_security_quotes([(market, code)])
        if not quotes:
            return (0.0, 0.0)

        price = float(quotes[0]['price'])
        finance_info = self._tdx_api.get_finance_info(market, code)
        total_shares = finance_info.get('zongguben', 0) if finance_info else 0
        market_cap = price * total_shares

        logger.debug(f"A股 {code} 通达信备用行情: 价格={price}, 总市值={market_cap:.0f}")
        return (price, market_cap)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """安全断开通达信备用连接（如果建立过）"""
        if self._tdx_api is not None:
            try:
                self._tdx_api.disconnect()
            except Exception:
                pass
        return False

    def __del__(self):
        """析构兜底"""
        if self._tdx_api is not None:
            try:
                self._tdx_api.disconnect()
            except Exception:
                pass

    def get_financial_abstract(self, code: str) -> pd.DataFrame:
        """
        获取A股财务摘要（Baostock 单一源）。

        Baostock 返回每期一行（statDate, netProfit, MBRevenue, gpMargin, totalShare, ...）。
        此处转换为 AStockAnalyzer 期望的"指标×日期"透视格式：
          - index: 指标名（归母净利润 / 营业总收入 / 营业成本）
          - 列: 指标 + 各报告期日期（YYYYMMDD 字符串）
          - 值: 元单位数值
        """
        try:
            with BaostockProvider() as bp:
                hist = bp.get_profit_history(code, num_years=5)
        except Exception as e:
            logger.warning(f"A股 {code} Baostock profit_history 查询异常: {e}")
            return pd.DataFrame()

        if hist is None or hist.empty:
            logger.warning(f"A股 {code} Baostock 财务数据为空")
            return pd.DataFrame()

        # 按日期构造列
        date_cols: list[str] = []
        metric_rows: dict[str, dict[str, float]] = {
            '归母净利润': {},
            '营业总收入': {},
            '营业成本': {},
        }
        for _, row in hist.iterrows():
            stat_date = str(row.get('statDate', '')).strip()
            if not stat_date:
                continue
            # analyzer 支持 'YYYY-MM-DD' 和 'YYYYMMDD' 两种日期列名，此处用 'YYYYMMDD' 更兼容
            date_key = stat_date.replace('-', '')
            date_cols.append(date_key)

            net_profit = float(row.get('netProfit', 0) or 0)
            revenue = float(row.get('MBRevenue', 0) or 0)
            gp_margin = float(row.get('gpMargin', 0) or 0)  # decimal, e.g. 0.913
            cost = revenue * (1.0 - gp_margin) if revenue > 0 else 0.0

            metric_rows['归母净利润'][date_key] = net_profit
            metric_rows['营业总收入'][date_key] = revenue
            metric_rows['营业成本'][date_key] = cost

        if not date_cols:
            return pd.DataFrame()

        # 构造 akshare 风格的 DataFrame（列: 指标 + 日期列；分析器会 set_index('指标').T）
        frame_rows: list[dict] = []
        for metric, by_date in metric_rows.items():
            entry = {'指标': metric}
            for d in date_cols:
                entry[d] = by_date.get(d, 0.0)
            frame_rows.append(entry)
        df_out = pd.DataFrame(frame_rows)
        logger.debug(f"A股 {code} Baostock 财务摘要构造完成: {len(date_cols)} 期")
        return df_out

    def get_historical_valuation(self, code: str, val_type: str = 'pe') -> pd.DataFrame:
        """
        获取A股历史估值走势（Baostock 单一源）。
        从日 K 线直接提取 peTTM / pbMRQ / psTTM。
        """
        try:
            with BaostockProvider() as bp:
                df = bp.get_valuation_history(code, val_type=val_type)
                if df is not None and not df.empty:
                    logger.debug(f"A股 {code} Baostock 历史估值获取成功 ({val_type})，共 {len(df)} 条")
                    return df
        except Exception as e:
            logger.warning(f"A股 {code} Baostock 历史估值失败: {e}")

        logger.warning(f"A股 {code} 历史估值数据获取失败")
        return pd.DataFrame()


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
        """获取历史估值走势（百度接口 → yfinance fallback）
        注意：百度接口无市销率(PS)，ps 配置时拉取市净率(PB)替代
        """
        symbol = self._normalize_code(code)
        # 百度接口无 PS 指标，ps 时使用 PB 替代
        indicator_map = {'pe': '市盈率(TTM)', 'ps': '市净率'}
        indicator = indicator_map.get(val_type, '市盈率(TTM)')

        try:
            df = self._fetch_valuation_api(symbol, indicator, "近五年")
            if df is not None and not df.empty:
                val_col = 'pe_ttm' if val_type == 'pe' else 'ps_ttm'
                df = df.rename(columns={'date': 'trade_date', 'value': val_col})
                logger.debug(f"{self.market_name} {code} 历史估值获取成功 ({indicator})，共 {len(df)} 条")
                return df
        except Exception as e:
            logger.debug(f"{self.market_name} {code} 百度估值接口失败: {e}")

        # yfinance fallback：用 Close / trailingEps 计算历史 PE
        try:
            yf_symbol = self._to_yfinance_symbol(code)
            import yfinance as yf
            ticker = yf.Ticker(yf_symbol)
            info = ticker.info or {}

            # 策略：优先取 trailingEps，若无则尝试用 regularMarketPrice / trailingPE 反推
            eps = float(info.get('trailingEps', 0) or 0)
            if eps <= 0:
                price = float(info.get('currentPrice', 0) or info.get('regularMarketPrice', 0) or 0)
                pe = float(info.get('trailingPE', 0) or 0)
                if price > 0 and pe > 0:
                    eps = price / pe

            # 若仍无 EPS，尝试从已抓取的财务摘要中获取最新一期 BASIC_EPS
            if eps <= 0:
                fin_df = self.get_financial_abstract(code)
                if not fin_df.empty and 'BASIC_EPS' in fin_df.columns:
                    eps = float(fin_df.iloc[0]['BASIC_EPS'] or 0)

            if eps > 0:
                hist = ticker.history(period='5y')
                if hist is not None and not hist.empty:
                    val_col = 'pe_ttm' if val_type == 'pe' else 'ps_ttm'
                    result = pd.DataFrame({
                        'trade_date': hist.index.strftime('%Y-%m-%d'),
                        val_col: hist['Close'].values / eps,
                    })
                    # 过滤无效值
                    result[val_col] = pd.to_numeric(result[val_col], errors='coerce')
                    result = result.dropna(subset=[val_col])
                    result = result[result[val_col] > 0]
                    if not result.empty:
                        logger.info(f"{self.market_name} {code} yfinance 历史PE获取成功 (Symbol={yf_symbol}, EPS={eps:.2f})，共 {len(result)} 条")
                        return result
            else:
                logger.debug(f"{self.market_name} {code} yfinance 无法获取有效 EPS (Symbol={yf_symbol})")
        except Exception as e:
            logger.debug(f"{self.market_name} {code} yfinance 历史估值也失败: {e}")

        logger.warning(f"{self.market_name} {code} 所有历史估值源均失败")
        return pd.DataFrame()

    def get_current_market_data(self, code: str) -> dict:
        """获取实时行情：价格取日线收盘价，市值从财务数据推算（akshare → yfinance fallback）"""
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

        # 2. 价格兜底 A：通过 PE * EPS 反推
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

        # 3. 价格兜底 B：yfinance
        if price == 0.0:
            try:
                yf_symbol = self._to_yfinance_symbol(code)
                import yfinance as yf
                info = yf.Ticker(yf_symbol).info or {}
                price = float(info.get('currentPrice', 0) or info.get('regularMarketPrice', 0) or 0)
                market_cap = float(info.get('marketCap', 0) or 0)
                if price > 0:
                    logger.info(f"{self.market_name} {code} akshare 不可用，yfinance 行情获取成功: 价格={price:.2f}, 市值={market_cap:.0f}")
                    return {"price": price, "market_cap": market_cap}
            except Exception as e:
                logger.debug(f"{self.market_name} {code} yfinance 行情也失败: {e}")

        # 4. 市值 = 股价 × 总股本
        total_shares = self._get_total_shares(code)
        if price > 0 and total_shares > 0:
            market_cap = price * total_shares

        logger.debug(f"{self.market_name} {code} 实时行情: 股价={price:.2f}, 总市值={market_cap:.0f}")
        return {"price": price, "market_cap": market_cap}

    def _to_yfinance_symbol(self, code: str) -> str:
        """将代码转换为 yfinance 格式。子类可覆盖。"""
        return code

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
    """港股数据获取器，使用 akshare（东方财富+百度）数据源，yfinance 兜底"""

    def __init__(self):
        super().__init__("港股")

    def _normalize_code(self, code: str) -> str:
        """去除港股代码后缀：00700.HK -> 00700"""
        return code.split('.')[0]

    def _to_yfinance_symbol(self, code: str) -> str:
        """港股 yfinance 格式：保证至少 4 位数字
        00700.HK -> 0700.HK
        09988.HK -> 9988.HK
        """
        pure = self._normalize_code(code).lstrip('0') or '0'
        return f"{pure.zfill(4)}.HK"

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
    """美股数据获取器，使用 akshare（东方财富+百度）数据源，yfinance 兜底"""

    def __init__(self):
        super().__init__("美股")

    def _get_net_profit_col(self, df: pd.DataFrame) -> str:
        """美股归母净利润列名可能为 PARENT_HOLDER_NETPROFIT 或 HOLDER_PROFIT"""
        return 'PARENT_HOLDER_NETPROFIT' if 'PARENT_HOLDER_NETPROFIT' in df.columns else 'HOLDER_PROFIT'

    def _to_yfinance_symbol(self, code: str) -> str:
        """美股 yfinance 格式与原始代码相同"""
        return code

    def _fetch_financial_api(self, symbol: str) -> pd.DataFrame:
        """调用东方财富美股财务分析指标接口"""
        return ak.stock_financial_us_analysis_indicator_em(symbol=symbol)

    def _fetch_valuation_api(self, symbol: str, indicator: str, period: str) -> pd.DataFrame:
        """调用百度美股历史估值接口"""
        return ak.stock_us_valuation_baidu(symbol=symbol, indicator=indicator, period=period)

    def _fetch_price_df(self, symbol: str) -> pd.DataFrame:
        """获取美股全量日线行情（前复权）"""
        return ak.stock_us_daily(symbol=symbol, adjust='qfq')
