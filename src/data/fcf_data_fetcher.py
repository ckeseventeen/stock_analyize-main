import akshare as ak
import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)

class FCFDataFetcher:
    """
    FCF (Free Cash Flow) 数据获取器
    支持 A股 (通过 akshare 东方财富接口) 及 港美股 (通过 yfinance)。
    主要获取四个指标：
    - 经营活动产生的现金流量净额 (Operating Cash Flow)
    - 资本支出 (Capital Expenditure)
    - 营业收入 (Total Revenue)
    - 归母净利润 (Net Income)
    """

    @staticmethod
    def fetch_a_share(code: str, is_annual: bool = True) -> pd.DataFrame:
        """
        获取 A 股 FCF 所需数据。
        """
        # 判断 SH/SZ/BJ 前缀
        code_str = str(code).split('.')[-1]  # 去除可能存在的 sh./sz. 等前缀
        if code_str.startswith(('6', '9')):
            em_symbol = f"SH{code_str}"
        elif code_str.startswith(('4', '8')):
            em_symbol = f"BJ{code_str}"
        else:
            em_symbol = f"SZ{code_str}"

        try:
            logger.info(f"正在获取 A 股 {em_symbol} 现金流量表...")
            cf_df = ak.stock_cash_flow_sheet_by_report_em(symbol=em_symbol)
            logger.info(f"正在获取 A 股 {em_symbol} 利润表...")
            inc_df = ak.stock_profit_sheet_by_report_em(symbol=em_symbol)
        except Exception as e:
            logger.error(f"获取 A 股 {code} FCF 报表失败: {e}")
            return pd.DataFrame()

        if cf_df is None or cf_df.empty or inc_df is None or inc_df.empty:
            logger.warning(f"A 股 {code} FCF 数据为空")
            return pd.DataFrame()

        # 筛选年度报告或季度报告
        if is_annual:
            cf_df = cf_df[cf_df['REPORT_DATE_NAME'].str.contains("年报", na=False)]
            inc_df = inc_df[inc_df['REPORT_DATE_NAME'].str.contains("年报", na=False)]

        # 提取关键字段
        # 现金流字段：NETCASH_OPERATE (经营现金流净额), CONSTRUCT_LONG_ASSET (购建固定资产支付的现金)
        # 利润表字段：TOTAL_OPERATE_INCOME (营业总收入), PARENT_NETPROFIT (归属于母公司所有者的净利润)
        cf_subset = cf_df[['REPORT_DATE', 'NETCASH_OPERATE', 'CONSTRUCT_LONG_ASSET']].copy()
        inc_subset = inc_df[['REPORT_DATE', 'TOTAL_OPERATE_INCOME', 'PARENT_NETPROFIT']].copy()

        # 某些财务数据可能由于报表类型不同而使用不同字段名（如营业总收入 vs 营业收入）
        if 'TOTAL_OPERATE_INCOME' not in inc_df.columns and 'OPERATE_INCOME' in inc_df.columns:
            inc_subset['TOTAL_OPERATE_INCOME'] = inc_df['OPERATE_INCOME']

        # 合并数据
        merged = pd.merge(cf_subset, inc_subset, on='REPORT_DATE', how='inner')
        merged['REPORT_DATE'] = pd.to_datetime(merged['REPORT_DATE'])
        merged = merged.sort_values('REPORT_DATE', ascending=True).reset_index(drop=True)

        result = pd.DataFrame()
        result['date'] = merged['REPORT_DATE']
        result['operating_cash_flow'] = pd.to_numeric(merged['NETCASH_OPERATE'], errors='coerce').fillna(0)
        # 资本支出通常是支付金额，这里统一为绝对值，方便后续用 经营现金流 - 资本支出 计算
        result['capex'] = pd.to_numeric(merged['CONSTRUCT_LONG_ASSET'], errors='coerce').abs().fillna(0)
        result['revenue'] = pd.to_numeric(merged['TOTAL_OPERATE_INCOME'], errors='coerce').fillna(0)
        result['net_profit'] = pd.to_numeric(merged['PARENT_NETPROFIT'], errors='coerce').fillna(0)

        # 转换为日期作为索引
        result.set_index('date', inplace=True)
        return result

    @staticmethod
    def fetch_yfinance_share(yf_symbol: str, is_annual: bool = True) -> pd.DataFrame:
        """
        通过 yfinance 获取港股/美股的 FCF 数据。
        """
        # 走带重试的统一客户端：Yahoo 限流时会返回空表且不抛异常，
        # 直接当"无数据"会把限流误报成"新股/代码不存在"（线上实测同一标的
        # 首次全空、隔几秒重试就 4 年齐全）
        from src.data.providers.yfinance_client import fetch_statements

        logger.info(f"正在获取 {yf_symbol} yfinance 报表...")
        res = fetch_statements(yf_symbol, is_annual)
        if not res.is_ok:
            logger.warning(f"{yf_symbol} yfinance FCF 取数未成功: {res.reason}")
            empty = pd.DataFrame()
            # 让上层能区分"数据源故障（可重试）"与"确实无记录"
            empty.attrs["fetch_status"] = res.status.value
            empty.attrs["fetch_reason"] = res.reason
            return empty
        cf, inc = res.data

        # 合并基于索引（Date）
        merged = pd.concat([cf, inc], axis=1, join='inner')
        merged = merged.sort_index(ascending=True)

        result = pd.DataFrame()
        result.index = merged.index

        # Operating Cash Flow
        ocf_col = 'Operating Cash Flow'
        if ocf_col in merged.columns:
            result['operating_cash_flow'] = pd.to_numeric(merged[ocf_col], errors='coerce').fillna(0)
        else:
            result['operating_cash_flow'] = 0.0

        # Capital Expenditure (yfinance 中该值通常为负数，因此取绝对值)
        capex_col = 'Capital Expenditure'
        if capex_col in merged.columns:
            result['capex'] = pd.to_numeric(merged[capex_col], errors='coerce').abs().fillna(0)
        else:
            result['capex'] = 0.0

        # Total Revenue
        rev_col = 'Total Revenue' if 'Total Revenue' in merged.columns else 'Operating Revenue'
        if rev_col in merged.columns:
            result['revenue'] = pd.to_numeric(merged[rev_col], errors='coerce').fillna(0)
        else:
            result['revenue'] = 0.0

        # Net Income
        net_inc_col = 'Net Income Common Stockholders'
        if net_inc_col not in merged.columns:
            net_inc_col = 'Net Income'
        if net_inc_col in merged.columns:
            result['net_profit'] = pd.to_numeric(merged[net_inc_col], errors='coerce').fillna(0)
        else:
            result['net_profit'] = 0.0

        return result

    @classmethod
    def fetch(cls, market: str, code: str, is_annual: bool = True) -> pd.DataFrame:
        """
        统一入口（PR 1：用 market_registry 派发，加新市场只需注册一次）。
        """
        from src.core.market_registry import get_market

        try:
            spec = get_market(market)
        except KeyError as e:
            raise ValueError(f"不支持的市场类型: {market}") from e

        if spec.key == "a":
            return cls.fetch_a_share(code, is_annual)
        if spec.yfinance_suffix == ".HK":
            # 港股：00700 → 0700.HK（按市场注册的 pad_width + suffix）
            code_pure = str(code).split('.')[0].lstrip('0') or '0'
            yf_symbol = f"{code_pure.zfill(4)}{spec.yfinance_suffix}"
            return cls.fetch_yfinance_share(yf_symbol, is_annual)
        if spec.key == "us":
            return cls.fetch_yfinance_share(code, is_annual)
        # 未来新市场：默认走 yfinance + suffix
        suffix = spec.yfinance_suffix
        yf_symbol = f"{str(code).split('.')[0]}{suffix}" if suffix else str(code)
        return cls.fetch_yfinance_share(yf_symbol, is_annual)
