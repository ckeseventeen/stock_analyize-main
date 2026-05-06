import re
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd

from src.utils.logger import setup_logger

logger = setup_logger()


# ---------------------- 抽象基类：封装全市场通用核心逻辑 ----------------------
class BaseAnalyzer(ABC):
    """
    估值分析器抽象基类
    封装全市场通用的 TTM 计算、估值区间、历史分位数、结果组装逻辑。
    子类仅需实现 _clean_financial_data() 处理各市场差异化的财务数据格式。
    """

    def __init__(self, raw_fin, hist_val, market_data, stock_config):
        self.raw_fin = raw_fin.copy()
        # 深拷贝历史估值数据，防止 process() 中 set_index 等操作污染原始数据
        self.hist_val = hist_val.copy() if hist_val is not None else pd.DataFrame()
        self.market_data = market_data
        self.config = stock_config
        self.market_name = self.config.get("market_name", self.config.get("market", "未知市场"))
        self.stock_name = self.config.get("name", "未知股票")
        self.code = self.config.get("code", "未知代码")
        self.category_name = self.config.get("category_name", "未分类")

    @abstractmethod
    def _clean_financial_data(self) -> pd.DataFrame:
        """
        抽象方法：子类实现差异化财务数据清洗
        【强制要求】返回统一格式的DataFrame：
          - 索引：pd.Datetime格式的财报日期，降序排列（最新日期在最前）
          - 列名：必须包含 归母净利润、营业总收入、营业成本 三个核心指标
          - 值：数值型，无千分位、无单位字符
        """
        pass

    def _detect_fiscal_year_month(self, fin_ts: pd.DataFrame) -> int:
        """
        自动检测财年结束月份
        A股 = 12月，美股苹果 = 9月，微软 = 6月 等
        逻辑：取出现在最多不同年份中的月份
        """
        if fin_ts.empty:
            return 12
        month_year_counts = {}
        for m in fin_ts.index.month.unique():
            years = fin_ts[fin_ts.index.month == m].index.year.nunique()
            month_year_counts[m] = years
        return max(month_year_counts, key=lambda m: (month_year_counts[m], m == 12, m))

    def _calc_ttm(self, fin_ts: pd.DataFrame, metric: str) -> float:
        """
        通用TTM计算逻辑
        规则：年报直接取当期值（自动检测财年月）；季报做累计差值；缺失数据做平滑年化
        """
        if metric not in fin_ts.columns:
            logger.warning(f"【{self.market_name}-{self.stock_name}】指标 '{metric}' 不在财务数据中")
            return 0.0

        latest_date = fin_ts.index[0]
        y_latest = latest_date.year
        fiscal_month = self._detect_fiscal_year_month(fin_ts)

        # 年报直接返回当期值（兼容非12月财年，如苹果9月、微软6月）
        if latest_date.month == fiscal_month:
            return fin_ts.loc[latest_date, metric]

        # 计算可比期
        last_year_same_period = latest_date.replace(year=y_latest - 1)
        last_year_end = pd.Timestamp(year=y_latest - 1, month=fiscal_month, day=28)
        # 模糊匹配：财报日期可能是28/29/30/31号
        matches = fin_ts[(fin_ts.index.year == y_latest - 1) & (fin_ts.index.month == fiscal_month)]
        if not matches.empty:
            last_year_end = matches.index[0]

        try:
            ytd_current = fin_ts.loc[latest_date, metric]
            ytd_last = fin_ts.loc[last_year_same_period, metric]
            annual_last = fin_ts.loc[last_year_end, metric]
            return ytd_current + annual_last - ytd_last
        except KeyError:
            # 缺失可比期数据，回退到线性年化（按已披露月份等比外推全年）
            # 注意：季节性强的行业（如零售、旅游）该估算偏差较大
            logger.warning(
                f"【{self.market_name}-{self.stock_name}】指标 '{metric}' 缺失可比期数据，"
                f"使用线性年化(月份={latest_date.month})，季节性行业结果可能不准确"
            )
            return fin_ts.loc[latest_date, metric] / (latest_date.month / 12.0)

    def process(self):
        """统一主流程入口：财务清洗 → TTM 计算 → 估值推演 → 历史分位数"""
        logger.info(f"【{self.market_name}-{self.stock_name}】开始执行估值分析")

        # 1. 子类实现的财务数据清洗
        fin_ts = self._clean_financial_data()
        if fin_ts.empty:
            logger.error(f"【{self.market_name}-{self.stock_name}】财务数据清洗后为空，分析终止")
            return {}

        latest_date = fin_ts.index[0]
        logger.debug(f"【{self.market_name}-{self.stock_name}】最新财报日期: {latest_date.strftime('%Y-%m-%d')}")

        # 2. TTM 指标计算
        ttm_net_profit = self._calc_ttm(fin_ts, '归母净利润')
        ttm_revenue = self._calc_ttm(fin_ts, '营业总收入')
        logger.debug(f"【{self.market_name}-{self.stock_name}】TTM 归母净利润: {ttm_net_profit:.2f}, TTM 营业总收入: {ttm_revenue:.2f}")

        # 3. 年度数据与毛利率计算（自动检测财年月，兼容非12月财年）
        fiscal_month = self._detect_fiscal_year_month(fin_ts)
        annual_df = fin_ts[fin_ts.index.month == fiscal_month].head(5).sort_index(ascending=True)
        if '营业总收入' in annual_df.columns and '营业成本' in annual_df.columns:
            annual_df['毛利率'] = (annual_df['营业总收入'] - annual_df['营业成本']) / annual_df['营业总收入']
        else:
            annual_df['毛利率'] = 0.0
            logger.warning(f"【{self.market_name}-{self.stock_name}】缺失营业总收入/营业成本，无法计算毛利率")

        # 4. 当前估值与目标价区间计算
        price = self.market_data['price']
        market_cap = self.market_data['market_cap']

        # 股价或市值无效时提前终止，避免产生全零无意义结果
        if price <= 0 or market_cap <= 0:
            logger.error(f"【{self.market_name}-{self.stock_name}】行情数据异常: 股价={price}, 市值={market_cap}，分析终止")
            return {}

        total_shares = market_cap / price if price > 0 else 0
        eps_ttm = ttm_net_profit / total_shares if total_shares > 0 else 0
        sps_ttm = ttm_revenue / total_shares if total_shares > 0 else 0  # 每股营收

        current_pe = price / eps_ttm if eps_ttm > 0 else np.nan
        current_ps = price / sps_ttm if sps_ttm > 0 else np.nan
        logger.debug(f"【{self.market_name}-{self.stock_name}】当前股价: {price}, PE(TTM): {current_pe:.2f}, PS(TTM): {current_ps:.4f}")

        # 适配原有配置格式
        val_type = self.config.get('valuation', 'pe').lower()
        val_range = self.config.get(f'{val_type}_range', [0, 0, 0])

        scenarios = []
        for v in val_range:
            if val_type == 'pe':
                target_price = v * eps_ttm
            else:
                target_price = v * sps_ttm
            scenarios.append(target_price)

        # 5. 历史估值分位数计算（通用逻辑，兼容三市场历史数据格式）
        # 使用副本操作，避免修改 self.hist_val 导致二次调用崩溃
        hist_val = self.hist_val.copy()
        hist_percentile = 0.0
        if not hist_val.empty and 'trade_date' in hist_val.columns:
            hist_val['trade_date'] = pd.to_datetime(hist_val['trade_date'])
            hist_val.set_index('trade_date', inplace=True)
            hist_val.sort_index(inplace=True)

            val_col = 'pe_ttm' if val_type == 'pe' else 'ps_ttm'
            if val_col in hist_val.columns:
                hist_series = pd.to_numeric(hist_val[val_col], errors='coerce').dropna()
                if not hist_series.empty:
                    current_val = current_pe if val_type == 'pe' else current_ps
                    hist_percentile = (hist_series < current_val).mean() * 100
                    logger.debug(f"【{self.market_name}-{self.stock_name}】历史 {val_type.upper()} 分位数: {hist_percentile:.1f}%")

        # 6. 组装返回结果
        logger.info(f"【{self.market_name}-{self.stock_name}】估值分析完成")
        return {
            'annual_df': annual_df,
            'ttm_net_profit': ttm_net_profit,
            'ttm_revenue': ttm_revenue,
            'current_pe': current_pe,
            'current_ps': current_ps,
            'scenarios': scenarios,
            'hist_val': hist_val,
            'hist_percentile': hist_percentile,
            'price': price,
            'stock_name': self.stock_name,
            'market_name': self.market_name
        }


# ---------------------- A股分析子类 ----------------------
class AStockAnalyzer(BaseAnalyzer):
    """A股专属分析器，处理 akshare 返回的中文财务摘要表格"""

    def _clean_financial_data(self) -> pd.DataFrame:
        """清洗A股财务数据：转置指标行列、解析日期、清理中文数值单位"""
        raw_fin = self.raw_fin

        # 强制去除重复的列名，防止 df['指标'] 变成 DataFrame
        raw_fin = raw_fin.loc[:, ~raw_fin.columns.duplicated()].copy()

        if '选项' in raw_fin.columns:
            raw_fin.drop(columns=['选项'], inplace=True, errors='ignore')

        # 尝试寻找并设置索引
        if '指标' in raw_fin.columns:
            raw_fin['指标'] = raw_fin['指标'].astype(str).str.strip()
            # 去除重复的指标行，仅保留第一个
            raw_fin = raw_fin.drop_duplicates(subset='指标', keep='first')
            raw_fin.set_index('指标', inplace=True)
        elif len(raw_fin.columns) > 0:
            first_col = raw_fin.columns[0]
            raw_fin[first_col] = raw_fin[first_col].astype(str).str.strip()
            raw_fin = raw_fin.drop_duplicates(subset=first_col, keep='first')
            raw_fin.set_index(first_col, inplace=True)

        # 提取日期列：支持 '20250930' (纯数字8位) 和 '2025-09-30' 两种格式
        date_cols = []
        for c in raw_fin.columns:
            col_str = str(c)
            if '-' in col_str or '/' in col_str:
                date_cols.append(c)
            elif re.match(r'^\d{8}$', col_str):
                date_cols.append(c)

        if not date_cols:
            logger.warning(f"【A股-{self.stock_name}】未找到日期列，原始列名: {raw_fin.columns.tolist()}")
            return pd.DataFrame()

        fin_ts = raw_fin[date_cols].T

        # 将索引转为日期时间格式
        try:
            fin_ts.index = pd.to_datetime(fin_ts.index, format='%Y%m%d')
        except ValueError:
            fin_ts.index = pd.to_datetime(fin_ts.index)

        fin_ts = fin_ts.sort_index(ascending=False)  # 按日期降序，最新在最前

        # 转换为数值型 (清理千分位或中文字符)
        for col in fin_ts.columns:
            fin_ts[col] = pd.to_numeric(
                fin_ts[col].astype(str).str.replace(',', '').str.replace('万', '0000').str.replace('亿',
                                                                                                   '00000000').str.replace(
                    'None', 'NaN'),
                errors='coerce'
            )

        return fin_ts


# ---------------------- 国际市场分析子类：港股/美股共用，适配 akshare 东方财富数据结构 ----------------------
class InternationalStockAnalyzer(BaseAnalyzer):
    """
    港股/美股通用分析器
    适配 akshare 东方财富返回的财务数据结构，自动将英文指标映射为中文以对齐基类核心逻辑。
    通过构造函数传入的 market_name 自动区分日志前缀，无需重复子类。
    """

    # 财务指标映射：东方财富英文指标 -> 基类中文指标
    METRIC_MAP = {
        "Net Income Common Stockholders": "归母净利润",
        "Total Revenue": "营业总收入",
        "Cost Of Revenue": "营业成本"
    }

    def _clean_financial_data(self) -> pd.DataFrame:
        """清洗港股/美股财务数据，统一为基类所需格式"""
        raw_fin = self.raw_fin

        if raw_fin.empty:
            logger.error(f"【{self.market_name}-{self.stock_name}】原始财务数据为空")
            return pd.DataFrame()

        # 1. 指标名称映射：英文转中文，对齐基类要求
        fin_ts = raw_fin.rename(columns=self.METRIC_MAP)

        # 2. 校验核心指标是否存在
        core_metrics = ["归母净利润", "营业总收入", "营业成本"]
        exist_metrics = [m for m in core_metrics if m in fin_ts.columns]
        if not exist_metrics:
            logger.error(f"【{self.market_name}-{self.stock_name}】核心财务指标缺失，原始列名: {raw_fin.columns.tolist()}")
            return pd.DataFrame()

        # 3. 索引格式处理：保证是 datetime 格式，降序排列
        try:
            fin_ts.index = pd.to_datetime(fin_ts.index)
        except Exception as e:
            logger.error(f"【{self.market_name}-{self.stock_name}】财报日期转换失败: {e}")
            return pd.DataFrame()

        fin_ts = fin_ts.sort_index(ascending=False)

        # 4. 数值型转换，清理异常值
        for col in fin_ts.columns:
            fin_ts[col] = pd.to_numeric(fin_ts[col], errors='coerce')

        logger.debug(f"【{self.market_name}-{self.stock_name}】财务数据清洗完成，有效日期数: {len(fin_ts)}")
        return fin_ts


# 向后兼容别名，避免外部引用报错
HKStockAnalyzer = InternationalStockAnalyzer
USStockAnalyzer = InternationalStockAnalyzer
