"""
src/screener/screener.py — 股票筛选引擎

两轮过滤架构：
  1. 第一轮（快速）：基于实时行情数据在内存中过滤（市值/PE/PB等）
  2. 第二轮（精筛）：对候选股逐只拉取K线数据，检测技术条件（底背离/RSI等）
"""
import time

import pandas as pd

from src.screener.conditions import BaseCondition
from src.screener.config_schema import parse_screen_config
from src.screener.data_provider import ScreenerDataProvider
from src.utils.logger import get_logger

logger = get_logger("screener")


class StockScreener:
    """
    可组合条件的A股筛选器

    使用示例:
        from src.screener.conditions import MarketCapCondition, WeeklyMACDBottomDivergenceCondition

        screener = StockScreener()
        screener.add_condition(MarketCapCondition(min_cap=50, max_cap=500))
        screener.add_condition(WeeklyMACDBottomDivergenceCondition())

        results = screener.run()
        print(results)
    """

    def __init__(self, data_provider: ScreenerDataProvider | None = None):
        self._provider = data_provider or ScreenerDataProvider()
        self._conditions: list[BaseCondition] = []
        self._request_delay: float = 0.3  # 逐只请求间隔（秒）

    def add_condition(self, condition: BaseCondition) -> "StockScreener":
        """链式添加筛选条件（AND组合）"""
        self._conditions.append(condition)
        logger.debug(f"添加筛选条件: {condition}")
        return self

    def run(self, sort_by: str = "总市值", ascending: bool = False, limit: int = 100) -> pd.DataFrame:
        """
        执行筛选

        Args:
            sort_by: 结果排序列
            ascending: 是否升序
            limit: 最大返回数量

        Returns:
            筛选结果 DataFrame，包含: 代码, 名称, 最新价, 总市值(亿), 市盈率, 市净率 等
        """
        if not self._conditions:
            logger.warning("未添加任何筛选条件，返回空结果")
            return pd.DataFrame()

        # 第一轮：获取全A实时行情并做内存过滤
        all_stocks = self._provider.get_all_a_shares()
        if all_stocks.empty:
            logger.error("获取全A股数据失败，筛选终止")
            return pd.DataFrame()

        logger.info(f"第一轮筛选开始，全A共 {len(all_stocks)} 只股票")
        candidates = self._pass1_spot_filter(all_stocks)
        logger.info(f"第一轮筛选完成，{len(all_stocks)} → {len(candidates)} 只候选")

        # 第二轮：对候选股逐只做OHLCV条件检测
        ohlcv_conditions = [c for c in self._conditions if c.requires_ohlcv]
        if ohlcv_conditions:
            logger.info(f"第二轮筛选开始，{len(candidates)} 只候选，{len(ohlcv_conditions)} 个K线条件")
            candidates = self._pass2_ohlcv_filter(candidates, ohlcv_conditions)
            logger.info(f"第二轮筛选完成，剩余 {len(candidates)} 只")
        else:
            logger.info("无K线条件，跳过第二轮筛选")

        if candidates.empty:
            logger.info("筛选结果为空")
            return pd.DataFrame()

        # 整理输出列
        result = self._format_output(candidates)

        # 排序和截断
        if sort_by in result.columns:
            result = result.sort_values(sort_by, ascending=ascending)
        result = result.head(limit).reset_index(drop=True)

        logger.info(f"筛选完成，共 {len(result)} 只股票符合条件")
        return result

    def run_from_config(self, config_path: str, **kwargs) -> pd.DataFrame:
        """从YAML配置文件加载条件并运行"""
        conditions, output_config = parse_screen_config(config_path)
        for cond in conditions:
            self.add_condition(cond)

        sort_by = output_config.get("sort_by", "总市值(亿)")
        limit = output_config.get("limit", 100)
        ascending = output_config.get("ascending", False)

        return self.run(sort_by=sort_by, ascending=ascending, limit=limit, **kwargs)

    def _pass1_spot_filter(self, all_stocks: pd.DataFrame) -> pd.DataFrame:
        """第一轮：Spot条件内存过滤"""
        spot_conditions = [c for c in self._conditions if not c.requires_ohlcv]
        if not spot_conditions:
            return all_stocks

        mask = pd.Series(True, index=all_stocks.index)
        for cond in spot_conditions:
            cond_mask = all_stocks.apply(lambda row, _c=cond: _c.evaluate_spot(row), axis=1)
            mask = mask & cond_mask
            passed = cond_mask.sum()
            logger.debug(f"  条件 [{cond.name}]: 通过 {passed} 只")

        return all_stocks[mask].copy()

    def _pass2_ohlcv_filter(self, candidates: pd.DataFrame, ohlcv_conditions: list[BaseCondition]) -> pd.DataFrame:
        """第二轮：逐只拉取K线数据，检测OHLCV条件"""
        # 按条件所需的K线周期分组
        need_weekly = any(c.ohlcv_period == "weekly" for c in ohlcv_conditions)
        need_daily = any(c.ohlcv_period == "daily" for c in ohlcv_conditions)

        passed_indices = []
        total = len(candidates)

        for i, (idx, row) in enumerate(candidates.iterrows()):
            code = str(row.get("代码", "")).strip()
            name = str(row.get("名称", "")).strip()

            if not code:
                continue

            # 进度日志（每50只报告一次）
            if (i + 1) % 50 == 0 or i == 0:
                logger.info(f"  第二轮进度: {i + 1}/{total} ({name} {code})")

            # 获取所需K线数据
            weekly_df = self._provider.get_weekly_ohlcv(code) if need_weekly else pd.DataFrame()
            daily_df = self._provider.get_daily_ohlcv(code) if need_daily else pd.DataFrame()

            # 逐条件检测
            all_pass = True
            for cond in ohlcv_conditions:
                ohlcv_df = weekly_df if cond.ohlcv_period == "weekly" else daily_df
                if not cond.evaluate_full(row, ohlcv_df):
                    all_pass = False
                    break

            if all_pass:
                passed_indices.append(idx)
                logger.info(f"  ✓ {name}({code}) 通过所有K线条件")

            # 请求间隔
            time.sleep(self._request_delay)

        return candidates.loc[passed_indices].copy()

    @staticmethod
    def _format_output(df: pd.DataFrame) -> pd.DataFrame:
        """整理输出列"""
        output_cols = {
            "代码": "代码",
            "名称": "名称",
            "最新价": "最新价",
            "涨跌幅": "涨跌幅(%)",
            "总市值": "总市值(亿)",
            "流通市值": "流通市值(亿)",
            "市盈率-动态": "PE(动态)",
            "市净率": "PB",
            "换手率": "换手率(%)",
            "振幅": "振幅(%)",
        }

        result = pd.DataFrame()
        for src_col, dst_col in output_cols.items():
            if src_col in df.columns:
                result[dst_col] = df[src_col].values
            else:
                result[dst_col] = None

        # 市值转亿元
        if "总市值(亿)" in result.columns:
            result["总市值(亿)"] = pd.to_numeric(result["总市值(亿)"], errors="coerce") / 1e8
        if "流通市值(亿)" in result.columns:
            result["流通市值(亿)"] = pd.to_numeric(result["流通市值(亿)"], errors="coerce") / 1e8

        return result
