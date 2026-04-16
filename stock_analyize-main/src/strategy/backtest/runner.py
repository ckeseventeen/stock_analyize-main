"""
src/strategy/backtest/runner.py — 回测运行器

封装 backtrader Cerebro，处理数据格式转换和性能统计。
"""
# matplotlib 后端必须在 backtrader 导入前设置，
# 否则 backtrader import 时会锁定 MacOS GUI 后端
import matplotlib
matplotlib.use("Agg", force=True)

import backtrader as bt
import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("backtest_runner")

# akshare 中文列名 -> backtrader 标准列名
_COL_MAP = {
    "日期": "date",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
}


class BacktestRunner:
    """
    回测运行器

    使用示例:
        from src.strategy.backtest.ma_crossover import MACrossoverStrategy

        runner = BacktestRunner(
            strategy_class=MACrossoverStrategy,
            data_df=daily_ohlcv_df,
            fast_period=10,
            slow_period=30,
        )
        result = runner.run(initial_cash=100000)
        print(result)
    """

    def __init__(self, strategy_class: type, data_df: pd.DataFrame, **strategy_params):
        self._strategy_class = strategy_class
        self._data_df = self._prepare_data(data_df)
        self._strategy_params = strategy_params
        self._cerebro: bt.Cerebro | None = None
        self._results = None
        self._initial_cash = 100000

    @staticmethod
    def _prepare_data(df: pd.DataFrame) -> pd.DataFrame:
        """将 akshare 格式的 DataFrame 转换为 backtrader 兼容格式"""
        result = df.copy()

        # 列名标准化
        rename_map = {}
        for col in result.columns:
            col_str = str(col).strip()
            if col_str in _COL_MAP:
                rename_map[col] = _COL_MAP[col_str]
            elif col_str.lower() in ("open", "high", "low", "close", "volume", "date"):
                rename_map[col] = col_str.lower()
        result.rename(columns=rename_map, inplace=True)

        # 确保有 date 列并设为索引
        if "date" in result.columns:
            result["date"] = pd.to_datetime(result["date"])
            result.set_index("date", inplace=True)
        elif not isinstance(result.index, pd.DatetimeIndex):
            result.index = pd.to_datetime(result.index)

        result.sort_index(inplace=True)

        # 数值转换
        for col in ("open", "high", "low", "close", "volume"):
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce")

        result.dropna(subset=["open", "high", "low", "close"], inplace=True)
        return result

    def run(self, initial_cash: float = 100000, commission: float = 0.001) -> dict:
        """
        执行回测

        Args:
            initial_cash: 初始资金
            commission: 手续费率

        Returns:
            回测结果字典
        """
        self._initial_cash = initial_cash

        cerebro = bt.Cerebro()
        cerebro.addstrategy(self._strategy_class, **self._strategy_params)

        # 添加数据
        data_feed = bt.feeds.PandasData(dataname=self._data_df)
        cerebro.adddata(data_feed)

        # 设置初始资金和手续费
        cerebro.broker.setcash(initial_cash)
        cerebro.broker.setcommission(commission=commission)

        # 添加分析器
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe", riskfreerate=0.03)
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trades")
        cerebro.addanalyzer(bt.analyzers.Returns, _name="returns")

        logger.info(f"回测启动: 策略={self._strategy_class.__name__}, 初始资金={initial_cash:.0f}, 手续费={commission:.4f}")

        self._results = cerebro.run()
        self._cerebro = cerebro

        return self.get_report()

    def get_report(self) -> dict:
        """生成回测绩效报告"""
        if self._results is None:
            return {}

        strat = self._results[0]
        final_value = self._cerebro.broker.getvalue()
        total_return = (final_value - self._initial_cash) / self._initial_cash * 100

        # 夏普比率
        sharpe = strat.analyzers.sharpe.get_analysis()
        sharpe_ratio = sharpe.get("sharperatio", 0) or 0

        # 最大回撤
        dd = strat.analyzers.drawdown.get_analysis()
        max_drawdown = dd.get("max", {}).get("drawdown", 0) or 0

        # 交易统计
        trades = strat.analyzers.trades.get_analysis()
        total_trades = trades.get("total", {}).get("total", 0) or 0
        won = trades.get("won", {}).get("total", 0) or 0
        win_rate = (won / total_trades * 100) if total_trades > 0 else 0

        # 年化收益
        returns = strat.analyzers.returns.get_analysis()
        annual_return = returns.get("rnorm100", 0) or 0

        report = {
            "策略": self._strategy_class.__name__,
            "初始资金": self._initial_cash,
            "最终资产": round(final_value, 2),
            "总收益率(%)": round(total_return, 2),
            "年化收益率(%)": round(annual_return, 2),
            "夏普比率": round(sharpe_ratio, 4),
            "最大回撤(%)": round(max_drawdown, 2),
            "总交易次数": total_trades,
            "胜率(%)": round(win_rate, 2),
        }

        logger.info(f"回测完成: 总收益={total_return:.2f}%, 夏普={sharpe_ratio:.4f}, 最大回撤={max_drawdown:.2f}%")
        return report
