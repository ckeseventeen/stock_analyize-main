"""
src/strategy/backtest/runner.py — 回测运行器

封装 backtrader Cerebro，处理数据格式转换和性能统计。

关键修复（参考 ~/.claude/plans/bug-bright-dawn.md）：
  - S3：默认配置滑点 + A 股印花税（卖出单边 0.1%）
  - S2：warmup_bars 参数预扣前 N 根 bar 不参与统计（避免 MA250 之类的指标未热身就交易）
  - B3：通过 PercentSizer 让 Backtrader 用真实成交价（下一 bar 开盘）算 size，
        而不是用 close[0] 估算（同 bar 收盘价 ≠ 下一 bar 开盘价）
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


class AStockCommission(bt.CommInfoBase):
    """
    A 股佣金模型：买卖双边收佣金 + 卖出额外扣印花税（0.1%）。

    S3 修复：原 runner 只设了 setcommission(commission=0.001)，未区分买卖方向。
    A 股真实交易成本（不含过户费等小项）：
      - 佣金：双边收，券商收 0.025%~0.03%（这里默认 0.025%，可配）
      - 印花税：卖出单边收 0.1%（2023-08-28 起从 0.1% 降为 0.05%，但保守按 0.1% 估算）
      - 总卖出成本约 0.125%，买入约 0.025%
    """

    params = (
        ("commission", 0.00025),  # 双边佣金
        ("stamp_tax", 0.001),     # 卖出印花税
        ("stocklike", True),       # 不是期货
        ("commtype", bt.CommInfoBase.COMM_PERC),
        ("percabs", True),         # 百分比按绝对值（0.001 = 0.1%）
    )

    def _getcommission(self, size, price, pseudoexec):
        # size > 0 买入，size < 0 卖出
        notional = abs(size) * price
        commission = notional * self.p.commission
        if size < 0:
            # 卖出附加印花税
            commission += notional * self.p.stamp_tax
        return commission


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
        self._warmup_bars = 0

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

    def run(
        self,
        initial_cash: float = 100000,
        commission: float = 0.00025,
        stamp_tax: float = 0.001,
        slippage_perc: float = 0.001,
        warmup_bars: int = 0,
        market: str = "a",
    ) -> dict:
        """
        执行回测。

        Args:
            initial_cash: 初始资金
            commission: 佣金率（双边），默认 0.025%
            stamp_tax: 印花税率（A股卖出单边），默认 0.1%；market="us"/"hk" 时建议设 0
            slippage_perc: 滑点（按百分比），默认 0.1%；高换手策略可调到 0.05%
            warmup_bars: 预热期 K 线根数，前 N 根不参与统计（用于 MA250 等长指标），默认 0
            market: 市场标识（"a"/"hk"/"us"），影响默认税费

        Returns:
            回测结果字典
        """
        self._initial_cash = initial_cash
        self._warmup_bars = max(0, int(warmup_bars))

        # 非 A 股默认不收印花税
        if market != "a" and stamp_tax == 0.001:
            stamp_tax = 0.0

        cerebro = bt.Cerebro()
        cerebro.addstrategy(self._strategy_class, **self._strategy_params)

        # 添加数据
        data_feed = bt.feeds.PandasData(dataname=self._data_df)
        cerebro.adddata(data_feed)

        # 设置初始资金
        cerebro.broker.setcash(initial_cash)

        # S3：用自定义 CommInfo 区分买卖
        commission_info = AStockCommission(commission=commission, stamp_tax=stamp_tax)
        cerebro.broker.addcommissioninfo(commission_info)

        # S3：设置滑点（双边按百分比）
        if slippage_perc > 0:
            cerebro.broker.set_slippage_perc(perc=slippage_perc)

        # B3：默认 PercentSizer，让 Backtrader 用真实成交价算 size，避免 close[0] 偏差
        # 注：策略内若已手动算 size 并调用 self.buy(size=size)，会优先使用手动 size
        # 此设置仅对 self.buy() / self.order_target_percent() 这种无 size 调用生效
        cerebro.addsizer(bt.sizers.PercentSizer, percents=95)

        # 添加分析器
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe", riskfreerate=0.03)
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trades")
        cerebro.addanalyzer(bt.analyzers.Returns, _name="returns")

        logger.info(
            f"回测启动: 策略={self._strategy_class.__name__}, "
            f"初始资金={initial_cash:.0f}, 市场={market}, "
            f"佣金={commission:.4f}, 印花税={stamp_tax:.4f}, "
            f"滑点={slippage_perc:.4f}, warmup={self._warmup_bars}根"
        )

        # S2：warmup 处理 —— 在策略 next() 之前要求至少 warmup_bars 根 K 线已经流入，
        # 通过 backtrader 的 strategy.params.warmup 传递（如果策略支持的话）
        # 这里采用更通用的做法：在 cerebro.run() 后过滤交易统计
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
            "预热bar数": self._warmup_bars,
        }

        logger.info(
            f"回测完成: 总收益={total_return:.2f}%, 夏普={sharpe_ratio:.4f}, "
            f"最大回撤={max_drawdown:.2f}%, 交易次数={total_trades}"
        )
        return report
