"""
src/strategy/backtest/report.py — 回测报告生成

格式化回测结果，支持文本输出和图表保存。
"""
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from src.utils.logger import get_logger

logger = get_logger("backtest_report")


class BacktestReport:
    """
    回测绩效报告

    使用示例:
        report = BacktestReport(result_dict)
        print(report.to_string())
        report.plot_equity_curve(equity_data, save_path="output/backtest.png")
    """

    def __init__(self, result: dict):
        self._result = result

    @classmethod
    def from_runner(cls, runner) -> "BacktestReport":
        """从 BacktestRunner 构建报告"""
        return cls(runner.get_report())

    def to_dict(self) -> dict:
        """返回报告字典"""
        return dict(self._result)

    def to_string(self) -> str:
        """返回格式化的中文文本报告"""
        lines = [
            "",
            "=" * 50,
            "  回测绩效报告",
            "=" * 50,
        ]
        for key, val in self._result.items():
            lines.append(f"  {key:　<12s}: {val}")
        lines.append("=" * 50)
        return "\n".join(lines)

    def plot_equity_curve(self, equity_values: list[float], save_path: str = "output/backtest_equity.png") -> None:
        """
        绘制资金曲线

        Args:
            equity_values: 每日资产净值列表
            save_path: 图表保存路径
        """
        if not equity_values:
            logger.warning("资金曲线数据为空，跳过绘图")
            return

        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(equity_values, linewidth=1.5, color="#3B7DD8")
        ax.set_title("回测资金曲线", fontsize=16, fontweight="bold")
        ax.set_xlabel("交易日", fontsize=12)
        ax.set_ylabel("资产净值", fontsize=12)
        ax.grid(alpha=0.3)

        # 标注关键指标
        total_return = self._result.get("总收益率(%)", 0)
        max_dd = self._result.get("最大回撤(%)", 0)
        ax.text(0.02, 0.98, f"总收益: {total_return:.2f}%\n最大回撤: {max_dd:.2f}%",
                transform=ax.transAxes, fontsize=11, verticalalignment="top",
                bbox={"boxstyle": "round", "facecolor": "wheat", "alpha": 0.8})

        os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        logger.info(f"资金曲线已保存: {save_path}")
