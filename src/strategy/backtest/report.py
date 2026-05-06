"""
src/strategy/backtest/report.py — 回测报告生成与持久化

支持文本输出、图表保存、CSV/JSON 历史持久化、多策略对比。
"""
import csv
import json
import os
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from src.utils.logger import get_logger

logger = get_logger("backtest_report")

# 默认历史记录目录
_DEFAULT_HISTORY_DIR = Path("output/backtest_history")


class BacktestReport:
    """
    回测绩效报告

    支持持久化到 CSV/JSON，可回溯对比历史回测结果。

    使用示例:
        report = BacktestReport(result_dict)
        print(report.to_string())
        report.save()                                         # 持久化
        report.plot_equity_curve(equity_data, save_path="output/backtest.png")
        history = BacktestReport.load_history()               # 加载历史
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

    # ========================
    # 持久化
    # ========================

    def save(self, history_dir: str | Path | None = None) -> Path:
        """
        将回测结果持久化到 CSV 和 JSON 文件。

        文件名格式: backtest_{策略名}_{YYYYMMDD_HHMMSS}.csv

        Args:
            history_dir: 历史记录目录，默认 output/backtest_history

        Returns:
            csv_path: 保存的 CSV 文件路径
        """
        save_dir = Path(history_dir) if history_dir else _DEFAULT_HISTORY_DIR
        save_dir.mkdir(parents=True, exist_ok=True)

        strategy_name = self._result.get("策略", "unknown")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = f"backtest_{strategy_name}_{timestamp}"

        # CSV 保存
        csv_path = save_dir / f"{base_name}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["指标", "值"])
            for key, val in self._result.items():
                writer.writerow([key, val])
        logger.info(f"回测报告已保存: {csv_path}")

        # JSON 保存（结构化，便于程序读取对比）
        json_path = save_dir / f"{base_name}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump({
                "timestamp": timestamp,
                "strategy": strategy_name,
                "metrics": self._result,
            }, f, ensure_ascii=False, indent=2)
        logger.debug(f"回测JSON已保存: {json_path}")

        return csv_path

    @classmethod
    def load_history(cls, strategy_name: str | None = None,
                     history_dir: str | Path | None = None) -> list[dict]:
        """
        加载历史回测记录。

        Args:
            strategy_name: 可选策略名过滤
            history_dir: 历史记录目录

        Returns:
            按时间降序排列的历史记录列表，每个元素含 timestamp/strategy/metrics
        """
        search_dir = Path(history_dir) if history_dir else _DEFAULT_HISTORY_DIR
        if not search_dir.exists():
            return []

        records = []
        for json_file in sorted(search_dir.glob("backtest_*.json"), reverse=True):
            try:
                with open(json_file, encoding="utf-8") as f:
                    record = json.load(f)
                if strategy_name and record.get("strategy") != strategy_name:
                    continue
                records.append(record)
            except Exception as e:
                logger.debug(f"读取历史回测记录失败 [{json_file}]: {e}")

        return records

    @classmethod
    def compare_strategies(cls, history_dir: str | Path | None = None) -> str:
        """
        对比所有历史回测策略的绩效。

        Returns:
            格式化的对比文本
        """
        records = cls.load_history(history_dir=history_dir)
        if not records:
            return "无历史回测记录"

        # 按策略分组并取每组最新的记录
        latest_by_strategy: dict[str, dict] = {}
        for rec in records:
            name = rec.get("strategy", "unknown")
            if name not in latest_by_strategy:
                latest_by_strategy[name] = rec

        key_metrics = ["总收益率(%)", "年化收益率(%)", "夏普比率", "最大回撤(%)", "交易次数", "胜率(%)"]

        lines = [
            "",
            "=" * 70,
            "  多策略回测对比",
            "=" * 70,
        ]
        header = f"  {'策略':<16s}" + "".join(f"{m:>12s}" for m in key_metrics)
        lines.append(header)
        lines.append("  " + "-" * (16 + 12 * len(key_metrics)))

        for strategy_name, rec in latest_by_strategy.items():
            metrics = rec.get("metrics", {})
            row = f"  {strategy_name:<16s}"
            for m in key_metrics:
                val = metrics.get(m, "N/A")
                if isinstance(val, (int, float)):
                    row += f"{val:>12.2f}"
                else:
                    row += f"{str(val):>12s}"
            lines.append(row)

        lines.append("=" * 70)
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
