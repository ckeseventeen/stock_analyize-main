"""
src/strategy/backtest/screener_rule.py — 桥接筛选器条件的策略

允许将 screen_config.yaml 中的筛选条件直接作为回测信号。
"""
import pandas as pd

from src.analysis.screening.conditions import CONDITION_REGISTRY, BaseCondition
from src.strategy.backtest.base_strategy import BaseStrategy
from src.utils.logger import get_logger

logger = get_logger("backtest_screener_rule")

class ScreenerRuleStrategy(BaseStrategy):
    """
    桥接策略：使用筛选器中的逻辑组件（如背离、突破等）进行回测。

    Params:
        buy_conditions: list[dict] — 买入条件配置（YAML格式）
        sell_conditions: list[dict] — 卖出条件配置
        buy_logic: str — "any" 或 "all"
        sell_logic: str — "any" 或 "all"
        position_size: float — 仓位比例
    """
    params = (
        ("buy_conditions", []),
        ("sell_conditions", []),
        ("buy_logic", "any"),         # 修复：原 "all" 默认几乎互斥
        ("sell_logic", "any"),
        ("position_size", 0.95),
        # 新增可调：滑动窗口最大回溯长度
        ("max_lookback", 300),
        # 继承自 BaseStrategy：warmup_bars / log_level
    )

    def __init__(self):
        self._buy_objs = self._init_conditions(self.params.buy_conditions)
        self._sell_objs = self._init_conditions(self.params.sell_conditions)

        # 预计算整个数据集的信号（为了性能，避免在 next() 里反复切片 DataFrame）
        # 警告：在大数据集上可能耗时，但比在 next() 里逐 bar 构造 DataFrame 快
        self._buy_signals = self._precompute_signals(self._buy_objs, self.params.buy_logic)
        self._sell_signals = self._precompute_signals(self._sell_objs, self.params.sell_logic)

        self._bar_idx = 0

    def _init_conditions(self, configs: list[dict]) -> list[BaseCondition]:
        """
        构建条件对象。

        B10 修复：过滤掉 SPOT_ONLY_TYPES（如 market_cap、pe_range、stop_loss 等），
        这些条件依赖 Spot 截面数据，在回测里拿不到数据会永远返回 False，导致信号永不触发。
        B21 修复：未知类型直接 raise ValueError，避免用户拼错配置后默默无效。
        """
        from src.analysis.screening.config_schema import _PARAM_MAP, SPOT_ONLY_TYPES

        objs = []
        skipped_spot: list[str] = []
        unknown: list[str] = []
        for cfg in configs:
            ctype = cfg.get("type")
            if not ctype:
                logger.warning(f"回测条件缺少 type 字段，跳过: {cfg}")
                continue
            if ctype in SPOT_ONLY_TYPES:
                skipped_spot.append(ctype)
                continue
            if ctype not in CONDITION_REGISTRY:
                unknown.append(ctype)
                continue
            cls = CONDITION_REGISTRY[ctype]
            param_map = _PARAM_MAP.get(ctype, {})
            kwargs = {}
            for yaml_key, init_key in param_map.items():
                if yaml_key in cfg:
                    kwargs[init_key] = cfg[yaml_key]
            try:
                objs.append(cls(**kwargs))
            except Exception as e:
                logger.error(f"构建条件 {ctype} 失败: {e}")

        if skipped_spot:
            logger.warning(
                f"回测条件包含 Spot-only 类型并已跳过（截面数据在回测中不可用）: "
                f"{skipped_spot}。如需基本面筛选，请在筛选阶段处理，回测里只用技术条件。"
            )
        if unknown:
            # B21：直接抛错，配置拼写错误必须暴露
            raise ValueError(
                f"未知的回测条件类型: {unknown}。"
                f"可用类型见 CONDITION_REGISTRY: {sorted(CONDITION_REGISTRY.keys())}"
            )
        return objs

    def _precompute_signals(self, condition_objs: list[BaseCondition], logic: str) -> list[bool]:
        if not condition_objs:
            return [False] * len(self.data)

        # 将 Backtrader 数据馈送转回 DataFrame 供筛选器组件使用
        df = pd.DataFrame({
            "open": self.data.open.array,
            "high": self.data.high.array,
            "low": self.data.low.array,
            "close": self.data.close.array,
            "volume": self.data.volume.array,
        })

        signals = []
        total = len(df)
        max_lookback = int(self.params.max_lookback)

        # 逐 bar 预计算，使用滑动窗口而非全量切片，将 O(n²) 优化为 O(n)
        for i in range(total):
            bar_spot = df.iloc[i]
            # 只取最近 max_lookback 根 K 线
            start_idx = max(0, i - max_lookback + 1)
            bar_history = df.iloc[start_idx:i + 1]

            cond_results = []
            for obj in condition_objs:
                try:
                    res = obj.evaluate_full(bar_spot, bar_history)
                    cond_results.append(res)
                except Exception:
                    cond_results.append(False)

            if logic == "all":
                signals.append(all(cond_results) if cond_results else False)
            else:
                signals.append(any(cond_results) if cond_results else False)

        return signals

    def next(self):
        if self._bar_idx >= len(self._buy_signals):
            return

        buy_sig = self._buy_signals[self._bar_idx]
        sell_sig = self._sell_signals[self._bar_idx]
        self._bar_idx += 1

        # S2：预热期内不交易（让长周期指标先填满）
        if not self.is_warmup_done():
            return

        if not self.position:
            if buy_sig:
                # B3：用 order_target_percent，由 Backtrader 用下一 bar 开盘价算 size，
                # 避免 close[0] 估算导致的仓位偏差
                order = self.buy_target_percent(target=self.params.position_size)
                if order is not None:
                    self.log(
                        f"筛选器买入信号 -> 目标仓位 {self.params.position_size:.0%}"
                    )
        else:
            if sell_sig:
                self.close_all()
                self.log("筛选器卖出信号 -> 全部卖出（下一 bar 开盘成交）")
