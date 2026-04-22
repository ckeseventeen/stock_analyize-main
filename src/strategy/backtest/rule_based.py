"""
src/strategy/backtest/rule_based.py — YAML 驱动的规则策略

通过字符串 DSL 在配置里表达买卖逻辑，无需写 Python。

规则格式:
    rule_config:
      indicators:
        rsi_14: {type: rsi, period: 14}
        ma_20:  {type: sma, period: 20}
        ma_60:  {type: sma, period: 60}
      buy_when:            # 买入条件列表
        - "rsi_14 < 30"
        - "ma_20 cross_up ma_60"
      sell_when:           # 卖出条件列表
        - "rsi_14 > 70"
      buy_logic: any       # any=任一命中即触发(OR)；all=全部命中(AND)；默认 any
      sell_logic: any
      position_size: 0.95  # 买入时占用现金比例，默认 0.95

支持的指标 type:
    rsi   -> bt.ind.RSI       (param: period)
    sma   -> bt.ind.SMA       (param: period)  别名 ma
    ema   -> bt.ind.EMA       (param: period)
    macd_line   -> bt.ind.MACD.macd    (params: fast=12, slow=26, signal=9)
    macd_signal -> bt.ind.MACD.signal
    atr   -> bt.ind.ATR       (param: period=14)

支持的运算符:
    <  <=  >  >=  ==  !=
    cross_up    值上穿（前一 bar <=，当前 >）
    cross_down  值下穿

条件两侧可以是：
    - 已注册的指标名字（如 rsi_14）
    - OHLCV 字段：close / open / high / low / volume
    - 数字字面量（如 30 / 0.05）
"""
from __future__ import annotations

import operator

import backtrader as bt

from src.strategy.backtest.base_strategy import BaseStrategy

# 比较运算符映射（cross_up/cross_down 单独处理）
_CMP_OPS = {
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "==": operator.eq,
    "!=": operator.ne,
}
_CROSS_OPS = {"cross_up", "cross_down"}


def _build_indicator(data, spec: dict):
    """根据 spec 构造单个 backtrader 指标 line。返回 (line, warn_msg)。"""
    t = str(spec.get("type", "")).lower().strip()
    if t in ("sma", "ma"):
        period = int(spec.get("period", 20))
        return bt.indicators.SMA(data.close, period=period), None
    if t == "ema":
        period = int(spec.get("period", 20))
        return bt.indicators.EMA(data.close, period=period), None
    if t == "rsi":
        period = int(spec.get("period", 14))
        return bt.indicators.RSI(data.close, period=period), None
    if t == "atr":
        period = int(spec.get("period", 14))
        return bt.indicators.ATR(data, period=period), None
    if t == "macd_line":
        fast = int(spec.get("fast", 12))
        slow = int(spec.get("slow", 26))
        signal = int(spec.get("signal", 9))
        macd = bt.indicators.MACD(data.close, period_me1=fast,
                                  period_me2=slow, period_signal=signal)
        return macd.macd, None
    if t == "macd_signal":
        fast = int(spec.get("fast", 12))
        slow = int(spec.get("slow", 26))
        signal = int(spec.get("signal", 9))
        macd = bt.indicators.MACD(data.close, period_me1=fast,
                                  period_me2=slow, period_signal=signal)
        return macd.signal, None
    return None, f"未知指标 type: {t}"


class RuleBasedStrategy(BaseStrategy):
    """
    YAML 规则驱动的策略

    Params:
        rule_config: dict — 见模块文档字符串
    """

    params = (
        ("rule_config", None),
    )

    # ------------------ 生命周期 ------------------

    def __init__(self):
        cfg = self.params.rule_config or {}
        if not isinstance(cfg, dict):
            raise ValueError("rule_config 必须是 dict（从 YAML 加载）")

        self._ind: dict = {}
        for name, spec in (cfg.get("indicators") or {}).items():
            if not isinstance(spec, dict):
                raise ValueError(f"指标 {name} 的 spec 必须是 dict，得到 {type(spec).__name__}")
            line, warn = _build_indicator(self.data, spec)
            if warn:
                raise ValueError(f"指标 '{name}': {warn}")
            self._ind[str(name)] = line

        self._buy_rules: list[str] = [str(x) for x in (cfg.get("buy_when") or [])]
        self._sell_rules: list[str] = [str(x) for x in (cfg.get("sell_when") or [])]
        self._buy_logic: str = str(cfg.get("buy_logic", "any")).lower()
        self._sell_logic: str = str(cfg.get("sell_logic", "any")).lower()
        self._position_size: float = float(cfg.get("position_size", 0.95))

        if not self._buy_rules:
            raise ValueError("rule_config.buy_when 不能为空")
        if not self._sell_rules:
            raise ValueError("rule_config.sell_when 不能为空")
        if self._buy_logic not in ("any", "all"):
            raise ValueError(f"buy_logic 必须是 'any' 或 'all'，得到 {self._buy_logic!r}")
        if self._sell_logic not in ("any", "all"):
            raise ValueError(f"sell_logic 必须是 'any' 或 'all'，得到 {self._sell_logic!r}")

    def next(self):
        if not self.position:
            if self._eval_rules(self._buy_rules, self._buy_logic):
                price = self.data.close[0]
                if price <= 0:
                    return
                size = int(self.broker.getcash() * self._position_size / price)
                if size > 0:
                    self.buy(size=size)
                    self.log(f"规则买入信号 -> {size} 股 @ {price:.2f}")
        else:
            if self._eval_rules(self._sell_rules, self._sell_logic):
                self.close()
                self.log(f"规则卖出信号 -> 全部卖出 @ {self.data.close[0]:.2f}")

    # ------------------ DSL 求值 ------------------

    def _resolve(self, token: str, offset: int = 0):
        """
        把 token 解析为当前 bar（offset=0）或前一 bar（offset=-1）的标量值。
        数字字面量 / OHLCV 字段 / 已注册指标名。
        """
        t = token.strip()
        # 数字字面量
        try:
            return float(t)
        except ValueError:
            pass
        # OHLCV
        ohlcv = {
            "close": self.data.close, "open": self.data.open,
            "high": self.data.high,   "low": self.data.low,
            "volume": self.data.volume,
        }
        if t in ohlcv:
            return float(ohlcv[t][offset])
        # 指标
        if t in self._ind:
            return float(self._ind[t][offset])
        raise ValueError(f"未知标识: {t!r}（既不是数字、也不是 OHLCV 字段、也不是已注册指标）")

    def _eval_cond(self, cond: str) -> bool:
        parts = cond.strip().split()
        if len(parts) != 3:
            raise ValueError(f"条件格式错误（期望 '左 op 右'，空格分隔）: {cond!r}")
        left, op, right = parts
        # cross_up / cross_down 需要前一 bar；前 2 个 bar 不可评估
        if op in _CROSS_OPS:
            if len(self) < 2:
                return False
            l0 = self._resolve(left, 0)
            l1 = self._resolve(left, -1)
            r0 = self._resolve(right, 0)
            r1 = self._resolve(right, -1)
            if op == "cross_up":
                return l0 > r0 and l1 <= r1
            return l0 < r0 and l1 >= r1
        # 普通比较
        if op not in _CMP_OPS:
            raise ValueError(f"未知运算符: {op!r}；支持 {list(_CMP_OPS) + list(_CROSS_OPS)}")
        return bool(_CMP_OPS[op](self._resolve(left, 0), self._resolve(right, 0)))

    def _eval_rules(self, rules: list[str], logic: str) -> bool:
        """对规则列表按 any / all 求值"""
        # 防御：过滤空行
        cleaned = [r for r in rules if r and str(r).strip()]
        if not cleaned:
            return False
        try:
            results = [self._eval_cond(r) for r in cleaned]
        except Exception as e:
            # 规则错误不应整页崩溃，写日志后视为不触发
            # 纯 ASCII，避免 Windows GBK 控制台 UnicodeEncodeError
            self.log(f"[WARN] 规则求值异常: {e}")
            return False
        return any(results) if logic == "any" else all(results)
