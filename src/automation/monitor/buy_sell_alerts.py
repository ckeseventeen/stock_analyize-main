"""
src/automation/monitor/buy_sell_alerts.py — 买入/卖出信号预警监控器

**唯一的预警入口**（替代旧 PriceMonitor + BatchSignalMonitor）。

数据模型：
    buy_alerts:    list[AlertRule]   # 命中后推送 📈 买入提醒
    sell_alerts:   list[AlertRule]   # 命中后推送 📉 卖出提醒

每条 AlertRule 形如：
    id: gzmt_oversold              # 唯一标识
    name: 贵州茅台 RSI 超卖         # 可读名

    # 股票范围（二选一）
    code: '600519'                 # 单股盯盘
    market: a                      # 默认 a
    # 或：
    scopes: [csi300, watchlist]    # 批量扫描股票池（取并集）

    signal:                         # 必填：信号定义
      type: rsi_oversold             # 复用 28 个 CONDITION_REGISTRY
      params: {threshold: 30, period: 14}

    cooldown_hours: 24
    # 批量模式专属
    max_results: 20                  # 推送 body 最多列出 N 只
    max_codes: 500                   # 单次扫描上限

输出：
  - 单股 + 命中 → 1 个 AlertEvent，title 含股票名
  - 批量 + 命中 N 只 → 1 个 AlertEvent 聚合，body 列出股票
  - 0 命中 → 不发事件
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.automation.alert import AlertEvent, AlertStateStore
from src.automation.monitor.base import BaseMonitor
from src.utils.logger import get_logger

logger = get_logger("buy_sell_alerts")


def _is_weekly_condition(condition_type: str) -> bool:
    """从 CONDITION_REGISTRY 取 ohlcv_period"""
    try:
        from src.analysis.screening.conditions import CONDITION_REGISTRY
        cls = CONDITION_REGISTRY.get(condition_type)
        if cls is None:
            return False
        return getattr(cls, "ohlcv_period", "daily") == "weekly"
    except Exception:
        return False


def _load_watchlist_codes() -> set[str]:
    """从 config/stocks/*.yaml 加载所有市场的关注列表代码（BUG-4 修复：原仅加载 A 股）"""
    from pathlib import Path

    import yaml

    codes: set[str] = set()
    # 遍历所有市场配置文件
    market_files = [
        ("a", Path("./config/stocks/a_stock.yaml"), 6),
        ("hk", Path("./config/stocks/hk_stock.yaml"), 5),
        ("us", Path("./config/stocks/us_stock.yaml"), 0),
    ]
    for market_key, p, pad_width in market_files:
        if not p.exists():
            continue
        try:
            with open(p, encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            for cat in (cfg.get("categories") or {}).values():
                if not isinstance(cat, dict):
                    continue
                for stock in cat.get("stocks", []) or []:
                    c = str(stock.get("code", "")).strip()
                    if c:
                        # A 股补零到 6 位，港股补零到 5 位，美股不补零
                        codes.add(c.zfill(pad_width) if pad_width > 0 else c)
        except Exception as e:
            logger.debug(f"加载 {market_key} 关注列表失败: {e}")
    return codes


def resolve_scope_codes(scopes: list[str], provider=None) -> set[str] | None:
    """
    scopes → 股票代码集合。
      - ["all"] / [] → None（不过滤）
      - 含 "watchlist" → 从 yaml 加载
      - 其他 → 调 ScreenerDataProvider.get_scope_codes
    """
    if not scopes or "all" in scopes:
        return None

    if provider is None:
        from src.analysis.screening.data_provider import ScreenerDataProvider
        provider = ScreenerDataProvider()

    use_watchlist = "watchlist" in scopes
    other_scopes = [s for s in scopes if s not in ("watchlist", "all")]

    codes: set[str] = set()
    if other_scopes:
        sub = provider.get_scope_codes(other_scopes)
        if sub:
            codes.update(sub)
    if use_watchlist:
        codes.update(_load_watchlist_codes())

    return codes if codes else set()


class BuySellAlertMonitor(BaseMonitor):
    """
    买入/卖出信号预警监控器。

    使用：
        monitor = BuySellAlertMonitor(
            buy_alerts=cfg["buy_alerts"],
            sell_alerts=cfg["sell_alerts"],
            channels=channels, state_store=store,
        )
        result = monitor.run()
    """

    name = "buy_sell_alerts"

    def __init__(
        self,
        buy_alerts: list[dict],
        sell_alerts: list[dict],
        channels,
        state_store: AlertStateStore | None = None,
        cooldown_hours: int = 24,
        output_dir: str = "./output",
        data_provider=None,
    ):
        super().__init__(channels, state_store, cooldown_hours, output_dir)
        self.buy_alerts = buy_alerts or []
        self.sell_alerts = sell_alerts or []
        self._provider = data_provider

    def _get_provider(self):
        if self._provider is None:
            from src.analysis.screening.data_provider import ScreenerDataProvider
            self._provider = ScreenerDataProvider()
        return self._provider

    def collect_events(self) -> list[AlertEvent]:
        events: list[AlertEvent] = []

        # TRIG-2 修复：预拉一次全市场行情，多条 batch 规则共享
        self._cached_spot_df = None
        try:
            provider = self._get_provider()
            self._cached_spot_df = provider.get_all_a_shares()
        except Exception as e:
            logger.warning(f"[预拉全市场行情失败] {e}")

        for rule in self.buy_alerts:
            # 跳过 enabled=False 的规则（保留配置但临时不跑）
            if not rule.get("enabled", True):
                logger.debug(f"[buy_alert] '{rule.get('id', '?')}' 已禁用，跳过")
                continue
            try:
                ev = self._eval_rule(rule, direction="buy")
                if ev is not None:
                    events.append(ev)
            except Exception as e:
                logger.error(f"[buy_alert] '{rule.get('id', '?')}' 异常: {e}",
                             exc_info=True)

        for rule in self.sell_alerts:
            if not rule.get("enabled", True):
                logger.debug(f"[sell_alert] '{rule.get('id', '?')}' 已禁用，跳过")
                continue
            try:
                ev = self._eval_rule(rule, direction="sell")
                if ev is not None:
                    events.append(ev)
            except Exception as e:
                logger.error(f"[sell_alert] '{rule.get('id', '?')}' 异常: {e}",
                             exc_info=True)

        self._cached_spot_df = None  # 释放内存
        return events

    def test_single_rule(self, rule: dict, direction: str) -> AlertEvent | None:
        """
        测试单条规则（前端「🧪 测试单条」按钮用）。
        无视 enabled 字段，不写状态、不发推送，仅评估并返回 AlertEvent（命中）或 None。
        """
        return self._eval_rule(rule, direction=direction)

    # ------------------------------------------------------------------
    # 单条规则评估
    # ------------------------------------------------------------------

    def _eval_rule(self, rule: dict, direction: str) -> AlertEvent | None:
        """评估单条规则；自动判断单股 / 批量模式"""
        rule_id = str(rule.get("id", "")).strip() or "unknown"
        signal_cfg = rule.get("signal") or {}
        signal_type = str(signal_cfg.get("type", "")).strip()
        if not signal_type:
            logger.warning(f"[{direction}:{rule_id}] 缺少 signal.type，跳过")
            return None

        # 模式判断：code 优先（单股），否则用 scopes（批量）
        code = str(rule.get("code", "")).strip()
        scopes = rule.get("scopes") or []

        if code:
            return self._eval_single(rule, direction, signal_type, signal_cfg)
        if scopes:
            return self._eval_batch(rule, direction, signal_type, signal_cfg)

        logger.warning(f"[{direction}:{rule_id}] 既无 code 也无 scopes，跳过")
        return None

    # ------------------------------------------------------------------
    # 单股模式
    # ------------------------------------------------------------------

    def _eval_single(self, rule: dict, direction: str,
                     signal_type: str, signal_cfg: dict) -> AlertEvent | None:
        rule_id = rule.get("id", "")
        code = str(rule.get("code", "")).strip()
        market = str(rule.get("market", "a")).lower()
        name = rule.get("name", code)

        # 拉数据（用 provider 自带缓存）
        provider = self._get_provider()
        try:
            if _is_weekly_condition(signal_type):
                df = provider.get_weekly_ohlcv(code)
            else:
                df = provider.get_daily_ohlcv(code)
        except Exception as e:
            logger.debug(f"[{direction}:{rule_id}] 拉 K 线失败: {e}")
            return None

        if df is None or df.empty:
            return None

        # 构造 spot_row 给 evaluate_full
        last_row = pd.Series({
            "代码": code, "名称": name,
            "最新价": float(df.iloc[-1].get("close", df.iloc[-1].get("收盘", 0)) or 0),
        })

        triggered = self._evaluate_signal(signal_type, signal_cfg, last_row, df)
        if not triggered:
            return None

        # 构造 event
        return self._build_event(
            direction=direction,
            rule_id=rule_id,
            rule_name=rule.get("name", code),
            signal_type=signal_type,
            signal_params=signal_cfg.get("params") or {},
            stock_code=code,
            stock_name=name,
            hits=[{"代码": code, "名称": name,
                   "最新价": float(last_row["最新价"])}],
            scope_desc=f"{market.upper()} 单股盯盘",
            is_batch=False,
            max_results=1,
        )

    # ------------------------------------------------------------------
    # 批量模式
    # ------------------------------------------------------------------

    def _eval_batch(self, rule: dict, direction: str,
                    signal_type: str, signal_cfg: dict) -> AlertEvent | None:
        rule_id = rule.get("id", "")
        rule_name = rule.get("name", rule_id)
        scopes = rule.get("scopes") or []
        max_codes = int(rule.get("max_codes", 500))
        max_results = int(rule.get("max_results", 20))

        provider = self._get_provider()
        codes = resolve_scope_codes(scopes, provider)
        if codes is not None and not codes:
            logger.warning(f"[{direction}:{rule_id}] 股票池为空")
            return None

        # TRIG-2 修复：优先使用 collect_events 预拉的缓存
        spot_df = getattr(self, '_cached_spot_df', None)
        if spot_df is None:
            try:
                spot_df = provider.get_all_a_shares()
            except Exception as e:
                logger.error(f"[{direction}:{rule_id}] 全市场行情失败: {e}")
                return None
        if spot_df is None or spot_df.empty:
            return None

        if codes is not None:
            spot_df = spot_df[spot_df["代码"].astype(str).isin(codes)].copy()
        if len(spot_df) > max_codes:
            spot_df = spot_df.head(max_codes)
        if spot_df.empty:
            return None

        logger.info(f"[{direction}:{rule_id}] 扫描 {len(spot_df)} 只，信号={signal_type}")

        # 批量预取 K 线
        all_codes = spot_df["代码"].astype(str).tolist()
        try:
            period = "weekly" if _is_weekly_condition(signal_type) else "daily"
            provider.prefetch_ohlcv_batch(all_codes, period=period, max_workers=8)
        except Exception as e:
            logger.warning(f"[{direction}:{rule_id}] 批量预取失败: {e}")

        # 逐只评估
        hits: list[dict] = []
        for _, row in spot_df.iterrows():
            code = str(row.get("代码", "")).strip()
            if not code:
                continue
            try:
                if _is_weekly_condition(signal_type):
                    df_ohlcv = provider.get_weekly_ohlcv(code)
                else:
                    df_ohlcv = provider.get_daily_ohlcv(code)
                if df_ohlcv is None or df_ohlcv.empty:
                    continue
                if self._evaluate_signal(signal_type, signal_cfg, row, df_ohlcv):
                    hits.append({
                        "代码": code,
                        "名称": str(row.get("名称", "")).strip(),
                        "最新价": float(row.get("最新价", 0) or 0),
                        "涨跌幅": float(row.get("涨跌幅", 0) or 0),
                    })
            except Exception:
                continue

        if not hits:
            logger.info(f"[{direction}:{rule_id}] 0 命中")
            return None

        scope_desc = ", ".join(scopes)
        return self._build_event(
            direction=direction,
            rule_id=rule_id,
            rule_name=rule_name,
            signal_type=signal_type,
            signal_params=signal_cfg.get("params") or {},
            stock_code="",
            stock_name="",
            hits=hits,
            scope_desc=f"股票池 [{scope_desc}]",
            is_batch=True,
            max_results=max_results,
        )

    # ------------------------------------------------------------------
    # 共用：评估信号 + 构造事件
    # ------------------------------------------------------------------

    @staticmethod
    def _evaluate_signal(signal_type: str, signal_cfg: dict,
                         spot_row: pd.Series, ohlcv_df: pd.DataFrame) -> bool:
        """实例化 Condition 并评估"""
        try:
            from src.analysis.screening.conditions import CONDITION_REGISTRY
            from src.analysis.screening.config_schema import (
                _PARAM_MAP,
                SPOT_ONLY_TYPES,
            )
        except ImportError:
            return False

        if signal_type in SPOT_ONLY_TYPES:
            logger.warning(
                f"signal '{signal_type}' 是 Spot-only，不适合做买/卖预警"
            )
            return False
        cls = CONDITION_REGISTRY.get(signal_type)
        if cls is None:
            logger.warning(f"未知 signal '{signal_type}'")
            return False

        raw_params = dict(signal_cfg.get("params") or {})
        param_map = _PARAM_MAP.get(signal_type, {})
        init_kwargs = {}
        for yaml_key, init_key in param_map.items():
            if yaml_key in raw_params:
                init_kwargs[init_key] = raw_params[yaml_key]
        for k, v in raw_params.items():
            if k not in param_map and k not in init_kwargs:
                init_kwargs[k] = v

        try:
            cond = cls(**init_kwargs)
        except Exception as e:
            logger.warning(f"signal '{signal_type}' 实例化失败: {e}")
            return False

        # 列名归一化
        try:
            from src.core.columns import normalize_ohlcv_columns
            ohlcv_df = normalize_ohlcv_columns(ohlcv_df)
        except Exception:
            pass

        try:
            return bool(cond.evaluate_full(spot_row, ohlcv_df))
        except Exception as e:
            logger.debug(f"signal '{signal_type}' 评估异常: {e}")
            return False

    @staticmethod
    def _build_event(
        *,
        direction: str,
        rule_id: str,
        rule_name: str,
        signal_type: str,
        signal_params: dict,
        stock_code: str,
        stock_name: str,
        hits: list[dict],
        scope_desc: str,
        is_batch: bool,
        max_results: int,
    ) -> AlertEvent:
        """统一构造 AlertEvent"""
        from src.analysis.screening.conditions import (
            CONDITION_LABELS,
            get_signal_direction,
        )

        # direction 是 buy/sell；signal 自身方向用于校验和图标
        sig_direction = get_signal_direction(signal_type, signal_params)
        # 如果用户把"卖出信号"放进 buy_alerts，仍然按 buy 推送（用户意图优先）
        emoji = "📈" if direction == "buy" else "📉"
        tag = "买入" if direction == "buy" else "卖出"
        friendly = CONDITION_LABELS.get(signal_type, signal_type)

        # TRIG-1 修复：event_key 不再嵌入日期，避免每天零点后重复触发。
        # 冷却去重完全由 AlertStateStore.was_fired() 的时间差计算负责。
        event_key = f"{direction}_alert:{rule_id}"

        if is_batch:
            shown = hits[:max_results]
            more_n = len(hits) - len(shown)
            lines = [
                f"规则：{rule_name}",
                f"扫描范围：{scope_desc}",
                f"信号：{friendly} ({signal_type})",
                f"参数：{signal_params or '默认'}",
                f"信号方向：{sig_direction}",
                "",
                f"命中 {len(hits)} 只" + (f"（仅展示前 {len(shown)}）" if more_n > 0 else ""),
                "─" * 30,
            ]
            for h in shown:
                lines.append(
                    f"  {h.get('名称', '')}({h.get('代码', '')})  "
                    f"最新 {h.get('最新价', 0):.2f}  "
                    f"涨跌 {h.get('涨跌幅', 0):+.2f}%"
                )
            if more_n > 0:
                lines.append(f"  ... 还有 {more_n} 只未列出")
            title = f"{emoji} {tag}预警：{rule_name}（命中 {len(hits)} 只）"
            body = "\n".join(lines)
        else:
            # 单股
            h = hits[0]
            title = f"{emoji} {tag}预警：{h.get('名称') or h.get('代码', '')}"
            body = (
                f"规则：{rule_name}\n"
                f"股票：{h.get('名称', '')} ({h.get('代码', '')})\n"
                f"信号：{friendly} ({signal_type})\n"
                f"参数：{signal_params or '默认'}\n"
                f"最新价：{h.get('最新价', 0):.2f}\n"
                f"信号方向：{sig_direction}"
            )

        return AlertEvent(
            title=title, body=body,
            event_key=event_key,
            stock_code=stock_code,
            stock_name=stock_name,
            event_type=f"{direction}_alert_{signal_type}",
        )
