"""
src/automation/monitor/batch_signal.py — 批量信号扫描监控器

按"股票池 × 信号"批量扫描，命中后一条规则推一条告警（body 列出命中股票）。
与 PriceMonitor 不同：
  - PriceMonitor：逐只股票 × 单股规则（个性化盯盘）
  - BatchSignalMonitor：股票池 × 信号规则（跑批选股 + 提醒）

配置示例（config/price_alerts.yaml 的 scan_rules 段）：
    scan_rules:
      - id: csi300_oversold
        name: 沪深300 RSI 超卖
        scopes: [csi300]               # 可多选: csi300/sh_main/chinext/watchlist/all
        signal:
          type: rsi_oversold
          params: {threshold: 30, period: 14}
        cooldown_hours: 24
        max_results: 20                  # body 中最多列出 N 只命中股票
        max_codes: 500                   # 单次扫描最多 N 只（防过载）

设计要点：
  - 复用 ScreenerDataProvider 拉数据 + Pass1/Pass2 优化
  - 复用 CONDITION_REGISTRY 评估信号
  - signal 与 PriceMonitor 共享同一份 SIGNAL_DIRECTION 映射，告警标题用 📈/📉/⚖️
  - 每条 scan_rule 一个 event_key 维度，命中股票集合也参与 key（防同信号反复轰炸）
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.automation.alert import AlertEvent, AlertStateStore
from src.automation.monitor.base import BaseMonitor
from src.utils.logger import get_logger

logger = get_logger("batch_signal")


# ============================================================================
# Scope 加载
# ============================================================================

def _load_watchlist_codes() -> set[str]:
    """
    从 config/stocks/{a,hk,us}_stock.yaml 加载关注列表股票代码（仅 A 股，
    其他市场用 ScreenerDataProvider 不支持，暂只支持 A 股扫描）。
    """
    from pathlib import Path

    import yaml

    codes: set[str] = set()
    cfg_dir = Path("./config/stocks")
    for fname in ("a_stock.yaml",):  # 当前 batch scan 仅支持 A 股
        p = cfg_dir / fname
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
                        codes.add(c.zfill(6))
        except Exception as e:
            logger.debug(f"加载 {fname} 失败: {e}")
    return codes


def resolve_scope_codes(scopes: list[str], provider=None) -> set[str] | None:
    """
    解析 scopes 配置项，返回股票代码集合。

    Args:
        scopes: 范围 key 列表，如 ["csi300", "watchlist"]
        provider: ScreenerDataProvider 实例（None 时新建）

    Returns:
        代码集合；包含 'all' 时返回 None（表示不过滤=全 A 股）
    """
    if not scopes or "all" in scopes:
        return None

    if provider is None:
        from src.analysis.screening.data_provider import ScreenerDataProvider
        provider = ScreenerDataProvider()

    # 'watchlist' 单独处理
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


# ============================================================================
# 监控器
# ============================================================================

class BatchSignalMonitor(BaseMonitor):
    """
    批量信号扫描监控器。

    使用：
        monitor = BatchSignalMonitor(scan_rules=[...], channels=[...], ...)
        result = monitor.run()
    """

    name = "batch_signal"

    def __init__(
        self,
        scan_rules: list[dict],
        channels,
        state_store: AlertStateStore | None = None,
        cooldown_hours: int = 24,
        output_dir: str = "./output",
        data_provider=None,
    ):
        super().__init__(channels, state_store, cooldown_hours, output_dir)
        self.scan_rules = scan_rules or []
        self._provider = data_provider

    def _get_provider(self):
        if self._provider is None:
            from src.analysis.screening.data_provider import ScreenerDataProvider
            self._provider = ScreenerDataProvider()
        return self._provider

    def collect_events(self) -> list[AlertEvent]:
        events: list[AlertEvent] = []

        for rule in self.scan_rules:
            try:
                ev = self._scan_one_rule(rule)
                if ev is not None:
                    events.append(ev)
            except Exception as e:
                logger.error(f"[batch] scan_rule '{rule.get('id', '?')}' 异常: {e}",
                             exc_info=True)

        return events

    def _scan_one_rule(self, rule: dict) -> AlertEvent | None:
        rule_id = str(rule.get("id", "")).strip() or "unknown"
        rule_name = rule.get("name", rule_id)
        scopes = rule.get("scopes") or []
        signal_cfg = rule.get("signal") or {}
        signal_type = str(signal_cfg.get("type", "")).strip()
        if not signal_type:
            logger.warning(f"[batch:{rule_id}] 缺少 signal.type，跳过")
            return None

        max_codes = int(rule.get("max_codes", 500))
        max_results = int(rule.get("max_results", 20))

        # 1. 解析股票池
        provider = self._get_provider()
        codes = resolve_scope_codes(scopes, provider)
        # codes None = 全 A 股；codes set 为空 = scope 都没匹配
        if codes is not None and not codes:
            logger.warning(f"[batch:{rule_id}] 股票池为空，跳过")
            return None

        # 拉全 A 股 spot（用于代码 → 名称映射 + 范围过滤）
        try:
            spot_df = provider.get_all_a_shares()
        except Exception as e:
            logger.error(f"[batch:{rule_id}] 全市场行情获取失败: {e}")
            return None
        if spot_df is None or spot_df.empty:
            logger.warning(f"[batch:{rule_id}] 全市场行情为空")
            return None

        # 过滤股票池
        if codes is not None:
            spot_df = spot_df[spot_df["代码"].astype(str).isin(codes)].copy()
        # 限制扫描数量
        if len(spot_df) > max_codes:
            spot_df = spot_df.head(max_codes)
        if spot_df.empty:
            logger.warning(f"[batch:{rule_id}] 过滤后无股票")
            return None

        logger.info(f"[batch:{rule_id}] 扫描 {len(spot_df)} 只股票, 信号={signal_type}")

        # 2. 实例化 Condition
        try:
            from src.analysis.screening.conditions import (
                CONDITION_LABELS,
                CONDITION_REGISTRY,
                get_signal_direction,
                signal_emoji,
            )
            from src.analysis.screening.config_schema import (
                _PARAM_MAP,
                SPOT_ONLY_TYPES,
            )
        except ImportError as e:
            logger.error(f"[batch:{rule_id}] 条件库导入失败: {e}")
            return None

        if signal_type in SPOT_ONLY_TYPES:
            logger.warning(f"[batch:{rule_id}] '{signal_type}' 是 Spot-only，不适合做信号扫描")
            return None

        cls = CONDITION_REGISTRY.get(signal_type)
        if cls is None:
            logger.warning(f"[batch:{rule_id}] 未知信号类型 '{signal_type}'")
            return None

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
            logger.error(f"[batch:{rule_id}] 条件实例化失败 (kwargs={init_kwargs}): {e}")
            return None

        # 3. 按需批量预取 OHLCV
        ohlcv_period = getattr(cls, "ohlcv_period", "daily")
        all_codes = spot_df["代码"].astype(str).tolist()
        try:
            if ohlcv_period == "weekly":
                provider.prefetch_ohlcv_batch(all_codes, period="weekly", max_workers=8)
            else:
                provider.prefetch_ohlcv_batch(all_codes, period="daily", max_workers=8)
        except Exception as e:
            logger.warning(f"[batch:{rule_id}] 批量预取 OHLCV 失败: {e}（将逐只串行拉）")

        # 4. 逐只评估
        hits: list[dict] = []
        for _idx, row in spot_df.iterrows():
            code = str(row.get("代码", "")).strip()
            sname = str(row.get("名称", "")).strip()
            if not code:
                continue
            try:
                if ohlcv_period == "weekly":
                    df_ohlcv = provider.get_weekly_ohlcv(code)
                else:
                    df_ohlcv = provider.get_daily_ohlcv(code)
                if df_ohlcv is None or df_ohlcv.empty:
                    continue
                triggered = bool(cond.evaluate_full(row, df_ohlcv))
            except Exception:
                continue
            if triggered:
                hits.append({"代码": code, "名称": sname,
                             "最新价": float(row.get("最新价", 0) or 0),
                             "涨跌幅": float(row.get("涨跌幅", 0) or 0)})

        if not hits:
            logger.info(f"[batch:{rule_id}] 0 命中")
            return None

        # 5. 构造 AlertEvent
        direction = get_signal_direction(signal_type, raw_params)
        emoji = signal_emoji(direction)
        tag = {"buy": "买点", "sell": "卖点",
               "neutral": "技术信号"}.get(direction, "信号")
        friendly = CONDITION_LABELS.get(signal_type, signal_type)

        # event_key：日期 + scan_id 维度去重（同一天同规则不重发）
        today = datetime.now().strftime("%Y-%m-%d")
        event_key = f"batch:{rule_id}:{today}"

        # body 列出命中股票（限 max_results）
        shown = hits[:max_results]
        more_n = len(hits) - len(shown)
        lines = [
            f"扫描规则：{rule_name}",
            f"股票池：{', '.join(scopes) if scopes else 'all'}",
            f"信号：{friendly} ({signal_type})",
            f"参数：{raw_params or '默认'}",
            "",
            f"命中 {len(hits)} 只" + (f"（仅展示前 {len(shown)}）" if more_n > 0 else ""),
            "─" * 30,
        ]
        for h in shown:
            lines.append(
                f"  {h['名称']}({h['代码']})  最新 {h['最新价']:.2f}  "
                f"涨跌 {h['涨跌幅']:+.2f}%"
            )
        if more_n > 0:
            lines.append(f"  ... 还有 {more_n} 只未列出")

        return AlertEvent(
            title=f"{emoji} 批量{tag}扫描：{rule_name}（命中 {len(hits)} 只）",
            body="\n".join(lines),
            event_key=event_key,
            stock_code="",
            stock_name="",
            event_type=f"batch_signal_{direction}_{signal_type}",
        )

    @staticmethod
    def hits_to_dataframe(hits: list[dict]) -> pd.DataFrame:
        """供 UI 渲染：命中列表 → DataFrame"""
        return pd.DataFrame(hits) if hits else pd.DataFrame(
            columns=["代码", "名称", "最新价", "涨跌幅"]
        )
