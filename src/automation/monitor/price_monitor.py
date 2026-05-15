"""
src/automation/monitor/price_monitor.py — 价格预警监控器

加载 config/price_alerts.yaml 定义的规则，批量查询实时行情，
对满足条件的股票生成 AlertEvent 并推送。

支持的规则类型：
  - price_below / price_above       : 绝对价格阈值（🔔 价格类）
  - pct_change_daily                : 当日涨跌幅超 ±X%（🔔 价格类）
  - pct_from_cost                   : 相对成本价涨跌超 X%（🔔 价格类）
  - ma_break                        : 跌破/突破 N 日均线（🔔 价格类）
  - signal                          : 买点信号，桥接 28 个筛选条件（📈 买点类）
                                       示例：
                                         type: signal
                                         signal: weekly_macd_divergence
                                         params: {lookback_bars: 60}
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.automation.alert import AlertEvent, AlertStateStore
from src.automation.monitor.base import BaseMonitor
from src.utils.logger import get_logger

logger = get_logger("monitor")


# 哪些 signal condition 需要周线 K，哪些只需日线。
# 调用方据此判断要不要额外拉一份 weekly_df。
def _signal_needs_weekly(condition_type: str) -> bool:
    """从 CONDITION_REGISTRY 取该条件需要的 K 线周期"""
    try:
        from src.analysis.screening.conditions import CONDITION_REGISTRY
        cls = CONDITION_REGISTRY.get(condition_type)
        if cls is None:
            return False
        # 类属性 ohlcv_period: "daily" / "weekly"
        return getattr(cls, "ohlcv_period", "daily") == "weekly"
    except Exception:
        return False


# ========================
# 规则求值器
# ========================

class _RuleEvaluator:
    """
    针对单只股票的单条规则求值。
    返回 (是否触发, 描述文本)。
    """

    @staticmethod
    def evaluate(
        rule: dict,
        price: float,
        prev_close: float | None = None,
        daily_df: pd.DataFrame | None = None,
        cost_basis: float | None = None,
        weekly_df: pd.DataFrame | None = None,
        spot_row: pd.Series | None = None,
    ) -> tuple[bool, str]:
        rtype = rule.get("type", "").lower()

        if rtype == "price_below":
            threshold = float(rule["value"])
            if price > 0 and price < threshold:
                return True, f"当前价 {price:.2f} 低于阈值 {threshold:.2f}"
            return False, ""

        if rtype == "price_above":
            threshold = float(rule["value"])
            if price > threshold:
                return True, f"当前价 {price:.2f} 突破阈值 {threshold:.2f}"
            return False, ""

        if rtype == "pct_change_daily":
            threshold = float(rule["threshold"])  # 可正可负
            if prev_close and prev_close > 0 and price > 0:
                pct = (price - prev_close) / prev_close * 100.0
                # 阈值为正：涨幅超；阈值为负：跌幅超
                if threshold >= 0 and pct >= threshold:
                    return True, f"当日涨幅 {pct:+.2f}%，达到阈值 {threshold:+.2f}%"
                if threshold < 0 and pct <= threshold:
                    return True, f"当日跌幅 {pct:+.2f}%，达到阈值 {threshold:+.2f}%"
            return False, ""

        if rtype == "pct_from_cost":
            threshold = float(rule["threshold"])
            basis = cost_basis if cost_basis is not None else rule.get("cost")
            if basis and float(basis) > 0 and price > 0:
                pct = (price - float(basis)) / float(basis) * 100.0
                if threshold >= 0 and pct >= threshold:
                    return True, f"相对成本 {basis:.2f} 涨 {pct:+.2f}%"
                if threshold < 0 and pct <= threshold:
                    return True, f"相对成本 {basis:.2f} 跌 {pct:+.2f}%"
            return False, ""

        if rtype == "ma_break":
            ma_period = int(rule.get("ma", 20))
            direction = rule.get("direction", "below").lower()  # below / above
            if daily_df is None or daily_df.empty or "收盘" not in daily_df.columns:
                return False, ""
            if len(daily_df) < ma_period + 1:
                return False, ""
            # 以最近 ma_period 个收盘价算 MA
            closes = daily_df["收盘"].astype(float).tail(ma_period)
            ma = closes.mean()
            if direction == "below" and price < ma:
                return True, f"当前价 {price:.2f} 跌破 MA{ma_period} = {ma:.2f}"
            if direction == "above" and price > ma:
                return True, f"当前价 {price:.2f} 突破 MA{ma_period} = {ma:.2f}"
            return False, ""

        # ---------------- signal: 桥接 28 个筛选条件作为买点 ----------------
        if rtype == "signal":
            signal_type = str(rule.get("signal", "")).lower().strip()
            if not signal_type:
                logger.warning("signal 规则缺少 'signal' 字段（指定哪个 condition_type）")
                return False, ""
            try:
                from src.analysis.screening.conditions import CONDITION_REGISTRY
                from src.analysis.screening.config_schema import (
                    _PARAM_MAP,
                    SPOT_ONLY_TYPES,
                )
            except ImportError as e:
                logger.warning(f"signal 条件依赖加载失败: {e}")
                return False, ""

            if signal_type in SPOT_ONLY_TYPES:
                logger.warning(
                    f"signal '{signal_type}' 是 Spot-only 条件（不依赖 K 线），"
                    f"用作买点意义不大；建议用 ma_break / price_below 等价格规则代替"
                )
                return False, ""

            cls = CONDITION_REGISTRY.get(signal_type)
            if cls is None:
                logger.warning(f"未知 signal 类型 '{signal_type}'；可用：{sorted(CONDITION_REGISTRY)}")
                return False, ""

            # 选 K 线：weekly 条件用 weekly_df，否则用 daily_df
            ohlcv_period = getattr(cls, "ohlcv_period", "daily")
            target_df = weekly_df if ohlcv_period == "weekly" else daily_df
            if target_df is None or target_df.empty:
                return False, ""

            # 列名归一化（Condition 内部多数能识别中英文，但提前归一更稳）
            try:
                from src.core.columns import normalize_ohlcv_columns
                target_df = normalize_ohlcv_columns(target_df)
            except Exception:
                pass

            # 用 _PARAM_MAP 把 yaml 风格的 params 翻译成 init kwargs
            param_map = _PARAM_MAP.get(signal_type, {})
            raw_params = dict(rule.get("params") or {})
            init_kwargs = {}
            for yaml_key, init_key in param_map.items():
                if yaml_key in raw_params:
                    init_kwargs[init_key] = raw_params[yaml_key]
            # 也允许直接传 init key（向后兼容）
            for k, v in raw_params.items():
                if k not in param_map and k not in init_kwargs:
                    init_kwargs[k] = v

            try:
                cond = cls(**init_kwargs)
            except Exception as e:
                logger.warning(f"signal '{signal_type}' 实例化失败 (params={init_kwargs}): {e}")
                return False, ""

            # spot_row：尽量构造（用最新价 + 昨收占位即可）
            row = spot_row if spot_row is not None else pd.Series({"代码": "", "最新价": price})

            try:
                triggered = bool(cond.evaluate_full(row, target_df))
            except Exception as e:
                logger.debug(f"signal '{signal_type}' 评估异常: {e}")
                return False, ""

            if triggered:
                # 用统一 SIGNAL_DIRECTION 决定方向
                from src.analysis.screening.conditions import (
                    CONDITION_LABELS,
                    get_signal_direction,
                    signal_emoji,
                )
                direction = get_signal_direction(signal_type, raw_params)
                emoji = signal_emoji(direction)
                tag = {
                    "buy":     "买点信号",
                    "sell":    "卖点信号",
                    "neutral": "技术信号",
                }.get(direction, "技术信号")
                friendly = CONDITION_LABELS.get(signal_type, signal_type)
                params_brief = ", ".join(f"{k}={v}" for k, v in raw_params.items()) or "默认参数"
                return True, f"{emoji} {tag}：{friendly}（{params_brief}）"
            return False, ""

        logger.warning(f"未知规则类型: {rtype}，忽略")
        return False, ""


# ========================
# 市场数据适配
# ========================

def _fetch_spot_price(code: str, market: str) -> tuple[float, float | None]:
    """
    获取实时价和昨收（用于涨跌幅计算）。

    Args:
        code: 股票代码
        market: a / hk / us

    Returns:
        (price, prev_close)；失败时 (0.0, None)
    """
    try:
        if market == "a":
            import akshare as ak
            try:
                # 尝试用大礼包全量获取
                df = ak.stock_zh_a_spot_em()
                row = df[df["代码"] == code]
                if not row.empty:
                    price = float(row.iloc[0]["最新价"])
                    pct = float(row.iloc[0]["涨跌幅"])
                    prev = price / (1 + pct / 100.0) if pct != 0 else price
                    return price, prev
            except Exception as e:
                logger.debug(f"东财接口受限，切换为单股通道(雪球): {e}")

            # 雪球单只股票极速接口 fallback
            prefix = "SH" if code.startswith("6") else "SZ" if code.startswith(("0", "3")) else "BJ"
            symbol = f"{prefix}{code}"
            df_xq = ak.stock_individual_spot_xq(symbol=symbol)
            if df_xq is not None and not df_xq.empty:
                price_val = df_xq.loc[df_xq['item'] == '现价', 'value'].values
                prev_val = df_xq.loc[df_xq['item'] == '昨收', 'value'].values
                if len(price_val) > 0 and price_val[0] is not None:
                    price = float(price_val[0])
                    prev = float(prev_val[0]) if len(prev_val) > 0 and prev_val[0] is not None else None
                    if price > 0:
                        return price, prev
            return 0.0, None

        if market == "hk":
            import akshare as ak
            df = ak.stock_hk_spot_em()
            row = df[df["代码"] == code]
            if row.empty:
                return 0.0, None
            price = float(row.iloc[0]["最新价"])
            pct = float(row.iloc[0].get("涨跌幅", 0))
            prev = price / (1 + pct / 100.0) if pct != 0 else price
            return price, prev

        if market == "us":
            import akshare as ak
            df = ak.stock_us_spot_em()
            row = df[df["代码"] == code]
            if row.empty:
                return 0.0, None
            price = float(row.iloc[0]["最新价"])
            pct = float(row.iloc[0].get("涨跌幅", 0))
            prev = price / (1 + pct / 100.0) if pct != 0 else price
            return price, prev
    except Exception as e:
        logger.warning(f"[{market}-{code}] 实时行情获取失败: {e}")

    return 0.0, None


def _fetch_daily_ohlcv(code: str, market: str, days_back: int = 120) -> pd.DataFrame:
    """为 MA 计算获取日线数据（仅在规则需要时才调用）

    A 股优先使用 Baostock（稳定不受代理影响），其余市场走 akshare。
    """
    # A 股优先走 Baostock
    if market == "a":
        try:
            from src.data.providers.baostock_provider import BaostockProvider
            with BaostockProvider() as bp:
                df = bp.get_k_data(code, days_back=days_back, frequency="d",
                                   fields="date,open,high,low,close,volume")
                if df is not None and not df.empty:
                    # 重命名列以兼容 _RuleEvaluator（需要 "收盘" 列）
                    df = df.rename(columns={"close": "收盘", "open": "开盘",
                                            "high": "最高", "low": "最低",
                                            "date": "日期", "volume": "成交量"})
                    logger.debug(f"[a-{code}] Baostock日线获取成功，共 {len(df)} 条")
                    return df
        except Exception as e:
            logger.debug(f"[a-{code}] Baostock日线失败，回退akshare: {e}")

    # akshare fallback（A股）/ 港股 / 美股
    try:
        import akshare as ak
        end_date = pd.Timestamp.now().strftime("%Y%m%d")
        start_date = (pd.Timestamp.now() - pd.Timedelta(days=days_back)).strftime("%Y%m%d")

        if market == "a":
            return ak.stock_zh_a_hist(symbol=code, period="daily",
                                      start_date=start_date, end_date=end_date, adjust="qfq")
        if market == "hk":
            return ak.stock_hk_hist(symbol=code, period="daily",
                                    start_date=start_date, end_date=end_date, adjust="qfq")
        if market == "us":
            return ak.stock_us_hist(symbol=code, period="daily",
                                    start_date=start_date, end_date=end_date, adjust="qfq")
    except Exception as e:
        logger.warning(f"[{market}-{code}] 日线数据获取失败: {e}")
    return pd.DataFrame()


# ========================
# 价格监控主类
# ========================

class PriceMonitor(BaseMonitor):
    """
    价格预警监控器（可被 CLI 或 APScheduler 调用）。

    典型用法：
        monitor = PriceMonitor(rules=cfg["rules"], channels=channels, state_store=store)
        result = monitor.run()
    """

    name = "price_monitor"

    def __init__(
        self,
        rules: list[dict],
        channels,
        state_store: AlertStateStore | None = None,
        cooldown_hours: int = 24,
        output_dir: str = "./output",
        # 可注入 price/ohlcv 查询函数，便于测试 mock
        price_fetcher=None,
        ohlcv_fetcher=None,
    ):
        super().__init__(channels, state_store, cooldown_hours, output_dir)
        self.rules = rules or []
        # 默认使用真实 akshare；测试时可注入 mock
        self._fetch_price = price_fetcher or _fetch_spot_price
        self._fetch_ohlcv = ohlcv_fetcher or _fetch_daily_ohlcv

    def collect_events(self) -> list[AlertEvent]:
        events: list[AlertEvent] = []

        for stock_rule in self.rules:
            code = str(stock_rule.get("code", "")).strip()
            market = str(stock_rule.get("market", "a")).lower()
            name = stock_rule.get("name", code)
            cost_basis = stock_rule.get("cost")
            conditions = stock_rule.get("conditions", [])

            if not code or not conditions:
                continue

            # 1. 实时价（所有规则都需要）
            price, prev_close = self._fetch_price(code, market)
            if price <= 0:
                logger.debug(f"[{market}-{code}] 跳过：无实时价")
                continue

            # 2. 智能按需拉 K 线：判断需要日线 / 周线
            needs_daily = any(c.get("type") == "ma_break" for c in conditions)
            needs_weekly = False
            for c in conditions:
                if c.get("type") == "signal":
                    sig = str(c.get("signal", "")).lower()
                    if _signal_needs_weekly(sig):
                        needs_weekly = True
                    else:
                        needs_daily = True

            daily_df = None
            weekly_df = None
            if needs_daily or needs_weekly:
                daily_df = self._fetch_ohlcv(code, market)
            # 周线由日线重采样
            if needs_weekly and daily_df is not None and not daily_df.empty:
                try:
                    weekly_df = self._resample_weekly(daily_df)
                except Exception as e:
                    logger.debug(f"[{market}-{code}] 周线重采样失败: {e}")

            # 3. 遍历条件，任一满足即生成事件
            today = datetime.now().strftime("%Y-%m-%d")
            # 构造一个最简 spot_row，避免 signal 评估时拿不到代码
            spot_row = pd.Series({"代码": code, "名称": name, "最新价": price})

            for cond in conditions:
                triggered, desc = _RuleEvaluator.evaluate(
                    cond, price, prev_close, daily_df, cost_basis,
                    weekly_df=weekly_df, spot_row=spot_row,
                )
                if not triggered:
                    continue

                rule_type = cond.get("type", "unknown")
                # signal 规则用 signal 名做事件 key 区分（同一股票多个 signal 互不冲突）
                if rule_type == "signal":
                    from src.analysis.screening.conditions import (
                        get_signal_direction,
                        signal_emoji,
                    )
                    sig_id = str(cond.get("signal", "")).lower() or "unknown"
                    direction = get_signal_direction(sig_id, cond.get("params") or {})
                    emoji = signal_emoji(direction)
                    tag = {
                        "buy":     "买点信号",
                        "sell":    "卖点信号",
                        "neutral": "技术信号",
                    }.get(direction, "技术信号")
                    event_key = f"{code}:signal:{sig_id}:{today}"
                    title = f"{emoji} {name} {tag}：{sig_id}"
                    event_type = f"signal_{direction}_{sig_id}"
                else:
                    event_key = f"{code}:{rule_type}:{today}"
                    title = f"🔔 {name} 价格预警：{rule_type}"
                    event_type = f"price_{rule_type}"

                body_lines = [
                    desc,
                    f"股票: {name} ({code})  市场: {market.upper()}",
                ]
                if prev_close:
                    body_lines.append(f"昨收: {prev_close:.2f}  当前: {price:.2f}")
                if cost_basis:
                    body_lines.append(f"成本: {cost_basis}")

                events.append(AlertEvent(
                    title=title,
                    body="\n".join(body_lines),
                    event_key=event_key,
                    stock_code=code,
                    stock_name=name,
                    event_type=event_type,
                ))

        return events

    @staticmethod
    def _resample_weekly(daily_df: pd.DataFrame) -> pd.DataFrame:
        """
        日线 → 周线 OHLCV 重采样（W-FRI）。容忍中英文列名。
        """
        df = daily_df.copy()
        # 列名归一化为中文（与下游 Condition 兼容）
        rename_zh = {
            "open": "开盘", "high": "最高", "low": "最低",
            "close": "收盘", "volume": "成交量", "date": "日期",
        }
        df = df.rename(columns={k: v for k, v in rename_zh.items() if k in df.columns})

        # 找日期列
        date_col = "日期" if "日期" in df.columns else None
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            df = df.dropna(subset=[date_col]).set_index(date_col)
        elif not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, errors="coerce")

        agg = {}
        if "开盘" in df.columns:
            agg["开盘"] = "first"
        if "最高" in df.columns:
            agg["最高"] = "max"
        if "最低" in df.columns:
            agg["最低"] = "min"
        if "收盘" in df.columns:
            agg["收盘"] = "last"
        if "成交量" in df.columns:
            agg["成交量"] = "sum"

        wk = df.resample("W-FRI").agg(agg).dropna(how="all")
        return wk
