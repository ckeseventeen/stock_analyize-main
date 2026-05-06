"""
src/automation/monitor/price_monitor.py — 价格预警监控器

加载 config/price_alerts.yaml 定义的规则，批量查询实时行情，
对满足条件的股票生成 AlertEvent 并推送。

支持的规则类型：
  - price_below / price_above       : 绝对价格阈值
  - pct_change_daily                : 当日涨跌幅超 ±X%
  - pct_from_cost                   : 相对成本价涨跌超 X%
  - ma_break                        : 跌破/突破 N 日均线
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.automation.alert import AlertEvent, AlertStateStore
from src.automation.monitor.base import BaseMonitor
from src.utils.logger import get_logger

logger = get_logger("monitor")


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

            # 2. 如果有 ma_break 规则，才拉日线
            daily_df = None
            if any(c.get("type") == "ma_break" for c in conditions):
                daily_df = self._fetch_ohlcv(code, market)

            # 3. 遍历条件，任一满足即生成事件
            today = datetime.now().strftime("%Y-%m-%d")
            for cond in conditions:
                triggered, desc = _RuleEvaluator.evaluate(
                    cond, price, prev_close, daily_df, cost_basis
                )
                if not triggered:
                    continue

                rule_type = cond.get("type", "unknown")
                event_key = f"{code}:{rule_type}:{today}"

                title = f"{name} 价格预警: {rule_type}"
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
                    event_type=f"price_{rule_type}",
                ))

        return events
