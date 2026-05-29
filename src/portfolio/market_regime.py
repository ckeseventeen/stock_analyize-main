"""
src/portfolio/market_regime.py — L4 大盘环境守门员

判断当前大盘 / 板块所处的趋势状态。
机构在板块崩盘时，先看 L4 再看个股 — 板块整体跌不要硬扛单股技术信号。

三个核心指数：
  - 沪深300 (000300)         整体大盘 / 价值股
  - 创业板指 (399006)        成长股 / 科技股暴露
  - 科创50 (000688)          硬科技 / 半导体

每个指数判断：
  - 是否站上 60 日线（中期趋势）
  - 是否站上 250 日线（年线，长期趋势）
  - 5 日 / 20 日跌幅

综合给出 regime: bull / sideways / bear，并附加权重系数（bear 时 sell 信号 ×1.5）。

数据源：复用 ScreenerDataProvider 的 K 线兜底链（akshare → pytdx → Baostock）。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("market_regime")


# 主要指数（pytdx 用，K 线接口）
# 沪深300 是 SH 000300, 创业板 SZ 399006, 科创50 SH 000688
# 通过 akshare 的 stock_zh_index_daily 或 pytdx 拿
INDEX_DEFINITIONS: list[dict] = [
    {"code": "000300", "name": "沪深300",   "type": "broad"},
    {"code": "399006", "name": "创业板指", "type": "growth"},
    {"code": "000688", "name": "科创50",    "type": "tech"},
]


@dataclass
class IndexState:
    code: str
    name: str
    type: str
    current: float                  # 当前点位
    ma60: Optional[float] = None
    ma250: Optional[float] = None
    chg_5d_pct: Optional[float] = None
    chg_20d_pct: Optional[float] = None
    above_ma60: Optional[bool] = None
    above_ma250: Optional[bool] = None


@dataclass
class MarketRegime:
    """
    大盘综合判定

    Attributes:
        regime: "bull" / "sideways" / "bear"
        weight_multiplier: sell 信号权重乘数（bull=1.0, sideways=1.2, bear=1.5）
        summary: 中文摘要（用户友好）
        indexes: 各指数状态
    """
    regime: str
    weight_multiplier: float
    summary: str
    indexes: list[IndexState] = field(default_factory=list)

    @property
    def is_bear(self) -> bool:
        return self.regime == "bear"

    @property
    def emoji(self) -> str:
        return {"bull": "🟢", "sideways": "🟡", "bear": "🔴"}.get(self.regime, "⚪")


class MarketRegimeAnalyzer:
    """
    大盘环境分析器。3 个指数加权判断 → regime 三档。

    用法:
        analyzer = MarketRegimeAnalyzer()
        regime = analyzer.analyze()
        if regime.is_bear:
            # 提示用户：当前空头市场，所有 sell 信号 ×1.5
            pass
    """

    def __init__(self, data_provider=None):
        """
        Args:
            data_provider: ScreenerDataProvider 实例（None 时延迟创建）
        """
        self._provider = data_provider

    def _ensure_provider(self):
        if self._provider is None:
            from src.analysis.screening.data_provider import ScreenerDataProvider
            self._provider = ScreenerDataProvider()
        return self._provider

    @staticmethod
    def _fetch_index_kline(provider, code: str, days_back: int = 365) -> pd.DataFrame:
        """
        拉指数日线。优先 akshare 的 stock_zh_index_daily_em（专门给指数用），
        失败回退到普通的 get_daily_ohlcv（pytdx 也能拉指数代码）。
        """
        # akshare 指数日线
        try:
            import akshare as ak
            df = ak.stock_zh_index_daily_em(symbol="sz" + code if code.startswith(("3", "39")) else "sh" + code)
            if df is not None and not df.empty and len(df) >= days_back // 2:
                df.rename(columns={"date": "日期", "open": "开盘", "high": "最高",
                                    "low": "最低", "close": "收盘", "volume": "成交量"},
                          inplace=True)
                df["日期"] = pd.to_datetime(df["日期"])
                return df.tail(days_back)
        except Exception as e:
            logger.debug(f"akshare 指数 {code} 失败: {e}")

        # 兜底：用 ScreenerDataProvider 拉（pytdx 支持指数代码）
        try:
            df = provider.get_daily_ohlcv(code, days_back=days_back)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.debug(f"provider 指数 {code} 失败: {e}")

        return pd.DataFrame()

    def _analyze_one(self, code: str, name: str, type_: str) -> Optional[IndexState]:
        df = self._fetch_index_kline(self._ensure_provider(), code, days_back=365)
        if df is None or df.empty or len(df) < 60:
            return None

        close_col = "收盘" if "收盘" in df.columns else "close"
        closes = pd.to_numeric(df[close_col], errors="coerce").dropna()
        if len(closes) < 60:
            return None

        cur = float(closes.iloc[-1])
        ma60 = float(closes.rolling(60).mean().iloc[-1])
        ma250 = float(closes.rolling(250).mean().iloc[-1]) if len(closes) >= 250 else None

        chg_5d = float((closes.iloc[-1] / closes.iloc[-6] - 1) * 100) if len(closes) >= 6 else None
        chg_20d = float((closes.iloc[-1] / closes.iloc[-21] - 1) * 100) if len(closes) >= 21 else None

        return IndexState(
            code=code, name=name, type=type_,
            current=cur,
            ma60=ma60, ma250=ma250,
            chg_5d_pct=chg_5d, chg_20d_pct=chg_20d,
            above_ma60=cur > ma60 if ma60 else None,
            above_ma250=cur > ma250 if ma250 else None,
        )

    def analyze(self) -> MarketRegime:
        """主入口：返回综合大盘判定"""
        states: list[IndexState] = []
        for idx_def in INDEX_DEFINITIONS:
            try:
                state = self._analyze_one(idx_def["code"], idx_def["name"], idx_def["type"])
                if state is not None:
                    states.append(state)
            except Exception as e:
                logger.warning(f"指数 {idx_def['code']} 分析失败: {e}")

        if not states:
            logger.warning("所有指数都拉不到数据，默认 sideways")
            return MarketRegime(
                regime="sideways",
                weight_multiplier=1.0,
                summary="⚠️ 无法获取指数数据，无法判断大盘环境",
                indexes=[],
            )

        # 评分：每个指数贡献分
        # +2 站上 MA250、+1 站上 MA60、-1 跌破 MA60、-2 跌破 MA250
        # 同时 5 日跌幅 < -5% 减 1，20 日跌幅 < -10% 减 1
        score = 0
        for s in states:
            if s.above_ma250 is True:
                score += 2
            elif s.above_ma250 is False:
                score -= 2
            if s.above_ma60 is True:
                score += 1
            elif s.above_ma60 is False:
                score -= 1
            if s.chg_5d_pct is not None and s.chg_5d_pct < -5:
                score -= 1
            if s.chg_20d_pct is not None and s.chg_20d_pct < -10:
                score -= 1

        # 满分: 3 个指数 × 3 = 9。
        # 区间：≥ 5 = bull, -3 ~ 4 = sideways, ≤ -4 = bear
        if score >= 5:
            regime = "bull"
            multiplier = 1.0
        elif score <= -4:
            regime = "bear"
            multiplier = 1.5
        else:
            regime = "sideways"
            multiplier = 1.2

        # 构造摘要
        lines = []
        for s in states:
            ma_status = []
            if s.above_ma60 is True:
                ma_status.append("站上 MA60")
            elif s.above_ma60 is False:
                ma_status.append("**跌破 MA60**")
            if s.above_ma250 is True:
                ma_status.append("站上年线")
            elif s.above_ma250 is False:
                ma_status.append("**跌破年线**")
            chg_str = ""
            if s.chg_5d_pct is not None:
                chg_str = f" | 5日 {s.chg_5d_pct:+.1f}%"
            lines.append(f"- {s.name}: {' / '.join(ma_status)}{chg_str}")

        regime_label = {"bull": "🟢 多头", "sideways": "🟡 震荡", "bear": "🔴 空头"}[regime]
        summary = f"**大盘环境：{regime_label}**（评分 {score}/9, 卖出信号权重 ×{multiplier}）\n" + "\n".join(lines)

        return MarketRegime(
            regime=regime,
            weight_multiplier=multiplier,
            summary=summary,
            indexes=states,
        )
