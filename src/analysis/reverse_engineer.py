# -*- coding: utf-8 -*-
"""
src/analysis/reverse_engineer.py — 荐股服务逆向工程特征提取

对每个 (推荐日, 标的) 提取触发日量价/形态/题材特征,
并与当日全市场做横截面对照(分位数),输出两张 CSV:

  1. picks_features.csv   : 推荐标的逐条特征
  2. market_context.csv   : 每个推荐日的全市场环境快照

数据源: 复用项目现有 BaostockProvider（单标的日线）
        + ScreenerDataProvider（全市场行情）
"""

import os
import time

import numpy as np
import pandas as pd

from src.data.providers.baostock_provider import BaostockProvider
from src.analysis.screening.data_provider import ScreenerDataProvider
from src.utils.logger import get_logger

logger = get_logger("reverse_engineer")


# ======================================================================
# 默认推荐记录
# ======================================================================
# 格式: (推荐日 YYYYMMDD, 股票代码(6位), 名称, 题材标签, 触发价)
DEFAULT_PICKS = [
    ("20260528", "301338", "凯格精机", "专业设备", 214.34),
    ("20260528", "600378", "昊华科技", "化学制品", 40.16),
    ("20260528", "600171", "上海贝岭", "半导体", 31.03),
    ("20260602", "000519", "中兵红箭", "专业设备", 19.40),
    ("20260602", "688411", "海博思创", "电源设备", 301.48),
    ("20260604", "300975", "商络电子", "贸易行业", 43.43),
    ("20260604", "600345", "长江通信", "通信系统设备", 54.28),
    ("20260608", "300031", "宝通科技", "游戏", 25.34),
    ("20260609", "301029", "怡合达", "专业设备", 29.97),
    ("20260609", "002733", "雄韬股份", "电池", 34.59),
    ("20260610", "605376", "博迁新材", "能源金属", 193.30),
]

# 空仓日(无入选)——用于对照"那天全市场为什么没有票满足条件"
DEFAULT_EMPTY_DAYS = ["20260529", "20260601", "20260603", "20260605"]


# ======================================================================
# 配置
# ======================================================================
LOOKBACK_DAYS = 90   # 触发日往前取的日历天数(覆盖约60个交易日)
FORWARD_DAYS = 15    # 触发日往后取的日历天数(覆盖 T+5 交易日)


# ======================================================================
# 工具函数
# ======================================================================
def limit_pct(code: str) -> float:
    """按板块返回涨停幅度(不考虑ST)。"""
    code = str(code).strip()
    if code.startswith(("30", "68")):
        return 0.20
    return 0.10


def shift_date(yyyymmdd: str, days: int) -> str:
    """日期偏移,返回 YYYYMMDD 格式。"""
    return (pd.Timestamp(yyyymmdd) + pd.Timedelta(days=days)).strftime("%Y%m%d")


def to_dash_date(yyyymmdd: str) -> str:
    """YYYYMMDD → YYYY-MM-DD"""
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"


def to_compact_date(dash_date: str) -> str:
    """YYYY-MM-DD → YYYYMMDD"""
    return dash_date.replace("-", "")


# ======================================================================
# 核心: 单标的特征提取
# ======================================================================
def extract_features(bp: BaostockProvider, trade_date: str, code: str,
                     name: str, theme: str, trigger_px: float) -> dict:
    """
    对单只推荐标的提取量价/形态/题材特征。

    Args:
        bp: 已登录的 BaostockProvider 实例
        trade_date: 推荐日 YYYYMMDD
        code: 6位股票代码
        name: 股票名称
        theme: 题材标签
        trigger_px: 触发价格

    Returns:
        特征字典
    """
    start = to_dash_date(shift_date(trade_date, -LOOKBACK_DAYS))
    end = to_dash_date(shift_date(trade_date, FORWARD_DAYS))
    trade_date_dash = to_dash_date(trade_date)

    # 从 baostock 拉日线, 含 peTTM/pbMRQ/psTTM/turn/pctChg
    fields = "date,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ"
    df = bp.get_k_data(code, start_date=start, end_date=end, fields=fields)

    if df is None or df.empty:
        return {"ts_code": code, "name": name, "trade_date": trade_date,
                "error": "no daily data"}

    df = df.sort_values("date").reset_index(drop=True)

    # 确保 trade_date 在数据中
    if trade_date_dash not in set(df["date"]):
        return {"ts_code": code, "name": name, "trade_date": trade_date,
                "error": "trigger date not in daily data (suspended?)"}

    # 添加 pre_close 列 (前一日收盘价)
    df["pre_close"] = df["close"].shift(1)

    i = df.index[df["date"] == trade_date_dash][0]
    hist = df.iloc[:i + 1]       # 含触发日
    fwd = df.iloc[i + 1:]        # 触发日之后

    row = hist.iloc[-1]
    close = row["close"]
    vol = row["volume"]
    amount = row["amount"]
    pre_close = row["pre_close"] if pd.notna(row["pre_close"]) else close
    lp = limit_pct(code)

    # pctChg 优先用 baostock 提供的, 否则自算
    pct_chg = row.get("pctChg", np.nan)
    if pd.isna(pct_chg) and pre_close > 0:
        pct_chg = (close / pre_close - 1) * 100

    feat = {
        "trade_date": trade_date,
        "ts_code": code,
        "name": name,
        "theme": theme,
        "trigger_px": trigger_px,
        "close_T": close,
        # ---- 当日量价 ----
        "pct_chg_T": pct_chg,
        "amplitude_T": (row["high"] - row["low"]) / pre_close * 100 if pre_close > 0 else np.nan,
        "close_pos_in_range": ((close - row["low"]) / max(row["high"] - row["low"], 1e-9)),
        "gap_open_pct": (row["open"] / pre_close - 1) * 100 if pre_close > 0 else np.nan,
        "is_limit_up_T": int(pct_chg >= lp * 100 * 0.98) if pd.notna(pct_chg) else 0,
        "near_limit_T": int(pct_chg >= lp * 100 * 0.90) if pd.notna(pct_chg) else 0,
        # 触发价相对当日收盘
        "close_vs_trigger_pct": (close / trigger_px - 1) * 100 if trigger_px else np.nan,
    }

    # ---- 量能 ----
    if len(hist) >= 6:
        v5 = hist["volume"].iloc[-6:-1].mean()
        feat["vol_ratio_5d"] = vol / v5 if v5 > 0 else np.nan
    if len(hist) >= 21:
        v20 = hist["volume"].iloc[-21:-1].mean()
        feat["vol_ratio_20d"] = vol / v20 if v20 > 0 else np.nan
    # baostock amount 单位是 "元", 转亿
    feat["amount_yi"] = amount / 1e8 if pd.notna(amount) else np.nan

    # ---- 均线与形态 ----
    c = hist["close"]
    for n in (5, 10, 20, 60):
        if len(c) >= n:
            ma = c.iloc[-n:].mean()
            feat[f"close_vs_ma{n}_pct"] = (close / ma - 1) * 100

    if len(hist) >= 61:
        prev_high_60 = hist["high"].iloc[-61:-1].max()
        feat["breakout_60d_high"] = int(close > prev_high_60)
        feat["dist_to_60d_high_pct"] = (close / prev_high_60 - 1) * 100
    if len(hist) >= 21:
        prev_high_20 = hist["high"].iloc[-21:-1].max()
        feat["breakout_20d_high"] = int(close > prev_high_20)

    # ---- 近期动量 / 涨停史 ----
    for n in (3, 5, 10, 20):
        if len(c) >= n + 1:
            feat[f"ret_{n}d_pct"] = (close / c.iloc[-(n + 1)] - 1) * 100

    # 近10日涨停次数
    if len(hist) >= 11:
        recent10 = hist.iloc[-11:-1]
        # 用 pctChg 字段判断涨停
        recent_pct = recent10["pctChg"]
        feat["limit_ups_prev10"] = int((recent_pct >= lp * 100 * 0.98).sum())
    else:
        feat["limit_ups_prev10"] = 0

    # 连涨天数
    pct_series = hist["pctChg"]
    up = (pct_series > 0).iloc[::-1]
    feat["consec_up_days"] = int(up.cummin().sum()) if len(up) else 0

    if len(c) >= 21:
        feat["volatility_20d_pct"] = c.pct_change().iloc[-20:].std() * 100

    # ---- 换手率/PE ----
    feat["turnover_rate"] = row.get("turn", np.nan)
    feat["pe_ttm"] = row.get("peTTM", np.nan)
    feat["pb_mrq"] = row.get("pbMRQ", np.nan)

    # ---- 前瞻收益(诚实口径) ----
    if not fwd.empty:
        nxt = fwd.iloc[0]
        feat["next_open_ret_pct"] = (nxt["open"] / close - 1) * 100  # 次日开盘溢价
        buy = nxt["open"]  # 假设次日开盘买入
        for n in (1, 3, 5):
            if len(fwd) >= n:
                feat[f"hold_T{n}_close_ret_pct"] = (fwd["close"].iloc[n - 1] / buy - 1) * 100
        if len(fwd) >= 1:
            h5 = fwd["high"].iloc[:5].max()
            l5 = fwd["low"].iloc[:5].min()
            # 复刻它的口径: 相对触发价的5日最高
            feat["max_high_5d_vs_trigger_pct"] = (h5 / trigger_px - 1) * 100 if trigger_px else np.nan
            # 诚实口径: 相对次日开盘买入价
            feat["max_high_5d_vs_buy_pct"] = (h5 / buy - 1) * 100 if buy > 0 else np.nan
            feat["max_dd_5d_vs_buy_pct"] = (l5 / buy - 1) * 100 if buy > 0 else np.nan

    return feat


# ======================================================================
# 核心: 全市场横截面分位
# ======================================================================
def market_percentiles(spot_df: pd.DataFrame, picks_today: list[str]) -> tuple[dict, dict]:
    """
    计算推荐标的在全市场横截面的分位数,以及当日市场环境快照。

    Args:
        spot_df: 全A实时行情 DataFrame (来自 ScreenerDataProvider.get_all_a_shares())
        picks_today: 当日推荐标的代码列表 (6位)

    Returns:
        (分位数字典, 市场环境字典)
    """
    if spot_df is None or spot_df.empty:
        return {}, {}

    m = spot_df.copy()

    pcts = {}
    for code in picks_today:
        sub = m[m["代码"].astype(str) == str(code)]
        if sub.empty:
            continue
        r = sub.iloc[0]
        entry = {}
        # 涨跌幅分位
        if "涨跌幅" in m.columns and pd.notna(r.get("涨跌幅")):
            entry["mkt_pctile_pct_chg"] = (m["涨跌幅"] < r["涨跌幅"]).mean() * 100
        # 换手率分位
        if "换手率" in m.columns and pd.notna(r.get("换手率")):
            entry["mkt_pctile_turnover"] = (m["换手率"] < r["换手率"]).mean() * 100
        # 成交额分位
        if "成交额" in m.columns and pd.notna(r.get("成交额")):
            entry["mkt_pctile_amount"] = (m["成交额"] < r["成交额"]).mean() * 100
        if entry:
            pcts[code] = entry

    # 市场环境快照
    ctx = {
        "n_stocks": len(m),
    }
    if "涨跌幅" in m.columns:
        ctx["pct_up"] = (m["涨跌幅"] > 0).mean() * 100
        ctx["median_pct_chg"] = m["涨跌幅"].median()
        ctx["n_limit_up_approx"] = int((m["涨跌幅"] >= 9.8).sum())
        ctx["n_down_7plus"] = int((m["涨跌幅"] <= -7).sum())
    if "成交额" in m.columns:
        ctx["total_amount_yi"] = m["成交额"].sum() / 1e8

    return pcts, ctx


# ======================================================================
# 主类: 荐股逆向工程引擎
# ======================================================================
class PicksReverseEngineer:
    """
    荐股逆向工程分析器。

    复用项目已有数据源:
    - BaostockProvider: 拉取单标的日线数据(含 OHLCV + turn + peTTM 等)
    - ScreenerDataProvider: 拉取全A实时行情做横截面分位

    输出:
    - picks_features.csv: 推荐标的逐条特征
    - market_context.csv: 每个推荐日的全市场环境快照
    """

    def __init__(self, picks=None, empty_days=None, output_dir="./output/reverse_engineer"):
        """
        Args:
            picks: 推荐记录列表, 每条为 (trade_date, code, name, theme, trigger_px)
            empty_days: 空仓日列表 (YYYYMMDD)
            output_dir: 输出目录
        """
        self.picks = picks if picks is not None else DEFAULT_PICKS
        self.empty_days = empty_days if empty_days is not None else DEFAULT_EMPTY_DAYS
        self.output_dir = output_dir

    def run(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        执行完整分析流程。

        Returns:
            (picks_features_df, market_context_df)
        """
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info("========== 荐股逆向工程分析启动 ==========")

        # 1. 逐标的提取特征
        rows = []
        with BaostockProvider() as bp:
            for trade_date, code, name, theme, px in self.picks:
                logger.info(f"[{trade_date}] {name} {code} ...")
                feat = extract_features(bp, trade_date, code, name, theme, px)
                rows.append(feat)
                time.sleep(0.35)  # 控制频率

        feats = pd.DataFrame(rows)

        # 2. 全市场横截面分位 + 市场环境(含空仓日)
        sdp = ScreenerDataProvider()
        spot_df = sdp.get_all_a_shares()

        ctx_rows = []
        all_days = sorted(set(p[0] for p in self.picks) | set(self.empty_days))
        for d in all_days:
            codes = [p[1] for p in self.picks if p[0] == d]
            logger.info(f"[{d}] market cross-section ...")
            pcts, ctx = market_percentiles(spot_df, codes)
            ctx["trade_date"] = d
            ctx["has_picks"] = int(len(codes) > 0)
            ctx_rows.append(ctx)

            # 将分位数合并回 feats
            for code, p in pcts.items():
                for k, v in p.items():
                    feats.loc[(feats["ts_code"] == code) &
                              (feats["trade_date"] == d), k] = v

        # 3. 输出
        picks_path = os.path.join(self.output_dir, "picks_features.csv")
        ctx_path = os.path.join(self.output_dir, "market_context.csv")

        feats.to_csv(picks_path, index=False, encoding="utf-8-sig")
        pd.DataFrame(ctx_rows).to_csv(ctx_path, index=False, encoding="utf-8-sig")

        logger.info(f"完成: {picks_path}")
        logger.info(f"完成: {ctx_path}")

        # 控制台预览
        preview_cols = ["trade_date", "name", "pct_chg_T", "vol_ratio_5d"]
        for col in ["close_vs_ma20_pct", "mkt_pctile_pct_chg"]:
            if col in feats.columns:
                preview_cols.append(col)
        available = [c for c in preview_cols if c in feats.columns]
        if available:
            logger.info("\n" + feats[available].to_string(index=False))

        logger.info("========== 荐股逆向工程分析完成 ==========")
        return feats, pd.DataFrame(ctx_rows)
