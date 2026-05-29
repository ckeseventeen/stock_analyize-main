"""
回测晶方科技 / 鸣志电器的 sell 信号触发点

方法：
  1. 拉 3 年日线 + 周线
  2. 从第 250 个交易日开始，每隔 N 日切片到当前日，跑所有 sell 条件
  3. 输出每个触发时点 + 触发后 10/20/30 日最大回撤
"""
import sys, logging
sys.path.insert(0, ".")
logging.basicConfig(level=logging.WARNING, format="%(levelname)s | %(message)s")

import pandas as pd

from src.analysis.screening.conditions import CONDITION_REGISTRY, CONDITION_LABELS
from src.analysis.screening.data_provider import ScreenerDataProvider


STOCKS = {
    "603005": "晶方科技",
    "603728": "鸣志电器",
}

# 要测试的 sell 条件清单（与 web 页面 _SELL_PROFILE 一致）
SELL_PROFILE = [
    ("weekly_macd_top_divergence", {"lookback_bars": 60}, 1),
    ("daily_macd_top_divergence",  {"lookback_bars": 120}, 1),
    ("bias",                       {"ma_period": 20, "threshold": 8.0, "direction": "above"}, 1),
    ("volume_price_divergence",    {"lookback_bars": 30, "direction": "top"}, 1),
    ("rsi_overbought",             {"threshold": 75, "period": 14}, 2),
    ("kdj_death_cross",            {"j_threshold": 70}, 2),
    ("ma_death_cross",             {"fast_period": 5, "slow_period": 20}, 2),
    ("break_below_ma",             {"ma_period": 60, "lookback": 5}, 2),
    ("bias",                       {"ma_period": 60, "threshold": 15.0, "direction": "above"}, 3),
    ("volume_blowoff",             {"lookback_bars": 60, "vol_multiple": 3.0, "min_price_change_pct": 5.0}, 3),
    ("bollinger_breakout",         {"period": 20, "std_dev": 2.0, "direction": "upper"}, 3),
]


def evaluate_at(daily_slice, weekly_slice, code, name):
    """对给定截止日的 K 线数据评估所有 sell 条件，返回触发的条件列表"""
    triggered = []
    for cond_type, kwargs, priority in SELL_PROFILE:
        cls = CONDITION_REGISTRY.get(cond_type)
        if cls is None:
            continue
        try:
            cond = cls(**kwargs)
        except Exception:
            continue
        period = getattr(cond, "ohlcv_period", "daily")
        df = weekly_slice if period == "weekly" else daily_slice
        if df is None or df.empty or len(df) < 25:
            continue
        try:
            spot = pd.Series({"代码": code, "名称": name})
            hit = bool(cond.evaluate_full(spot, df))
            if hit:
                label = CONDITION_LABELS.get(cond_type, cond_type)
                params = ",".join(f"{k}={v}" for k, v in kwargs.items() if k != "direction")
                triggered.append({
                    "type": cond_type,
                    "label": label,
                    "params": params,
                    "priority": priority,
                })
        except Exception:
            continue
    return triggered


def max_drawdown_forward(daily_df, anchor_idx, days):
    """从 anchor_idx 起的未来 N 个交易日内，最大回撤百分比（相对触发当日收盘）"""
    close_col = "收盘" if "收盘" in daily_df.columns else "close"
    anchor_close = float(daily_df.iloc[anchor_idx][close_col])
    if anchor_close <= 0:
        return None
    end = min(anchor_idx + days + 1, len(daily_df))
    if end <= anchor_idx + 1:
        return None
    future = daily_df.iloc[anchor_idx + 1:end]
    low_col = "最低" if "最低" in daily_df.columns else "low"
    if low_col not in future.columns:
        return None
    min_low = float(future[low_col].min())
    return (min_low - anchor_close) / anchor_close * 100  # 负值表示下跌


def backtest_one(provider, code, name):
    print(f"\n{'='*70}\n  {name} ({code})\n{'='*70}")
    # 拉 3 年日线 + 3 年周线
    daily_df = provider.get_daily_ohlcv(code, days_back=365 * 3)
    weekly_df = provider.get_weekly_ohlcv(code, days_back=365 * 3)
    if daily_df is None or daily_df.empty:
        print(f"  !! 无日线数据")
        return
    print(f"  日线 {len(daily_df)} 根，周线 {len(weekly_df) if weekly_df is not None else 0} 根")
    date_col = "日期" if "日期" in daily_df.columns else "date"
    close_col = "收盘" if "收盘" in daily_df.columns else "close"
    daily_df = daily_df.sort_values(date_col, ignore_index=True)
    if weekly_df is not None and not weekly_df.empty:
        weekly_df = weekly_df.sort_values(date_col, ignore_index=True)

    # 从第 120 个交易日开始评估（保证 MA60、MACD 等指标稳定）
    START_OFFSET = 120
    if len(daily_df) <= START_OFFSET + 30:
        print(f"  数据不足以回测")
        return

    triggers = []
    for i in range(START_OFFSET, len(daily_df) - 1):
        d_slice = daily_df.iloc[:i + 1]
        cur_date = d_slice.iloc[-1][date_col]
        # 周线切片（取截止当前日的所有周）
        if weekly_df is not None and not weekly_df.empty:
            w_slice = weekly_df[weekly_df[date_col] <= cur_date]
        else:
            w_slice = pd.DataFrame()
        triggered = evaluate_at(d_slice, w_slice, code, name)
        if not triggered:
            continue
        triggers.append({
            "idx": i,
            "date": cur_date,
            "close": float(d_slice.iloc[-1][close_col]),
            "signals": triggered,
        })

    if not triggers:
        print(f"  没有任何触发点")
        return

    # 去重：同一周内同类型只保留一次（避免连续触发刷屏）
    dedup = []
    last_by_type: dict = {}
    for t in triggers:
        key_signals = []
        for s in t["signals"]:
            key = s["type"] + "|" + s["params"]
            last_idx = last_by_type.get(key)
            if last_idx is None or t["idx"] - last_idx > 10:  # > 10 交易日才算新触发
                key_signals.append(s)
                last_by_type[key] = t["idx"]
        if key_signals:
            dedup.append({**t, "signals": key_signals})

    print(f"  共 {len(dedup)} 个独立触发时点（去重 10 日内重复）")

    # 输出表格
    rows = []
    for t in dedup:
        labels = " + ".join(f"{s['label']}({s['priority']})" for s in t["signals"])
        d10 = max_drawdown_forward(daily_df, t["idx"], 10)
        d20 = max_drawdown_forward(daily_df, t["idx"], 20)
        d30 = max_drawdown_forward(daily_df, t["idx"], 30)
        rows.append({
            "日期": pd.Timestamp(t["date"]).strftime("%Y-%m-%d"),
            "收盘": f"{t['close']:.2f}",
            "信号": labels,
            "高优数": sum(1 for s in t["signals"] if s["priority"] == 1),
            "10日最大回撤%": f"{d10:.1f}" if d10 is not None else "",
            "20日最大回撤%": f"{d20:.1f}" if d20 is not None else "",
            "30日最大回撤%": f"{d30:.1f}" if d30 is not None else "",
        })

    out_df = pd.DataFrame(rows)
    # 按高优数 + 30日回撤排序
    def _drawdown_sort_key(s):
        try:
            return float(s) if s else 0
        except Exception:
            return 0
    out_df["_sort"] = out_df["30日最大回撤%"].apply(_drawdown_sort_key)
    out_df = out_df.sort_values(["高优数", "_sort"], ascending=[False, True]).drop(columns="_sort")

    pd.set_option("display.max_rows", 100)
    pd.set_option("display.max_colwidth", 70)
    pd.set_option("display.width", 200)
    print(out_df.to_string(index=False))

    # 统计
    dd30s = [float(r["30日最大回撤%"]) for r in rows
              if r["30日最大回撤%"] and r["30日最大回撤%"].replace("-","").replace(".","").isdigit()]
    if dd30s:
        avg_dd = sum(dd30s) / len(dd30s)
        worst_dd = min(dd30s)
        big_drops = sum(1 for d in dd30s if d <= -10)
        print(f"\n  统计：平均 30 日回撤 {avg_dd:.1f}%, 最大 {worst_dd:.1f}%, "
              f"≥ -10% 回撤次数 {big_drops}/{len(dd30s)}")


provider = ScreenerDataProvider()
for code, name in STOCKS.items():
    backtest_one(provider, code, name)
