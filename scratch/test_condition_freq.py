import sys
import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.screener.data_provider import ScreenerDataProvider
from src.screener.conditions import (
    MultiMABullCondition,
    VolumeBreakCondition,
    WeeklyMACDGoldCrossCondition,
    VolumeShrinkCondition,
    SupportMACondition,
    BoxBreakoutCondition,
    DowntrendBreakoutCondition
)
import pandas as pd

provider = ScreenerDataProvider()
df_daily = provider._fetch_k_akshare("000001", 3000, "d")
df_weekly = provider._fetch_k_akshare("000001", 3000, "w")

conditions = {
    "multi_ma_bull": (MultiMABullCondition(ma_list=[5,10,20,60], require_all_above=True, require_up_trend=True), df_daily),
    "volume_break": (VolumeBreakCondition(vol_multiple=1.5), df_daily),
    "weekly_macd_gold_cross": (WeeklyMACDGoldCrossCondition(), df_weekly),
    "volume_shrink": (VolumeShrinkCondition(shrink_ratio=0.6), df_daily),
    "support_ma": (SupportMACondition(), df_daily),
    "box_breakout": (BoxBreakoutCondition(lookback_bars=20, breakout_pct=0.02, consolidation_pct=0.10), df_daily),
    "downtrend_breakout": (DowntrendBreakoutCondition(), df_daily)
}

print(f"Total Daily Bars: {len(df_daily) if df_daily is not None else 0}")
print(f"Total Weekly Bars: {len(df_weekly) if df_weekly is not None else 0}")

for name, (cond, df) in conditions.items():
    if df is None or df.empty:
        print(f"{name}: No data")
        continue
    
    count = 0
    test_range = min(500, len(df) - 100)
    for i in range(test_range):
        sub_df = df.iloc[:len(df)-test_range+i+1]
        try:
            if cond.evaluate_full(pd.Series(), sub_df):
                count += 1
        except Exception:
            pass
    print(f"{name} fired {count} times out of {test_range} bars.")
