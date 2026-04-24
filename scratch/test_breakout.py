import sys
import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.screener.data_provider import ScreenerDataProvider
from src.screener.conditions import BoxBreakoutCondition, DowntrendBreakoutCondition, VolumeBreakCondition
import pandas as pd

provider = ScreenerDataProvider()
df_daily = provider._fetch_k_akshare("002156", 2000, "d")

cond1 = BoxBreakoutCondition(lookback_bars=20, breakout_pct=0.02, consolidation_pct=0.10)
cond2 = DowntrendBreakoutCondition(lookback_bars=60, min_touches=2, breakout_pct=0.01)
cond3 = VolumeBreakCondition(lookback_bars=20, vol_multiple=1.4)

if df_daily is None or df_daily.empty:
    print("No data for 002156")
    sys.exit()

count1, count2, count3, count_all = 0, 0, 0, 0
test_range = len(df_daily) - 100
for i in range(100, test_range):
    sub_df = df_daily.iloc[:i]
    spot = pd.Series()
    res1 = cond1.evaluate_full(spot, sub_df)
    res2 = cond2.evaluate_full(spot, sub_df)
    res3 = cond3.evaluate_full(spot, sub_df)
    
    if res1: count1 += 1
    if res2: count2 += 1
    if res3: count3 += 1
    if res1 and res2 and res3: count_all += 1

print(f"box_breakout: {count1}")
print(f"downtrend_breakout: {count2}")
print(f"volume_break: {count3}")
print(f"ALL: {count_all}")
