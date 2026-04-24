import sys
from pathlib import Path
import pandas as pd
import numpy as np

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.screener.data_provider import ScreenerDataProvider
from src.screener.conditions import BoxBreakoutCondition, DowntrendBreakoutCondition, VolumeBreakCondition

provider = ScreenerDataProvider()
# 晶方科技 002676
df = provider.get_daily_ohlcv("002676", days_back=1500, prefer="baostock", market="a")

if df is None or df.empty:
    print("No data!")
    exit()

print(f"Total bars: {len(df)}")
print(f"Columns: {list(df.columns)}")

# 看一下过去20天的波动幅度统计
df_copy = df.copy()
rename = {"最高": "high", "最低": "low", "收盘": "close", "成交量": "volume"}
df_copy.rename(columns={k:v for k,v in rename.items() if k in df_copy.columns}, inplace=True)

print("\n--- 近100天任意20天箱体振幅 ---")
amplitudes = []
for i in range(len(df_copy)-100, len(df_copy)-20):
    window = df_copy.iloc[i:i+20]
    box_high = float(window["high"].max())
    box_low = float(window["low"].min())
    amp = (box_high - box_low) / box_low if box_low > 0 else 99
    amplitudes.append(amp)
print(f"振幅 min={min(amplitudes):.2%}, max={max(amplitudes):.2%}, avg={np.mean(amplitudes):.2%}")
print(f"振幅 <= 10% 的窗口数: {sum(1 for a in amplitudes if a <= 0.10)} / {len(amplitudes)}")
print(f"振幅 <= 20% 的窗口数: {sum(1 for a in amplitudes if a <= 0.20)} / {len(amplitudes)}")
print(f"振幅 <= 30% 的窗口数: {sum(1 for a in amplitudes if a <= 0.30)} / {len(amplitudes)}")

# 测每个条件历史触发次数
cond_box = BoxBreakoutCondition(lookback_bars=20, breakout_pct=0.02, consolidation_pct=0.10)
cond_down = DowntrendBreakoutCondition(lookback_bars=60, min_touches=2, breakout_pct=0.01)
cond_vol = VolumeBreakCondition(lookback_bars=20, vol_multiple=1.4)

count_box, count_down, count_vol, count_all = 0, 0, 0, 0
for i in range(80, len(df)):
    sub = df.iloc[:i]
    spot = pd.Series()
    r1 = cond_box.evaluate_full(spot, sub)
    r2 = cond_down.evaluate_full(spot, sub)
    r3 = cond_vol.evaluate_full(spot, sub)
    if r1: count_box += 1
    if r2: count_down += 1
    if r3: count_vol += 1
    if r1 and r2 and r3: count_all += 1

total = len(df) - 80
print(f"\n--- 历史触发次数 (共 {total} 天) ---")
print(f"box_breakout: {count_box} 次")
print(f"downtrend_breakout: {count_down} 次")
print(f"volume_break: {count_vol} 次")
print(f"三重突破(全AND): {count_all} 次")
