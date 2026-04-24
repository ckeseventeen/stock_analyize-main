import sys
from pathlib import Path
import pandas as pd
import numpy as np

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.screener.data_provider import ScreenerDataProvider
from src.screener.conditions import MultiMABullCondition
from src.analysis.technical.indicators import TechnicalAnalyzer

provider = ScreenerDataProvider()
# 三花智控 002050
df = provider.get_daily_ohlcv("002050", days_back=1500, prefer="baostock", market="a")

if df is None or df.empty:
    print("No data!")
    exit()

print(f"Total bars: {len(df)}")

# 对最后一天详细诊断
df_copy = df.copy()
ta = TechnicalAnalyzer(df_copy)
ta.add_moving_averages([5, 10, 20, 60, 250])
df_ta = ta.get_dataframe()

last = df_ta.iloc[-1]
prev = df_ta.iloc[-2]
print(f"\n--- 最新一天各均线情况 ---")
print(f"close: {last['close']:.2f}")
for ma in [5, 10, 20, 60, 250]:
    col = f"ma_{ma}"
    val = last[col]
    val_prev = prev[col]
    above = "OK" if last['close'] > val else "NO"
    trend = "UP" if val > val_prev else "DN"
    print(f"  MA{ma}: {val:.2f} {trend}  close>{col}? {above}")

# 检查多头排列
mas = sorted([5, 10, 20, 60, 250])
print(f"\n--- 均线多头排列检查 (MA5>MA10>MA20>MA60>MA250) ---")
for i in range(len(mas)-1):
    a, b = mas[i], mas[i+1]
    va, vb = last[f"ma_{a}"], last[f"ma_{b}"]
    ok = "OK" if va > vb else "NO"
    print(f"  MA{a}({va:.2f}) > MA{b}({vb:.2f})? {ok}")

# 历史上多少天满足多头排列（不含require_up_trend）
cond_loose = MultiMABullCondition(ma_list=[5,10,20,60,250], require_all_above=True, require_up_trend=False)
cond_strict = MultiMABullCondition(ma_list=[5,10,20,60,250], require_all_above=True, require_up_trend=True)
cond_no250 = MultiMABullCondition(ma_list=[5,10,20,60], require_all_above=True, require_up_trend=True)

count_loose, count_strict, count_no250 = 0, 0, 0
for i in range(260, len(df)):
    sub = df.iloc[:i]
    if cond_loose.evaluate_full(pd.Series(), sub): count_loose += 1
    if cond_strict.evaluate_full(pd.Series(), sub): count_strict += 1
    if cond_no250.evaluate_full(pd.Series(), sub): count_no250 += 1

total = len(df) - 260
print(f"\n--- 历史触发次数 (共{total}天) ---")
print(f"均线多头(不要求趋势向上):     {count_loose} 次")
print(f"均线多头(要求所有均线向上):   {count_strict} 次  ← 当前策略")
print(f"MA5>10>20>60(去掉250年线):   {count_no250} 次")
