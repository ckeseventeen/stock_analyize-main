import sys
import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.screener.data_provider import ScreenerDataProvider
from src.screener.conditions import (
    MarketCapCondition,
    PERangeCondition,
    WeeklyMACDBottomDivergenceCondition
)
import akshare as ak
import pandas as pd

# Let's bypass the slow get_all_a_shares and just get spot directly
try:
    df = ak.stock_zh_a_spot_em()
except Exception as e:
    print(f"akshare failed: {e}")
    sys.exit(1)

df = df[~df["名称"].str.contains("ST|退市", na=False)].copy()
for col in ("总市值", "流通市值", "市盈率-动态", "市净率", "换手率", "最新价", "涨跌幅", "振幅"):
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

print(f"Total stocks: {len(df)}")

cond_cap = MarketCapCondition(min_cap=50, max_cap=2000)
mask_cap = cond_cap.evaluate_vectorized(df)
df = df[mask_cap]
print(f"Passed Market Cap (50-2000): {len(df)}")

cond_pe = PERangeCondition(min_pe=0, max_pe=30)
mask_pe = cond_pe.evaluate_vectorized(df)
df = df[mask_pe]
print(f"Passed PE (0-30): {len(df)}")

provider = ScreenerDataProvider()
cond_div = WeeklyMACDBottomDivergenceCondition(lookback_bars=60)
passed_div = []

print(f"Testing {min(100, len(df))} stocks for divergence...")
for idx, row in df.head(100).iterrows():
    code = str(row.get("代码", "")).strip()
    df_k = provider.get_weekly_ohlcv(code)
    if cond_div.evaluate_full(row, df_k):
        passed_div.append(code)

print(f"Passed Divergence: {len(passed_div)}")
print("Passed Codes:", passed_div)
