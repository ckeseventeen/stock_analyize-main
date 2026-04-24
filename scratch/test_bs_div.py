import sys
import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.screener.data_provider import ScreenerDataProvider
from src.screener.conditions import WeeklyMACDBottomDivergenceCondition

provider = ScreenerDataProvider()
cond_div = WeeklyMACDBottomDivergenceCondition(lookback_bars=60)

code = "000001"
df_k_bs = provider._fetch_k_baostock(code, 365*3, "w")
print(f"Baostock returned {len(df_k_bs)} rows")
import pandas as pd
res = cond_div.evaluate_full(pd.Series(), df_k_bs)
print(f"Divergence on Baostock data: {res}")
