import sys
import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.screener.data_provider import ScreenerDataProvider
from src.screener.conditions import MultiMABullCondition
import pandas as pd

provider = ScreenerDataProvider()
cond = MultiMABullCondition(ma_list=[5, 10, 20, 60, 250], require_all_above=True, require_up_trend=True)

for code in ["000001", "600519", "000858", "600036"]:
    df = provider.get_daily_ohlcv(code)
    if df is not None and not df.empty:
        res = cond.evaluate_full(pd.Series(), df)
        print(f"{code} Multi-MA Bull: {res}")
        
        # Test backtesting mode manually (what does a random day return?)
        # Let's count how many days in the last 1000 days this condition was True
        count = 0
        from src.strategy.backtest.screener_rule import ScreenerRuleStrategy
        import numpy as np
        
        # Instead of using backtrader directly, just simulate _precompute_signals logic
        # Actually just iterate backwards a bit
        for i in range(10, 500, 10):
            sub_df = df.iloc[:-i]
            if cond.evaluate_full(pd.Series(), sub_df):
                count += 1
        print(f"  -> Out of 50 sampled days, was true {count} times.")
    else:
        print(f"{code}: No data")

