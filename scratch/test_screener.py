import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.screener.screener import StockScreener
from src.screener.conditions import (
    MarketCapCondition,
    PERangeCondition,
    WeeklyMACDBottomDivergenceCondition
)
from src.screener.data_provider import ScreenerDataProvider

provider = ScreenerDataProvider()

# First, let's see how many stocks pass Phase 1 (Spot)
print("--- Testing Phase 1 ---")
all_stocks = provider.get_all_a_shares()
print(f"Total stocks: {len(all_stocks)}")

cond_cap = MarketCapCondition(min_cap=50, max_cap=2000)
mask_cap = cond_cap.evaluate_vectorized(all_stocks)
passed_cap = all_stocks[mask_cap]
print(f"Passed Market Cap (50-2000): {len(passed_cap)}")

cond_pe = PERangeCondition(min_pe=0, max_pe=30)
mask_pe = cond_pe.evaluate_vectorized(passed_cap)
passed_phase1 = passed_cap[mask_pe]
print(f"Passed PE (0-30): {len(passed_phase1)}")

# Now Phase 2
print("--- Testing Phase 2 (Weekly MACD Divergence) ---")
# To speed up, we will just test the first 100 passed stocks
sample_stocks = passed_phase1.head(100)
print(f"Testing {len(sample_stocks)} stocks for divergence...")

cond_div = WeeklyMACDBottomDivergenceCondition(lookback_bars=60)
passed_div = []

for idx, row in sample_stocks.iterrows():
    code = str(row.get("代码", "")).strip()
    df = provider.get_weekly_ohlcv(code)
    if cond_div.evaluate_full(row, df):
        passed_div.append(code)

print(f"Passed Divergence: {len(passed_div)}")
print("Passed Codes:", passed_div)
