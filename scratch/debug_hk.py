import akshare as ak
import pandas as pd

code = "00700" # Tencent
print(f"--- Testing stock_hk_hist for {code} ---")
try:
    df = ak.stock_hk_hist(symbol=code, period="daily", start_date="20200101", end_date="20240423", adjust="qfq")
    print(f"Success with stock_hk_hist: {len(df) if df is not None else 'None'}")
except Exception as e:
    print(f"Error with stock_hk_hist: {e}")

print(f"\n--- Testing stock_hk_daily for {code} ---")
try:
    df = ak.stock_hk_daily(symbol=code, adjust="qfq")
    print(f"Success with stock_hk_daily: {len(df) if df is not None else 'None'}")
except Exception as e:
    print(f"Error with stock_hk_daily: {e}")
