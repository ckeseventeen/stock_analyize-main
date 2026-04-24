import akshare as ak
import pandas as pd
import os

# Clear proxy
for k in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"]:
    os.environ.pop(k, None)
os.environ["NO_PROXY"] = "*"

def test_amd():
    symbol = "AMD"
    print(f"Testing for {symbol}...")
    
    # 1. Sina
    try:
        print("Trying ak.stock_us_daily (Sina)...")
        df = ak.stock_us_daily(symbol=symbol, adjust="qfq")
        if df is not None and not df.empty:
            print(f"Success with Sina! Rows: {len(df)}")
            print(df.head(2))
            print(df.tail(2))
        else:
            print("Sina returned empty")
    except Exception as e:
        print(f"Sina failed: {e}")

    # 2. Eastmoney
    try:
        print(f"Trying ak.stock_us_hist for 105.{symbol}...")
        df = ak.stock_us_hist(symbol=f"105.{symbol}", period="daily", adjust="qfq")
        if df is not None and not df.empty:
            print(f"Success with Eastmoney! Rows: {len(df)}")
            print(df.tail(2))
    except Exception as e:
        print(f"Eastmoney failed: {e}")

if __name__ == "__main__":
    test_amd()
