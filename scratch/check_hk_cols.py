import akshare as ak
code = "00700"
df = ak.stock_hk_daily(symbol=code, adjust="qfq")
print(df.columns.tolist())
print(df.head(1))
