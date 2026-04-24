"""
模拟 Backtrader 内部 _precompute_signals 的运行环境，
验证修复后的 screener_rule 是否真的在英文列数据上正确触发信号。
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.screener.data_provider import ScreenerDataProvider
from src.screener.conditions import MultiMABullCondition
from src.strategy.backtest.runner import BacktestRunner
from src.strategy.backtest.screener_rule import ScreenerRuleStrategy

provider = ScreenerDataProvider()
df_raw = provider.get_daily_ohlcv("002050", days_back=1500, prefer="baostock", market="a")
print(f"Raw data bars: {len(df_raw)}")
print(f"Raw columns: {list(df_raw.columns)}")

# 模拟 BacktestRunner._prepare_data 的操作（中文 -> 英文）
runner = BacktestRunner(
    strategy_class=ScreenerRuleStrategy,
    data_df=df_raw,
    buy_conditions=[{"type": "multi_ma_bull", "ma_list": [5,10,20,60,250], "require_all_above": True, "require_up_trend": True}],
    sell_conditions=[{"type": "rsi_oversold", "threshold": 80, "period": 14}],
    buy_logic="all",
    sell_logic="any",
    position_size=0.95
)

# 拿到 runner 处理后的英文列 DataFrame
df_bt = runner._data_df
print(f"\nAfter _prepare_data columns: {list(df_bt.columns)}")

# 现在模拟 _precompute_signals 内部创建的 df（只有5列英文）
sim_df = pd.DataFrame({
    "open":   df_bt["open"].values,
    "high":   df_bt["high"].values,
    "low":    df_bt["low"].values,
    "close":  df_bt["close"].values,
    "volume": df_bt["volume"].values,
})
print(f"Simulated BT DataFrame shape: {sim_df.shape}")
print(f"Simulated BT DataFrame columns: {list(sim_df.columns)}")

# 手动跑 _precompute_signals 逻辑
cond = MultiMABullCondition(ma_list=[5,10,20,60,250], require_all_above=True, require_up_trend=True)

count = 0
errors = []
for i in range(len(sim_df)):
    bar_history = sim_df.iloc[:i+1]
    try:
        res = cond.evaluate_full(pd.Series(), bar_history)
        if res:
            count += 1
    except Exception as e:
        errors.append((i, str(e)))

print(f"\n--- 结果 ---")
print(f"总 bars: {len(sim_df)}")
print(f"信号触发次数: {count}")
print(f"错误次数: {len(errors)}")
if errors:
    print(f"第一个错误: bar {errors[0][0]}: {errors[0][1]}")

# 运行实际回测
print("\n--- 实际运行 Backtrader 回测 ---")
report = runner.run()
print(f"总交易次数: {report.get('总交易次数', 0)}")
