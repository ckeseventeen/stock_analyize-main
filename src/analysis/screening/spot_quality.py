"""
src/analysis/screening/spot_quality.py — 全A行情数据质量校验

从 data_provider 提取。这套阈值是 2026-07 线上事故的产物：问财分页只回
2649/5300 只、97.9% 市值为 NaN 且缺全部蓝筹，却因"非空+列名齐全"被当作
正常数据缓存 12 小时，导致全站筛选静默归零。
"""
from __future__ import annotations

import pandas as pd

# 全A 实时行情数据"够用"的最小字段集合：缺这些就视为数据源退化，
# 触发上层走 Baostock 全字段路径，避免 PE/PB 等条件因字段缺失而把整张表清零。
_SPOT_REQUIRED_COLUMNS = ("代码", "名称", "总市值")

# ── 全A spot 数据质量门槛（缺一不可，历史教训见 validate_spot_quality）──
# A 股全市场约 5300 只（含北交所），留足余量取 4000 为"覆盖不全"红线
_SPOT_MIN_ROWS = 4000
# 总市值有效（非 NaN 且 > 0）占比下限：市值是绝大多数策略的第一道过滤条件
_SPOT_MIN_MKTCAP_RATIO = 0.80
# 基准蓝筹：任何一份完整的全A行情都必然包含它们，缺失说明分页/排序截断
_SPOT_SENTINEL_CODES = ("600519", "000001", "600036")



def validate_spot_quality(df: pd.DataFrame) -> tuple[bool, str]:
    """
    校验全A行情数据的**完整性与可用性**（不只是"非空"）。

    背景（2026-07 线上事故）：问财等降级源分页不稳定，返回过
    2649 行且其中 97.9% 总市值为 NaN 的"半份数据"，且系统性丢失
    茅台/平安银行等全部大盘蓝筹。旧代码只校验列名存在 + 非空，
    于是这份残缺数据被当作正常数据缓存 12 小时，期间：
      - 指数成分股筛选（沪深300/上证50）与 spot 交集≈1 → 恒 0 命中
      - 全市场筛选也只能从少数有市值的股票里选
    且用户完全看不到原因（界面只显示"0 只符合"）。

    Returns:
        (是否可用, 不可用原因)；可用时原因为空串
    """
    if df is None or df.empty:
        return False, "数据为空"

    missing = [c for c in _SPOT_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        return False, f"缺关键字段 {missing}"

    if len(df) < _SPOT_MIN_ROWS:
        return False, f"覆盖不全：仅 {len(df)} 只（全A应 >{_SPOT_MIN_ROWS}），疑似分页截断"

    mktcap = pd.to_numeric(df["总市值"], errors="coerce")
    valid_ratio = float((mktcap > 0).sum()) / max(len(df), 1)
    if valid_ratio < _SPOT_MIN_MKTCAP_RATIO:
        return False, (
            f"总市值有效率仅 {valid_ratio:.1%}（需 ≥{_SPOT_MIN_MKTCAP_RATIO:.0%}）"
            f"，市值类条件将大面积失效"
        )

    codes = set(df["代码"].astype(str).str.zfill(6))
    missing_sentinels = [c for c in _SPOT_SENTINEL_CODES if c not in codes]
    if missing_sentinels:
        return False, (
            f"缺失基准蓝筹 {missing_sentinels}（共 {len(df)} 只），"
            f"疑似按市值排序的分页未取完"
        )

    return True, ""
