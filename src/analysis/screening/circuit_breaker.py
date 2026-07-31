"""
src/analysis/screening/circuit_breaker.py — akshare/东财 接口熔断器

从 data_provider 提取：熔断状态是**进程级共享**的，独立成模块后
边界清晰，也便于测试直接复位状态。
"""
from __future__ import annotations

import time

from src.utils.logger import get_logger

logger = get_logger("akshare_circuit")


# ========================
# akshare 主接口熔断器（process-wide）
# ========================
# 背景：akshare 主接口 stock_zh_a_spot_em() 与个股 K 线接口对东方财富的 IP/反爬
# 敏感，出问题时每次重试要吃 ~7s × 3 = 21s 空等。失败累计达到阈值后进入冷却窗口，
# 期间直接跳过主接口，提升体验。
_AKSHARE_CB_FAIL_THRESHOLD = 2     # 连续失败 N 次进入熔断
_AKSHARE_CB_COOL_SECONDS = 600     # 熔断冷却 10 分钟
_akshare_cb_state = {"fail_count": 0, "cool_until": 0.0}


def _akshare_circuit_open() -> bool:
    """熔断器当前是否处于开启状态（直接短路调用）"""
    return time.time() < _akshare_cb_state["cool_until"]


def _akshare_record_fail() -> None:
    """记一次主接口失败，达到阈值则开启熔断窗口"""
    _akshare_cb_state["fail_count"] += 1
    if _akshare_cb_state["fail_count"] >= _AKSHARE_CB_FAIL_THRESHOLD:
        _akshare_cb_state["cool_until"] = time.time() + _AKSHARE_CB_COOL_SECONDS
        logger.warning(
            f"akshare 主接口连续失败 {_akshare_cb_state['fail_count']} 次，"
            f"进入熔断冷却 {_AKSHARE_CB_COOL_SECONDS}s（其间直接走 Baostock）"
        )


def _akshare_record_success() -> None:
    """成功后清零失败计数和冷却窗口"""
    if _akshare_cb_state["fail_count"] or _akshare_cb_state["cool_until"]:
        _akshare_cb_state["fail_count"] = 0
        _akshare_cb_state["cool_until"] = 0.0
