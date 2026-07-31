"""
src/core/v8_guard.py — py_mini_racer(V8) 崩溃守卫

## 为什么需要这个

akshare **自身依赖** mini-racer（用于执行部分数据源的 JS 加密逻辑），
pywencai 也依赖它。而在部分 Windows 环境下，V8 初始化会直接触发

    [FATAL:partition_address_space.cc] Check failed: !IsConfigurablePoolInitialized()

这是 **FATAL 级中止**：进程被当场杀死，`try/except` 完全无法捕获，
uvicorn 服务器直接消失（用户侧表现为"所有功能都用不了"）。

## 做法

进程启动时把 `MiniRacer` 换成一个"实例化即抛 RuntimeError"的桩。
这样：

  - akshare / pywencai 里依赖 JS 的少数接口会**抛异常**（可被捕获）
    → 走本项目已有的多源降级链，而不是杀死进程
  - 不依赖 JS 的接口（东财 clist、stock_zh_a_hist、财报三表等，
    也就是本项目实际用的绝大多数）完全不受影响

**可用性 > 多一个数据源**：宁可少一个问财兜底，也不能让整个服务随机猝死。

## 开关

V8 在你的环境下正常时，设 `STOCK_ANALYZE_ENABLE_V8=1` 可关闭本守卫。
守卫状态可通过 `guard_status()` 查询（/api/health 会带上）。
"""
from __future__ import annotations

import os
import sys

_GUARD_APPLIED = False
_GUARD_REASON = ""

# 需要保护的模块名（py_mini_racer 是经典名；mini_racer 是新包名）
_TARGET_MODULES = ("py_mini_racer", "mini_racer")

_ERROR_MESSAGE = (
    "py_mini_racer(V8) 在本环境会触发 FATAL 崩溃，已被 v8_guard 主动禁用。"
    "依赖 JS 解密的数据源（如同花顺问财）不可用，请改用其他数据源；"
    "确认本机 V8 正常可设 STOCK_ANALYZE_ENABLE_V8=1 关闭守卫。"
)


class V8Disabled(RuntimeError):
    """V8 被守卫禁用（可捕获，用于触发数据源降级）"""


def _make_stub():
    class _DisabledMiniRacer:
        """MiniRacer 替身：实例化即抛可捕获异常，而不是让 V8 杀死进程"""

        def __init__(self, *args, **kwargs):
            raise V8Disabled(_ERROR_MESSAGE)

    return _DisabledMiniRacer


def apply_guard(force: bool = False) -> bool:
    """
    安装守卫。返回是否实际生效。

    Args:
        force: 忽略 STOCK_ANALYZE_ENABLE_V8 强制安装（测试用）
    """
    global _GUARD_APPLIED, _GUARD_REASON

    if _GUARD_APPLIED:
        return True
    if not force and os.environ.get("STOCK_ANALYZE_ENABLE_V8") == "1":
        _GUARD_REASON = "STOCK_ANALYZE_ENABLE_V8=1，用户显式启用 V8"
        return False

    stub = _make_stub()
    patched: list[str] = []
    for name in _TARGET_MODULES:
        try:
            # 只 import 不实例化——V8 在 MiniRacer() 时才初始化，import 是安全的
            module = __import__(name)
        except Exception:
            continue
        if hasattr(module, "MiniRacer"):
            module.MiniRacer = stub
            patched.append(name)
        sys.modules[name] = module

    _GUARD_APPLIED = bool(patched)
    _GUARD_REASON = (f"已禁用 {patched} 的 MiniRacer" if patched
                     else "未安装 py_mini_racer/mini_racer，无需守卫")
    return _GUARD_APPLIED


def guard_status() -> dict:
    """守卫状态（供 /api/health 暴露，便于线上确认）"""
    return {
        "applied": _GUARD_APPLIED,
        "reason": _GUARD_REASON,
        "env_override": os.environ.get("STOCK_ANALYZE_ENABLE_V8") == "1",
    }
