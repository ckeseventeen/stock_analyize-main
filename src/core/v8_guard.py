"""
src/core/v8_guard.py — py_mini_racer(V8) 崩溃守卫（自适应探测版）

## 为什么需要这个

akshare **自身依赖** mini-racer（新浪美股日线等接口要执行 JS 解密），
pywencai 也依赖它。而在部分环境下，V8 初始化会直接触发

    [FATAL:partition_address_space.cc] Check failed: !IsConfigurablePoolInitialized()

这是 **FATAL 级中止**：进程被当场杀死，`try/except` 完全无法捕获，
uvicorn 服务器直接消失（用户侧表现为"所有功能都用不了"）。

## 为什么不能一刀切禁用

初版守卫把 MiniRacer 换成"实例化即抛异常"的桩。它确实挡住了崩溃，
但**误伤了美股**：`ak.stock_us_daily`（新浪）要靠 V8 解密，而东财美股接口
在国内网络常年不通，新浪一断美股就彻底没数据源了。

而 V8 是否会 FATAL **因环境而异**——同一份代码在 A 机器崩、B 机器完全正常。
所以正确做法不是假定它一定崩，而是**实测这台机器到底崩不崩**。

## 做法：隔离子进程懒探测

首次真正用到 V8 时，先在**独立子进程**里初始化一次 V8：

  - 子进程正常退出 → 本环境 V8 安全，放行（美股等数据源照常可用）
  - 子进程被 FATAL 杀死（非零退出码）→ 装桩，后续实例化抛可捕获异常，
    走项目已有的多源降级链，而不是杀死主进程

探测结果按 (解释器路径, mini-racer 版本) 落盘缓存 7 天，只有第一次付
~1-2s 子进程启动成本；从不碰 V8 的进程则一分钱不花（懒触发）。

**注意这是强信号而非证明**：子进程能初始化，不代表主进程在任意并发时序下
都安全。若线上仍观测到 FATAL，用 `STOCK_ANALYZE_DISABLE_V8=1` 硬禁用。

## 开关

  - `STOCK_ANALYZE_ENABLE_V8=1`  完全不装守卫（V8 直通，自担风险）
  - `STOCK_ANALYZE_DISABLE_V8=1` 跳过探测直接硬禁用（已知会崩的环境）
  - 默认                          装懒守卫，按探测结果放行或拦截

守卫状态可通过 `guard_status()` 查询（/api/health 会带上）。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

_GUARD_APPLIED = False
_GUARD_REASON = ""
_GUARD_MODE = ""          # "lazy" | "hard" | ""

# 需要保护的模块名（py_mini_racer 是经典名；mini_racer 是新包名）
_TARGET_MODULES = ("py_mini_racer", "mini_racer")

# 探测子进程内设此标记，防止子进程自己再装守卫 → 递归探测
_PROBE_CHILD_MARKER = "STOCK_ANALYZE_V8_PROBE_CHILD"

# 探测子进程执行的代码：只初始化 V8 并求值，不引入本项目任何模块
_PROBE_CODE = "import py_mini_racer as m; assert m.MiniRacer().eval('1+1') == 2"

_PROBE_TIMEOUT_S = 30
_PROBE_CACHE_TTL_S = 7 * 24 * 3600

_ERROR_MESSAGE = (
    "py_mini_racer(V8) 在本环境会触发 FATAL 崩溃（已由子进程探测确认），"
    "已被 v8_guard 拦截。依赖 JS 解密的数据源（新浪美股、同花顺问财）不可用，"
    "请改用其他数据源；确认本机 V8 正常可设 STOCK_ANALYZE_ENABLE_V8=1。"
)

# 进程级探测结果缓存（None = 未探测）
_probe_result: bool | None = None
_probe_reason: str = "未探测"


class V8Disabled(RuntimeError):
    """V8 被守卫拦截（可捕获，用于触发数据源降级）"""


# ============================================================================
# 子进程探测
# ============================================================================

def _probe_cache_path() -> Path:
    return Path(__file__).resolve().parents[2] / "cache" / "v8_probe.json"


def _mini_racer_version() -> str:
    """mini-racer 换版本可能改变 V8 行为，故纳入缓存键"""
    from importlib.metadata import version
    for dist in ("mini-racer", "py-mini-racer"):
        try:
            return version(dist)
        except Exception:
            continue
    return "unknown"


def _cache_key() -> str:
    return f"{sys.executable}|{_mini_racer_version()}"


def _read_cached_probe() -> tuple[bool, str] | None:
    try:
        raw = json.loads(_probe_cache_path().read_text(encoding="utf-8"))
    except Exception:
        return None
    if raw.get("key") != _cache_key():
        return None
    if time.time() - float(raw.get("ts", 0)) > _PROBE_CACHE_TTL_S:
        return None
    return bool(raw.get("ok")), str(raw.get("reason", ""))


def _write_cached_probe(ok: bool, reason: str) -> None:
    try:
        path = _probe_cache_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(
            {"key": _cache_key(), "ok": ok, "reason": reason, "ts": time.time()},
            ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass  # 缓存写失败只是下次多探测一遍，不影响正确性


def probe_v8(force: bool = False) -> bool:
    """
    探测本环境 V8 能否安全初始化。

    在独立子进程里初始化一次 V8：子进程被 FATAL 杀死不会影响主进程，
    而这正是我们要探测的现象。

    Args:
        force: 忽略进程级与磁盘缓存，强制重新探测
    """
    global _probe_result, _probe_reason

    if not force and _probe_result is not None:
        return _probe_result

    if not force:
        cached = _read_cached_probe()
        if cached is not None:
            _probe_result, _probe_reason = cached[0], f"{cached[1]}（缓存）"
            return _probe_result

    env = dict(os.environ)
    env[_PROBE_CHILD_MARKER] = "1"
    try:
        proc = subprocess.run([sys.executable, "-c", _PROBE_CODE],
                              capture_output=True, timeout=_PROBE_TIMEOUT_S, env=env)
        ok = proc.returncode == 0
        if ok:
            reason = "子进程 V8 初始化成功"
        else:
            tail = (proc.stderr or b"").decode("utf-8", "replace").strip()[-200:]
            reason = f"子进程退出码 {proc.returncode}：{tail or '无输出（疑似 FATAL 中止）'}"
    except subprocess.TimeoutExpired:
        ok, reason = False, f"探测超时（>{_PROBE_TIMEOUT_S}s）"
    except Exception as e:
        ok, reason = False, f"探测无法执行：{type(e).__name__}: {e}"

    _probe_result, _probe_reason = ok, reason
    _write_cached_probe(ok, reason)
    return ok


def probe_status() -> dict:
    return {"probed": _probe_result is not None,
            "safe": _probe_result,
            "reason": _probe_reason}


def _reset_probe_for_tests() -> None:
    """仅供测试：清掉进程级探测结果"""
    global _probe_result, _probe_reason
    _probe_result, _probe_reason = None, "未探测"


# ============================================================================
# 替身类
# ============================================================================

def _make_hard_stub():
    """硬禁用：实例化即抛可捕获异常，绝不碰 V8"""
    class _DisabledMiniRacer:
        def __init__(self, *args, **kwargs):
            raise V8Disabled(_ERROR_MESSAGE)

    return _DisabledMiniRacer


def _make_lazy_guard(real_cls):
    """
    懒守卫：首次实例化时先探测，安全则**直通真实 MiniRacer**，否则抛异常。

    `__new__` 返回的不是本类实例时 Python 不会再调 `__init__`，
    而 real_cls(...) 已完成构造，因此直通路径与原生行为完全一致。
    """
    class _GuardedMiniRacer:
        _real = real_cls

        def __new__(cls, *args, **kwargs):
            if probe_v8():
                return cls._real(*args, **kwargs)
            raise V8Disabled(_ERROR_MESSAGE)

    return _GuardedMiniRacer


# ============================================================================
# 安装
# ============================================================================

def apply_guard(force: bool = False, hard: bool | None = None) -> bool:
    """
    安装守卫。返回是否实际生效。

    Args:
        force: 忽略 STOCK_ANALYZE_ENABLE_V8 强制安装（测试用）
        hard:  True=硬禁用（不探测）；False=懒守卫；None=看 STOCK_ANALYZE_DISABLE_V8
    """
    global _GUARD_APPLIED, _GUARD_REASON, _GUARD_MODE

    if _GUARD_APPLIED:
        return True
    if os.environ.get(_PROBE_CHILD_MARKER) == "1":
        _GUARD_REASON = "探测子进程内不装守卫"
        return False
    if not force and os.environ.get("STOCK_ANALYZE_ENABLE_V8") == "1":
        _GUARD_REASON = "STOCK_ANALYZE_ENABLE_V8=1，用户显式启用 V8"
        return False

    if hard is None:
        hard = os.environ.get("STOCK_ANALYZE_DISABLE_V8") == "1"

    patched: list[str] = []
    for name in _TARGET_MODULES:
        try:
            # 只 import 不实例化——V8 在 MiniRacer() 时才初始化，import 是安全的
            module = __import__(name)
        except Exception:
            continue
        real = getattr(module, "MiniRacer", None)
        if real is None:
            continue
        module.MiniRacer = _make_hard_stub() if hard else _make_lazy_guard(real)
        patched.append(name)
        sys.modules[name] = module

    _GUARD_APPLIED = bool(patched)
    _GUARD_MODE = ("hard" if hard else "lazy") if patched else ""
    if not patched:
        _GUARD_REASON = "未安装 py_mini_racer/mini_racer，无需守卫"
    elif hard:
        _GUARD_REASON = f"已硬禁用 {patched} 的 MiniRacer（STOCK_ANALYZE_DISABLE_V8=1）"
    else:
        _GUARD_REASON = f"已为 {patched} 的 MiniRacer 装懒守卫（首次使用时子进程探测）"
    return _GUARD_APPLIED


def guard_status() -> dict:
    """守卫状态（供 /api/health 暴露，便于线上确认）"""
    return {
        "applied": _GUARD_APPLIED,
        "mode": _GUARD_MODE,
        "reason": _GUARD_REASON,
        "env_override": os.environ.get("STOCK_ANALYZE_ENABLE_V8") == "1",
        "probe": probe_status(),
    }


def v8_usable() -> bool:
    """
    V8 当前是否可用（供数据源判断要不要走 JS 解密路径）。

    未装守卫 = 用户显式启用，视为可用；硬禁用 = 不可用；懒守卫 = 看探测结果。
    """
    if not _GUARD_APPLIED:
        return True
    if _GUARD_MODE == "hard":
        return False
    return probe_v8()
