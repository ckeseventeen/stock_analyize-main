"""
src/ — stock_analyize 包根

这里只做一件事：**在任何子模块（进而 akshare/pywencai）被导入之前，
安装 V8 崩溃守卫**。

背景：akshare 自身依赖 mini-racer，在部分 Windows 环境下 V8 初始化会触发
FATAL 中止，直接杀死进程——不是异常，try/except 拦不住，表现为 uvicorn
服务器毫无征兆地消失、所有功能一起不可用。守卫把它降级为可捕获的异常，
让项目已有的多源降级链正常工作。详见 src/core/v8_guard.py。
"""
from __future__ import annotations

try:
    from src.core.v8_guard import apply_guard as _apply_v8_guard

    _apply_v8_guard()
except Exception:  # 守卫自身绝不能影响包导入
    pass
