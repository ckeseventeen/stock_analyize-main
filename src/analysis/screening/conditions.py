"""
src/analysis/screening/conditions.py — 向后兼容 re-export shim

真实实现已拆分到 `src/analysis/screening/conditions_pkg/`（原单文件 1869 行、
40 个条件类，改动时极易冲突且难以定位）：

    conditions_pkg/base.py         条件契约与共享基类
    conditions_pkg/fundamental.py  基本面/Spot 快照条件
    conditions_pkg/entry.py        买入向条件
    conditions_pkg/exit.py         卖出向条件
    conditions_pkg/meta.py         分类表与方向推断

本文件保留是为了让既有的 `from src.analysis.screening.conditions import X`
继续工作（下游有 10+ 处引用）。**新代码请直接从 conditions_pkg 的子模块导入**，
按需引入更清晰。
"""
from __future__ import annotations

from src.analysis.screening.conditions_pkg import *  # noqa: F401,F403
from src.analysis.screening.conditions_pkg import __all__  # noqa: F401
