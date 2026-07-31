"""
tests/test_architecture.py — 架构约束的可执行守卫（A2/A3）

把"分层与依赖方向"变成会失败的测试，防止架构随新功能腐化：
  1. 模块级依赖图必须无环（曾有 4 对循环依赖，靠 147 处延迟导入掩盖）
  2. 底层模块不得反向依赖业务层
  3. 已下沉的横切能力（notify）不得再依赖任何业务层
"""
from __future__ import annotations

import collections
import os
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"


def _module_deps() -> dict[str, set[str]]:
    """只统计**模块级** import（无缩进）——函数内延迟导入不构成静态依赖"""
    deps: dict[str, set[str]] = collections.defaultdict(set)
    for p in SRC.rglob("*.py"):
        if "__pycache__" in str(p):
            continue
        mod = "/".join(p.relative_to(ROOT).as_posix().split("/")[:2])
        for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
            m = re.match(r"(?:from|import)\s+(src\.[\w.]+)", line)
            if not m:
                continue
            target = "/".join(m.group(1).replace(".", "/").split("/")[:2])
            if target != mod:
                deps[mod].add(target)
    return deps


@pytest.mark.unit
class TestNoCycles:
    def test_no_circular_dependencies(self):
        deps = _module_deps()
        cycles = {
            tuple(sorted((a, b)))
            for a in deps for b in deps[a]
            if a in deps.get(b, set())
        }
        assert not cycles, (
            f"出现循环依赖 {sorted(cycles)}。"
            f"通常意味着某个能力放错了层——考虑把共享部分下沉为独立包"
            f"（如 notify/monitors 的做法），而不是靠函数内延迟导入掩盖")

    def test_dependency_graph_is_dag(self):
        """完整拓扑排序，捕捉三元及以上的长环"""
        deps = _module_deps()
        nodes = set(deps) | {v for s in deps.values() for v in s}
        indeg = dict.fromkeys(nodes, 0)
        for a in deps:
            for b in deps[a]:
                indeg[b] += 1
        queue = [n for n, d in indeg.items() if d == 0]
        seen = 0
        while queue:
            n = queue.pop()
            seen += 1
            for b in deps.get(n, ()):
                indeg[b] -= 1
                if indeg[b] == 0:
                    queue.append(b)
        assert seen == len(nodes), "依赖图存在环（拓扑排序未覆盖全部模块）"


@pytest.mark.unit
class TestLayerDirection:
    """底层不得反向依赖上层"""

    # 模块 → 不允许依赖的模块
    FORBIDDEN = {
        "src/core": ("src/services", "src/api", "src/automation", "src/analysis",
                     "src/strategy", "src/portfolio", "src/ml", "src/monitors"),
        "src/utils": ("src/services", "src/api", "src/automation", "src/analysis",
                      "src/core", "src/data", "src/ml"),
        "src/notify": ("src/services", "src/api", "src/automation", "src/analysis",
                       "src/strategy", "src/portfolio", "src/ml", "src/monitors"),
        "src/data": ("src/services", "src/api", "src/automation", "src/analysis",
                     "src/strategy", "src/ml", "src/monitors"),
    }

    def test_low_layers_do_not_depend_upward(self):
        deps = _module_deps()
        violations = []
        for mod, forbidden in self.FORBIDDEN.items():
            for bad in deps.get(mod, set()) & set(forbidden):
                violations.append(f"{mod} -> {bad}")
        assert not violations, f"底层反向依赖业务层: {violations}"

    def test_notify_is_pure_crosscutting(self):
        """notify 是横切能力（services/automation/ml 都用），只能依赖 core/utils"""
        deps = _module_deps()
        allowed = {"src/core", "src/utils"}
        extra = deps.get("src/notify", set()) - allowed
        assert not extra, f"notify 不应依赖业务层: {sorted(extra)}"


@pytest.mark.unit
class TestModuleRelocations:
    """A2 迁移后的位置约束（防止有人挪回去）"""

    def test_alert_moved_out_of_automation(self):
        assert not (SRC / "automation" / "alert").exists(), \
            "告警包应在 src/notify（横切能力），不应回到 automation 下"
        assert (SRC / "notify" / "__init__.py").exists()

    def test_monitors_moved_out_of_automation(self):
        assert not (SRC / "automation" / "monitor").exists(), \
            "监控业务逻辑应在 src/monitors，automation 只负责调度"
        assert (SRC / "monitors" / "__init__.py").exists()

    def test_fetchers_moved_to_data_layer(self):
        assert not (SRC / "core" / "data_fetcher.py").exists(), \
            "取数器属于 data 层，不应在 core（曾导致 core<->data 循环依赖）"
        assert (SRC / "data" / "fetchers.py").exists()

    def test_dead_web_package_removed(self):
        assert not (SRC / "web").exists(), "Streamlit 残留目录应已删除"


@pytest.mark.unit
class TestFileSizeBudget:
    """超大文件预算：单文件超阈值通常意味着职责过多（B1/B2 的守卫）"""

    MAX_LINES = 900
    # 既有的大文件豁免（新代码不应再加入此列表）
    EXEMPT = {"src/api/main.py", "src/services/market_monitor_service.py",
              "src/automation/scheduler.py"}

    def test_no_oversized_modules(self):
        offenders = []
        for p in SRC.rglob("*.py"):
            if "__pycache__" in str(p):
                continue
            rel = p.relative_to(ROOT).as_posix()
            if rel in self.EXEMPT:
                continue
            n = len(p.read_text(encoding="utf-8", errors="ignore").splitlines())
            if n > self.MAX_LINES:
                offenders.append(f"{rel} ({n} 行)")
        assert not offenders, (
            f"以下模块超过 {self.MAX_LINES} 行，建议按职责拆分: {offenders}")
