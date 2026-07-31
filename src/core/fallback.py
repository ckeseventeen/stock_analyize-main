"""
src/core/fallback.py — 统一的多源降级链

解决什么问题：项目里"多源降级"这一个概念曾有至少三份手写实现
（data_provider 25 处、core/data_fetcher 12 处、stock_service 4 处），
行为各不相同——有的校验数据质量有的不校验、有的记熔断有的不记、
失败时有的返回空表有的抛异常。于是同类 bug 要修好几遍
（如熔断器被港美股请求误伤、残缺数据被当合格数据缓存）。

这里把降级链变成**声明式**：给一串数据源 + 一个质量校验函数，
链本身负责按序尝试、校验、记录、以及"全都不合格时返回最好的一份并
标记退化"这套统一语义。

用法::
    from src.core.fallback import DataSource, FallbackChain

    chain = FallbackChain(
        name="全A行情",
        sources=[
            DataSource("东财直连", fetch_em, weight=100),
            DataSource("akshare", fetch_ak),
            DataSource("问财", fetch_wencai),
        ],
        validator=validate_spot_quality,   # (data) -> (ok: bool, reason: str)
        size_of=len,                       # 用于"哪份最完整"的比较
    )
    result = chain.run()      # -> DataResult（OK / DEGRADED / FAILED）
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable

from src.core.data_health import DataResult
from src.utils.logger import get_logger

logger = get_logger("fallback")

# (data) -> (是否合格, 不合格原因)
Validator = Callable[[Any], "tuple[bool, str]"]


def _is_empty(data: Any) -> bool:
    """判空：兼容 DataFrame / 容器 / None"""
    if data is None:
        return True
    empty_attr = getattr(data, "empty", None)
    if isinstance(empty_attr, bool):
        return empty_attr
    try:
        return len(data) == 0
    except TypeError:
        return False


@dataclass
class DataSource:
    """
    降级链里的一个数据源。

    Attributes:
        label: 人类可读名（日志/告警里显示）
        fetcher: 无参可调用对象，返回数据（抛异常也可以，链会捕获）
        on_success / on_failure: 可选回调（如熔断器记账）
        skip_if: 返回 True 时跳过本源（如熔断器开启中）
    """

    label: str
    fetcher: Callable[[], Any]
    on_success: Callable[[], None] | None = None
    on_failure: Callable[[], None] | None = None
    skip_if: Callable[[], bool] | None = None


@dataclass
class FallbackChain:
    """
    多源降级链：按序尝试直到拿到**通过质量校验**的数据。

    语义（这套统一语义正是手写实现最容易漏掉的部分）：
      - 某源抛异常 / 返回空 / 未通过校验 → 记日志，继续下一个源
      - 拿到合格数据 → 立即返回 DataResult.ok
      - 全部不合格但有数据 → 返回其中"最完整"的一份，标记 DEGRADED + 原因
        （半份数据往往仍有价值，但必须让用户知道）
      - 全部失败且无任何数据 → DataResult.failed（**不是**空数据）

    Attributes:
        validator: 质量校验；None 表示只要非空即合格
        size_of: 比较"哪份更完整"的度量，默认 len()
    """

    name: str
    sources: list[DataSource]
    validator: Validator | None = None
    size_of: Callable[[Any], int] = field(default=lambda d: len(d) if d is not None else 0)

    def run(self) -> DataResult:
        best_data: Any = None
        best_size = -1
        best_reason = "所有数据源均不可用"
        best_source = ""
        attempted = 0

        for src in self.sources:
            if src.skip_if is not None:
                try:
                    if src.skip_if():
                        logger.debug(f"[{self.name}] 跳过 {src.label}（skip_if）")
                        continue
                except Exception:
                    pass

            attempted += 1
            t0 = time.perf_counter()
            try:
                data = src.fetcher()
            except Exception as e:
                logger.warning(
                    f"[{self.name}] {src.label} 抛异常: {type(e).__name__}: {e}")
                if src.on_failure:
                    try:
                        src.on_failure()
                    except Exception:
                        pass
                continue

            elapsed = time.perf_counter() - t0
            if _is_empty(data):
                logger.warning(f"[{self.name}] {src.label} 返回空（{elapsed:.1f}s），继续降级")
                if src.on_failure:
                    try:
                        src.on_failure()
                    except Exception:
                        pass
                continue

            ok, reason = (True, "")
            if self.validator is not None:
                try:
                    ok, reason = self.validator(data)
                except Exception as e:
                    ok, reason = False, f"质量校验异常: {type(e).__name__}: {e}"

            if ok:
                logger.info(
                    f"[{self.name}] {src.label} 数据合格"
                    f"（{self._size(data)} 条，{elapsed:.1f}s），采用")
                if src.on_success:
                    try:
                        src.on_success()
                    except Exception:
                        pass
                return DataResult.ok(data, source=src.label,
                                     size=self._size(data), elapsed=round(elapsed, 2))

            logger.warning(f"[{self.name}] {src.label} 数据不合格（{reason}），继续降级")
            size = self._size(data)
            if size > best_size:
                best_data, best_size, best_reason, best_source = (
                    data, size, reason, src.label)

        if best_data is not None:
            logger.error(
                f"[{self.name}] 全部数据源未通过质量校验，退而使用最完整的一份"
                f"（来自 {best_source}，{best_size} 条；问题：{best_reason}）")
            return DataResult.degraded(best_data, best_reason, source=best_source,
                                       size=best_size, attempted=attempted)

        logger.error(f"[{self.name}] 全部 {attempted} 个数据源均失败")
        return DataResult.failed(best_reason, source=self.name, attempted=attempted)

    def _size(self, data: Any) -> int:
        try:
            return int(self.size_of(data))
        except Exception:
            return 0
