"""
src/core/data_health.py — 数据健康度契约

解决什么问题：本项目曾有 294 处 `except Exception`，其中 162 处把异常静默
吞成空值（`pass` / `return pd.DataFrame()`）。故障信号层层丢失后，用户只看到
"0 只符合条件"，无从判断是策略太严、还是数据源挂了。典型事故：全A行情表只
返回 2649/5300 只且 97.9% 市值为 NaN，却被当作正常数据缓存 12 小时。

核心思想：**数据获取的返回值必须携带健康状态**，让"没有数据"和"拿数据失败"
在类型层面就区分开：

    OK        数据完整可用
    DEGRADED  拿到了，但不完整/质量存疑（可用但要告知用户）
    EMPTY     数据源正常，确实没有数据（如非交易日、新股无财报）
    FAILED    获取失败（网络/反爬/接口变更），**不是"没有数据"**

用法::
    from src.core.data_health import DataResult, HealthStatus

    def fetch_something() -> DataResult:
        try:
            df = call_api()
        except Exception as e:
            return DataResult.failed("接口超时", error=e)
        if df.empty:
            return DataResult.empty("当日无数据")
        ok, reason = validate(df)
        return DataResult.ok(df) if ok else DataResult.degraded(df, reason)

    r = fetch_something()
    r.raise_if_failed()                   # 上层可转 502 而不是假装"0 条"
    df = r.data
    if r.is_degraded:
        warnings.append(r.user_message)   # 告诉用户结果不完整及原因
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class HealthStatus(str, Enum):
    """数据健康状态（继承 str 便于日志/JSON 序列化）"""

    OK = "ok"
    DEGRADED = "degraded"
    EMPTY = "empty"
    FAILED = "failed"


@dataclass
class DataResult:
    """
    带健康状态的数据容器。

    Attributes:
        data: 实际数据（FAILED 时可能为 None/空）
        status: 健康状态
        reason: 非 OK 时的原因（面向开发/日志）
        source: 数据来自哪个源（降级链里很有用）
        error: 原始异常（FAILED 时保留，便于 raise ... from）
        meta: 附加信息（行数、耗时、质量指标等）
    """

    data: Any = None
    status: HealthStatus = HealthStatus.OK
    reason: str = ""
    source: str = ""
    error: BaseException | None = None
    meta: dict = field(default_factory=dict)

    # ── 构造快捷方式 ──

    @classmethod
    def ok(cls, data: Any, source: str = "", **meta) -> DataResult:
        return cls(data=data, status=HealthStatus.OK, source=source, meta=meta)

    @classmethod
    def degraded(cls, data: Any, reason: str, source: str = "", **meta) -> DataResult:
        return cls(data=data, status=HealthStatus.DEGRADED, reason=reason,
                   source=source, meta=meta)

    @classmethod
    def empty(cls, reason: str = "无数据", source: str = "", **meta) -> DataResult:
        return cls(data=None, status=HealthStatus.EMPTY, reason=reason,
                   source=source, meta=meta)

    @classmethod
    def failed(cls, reason: str, error: BaseException | None = None,
               source: str = "", **meta) -> DataResult:
        return cls(data=None, status=HealthStatus.FAILED, reason=reason,
                   source=source, error=error, meta=meta)

    # ── 状态判定（统一 is_* 前缀，避免与同名构造器冲突）──

    @property
    def is_ok(self) -> bool:
        return self.status is HealthStatus.OK

    @property
    def is_degraded(self) -> bool:
        return self.status is HealthStatus.DEGRADED

    @property
    def is_empty(self) -> bool:
        return self.status is HealthStatus.EMPTY

    @property
    def is_failed(self) -> bool:
        return self.status is HealthStatus.FAILED

    @property
    def usable(self) -> bool:
        """是否有可用数据（OK 或 DEGRADED）"""
        return self.status in (HealthStatus.OK, HealthStatus.DEGRADED)

    @property
    def user_message(self) -> str:
        """面向用户的一句话说明（前端直接展示）"""
        if self.status is HealthStatus.OK:
            return ""
        if self.status is HealthStatus.DEGRADED:
            return f"⚠️ 数据不完整：{self.reason}" + (
                f"（来源：{self.source}）" if self.source else "")
        if self.status is HealthStatus.EMPTY:
            return f"无数据：{self.reason}"
        return f"❌ 数据获取失败：{self.reason}" + (
            f"（来源：{self.source}）" if self.source else "")

    def unwrap(self, default: Any = None) -> Any:
        """取数据；不可用时返回 default（显式表达"我接受降级"）"""
        return self.data if self.usable and self.data is not None else default

    def raise_if_failed(self) -> DataResult:
        """FAILED 时抛 RuntimeError（链式调用友好）"""
        if self.is_failed:
            raise RuntimeError(self.reason) from self.error
        return self

    def to_dict(self) -> dict:
        """序列化给 API（不含 data 本体）"""
        return {
            "status": self.status.value,
            "reason": self.reason,
            "source": self.source,
            "message": self.user_message,
            **({"meta": self.meta} if self.meta else {}),
        }
