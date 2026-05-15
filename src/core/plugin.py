"""
src/core/plugin.py — 通用插件注册系统

替代项目中散落的 5 个手写 dict 注册表（STRATEGY_REGISTRY / FACTOR_REGISTRY /
CONDITION_REGISTRY / CHANNEL_REGISTRY / JOB_BUILDERS），提供统一的：

  1. **装饰器注册**：`@registry.register("key")` 直接装饰类即注册
  2. **重复检测**：默认禁止覆盖，强制显式 replace=True
  3. **Schema 自省**：`registry.schema_for(key)` 自动从 dataclass field / __init__
     签名推导出 Web 表单 / YAML 校验需要的字段元数据
  4. **自动发现**：`autodiscover(package)` 递归 import 包下所有 .py，触发装饰器

用法：
    from src.core.plugin import PluginRegistry

    STRATEGY_REGISTRY = PluginRegistry[BaseStrategy]("strategy")

    @STRATEGY_REGISTRY.register("ma_crossover")
    class MACrossoverStrategy(BaseStrategy):
        ...

    cls = STRATEGY_REGISTRY.get("ma_crossover")
    schema = STRATEGY_REGISTRY.schema_for("ma_crossover")  # 给 SchemaForm 用
"""
from __future__ import annotations

import importlib
import pkgutil
from typing import Callable, Generic, Iterator, TypeVar

from src.utils.logger import get_logger

logger = get_logger("plugin")

T = TypeVar("T")


class PluginRegistry(Generic[T]):
    """
    类型安全的插件注册表。

    Args:
        name: 注册表名（仅用于日志/错误信息，如 "strategy"/"factor"）
        allow_replace_default: 是否默认允许覆盖（一般 False，强制显式）
    """

    def __init__(self, name: str, *, allow_replace_default: bool = False):
        self.name = name
        self._registry: dict[str, type[T]] = {}
        self._labels: dict[str, str] = {}
        self._allow_replace = allow_replace_default

    # ------------------------------------------------------------------
    # 注册 / 查询
    # ------------------------------------------------------------------

    def register(
        self,
        key: str,
        *,
        label: str | None = None,
        replace: bool | None = None,
    ) -> Callable[[type[T]], type[T]]:
        """
        装饰器：把类注册到本 registry。

        Args:
            key: 唯一键（如 "ma_crossover"）
            label: 可读名（如 "双均线交叉"），用于 UI 显示；None 用 key
            replace: 是否允许覆盖已有；None 走默认策略

        用法：
            @STRATEGY_REGISTRY.register("ma_crossover", label="双均线交叉")
            class MACrossoverStrategy(BaseStrategy):
                ...
        """
        allow = self._allow_replace if replace is None else replace

        def _decorator(cls: type[T]) -> type[T]:
            k = str(key).strip()
            if not k:
                raise ValueError(f"[{self.name}] register key 不能为空")
            if k in self._registry and not allow:
                raise ValueError(
                    f"[{self.name}] '{k}' 已被 {self._registry[k].__name__} 注册；"
                    f"如需覆盖请传 replace=True"
                )
            self._registry[k] = cls
            self._labels[k] = label or k
            logger.debug(f"[{self.name}] 注册 {k} → {cls.__name__}")
            return cls

        return _decorator

    def register_class(
        self,
        key: str,
        cls: type[T],
        *,
        label: str | None = None,
        replace: bool = False,
    ) -> None:
        """
        命令式注册（不通过装饰器）。

        用于：兼容旧代码迁移期，或从配置文件动态注册。
        """
        self.register(key, label=label, replace=replace)(cls)

    def unregister(self, key: str) -> None:
        """主动注销（测试常用）"""
        self._registry.pop(key, None)
        self._labels.pop(key, None)

    def get(self, key: str) -> type[T]:
        """按 key 获取类；不存在抛 KeyError + 提示可用列表"""
        if key not in self._registry:
            available = ", ".join(sorted(self._registry.keys())) or "(空)"
            raise KeyError(
                f"[{self.name}] 未知 key '{key}'。已注册: {available}"
            )
        return self._registry[key]

    def has(self, key: str) -> bool:
        return key in self._registry

    def keys(self) -> list[str]:
        return list(self._registry.keys())

    def labels(self) -> dict[str, str]:
        return dict(self._labels)

    def items(self) -> Iterator[tuple[str, type[T]]]:
        return iter(self._registry.items())

    def __len__(self) -> int:
        return len(self._registry)

    def __contains__(self, key: str) -> bool:
        return key in self._registry

    def __getitem__(self, key: str) -> type[T]:
        """支持 registry[key] 下标语法（向后兼容老 dict 风格调用）"""
        return self.get(key)

    def __iter__(self):
        return iter(self._registry)

    # ------------------------------------------------------------------
    # Schema 自省
    # ------------------------------------------------------------------

    def schema_for(self, key: str) -> list[dict]:
        """
        从注册的类自省其参数 schema，返回 Web 表单/YAML 校验都能消费的字段列表。

        优先级：
          1. 类显式定义 `_param_schema: list[dict]` 类属性（手动覆盖）
          2. dataclass `__dataclass_fields__` 的 metadata
          3. backtrader 风格 `params = (("name", default), ...)`
          4. `__init__` 签名（inspect.signature）

        Returns:
            [{"key": "fast_period", "type": "int", "default": 10,
              "label": "短期均线", "min": 3, "max": 60, "help": "..."}, ...]
        """
        from src.core.schema_inspect import derive_schema
        cls = self.get(key)
        return derive_schema(cls)


# =============================================================================
# 自动发现
# =============================================================================

def autodiscover(package: str, *, silent_errors: bool = True) -> int:
    """
    递归 import 一个包下所有 .py，触发模块顶层的 @register 装饰器。

    Args:
        package: 包名，如 "src.strategy.backtest.strategies"
        silent_errors: True 时单个模块 import 失败仅 warning；False 抛出

    Returns:
        成功 import 的模块数

    用法：
        # src/strategy/backtest/__init__.py 末尾：
        from src.core.plugin import autodiscover
        autodiscover("src.strategy.backtest")  # 触发所有策略文件的 @register
    """
    try:
        pkg = importlib.import_module(package)
    except ImportError as e:
        if silent_errors:
            logger.warning(f"autodiscover: 包 {package} 不存在: {e}")
            return 0
        raise

    if not hasattr(pkg, "__path__"):
        # 不是 package（是 module），不需要递归
        return 1

    loaded = 0
    for _finder, mod_name, _ispkg in pkgutil.walk_packages(
        pkg.__path__, prefix=f"{package}."
    ):
        try:
            importlib.import_module(mod_name)
            loaded += 1
        except Exception as e:
            if silent_errors:
                logger.warning(f"autodiscover: import {mod_name} 失败: {e}")
            else:
                raise
    return loaded
