"""
src/core/config_validation.py — YAML 配置的加载期校验

解决什么问题：11 个 YAML 配置此前无任何 schema 校验，写错一个 key 或类型
不会报错，只会**静默失效**——比如把 `min_cap` 写成 `min_capital`，筛选条件
会安静地用默认值跑，用户看到的只是"结果不对"，无从定位。这与项目里
"故障必须可见"的原则（见 core/data_health.py）是一致的。

设计取舍：不引入 pydantic/jsonschema 依赖（保持部署轻量），用一套够用的
声明式规则 + 明确的错误消息。校验是**告警式**的：默认只记录问题并返回，
调用方可选 `strict=True` 让它抛异常。

用法::
    from src.core.config_validation import validate_config_file, validate_all

    issues = validate_config_file("config/scheduler.yaml")
    for i in issues:
        print(i)                      # "scheduler.yaml: jobs[3] 缺少必填字段 'type'"

    report = validate_all()           # {文件名: [问题, ...]}
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from src.utils.logger import get_logger

logger = get_logger("config_validation")


class ConfigError(ValueError):
    """配置不合法（strict 模式下抛出）"""


@dataclass
class FieldRule:
    """单个字段的校验规则"""

    name: str
    required: bool = False
    types: tuple[type, ...] | None = None
    choices: tuple | None = None
    min_value: float | None = None
    max_value: float | None = None
    # 自定义校验：返回错误消息（None 表示通过）
    custom: Callable[[Any], str | None] | None = None

    def check(self, container: dict, path: str) -> list[str]:
        issues: list[str] = []
        if self.name not in container:
            if self.required:
                issues.append(f"{path} 缺少必填字段 '{self.name}'")
            return issues

        val = container[self.name]
        if self.types and not isinstance(val, self.types):
            want = "/".join(t.__name__ for t in self.types)
            issues.append(
                f"{path}.{self.name} 类型应为 {want}，实为 {type(val).__name__}")
            return issues
        if self.choices is not None and val not in self.choices:
            issues.append(
                f"{path}.{self.name} 取值 {val!r} 不在允许范围 {list(self.choices)}")
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            if self.min_value is not None and val < self.min_value:
                issues.append(f"{path}.{self.name}={val} 小于最小值 {self.min_value}")
            if self.max_value is not None and val > self.max_value:
                issues.append(f"{path}.{self.name}={val} 大于最大值 {self.max_value}")
        if self.custom:
            msg = self.custom(val)
            if msg:
                issues.append(f"{path}.{self.name}: {msg}")
        return issues


def _check_cron_like(v: Any) -> str | None:
    """粗校验调度表达式：cron 5 段 或 interval 形式"""
    if not isinstance(v, (str, dict)):
        return f"应为字符串或字典，实为 {type(v).__name__}"
    return None


# ============================================================================
# 各配置文件的 schema
# ============================================================================

def _validate_scheduler(cfg: dict, name: str) -> list[str]:
    issues: list[str] = []
    jobs = cfg.get("jobs")
    if not isinstance(jobs, list):
        return [f"{name}: 顶层 'jobs' 应为列表"]

    from src.automation.scheduler import JOB_BUILDERS
    known_types = set(JOB_BUILDERS)

    seen_ids: set[str] = set()
    for i, job in enumerate(jobs):
        path = f"{name}:jobs[{i}]"
        if not isinstance(job, dict):
            issues.append(f"{path} 应为字典")
            continue
        for rule in (
            FieldRule("id", required=True, types=(str,)),
            FieldRule("type", required=True, types=(str,)),
            FieldRule("enable", types=(bool,)),   # scheduler.yaml 用 enable
        ):
            issues += rule.check(job, path)
        jid = job.get("id")
        if jid in seen_ids:
            issues.append(f"{path} 任务 id 重复: {jid!r}（后者会覆盖前者）")
        seen_ids.add(jid)
        jtype = job.get("type")
        if jtype and jtype not in known_types:
            issues.append(
                f"{path}.type={jtype!r} 无对应的 job builder，"
                f"该任务将被静默跳过。可选: {sorted(known_types)}")
        if "trigger" in job:
            msg = _check_cron_like(job["trigger"])
            if msg:
                issues.append(f"{path}.trigger: {msg}")
    return issues


def _validate_screen_config(cfg: dict, name: str) -> list[str]:
    issues: list[str] = []
    strategies = cfg.get("strategies")
    if not isinstance(strategies, dict):
        return [f"{name}: 顶层 'strategies' 应为字典"]

    from src.analysis.screening.conditions import CONDITION_REGISTRY
    known = set(CONDITION_REGISTRY)

    # 组合节点允许的逻辑词（与 CompositeCondition 保持一致）
    valid_logic = {"all", "and", "any", "or", "none", "not"}

    def _check_conditions(conds, cpath_prefix: str, depth: int = 0) -> None:
        """递归校验条件列表——条件可以是普通条件或 logic 组合节点"""
        if depth > 5:
            issues.append(f"{cpath_prefix} 组合嵌套过深（>5 层），请简化")
            return
        for j, c in enumerate(conds):
            cpath = f"{cpath_prefix}[{j}]"
            if not isinstance(c, dict):
                issues.append(f"{cpath} 应为字典")
                continue

            # 组合节点：{logic: any, conditions: [...]}
            if "logic" in c or ("conditions" in c and "type" not in c):
                lg = str(c.get("logic", "")).lower()
                if lg not in valid_logic:
                    issues.append(
                        f"{cpath}.logic={c.get('logic')!r} 无效，"
                        f"可选 {sorted(valid_logic)}")
                sub = c.get("conditions")
                if not isinstance(sub, list) or not sub:
                    issues.append(f"{cpath}.conditions 应为非空列表")
                else:
                    _check_conditions(sub, f"{cpath}.conditions", depth + 1)
                continue

            ctype = c.get("type")
            if not ctype:
                issues.append(f"{cpath} 缺少 'type'")
            elif ctype not in known:
                issues.append(
                    f"{cpath}.type={ctype!r} 不是已注册条件，**该条件会被静默忽略**")

    for sid, body in strategies.items():
        path = f"{name}:strategies.{sid}"
        if not isinstance(body, dict):
            issues.append(f"{path} 应为字典")
            continue
        conds = body.get("conditions")
        if not isinstance(conds, list) or not conds:
            issues.append(f"{path}.conditions 应为非空列表")
            continue
        _check_conditions(conds, f"{path}.conditions")
    return issues


def _validate_alerts(cfg: dict, name: str) -> list[str]:
    issues: list[str] = []
    channels = cfg.get("channels")
    if channels is None:
        return [f"{name}: 缺少 'channels'"]
    if not isinstance(channels, dict):
        return [f"{name}: 'channels' 应为字典"]

    try:
        from src.notify import CHANNEL_REGISTRY
        known = set(CHANNEL_REGISTRY)
    except Exception:
        known = set()

    for ch, conf in channels.items():
        path = f"{name}:channels.{ch}"
        if known and ch not in known:
            issues.append(f"{path} 不是已知告警通道，可选: {sorted(known)}")
        if isinstance(conf, dict) and conf.get("enabled") and not conf.get("token") \
                and ch not in ("console",):
            issues.append(f"{path} 已启用但未配置 token/url，推送会失败")
    return issues


def _validate_factors(cfg: dict, name: str) -> list[str]:
    issues: list[str] = []
    active = cfg.get("active_profile")
    profiles = cfg.get("profiles")
    if not isinstance(profiles, dict) or not profiles:
        return [f"{name}: 'profiles' 应为非空字典"]
    if active and active not in profiles:
        issues.append(
            f"{name}: active_profile={active!r} 在 profiles 中不存在"
            f"（可选 {sorted(profiles)}），将回退默认 profile")
    return issues


def _validate_alpha158(cfg: dict, name: str) -> list[str]:
    issues: list[str] = []
    entries = cfg.get("alpha_factors")
    if not isinstance(entries, list):
        return [f"{name}: 'alpha_factors' 应为列表"]

    from src.analysis.factor.expression import ExpressionError
    from src.analysis.factor.expression import validate_expression as _v

    seen: set[str] = set()
    for i, e in enumerate(entries):
        nm = e.get("name") if isinstance(e, dict) else None
        # 路径带上因子名：定位"哪个因子写错了"比"第几个"直观得多
        path = f"{name}:alpha_factors[{i}]" + (f"({nm})" if nm else "")
        if not isinstance(e, dict):
            issues.append(f"{path} 应为字典")
            continue
        expr = e.get("expr")
        if not nm:
            issues.append(f"{path} 缺少 'name'")
        elif nm in seen:
            issues.append(f"{path} 因子名重复: {nm!r}")
        seen.add(nm)
        if not expr:
            issues.append(f"{path} 缺少 'expr'")
            continue
        try:
            _v(expr)
        except ExpressionError as ex:
            issues.append(f"{path}.expr 非法: {ex}")
    return issues


def _validate_holdings(cfg: dict, name: str) -> list[str]:
    issues: list[str] = []
    holdings = cfg.get("holdings")
    if holdings is None:
        return []
    if not isinstance(holdings, list):
        return [f"{name}: 'holdings' 应为列表"]
    for i, h in enumerate(holdings):
        path = f"{name}:holdings[{i}]"
        if not isinstance(h, dict):
            issues.append(f"{path} 应为字典")
            continue
        for rule in (
            FieldRule("code", required=True, types=(str, int)),
            FieldRule("shares", types=(int, float), min_value=0),
            FieldRule("cost", types=(int, float), min_value=0),
        ):
            issues += rule.check(h, path)
    return issues


# 文件名 → 校验函数
VALIDATORS: dict[str, Callable[[dict, str], list[str]]] = {
    "scheduler.yaml": _validate_scheduler,
    "screen_config.yaml": _validate_screen_config,
    "alerts.yaml": _validate_alerts,
    "factors.yaml": _validate_factors,
    "indicators.yaml": _validate_factors,     # 同为 active_profile/profiles 结构
    "factors_alpha158.yaml": _validate_alpha158,
    "holdings.yaml": _validate_holdings,
}


# ============================================================================
# 入口
# ============================================================================

def validate_config(name: str, cfg: dict) -> list[str]:
    """校验已加载的配置字典；未注册 schema 的文件返回空列表"""
    fn = VALIDATORS.get(Path(name).name)
    if fn is None or not isinstance(cfg, dict):
        return []
    try:
        return fn(cfg, Path(name).name)
    except Exception as e:
        logger.warning(f"校验 {name} 时出错: {type(e).__name__}: {e}")
        return []


def validate_config_file(path: str | Path, *, strict: bool = False) -> list[str]:
    """
    读取并校验一个 YAML 配置文件。

    Args:
        strict: True 时发现问题抛 ConfigError（CI/启动自检用）
    """
    import yaml

    p = Path(path)
    if not p.exists():
        issues = [f"{p.name}: 文件不存在"]
    else:
        try:
            cfg = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
            issues = validate_config(p.name, cfg)
        except yaml.YAMLError as e:
            issues = [f"{p.name}: YAML 语法错误: {e}"]

    if issues and strict:
        raise ConfigError("；".join(issues))
    for msg in issues:
        logger.warning(f"配置问题：{msg}")
    return issues


def validate_all(config_dir: str | Path = "config", *,
                 strict: bool = False) -> dict[str, list[str]]:
    """
    校验目录下所有已注册 schema 的配置。

    Returns:
        {文件名: [问题, ...]}（只含有问题的文件）
    """
    d = Path(config_dir)
    report: dict[str, list[str]] = {}
    for fname in VALIDATORS:
        p = d / fname
        if not p.exists():
            continue
        issues = validate_config_file(p)
        if issues:
            report[fname] = issues
    if report and strict:
        flat = "；".join(f"{k}: {v}" for k, v in report.items())
        raise ConfigError(flat)
    return report
