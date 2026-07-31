"""
src/services/strategy_market.py — 策略市场（发布 / 导入 / 本地市场目录）

策略包（Strategy Package）是自包含、可分享的 JSON 文件：
    {
      "schema_version": 1,
      "kind": "strategy_package",
      "id": "my_strategy",           # 建议导入时的策略 ID
      "name": "我的策略",
      "author": "张三",
      "version": "1.0.0",
      "description": "周线 MACD 底背离 + 中小市值",
      "tags": ["技术面", "左侧"],
      "created_at": "2026-07-10T15:00:00",
      "strategy": {...},             # 与 screen_config.yaml 中策略体同构
      "checksum": "sha256:..."       # strategy 体的完整性校验
    }

本地市场目录：<项目根>/market/，文件名 <id>@<version>.json。
SaaS 阶段把目录换成对象存储 + DB 索引即可，包格式不变。
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from src.core.config_io import PROJECT_ROOT
from src.services import screening_service as svc
from src.utils.logger import get_logger

logger = get_logger("strategy_market")

SCHEMA_VERSION = 1
PACKAGE_KIND = "strategy_package"
MARKET_DIR: Path = PROJECT_ROOT / "market"

_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")
_VERSION_RE = re.compile(r"^\d+(\.\d+){0,2}$")


# ============================================================================
# 包构建 / 校验
# ============================================================================

def compute_checksum(strategy_body: dict) -> str:
    """对策略体做规范化 JSON 序列化后取 sha256（键排序，与格式无关）"""
    canonical = json.dumps(strategy_body, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_package(
    sid: str,
    strategy_body: dict,
    *,
    author: str = "",
    version: str = "1.0.0",
    description: str = "",
    tags: list[str] | None = None,
) -> dict:
    """把一个策略体打包成可分享的策略包 dict"""
    body = dict(strategy_body)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": PACKAGE_KIND,
        "id": sid,
        "name": body.get("name", sid),
        "author": author,
        "version": version,
        "description": description,
        "tags": list(tags or []),
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "strategy": body,
        "checksum": compute_checksum(body),
    }


def validate_package(pkg: dict) -> list[str]:
    """
    校验策略包结构与内容。

    Returns:
        错误信息列表；空 = 通过
    """
    errors: list[str] = []
    if not isinstance(pkg, dict):
        return ["包必须是 JSON 对象"]

    if pkg.get("kind") != PACKAGE_KIND:
        errors.append(f"kind 必须是 '{PACKAGE_KIND}'")
    sv = pkg.get("schema_version")
    if not isinstance(sv, int) or sv > SCHEMA_VERSION:
        errors.append(f"schema_version 不支持: {sv}（当前支持 <= {SCHEMA_VERSION}）")

    sid = pkg.get("id", "")
    if not isinstance(sid, str) or not _ID_RE.match(sid):
        errors.append(f"id 非法（仅限字母数字下划线连字符，<=64 字符）: '{sid}'")

    version = pkg.get("version", "")
    if not isinstance(version, str) or not _VERSION_RE.match(version):
        errors.append(f"version 非法（需形如 1.0.0）: '{version}'")

    strategy = pkg.get("strategy")
    if not isinstance(strategy, dict):
        errors.append("缺少 strategy 段或不是对象")
        return errors

    # 条件合法性（复用筛选服务的校验）
    errors.extend(svc.validate_conditions(strategy.get("conditions", [])))

    # 完整性校验（checksum 可选，但存在时必须匹配）
    declared = pkg.get("checksum")
    if declared:
        actual = compute_checksum(strategy)
        if declared != actual:
            errors.append("checksum 不匹配，策略体可能被篡改")

    return errors


# ============================================================================
# 本地市场目录
# ============================================================================

@dataclass
class MarketEntry:
    """市场目录中的一个策略包条目"""
    path: Path
    package: dict = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def valid(self) -> bool:
        return not self.errors

    @property
    def display_name(self) -> str:
        p = self.package
        return f"{p.get('name', '?')} v{p.get('version', '?')}"


def _package_filename(pkg: dict) -> str:
    return f"{pkg['id']}@{pkg.get('version', '0')}.json"


def publish_strategy(
    sid: str,
    *,
    author: str = "",
    version: str = "1.0.0",
    description: str = "",
    tags: list[str] | None = None,
    market_dir: Path | None = None,
    config_path: Path | str | None = None,
) -> tuple[bool, str, Path | None]:
    """
    把 screen_config.yaml 中的策略发布到本地市场目录。

    Returns:
        (成功?, 消息, 包文件路径)
    """
    body = svc.get_strategy(sid, config_path)
    if not body:
        return False, f"策略不存在: {sid}", None

    pkg = build_package(sid, body, author=author, version=version,
                        description=description, tags=tags)
    errors = validate_package(pkg)
    if errors:
        return False, "包校验失败: " + "; ".join(errors), None

    target_dir = market_dir or MARKET_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / _package_filename(pkg)
    try:
        target.write_text(
            json.dumps(pkg, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        return False, f"写入失败: {e}", None
    logger.info(f"策略已发布: {sid} v{version} → {target}")
    return True, f"已发布 {pkg['name']} v{version}", target


def list_packages(market_dir: Path | None = None) -> list[MarketEntry]:
    """扫描市场目录，返回全部包条目（含校验状态），按修改时间倒序"""
    target_dir = market_dir or MARKET_DIR
    if not target_dir.exists():
        return []
    entries: list[MarketEntry] = []
    for f in sorted(target_dir.glob("*.json"),
                    key=lambda p: p.stat().st_mtime, reverse=True):
        entry = MarketEntry(path=f)
        try:
            entry.package = json.loads(f.read_text(encoding="utf-8"))
            entry.errors = validate_package(entry.package)
        except Exception as e:
            entry.errors = [f"文件解析失败: {e}"]
        entries.append(entry)
    return entries


def load_package_text(text: str) -> tuple[dict | None, list[str]]:
    """解析上传的包文本；返回 (包 dict 或 None, 错误列表)"""
    try:
        pkg = json.loads(text)
    except Exception as e:
        return None, [f"JSON 解析失败: {e}"]
    return pkg, validate_package(pkg)


def import_package(
    pkg: dict,
    *,
    new_sid: str | None = None,
    overwrite: bool = False,
    config_path: Path | str | None = None,
) -> tuple[bool, str]:
    """
    把策略包导入到 screen_config.yaml。

    Args:
        new_sid: 导入时改用的策略 ID（None 用包内 id）
        overwrite: 已存在同 ID 策略时是否覆盖
    """
    errors = validate_package(pkg)
    if errors:
        return False, "包校验失败: " + "; ".join(errors)

    sid = (new_sid or pkg["id"]).strip()
    if not _ID_RE.match(sid):
        return False, f"策略 ID 非法: '{sid}'"

    existing = svc.load_all_strategies(config_path)
    if sid in existing and not overwrite:
        return False, f"策略 '{sid}' 已存在；如需覆盖请勾选「覆盖已有」"

    body = dict(pkg["strategy"])
    # 保留来源元数据，便于追溯（不影响筛选引擎，engine 只读已知键）
    body["_market_meta"] = {
        "author": pkg.get("author", ""),
        "version": pkg.get("version", ""),
        "imported_at": datetime.now().isoformat(timespec="seconds"),
        "source_id": pkg.get("id", ""),
    }
    ok, msg = svc.upsert_strategy(sid, body, config_path)
    if ok:
        logger.info(f"策略包已导入: {pkg.get('id')} → {sid}")
        return True, f"已导入为策略 '{sid}'"
    return False, msg
