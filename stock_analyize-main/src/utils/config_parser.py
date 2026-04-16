"""
src/utils/config_parser.py — 统一配置解析工具

支持：
  - YAML / JSON 配置文件读写
  - 环境变量覆盖（敏感配置如 API Key 不写入文件）
  - 配置合并（基础配置 + 环境配置）
  - Schema 基础验证
"""
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger("stock_analyzer")


class ConfigParser:
    """
    配置解析器。

    使用示例:
        parser = ConfigParser("config/config.yaml")
        token = parser.get("tushare.token", default="")
        db_config = parser.get_section("database")
    """

    def __init__(self, config_path: str, env_prefix: str = "STOCK_"):
        """
        Args:
            config_path: 配置文件路径（YAML/JSON）
            env_prefix: 环境变量前缀（如 STOCK_DB_PASSWORD 覆盖 db.password）
        """
        self._path = Path(config_path)
        self._env_prefix = env_prefix
        self._data: dict = {}
        self._load()

    def _load(self) -> None:
        """加载配置文件"""
        if not self._path.exists():
            logger.warning(f"配置文件不存在: {self._path}，使用空配置")
            return

        try:
            with open(self._path, encoding="utf-8") as f:
                if self._path.suffix in (".yaml", ".yml"):
                    self._data = yaml.safe_load(f) or {}
                elif self._path.suffix == ".json":
                    self._data = json.load(f)
                else:
                    raise ValueError(f"不支持的配置格式: {self._path.suffix}")
            logger.debug(f"配置文件加载成功: {self._path}")
        except Exception as e:
            logger.error(f"配置文件加载失败 [{self._path}]: {e}")
            self._data = {}

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        按点分路径获取配置值，支持环境变量覆盖。

        Args:
            key_path: 配置键路径（如 "database.host"）
            default: 键不存在时的默认值

        Returns:
            配置值，优先级：环境变量 > 配置文件 > default
        """
        # 1. 检查环境变量（STOCK_DATABASE_HOST -> database.host）
        env_key = self._env_prefix + key_path.upper().replace(".", "_")
        env_val = os.environ.get(env_key)
        if env_val is not None:
            logger.debug(f"使用环境变量 {env_key}={env_val[:4]}***")
            return env_val

        # 2. 按点路径查找配置项
        keys = key_path.split(".")
        val = self._data
        for k in keys:
            if isinstance(val, dict):
                val = val.get(k)
            else:
                return default
        return val if val is not None else default

    def get_section(self, section: str) -> dict:
        """获取整个配置段落（dict），不存在则返回 {}"""
        return self._data.get(section, {})

    def get_all(self) -> dict:
        """获取完整配置字典"""
        return dict(self._data)

    def set(self, key_path: str, value: Any) -> None:
        """动态设置配置值（仅内存，不写入文件）"""
        keys = key_path.split(".")
        d = self._data
        for k in keys[:-1]:
            d = d.setdefault(k, {})
        d[keys[-1]] = value

    def save(self, output_path: Optional[str] = None) -> None:
        """将当前配置写回文件（默认覆盖原文件）"""
        path = Path(output_path) if output_path else self._path
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(self._data, f, allow_unicode=True, default_flow_style=False)
        logger.info(f"配置已保存: {path}")

    def __repr__(self) -> str:
        return f"ConfigParser(path={self._path}, keys={list(self._data.keys())})"


def load_yaml(path: str) -> dict:
    """便捷函数：直接加载 YAML 文件，失败返回 {}"""
    try:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"load_yaml 失败 [{path}]: {e}")
        return {}


def load_json(path: str) -> dict:
    """便捷函数：直接加载 JSON 文件，失败返回 {}"""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"load_json 失败 [{path}]: {e}")
        return {}
