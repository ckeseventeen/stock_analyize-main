"""
src/core/settings.py — 全局环境变量配置（pydantic-settings）

代替散落在 4+ 个文件里的 `os.environ.get` 调用，做到：
  - 单点定义所有环境变量名 + 类型 + 默认值
  - 启动时自动读 .env / .env.local（不存在不报错）
  - IDE 类型补全 + 误用立刻报错

用法：
    from src.core.settings import settings

    if settings.scheduler_disabled:
        ...
    if settings.serverchan_key:
        ...

环境变量优先级（pydantic-settings 默认）：
  1. 显式 init 参数
  2. 进程环境变量（os.environ）
  3. .env 文件
  4. 类默认值
"""
from __future__ import annotations

from pathlib import Path

try:
    from pydantic import Field
    from pydantic_settings import BaseSettings, SettingsConfigDict
    _PYDANTIC_OK = True
except ImportError:
    # 降级方案：纯 dataclass + 手动读 os.environ
    _PYDANTIC_OK = False


if _PYDANTIC_OK:
    class Settings(BaseSettings):
        """
        全局设置单例。任何新增环境变量都加在这里，禁止直接 os.environ.get。
        """

        model_config = SettingsConfigDict(
            env_file=(".env", ".env.local"),
            env_file_encoding="utf-8",
            case_sensitive=False,
            extra="ignore",
        )

        # ---------------- 调度器 ----------------
        # B24/SEC6：Web 容器内不启动内嵌调度器（独立 scheduler 服务跑）
        scheduler_disabled: bool = Field(default=False)
        scheduler_enabled: bool = Field(default=True)

        # ---------------- 筛选器 ----------------
        # SCREENER_STRICT=1 时 yaml 拼错条件类型抛错而非 warning
        screener_strict: bool = Field(default=False)

        # ---------------- Tushare（荐股逆向工程历史截面，可选） ----------------
        tushare_token: str = Field(default="")

        # ---------------- 告警通道密钥（SEC1：禁止入 yaml）----------------
        serverchan_key: str = Field(default="")
        bark_key: str = Field(default="")
        pushplus_token: str = Field(default="")

        # ---------------- 数据库（docker-compose 兼容，目前未使用）----------------
        db_host: str = Field(default="")
        db_port: int = Field(default=3306)
        db_name: str = Field(default="")
        db_user: str = Field(default="")
        db_password: str = Field(default="")

        # ---------------- 日志 ----------------
        log_level: str = Field(default="INFO")

        # ---------------- 数据源 / 路径 ----------------
        cache_dir: Path = Field(default=Path("./cache"))
        output_dir: Path = Field(default=Path("./output"))
        config_dir: Path = Field(default=Path("./config"))

else:
    # 兜底：不依赖 pydantic 也能用（仅 os.environ.get 包装）
    import os
    from dataclasses import dataclass

    @dataclass
    class Settings:
        scheduler_disabled: bool = False
        scheduler_enabled: bool = True
        screener_strict: bool = False
        tushare_token: str = ""
        serverchan_key: str = ""
        bark_key: str = ""
        pushplus_token: str = ""
        db_host: str = ""
        db_port: int = 3306
        db_name: str = ""
        db_user: str = ""
        db_password: str = ""
        log_level: str = "INFO"
        cache_dir: Path = Path("./cache")
        output_dir: Path = Path("./output")
        config_dir: Path = Path("./config")

        def __post_init__(self):
            self.scheduler_disabled = os.environ.get("SCHEDULER_DISABLED", "0") == "1"
            self.scheduler_enabled = os.environ.get("SCHEDULER_ENABLED", "1") == "1"
            self.screener_strict = os.environ.get("SCREENER_STRICT", "0") == "1"
            self.tushare_token = os.environ.get("TUSHARE_TOKEN", "")
            self.serverchan_key = os.environ.get("SERVERCHAN_KEY", "")
            self.bark_key = os.environ.get("BARK_KEY", "")
            self.pushplus_token = os.environ.get("PUSHPLUS_TOKEN", "")
            self.db_host = os.environ.get("DB_HOST", "")
            self.db_port = int(os.environ.get("DB_PORT", "3306") or 3306)
            self.db_name = os.environ.get("DB_NAME", "")
            self.db_user = os.environ.get("DB_USER", "")
            self.db_password = os.environ.get("DB_PASSWORD", "")
            self.log_level = os.environ.get("LOG_LEVEL", "INFO")


# 模块级单例
settings = Settings()


def reload_settings() -> Settings:
    """重新读取环境变量（用于测试或 .env 改动后热重载）"""
    global settings
    settings = Settings()
    return settings
