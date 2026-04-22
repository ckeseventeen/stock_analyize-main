"""
src/utils/name_resolver.py — 股票代码与名称互转工具
"""
from __future__ import annotations
import yaml
from pathlib import Path
from typing import Dict

class StockNameResolver:
    """
    单例模式：解析本地 config/*.yaml，提供 code -> name 映射。
    """
    _instance = None
    _mapping: Dict[str, str] = {}  # "market:code" -> "name"
    _loaded = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(StockNameResolver, cls).__new__(cls)
        return cls._instance

    def __init__(self, config_dir: Path | str | None = None):
        if not self._loaded:
            if config_dir is None:
                # 默认寻找项目根目录下的 config
                config_dir = Path(__file__).resolve().parents[2] / "config"
            self.config_dir = Path(config_dir)
            self._load_all()
            self._loaded = True

    def _load_all(self):
        """加载 A/HK/US 市场配置"""
        for market, filename in [("a", "a_stock.yaml"), ("hk", "hk_stock.yaml"), ("us", "us_stock.yaml")]:
            p = self.config_dir / filename
            if not p.exists():
                continue
            try:
                with open(p, encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                    if data and "categories" in data:
                        for cat in data["categories"].values():
                            for stock in cat.get("stocks", []):
                                code = str(stock.get("code", "")).strip()
                                name = str(stock.get("name", "")).strip()
                                if code and name:
                                    # 归一化代码，去除后缀
                                    pure_code = code.split('.')[0]
                                    self._mapping[f"{market}:{pure_code}"] = name
                                    # 同时保存带后缀的版本
                                    self._mapping[f"{market}:{code}"] = name
            except Exception:
                pass

    def get_name(self, code: str, market: str = "a") -> str:
        """根据代码和市场获取名称，找不到返回代码本身"""
        code_s = str(code).strip()
        # 降级尝试：600519.SH -> 600519
        pure_code = code_s.split('.')[0]
        
        # 尝试市场前缀匹配
        name = self._mapping.get(f"{market}:{code_s}") or self._mapping.get(f"{market}:{pure_code}")
        if name:
            return name
        
        # 尝试全量匹配（不看市场）
        for k, v in self._mapping.items():
            if k.endswith(f":{code_s}") or k.endswith(f":{pure_code}"):
                return v
        
        return code_s
