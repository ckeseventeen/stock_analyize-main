"""
src/analysis/factor/base.py — 因子抽象基类

所有因子继承 BaseFactor，实现 calculate 和 validate 方法。
"""
from abc import ABC, abstractmethod


class BaseFactor(ABC):
    """
    因子抽象基类

    Attributes:
        name: 因子标识符（如 "pe_ttm"）
        description: 因子中文描述
        higher_is_better: 值越大越好（用于排序方向）
        requires_ohlcv: 是否需要历史K线数据
        ohlcv_period: 所需K线周期 "daily" / "weekly"（仅 requires_ohlcv=True 时有效）

    data 参数说明:
        - "spot": 来自 ak.stock_zh_a_spot_em() 的单行 Series
        - "daily_df": 日线 OHLCV DataFrame（可选）
        - "weekly_df": 周线 OHLCV DataFrame（可选）
    """

    name: str = ""
    description: str = ""
    higher_is_better: bool = True
    requires_ohlcv: bool = False
    ohlcv_period: str = "daily"

    @abstractmethod
    def calculate(self, data: dict) -> float:
        """计算因子值，返回标量"""

    @abstractmethod
    def validate(self, data: dict) -> bool:
        """检查所需数据是否存在"""

    def safe_calculate(self, data: dict) -> float:
        """安全计算，数据缺失时返回 NaN"""
        if not self.validate(data):
            return float("nan")
        try:
            return self.calculate(data)
        except Exception:
            return float("nan")
