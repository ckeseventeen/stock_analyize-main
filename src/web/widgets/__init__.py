"""
src/web/widgets/ — 可复用的 Streamlit Widget 库（PR 4）

每个 widget 都是 pure 渲染函数，无副作用（除了显式 callback / yaml 写入）。
约定：每个 widget 必接 `key=` 参数避免 session_state 冲突。

主要 widget：
  - SchemaForm        : 喂 schema 自动渲染参数表单（消费 PluginRegistry.schema_for）
  - CRUDTable         : 列表 + 表单新增 + 删除（带二次确认）
  - MarketStockPicker : 市场选择 + YAML 选股 + 手动输入
  - stats_row         : 多 metric 卡片一行展示
  - PresetLoader      : 加载 / 保存 / 删除 yaml 预设
"""

from src.web.widgets.crud_table import CRUDTable
from src.web.widgets.market_stock_picker import MarketStockPicker
from src.web.widgets.preset_loader import PresetLoader
from src.web.widgets.schema_form import SchemaForm
from src.web.widgets.stats_cards import stats_row

__all__ = [
    "SchemaForm",
    "CRUDTable",
    "MarketStockPicker",
    "PresetLoader",
    "stats_row",
]
