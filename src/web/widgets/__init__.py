"""
src/web/widgets/ — 可复用的 Streamlit Widget 库

每个 widget 都是 pure 渲染函数，无副作用（除了显式 callback / yaml 写入）。
约定：每个 widget 必接 `key=` 参数避免 session_state 冲突。

主要 widget：
  - SchemaForm : 喂 schema 自动渲染参数表单（消费 PluginRegistry.schema_for）
  - stats_row  : 多 metric 卡片一行展示
"""

from src.web.widgets.schema_form import SchemaForm
from src.web.widgets.stats_cards import stats_row

__all__ = [
    "SchemaForm",
    "stats_row",
]
