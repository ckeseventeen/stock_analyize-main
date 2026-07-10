"""
src/services/ — 无头服务层（headless service layer）

页面（Streamlit）/ CLI / 未来的 HTTP API 共用的业务编排入口。
规则：
  - 本层不 import streamlit，不碰 session_state
  - 输入输出用普通 dict / dataclass / DataFrame，全部可序列化或可直接渲染
  - 页面只做「收集参数 → 调服务 → 渲染结果」
"""
