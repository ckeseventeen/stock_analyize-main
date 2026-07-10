"""
src/api/ — REST API 层（FastAPI）

服务层（src/services）的 HTTP 映射 + 新版单页前端（static/index.html）。
启动：uvicorn src.api.main:app --port 8600
这是 SaaS 化的正式出口：Streamlit 界面与本 API 并存，逐步迁移。
"""
