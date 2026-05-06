# MEMORY.md — 长期记忆

## 项目结构变更 (2026-05-06)
- analyzer.py / data_fetcher.py / visualizer.py → src/core/
- src/screener/ → src/analysis/screening/
- src/data/fetcher/ → src/data/providers/
- src/data/scraper/ → src/data/scrapers/
- src/visualization/gui/ → src/web/ (streamlit_app.py → app.py)
- config/a_stock.yaml / hk_stock.yaml / us_stock.yaml → config/stocks/
- 删除 config/*.bak.1 备份文件
- output/ 按类型子目录组织

## 用户偏好
- 中文沟通，正式金融术语
- A/B/C/D/E 等级评分体系
- HTML报告输出，中文结构化周报
- 工作流程: 读取 → 执行 → 输出 → 交付 → 总结
