"""
src/report/ — AI 解读报告（周报）

数据流：
  组合诊断 / 筛选结果（结构化 dict） → 提示词 → LLM（Claude API）→ Markdown
  → 导出 .md / .html（可打印为 PDF）/ .pdf（fpdf2 可用时）

无 ANTHROPIC_API_KEY 时自动降级为规则模板模式（无 AI 点评，仍可生成结构化周报）。
"""
from src.report.generator import build_weekly_context, export_report, generate_report

__all__ = ["build_weekly_context", "generate_report", "export_report"]
