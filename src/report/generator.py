"""
src/report/generator.py — 周报组装与导出

流程：
  build_weekly_context()  → 聚合持仓体检 + 大盘环境 +（可选）筛选结果为纯 dict
  generate_report(ctx)    → 模板骨架（确定性数据段）+ LLM 解读段 → Markdown
  export_report(md)       → output/reports/ 下的 .md / .html（打印即 PDF）/ .pdf（可选）
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from src.report.llm import LLMProvider, get_provider
from src.utils.logger import get_logger
from src.core.config_io import OUTPUT_DIR

logger = get_logger("report_gen")

REPORTS_DIR = OUTPUT_DIR / "reports"

_SYSTEM_PROMPT = (
    "你是一位严谨的投资组合分析助手，为个人投资者撰写每周持仓解读报告。\n"
    "规则：\n"
    "1. 只基于用户提供的结构化数据分析，不要编造数据中不存在的数字；\n"
    "2. 输出 Markdown，使用二级/三级标题组织；\n"
    "3. 内容包括：本周组合状态总评、需要重点关注的持仓（引用风险分/信号）、"
    "集中度与仓位结构建议、结合大盘环境的下周注意事项；\n"
    "4. 语言平实客观，避免夸张措辞；\n"
    "5. 结尾必须包含一行免责声明：本报告由数据工具生成，不构成投资建议。"
)


def build_weekly_context(
    diagnosis_dict: Optional[dict] = None,
    screening_df: Optional[pd.DataFrame] = None,
    screening_meta: Optional[dict] = None,
) -> dict:
    """
    组装报告上下文（纯 dict，可序列化）。

    Args:
        diagnosis_dict: PortfolioDiagnosis.to_report_dict() 的输出；
                        None 时联网跑一次完整体检
        screening_df: 可选，最近一次筛选结果
        screening_meta: 可选，筛选元信息（策略 ID、耗时等）
    """
    if diagnosis_dict is None:
        from src.portfolio.diagnostics import run_diagnosis
        diagnosis_dict = run_diagnosis().to_report_dict()

    ctx: dict = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "portfolio": diagnosis_dict,
    }
    if screening_df is not None and not screening_df.empty:
        ctx["screening"] = {
            "meta": dict(screening_meta or {}),
            "top_results": screening_df.head(20).to_dict(orient="records"),
        }
    return ctx


def _render_data_section(ctx: dict) -> str:
    """确定性数据段：不依赖 LLM，模板模式下也能输出完整结构"""
    p = ctx.get("portfolio", {})
    lines = [
        f"# 投资组合周报（{ctx.get('generated_at', '')[:10]}）",
        "",
        "## 组合概览",
        "",
        f"- **综合健康分**：{p.get('score', '?')} / 100（等级 {p.get('grade', '?')}）",
        f"- **持仓市值**：{p.get('total_market_value', 0):,.0f}"
        f"（现金 {p.get('cash', 0):,.0f}）",
        f"- **总浮盈**：{p.get('total_pnl_pct', 0):+.1f}%",
        f"- **持仓数量**：{p.get('position_count', 0)} 只",
        f"- **大盘环境**：{ {'bull': '🟢 多头', 'sideways': '🟡 震荡', 'bear': '🔴 空头'}.get(p.get('regime'), '未知') }",
        "",
        "## 分维度得分",
        "",
        "| 维度 | 得分 | 权重 | 说明 |",
        "|------|------|------|------|",
    ]
    for d in p.get("dimensions", []):
        lines.append(
            f"| {d.get('label')} | {d.get('score')} | {d.get('weight'):.0%} "
            f"| {d.get('detail', '')} |"
        )

    lines += ["", "## 持仓明细", "",
              "| 代码 | 名称 | 行业 | 占比% | 浮盈% | 风险分% | 建议 |",
              "|------|------|------|-------|-------|---------|------|"]
    action_labels = {"hold": "持有", "reduce_half": "减仓50%",
                     "reduce_all": "清仓", "stop_loss": "止损", "n/a": "—"}
    for h in p.get("holdings", []):
        lines.append(
            f"| {h.get('code')} | {h.get('name')} | {h.get('industry')} "
            f"| {h.get('weight_pct')} | {h.get('pnl_pct')} | {h.get('risk_pct')} "
            f"| {action_labels.get(h.get('action', 'n/a'), h.get('action'))} |"
        )

    issues = p.get("issues", [])
    if issues:
        lines += ["", "## ⚠️ 需要处理", ""]
        lines += [f"- {i}" for i in issues]
    highlights = p.get("highlights", [])
    if highlights:
        lines += ["", "## ✅ 良好项", ""]
        lines += [f"- {h}" for h in highlights]

    scr = ctx.get("screening")
    if scr:
        lines += ["", "## 本周筛选摘要", "",
                  f"命中 {len(scr.get('top_results', []))} 只（展示前 20）。"]
        rows = scr.get("top_results", [])
        if rows:
            cols = list(rows[0].keys())[:6]
            lines.append("| " + " | ".join(str(c) for c in cols) + " |")
            lines.append("|" + "---|" * len(cols))
            for r in rows:
                lines.append("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |")

    return "\n".join(lines)


def generate_report(
    ctx: dict,
    provider: Optional[LLMProvider] = None,
) -> tuple[str, str]:
    """
    生成完整周报 Markdown。

    Returns:
        (markdown, provider_name)
    """
    provider = provider or get_provider()
    data_section = _render_data_section(ctx)

    prompt = (
        "以下是我的投资组合本周的结构化诊断数据（JSON）。"
        "请撰写解读报告正文（不要重复罗列原始表格，数据段我已单独呈现）：\n\n"
        "```json\n"
        + json.dumps(ctx, ensure_ascii=False, indent=1)
        + "\n```"
    )
    try:
        ai_section = provider.generate(_SYSTEM_PROMPT, prompt)
    except Exception as e:
        logger.error(f"LLM 生成失败: {e}")
        ai_section = f"> ⚠️ AI 解读生成失败：{e}"

    markdown = (
        data_section
        + "\n\n---\n\n## 🤖 AI 解读\n\n"
        + ai_section
        + "\n\n---\n\n*本报告由数据工具自动生成，不构成投资建议。*\n"
    )
    return markdown, provider.name


# ============================================================================
# 导出
# ============================================================================

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>{title}</title>
<style>
  body {{ font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
         max-width: 860px; margin: 2rem auto; padding: 0 1rem;
         color: #1f2937; line-height: 1.7; }}
  h1 {{ border-bottom: 2px solid #2563eb; padding-bottom: .4rem; }}
  h2 {{ border-left: 4px solid #2563eb; padding-left: .5rem; margin-top: 2rem; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
  th, td {{ border: 1px solid #d1d5db; padding: 6px 10px; text-align: left; }}
  th {{ background: #f3f4f6; }}
  blockquote {{ border-left: 4px solid #f59e0b; margin: 0; padding: .3rem .8rem;
               background: #fffbeb; color: #92400e; }}
  @media print {{ body {{ margin: 0; max-width: none; }} }}
</style>
</head>
<body>
{body}
</body>
</html>
"""


def _markdown_to_html(md_text: str) -> str:
    """markdown → html；markdown 包缺失时降级 <pre>"""
    try:
        import markdown as md_lib
        return md_lib.markdown(md_text, extensions=["tables"])
    except ImportError:
        import html
        return f"<pre>{html.escape(md_text)}</pre>"


def _find_chinese_font() -> Optional[Path]:
    """在 Windows/Linux 常见位置找中文 TTF（fpdf2 PDF 导出用）"""
    candidates = [
        Path("C:/Windows/Fonts/simhei.ttf"),
        Path("C:/Windows/Fonts/simsun.ttc"),
        Path("C:/Windows/Fonts/msyh.ttc"),
        Path("/usr/share/fonts/truetype/wqy/wqy-microhei.ttc"),
    ]
    return next((p for p in candidates if p.exists()), None)


def export_report(markdown: str, basename: str | None = None) -> dict[str, Path]:
    """
    导出报告到 output/reports/。

    Returns:
        {"md": Path, "html": Path, "pdf": Path?}  — pdf 仅在 fpdf2 + 中文字体可用时生成
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    stem = basename or f"weekly_report_{datetime.now():%Y%m%d_%H%M%S}"
    out: dict[str, Path] = {}

    md_path = REPORTS_DIR / f"{stem}.md"
    md_path.write_text(markdown, encoding="utf-8")
    out["md"] = md_path

    html_path = REPORTS_DIR / f"{stem}.html"
    title = markdown.splitlines()[0].lstrip("# ").strip() if markdown else "周报"
    html_path.write_text(
        _HTML_TEMPLATE.format(title=title, body=_markdown_to_html(markdown)),
        encoding="utf-8",
    )
    out["html"] = html_path

    pdf_path = _try_export_pdf(markdown, REPORTS_DIR / f"{stem}.pdf")
    if pdf_path:
        out["pdf"] = pdf_path
    return out


def _try_export_pdf(markdown: str, target: Path) -> Optional[Path]:
    """fpdf2 + 本机中文字体可用时生成简版 PDF（纯文本排版）；否则返回 None"""
    font = _find_chinese_font()
    if font is None or font.suffix.lower() != ".ttf":
        # fpdf2 不支持 .ttc 集合字体，仅接受 .ttf
        return None
    try:
        from fpdf import FPDF
    except ImportError:
        return None
    try:
        pdf = FPDF()
        pdf.add_page()
        pdf.add_font("cn", "", str(font))
        pdf.set_font("cn", size=11)
        for line in markdown.splitlines():
            text = line.replace("|", " ").replace("#", "").strip()
            pdf.multi_cell(0, 6, text if text else " ")
        pdf.output(str(target))
        return target
    except Exception as e:
        logger.warning(f"PDF 导出失败（已生成 HTML 可打印为 PDF）: {e}")
        return None
