"""
tests/test_report.py — AI 周报模块单元测试（无网络，注入 fake provider）
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.report.generator import (
    _render_data_section,
    build_weekly_context,
    generate_report,
)
from src.report.llm import LLMProvider, TemplateProvider, get_provider


class FakeProvider(LLMProvider):
    name = "fake"

    def __init__(self, reply: str = "AI 点评正文", raise_error: bool = False):
        self.reply = reply
        self.raise_error = raise_error
        self.last_prompt = ""

    def generate(self, system: str, prompt: str) -> str:
        if self.raise_error:
            raise RuntimeError("boom")
        self.last_prompt = prompt
        return self.reply


@pytest.fixture
def sample_diag_dict() -> dict:
    return {
        "generated_at": "2026-07-10T15:00:00",
        "score": 82.5,
        "grade": "B",
        "total_market_value": 250000.0,
        "total_pnl_pct": 5.2,
        "cash": 10000.0,
        "position_count": 4,
        "dimensions": [
            {"key": "signal", "label": "卖出信号", "score": 90.0, "weight": 0.4,
             "detail": "市值加权风险分 10%"},
            {"key": "concentration", "label": "集中度", "score": 75.0, "weight": 0.25,
             "detail": "单票占比 28%"},
        ],
        "issues": ["🟠 测试股(000001) 风险分 65%，建议减仓"],
        "highlights": ["✅ 卖出信号良好"],
        "regime": "sideways",
        "holdings": [
            {"code": "000001", "name": "测试股", "industry": "电子",
             "weight_pct": 28.0, "pnl_pct": 12.0, "risk_pct": 65, "action": "reduce_half"},
        ],
    }


@pytest.mark.unit
class TestContextAndRender:
    def test_build_context_with_injected_diag(self, sample_diag_dict):
        ctx = build_weekly_context(diagnosis_dict=sample_diag_dict)
        assert ctx["portfolio"]["grade"] == "B"
        assert "generated_at" in ctx
        assert "screening" not in ctx

    def test_build_context_with_screening(self, sample_diag_dict):
        import pandas as pd
        df = pd.DataFrame([{"代码": "600519", "名称": "茅台", "最新价": 1600}])
        ctx = build_weekly_context(sample_diag_dict, screening_df=df,
                                   screening_meta={"strategy": "s1"})
        assert len(ctx["screening"]["top_results"]) == 1
        assert ctx["screening"]["meta"]["strategy"] == "s1"

    def test_data_section_contains_key_facts(self, sample_diag_dict):
        ctx = build_weekly_context(diagnosis_dict=sample_diag_dict)
        md = _render_data_section(ctx)
        assert "82.5" in md
        assert "测试股" in md
        assert "减仓50%" in md
        assert "需要处理" in md


@pytest.mark.unit
class TestGenerate:
    def test_generate_with_fake_provider(self, sample_diag_dict):
        ctx = build_weekly_context(diagnosis_dict=sample_diag_dict)
        provider = FakeProvider(reply="这是 AI 解读段落")
        md, name = generate_report(ctx, provider=provider)
        assert name == "fake"
        assert "这是 AI 解读段落" in md
        assert "不构成投资建议" in md
        # prompt 里应包含诊断 JSON
        assert "000001" in provider.last_prompt

    def test_generate_survives_provider_error(self, sample_diag_dict):
        ctx = build_weekly_context(diagnosis_dict=sample_diag_dict)
        md, _ = generate_report(ctx, provider=FakeProvider(raise_error=True))
        assert "AI 解读生成失败" in md
        # 数据段仍完整
        assert "组合概览" in md

    def test_template_provider_no_network(self, sample_diag_dict):
        ctx = build_weekly_context(diagnosis_dict=sample_diag_dict)
        md, name = generate_report(ctx, provider=TemplateProvider())
        assert name == "template"
        assert "ANTHROPIC_API_KEY" in md

    def test_get_provider_without_key_returns_template(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        assert get_provider().name == "template"


@pytest.mark.unit
class TestExport:
    def test_export_writes_md_and_html(self, sample_diag_dict, tmp_path, monkeypatch):
        import src.report.generator as gen
        monkeypatch.setattr(gen, "REPORTS_DIR", tmp_path)

        ctx = build_weekly_context(diagnosis_dict=sample_diag_dict)
        md, _ = generate_report(ctx, provider=FakeProvider())
        paths = gen.export_report(md, basename="test_report")

        assert paths["md"].exists()
        assert paths["md"].read_text(encoding="utf-8") == md
        assert paths["html"].exists()
        html = paths["html"].read_text(encoding="utf-8")
        assert "<html" in html and "测试股" in html
