"""
src/report/llm.py — LLM Provider 抽象

两个实现：
  - AnthropicProvider：调 Claude API 生成解读（需要 ANTHROPIC_API_KEY + anthropic 包）
  - TemplateProvider：规则模板兜底（零依赖、零成本，输出确定性 Markdown）

get_provider() 自动选择：有 key 且 anthropic 可导入 → Anthropic；否则模板。
"""
from __future__ import annotations

import os
from abc import ABC, abstractmethod

from src.utils.logger import get_logger

logger = get_logger("report_llm")

# 默认模型；可用环境变量 LLM_MODEL 覆盖
DEFAULT_MODEL = "claude-opus-4-8"


class LLMProvider(ABC):
    """报告生成 Provider 抽象"""

    name: str = "base"

    @abstractmethod
    def generate(self, system: str, prompt: str) -> str:
        """输入系统提示 + 用户提示，返回 Markdown 文本"""
        raise NotImplementedError


class AnthropicProvider(LLMProvider):
    """Claude API Provider"""

    name = "anthropic"

    def __init__(self, model: str | None = None):
        import anthropic  # 延迟 import：未安装时由 get_provider 捕获

        self._client = anthropic.Anthropic()  # 从环境变量解析 ANTHROPIC_API_KEY
        self.model = model or os.environ.get("LLM_MODEL", "") or DEFAULT_MODEL

    def generate(self, system: str, prompt: str) -> str:
        response = self._client.messages.create(
            model=self.model,
            max_tokens=8192,
            thinking={"type": "adaptive"},
            system=system,
            messages=[{"role": "user", "content": prompt}],
        )
        if response.stop_reason == "refusal":
            logger.warning("LLM 拒绝生成本次报告内容")
            return "> ⚠️ AI 服务拒绝了本次生成请求，请检查输入内容或稍后重试。"
        text = "".join(b.text for b in response.content if b.type == "text")
        if response.stop_reason == "max_tokens":
            text += "\n\n> ⚠️ 报告因长度限制被截断。"
        return text


class TemplateProvider(LLMProvider):
    """规则模板兜底：不调用任何外部服务，基于诊断数据生成确定性点评"""

    name = "template"

    def generate(self, system: str, prompt: str) -> str:  # noqa: ARG002
        # 模板模式不使用提示词；实际渲染在 generator 中完成，
        # 这里只返回提示语，说明 AI 点评部分不可用。
        return (
            "> ℹ️ 未配置 ANTHROPIC_API_KEY（或未安装 anthropic 包），"
            "本报告为规则模板模式，未包含 AI 解读点评。\n"
            "> 配置 API Key 后重新生成即可获得 AI 深度解读。"
        )


def get_provider(model: str | None = None) -> LLMProvider:
    """自动选择 Provider：Claude API 优先，不可用时降级模板"""
    if os.environ.get("ANTHROPIC_API_KEY", "").strip():
        try:
            return AnthropicProvider(model=model)
        except ImportError:
            logger.warning("anthropic 包未安装（pip install anthropic），降级为模板模式")
        except Exception as e:
            logger.warning(f"Anthropic Provider 初始化失败，降级模板模式: {e}")
    else:
        logger.info("未配置 ANTHROPIC_API_KEY，使用模板模式")
    return TemplateProvider()
