"""
tests/test_config_validation.py — YAML 配置加载期校验（B5）

背景：11 个配置文件此前无 schema 校验，写错 key/类型只会静默失效
（未知条件类型被跳过、未知 job type 不注册、非法因子表达式被忽略），
用户只看到"结果不对"。
"""
from __future__ import annotations

import os
import sys

import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core.config_validation import (
    ConfigError,
    FieldRule,
    validate_all,
    validate_config,
    validate_config_file,
)


@pytest.mark.unit
class TestFieldRule:
    def test_required_missing(self):
        assert FieldRule("x", required=True).check({}, "p") == ["p 缺少必填字段 'x'"]

    def test_optional_missing_ok(self):
        assert FieldRule("x").check({}, "p") == []

    def test_type_mismatch(self):
        issues = FieldRule("n", types=(int,)).check({"n": "5"}, "p")
        assert issues and "类型应为 int" in issues[0]

    def test_choices(self):
        issues = FieldRule("m", choices=("a", "b")).check({"m": "z"}, "p")
        assert issues and "不在允许范围" in issues[0]

    def test_range(self):
        assert FieldRule("v", min_value=0).check({"v": -1}, "p")
        assert FieldRule("v", max_value=10).check({"v": 99}, "p")
        assert not FieldRule("v", min_value=0, max_value=10).check({"v": 5}, "p")

    def test_bool_not_treated_as_number(self):
        """bool 是 int 子类，不应被 min/max 规则误判"""
        assert not FieldRule("flag", types=(bool,)).check({"flag": True}, "p")


@pytest.mark.unit
class TestRealConfigsAreValid:
    def test_shipped_configs_pass(self):
        """仓库自带的 11 个配置必须全部合法（否则就是真 bug）"""
        report = validate_all()
        assert report == {}, f"自带配置存在问题: {report}"


@pytest.mark.unit
class TestSchemaCatchesRealMistakes:
    def test_unknown_condition_type_caught(self):
        cfg = {"strategies": {"s1": {"conditions": [
            {"type": "market_cap", "min": 10},
            {"type": "typo_condition", "min": 1},
        ]}}}
        issues = validate_config("screen_config.yaml", cfg)
        assert any("typo_condition" in i and "静默忽略" in i for i in issues)

    def test_empty_conditions_caught(self):
        cfg = {"strategies": {"s1": {"conditions": []}}}
        issues = validate_config("screen_config.yaml", cfg)
        assert any("非空列表" in i for i in issues)

    def test_unknown_job_type_caught(self):
        cfg = {"jobs": [{"id": "j1", "type": "nonexistent_job"}]}
        issues = validate_config("scheduler.yaml", cfg)
        assert any("无对应的 job builder" in i for i in issues)

    def test_duplicate_job_id_caught(self):
        cfg = {"jobs": [{"id": "dup", "type": "screener"},
                        {"id": "dup", "type": "scraper"}]}
        issues = validate_config("scheduler.yaml", cfg)
        assert any("id 重复" in i for i in issues)

    def test_missing_job_id_caught(self):
        cfg = {"jobs": [{"type": "screener"}]}
        issues = validate_config("scheduler.yaml", cfg)
        assert any("缺少必填字段 'id'" in i for i in issues)

    def test_bad_active_profile_caught(self):
        cfg = {"active_profile": "nope", "profiles": {"default": {"factors": []}}}
        issues = validate_config("factors.yaml", cfg)
        assert any("active_profile" in i for i in issues)

    def test_illegal_factor_expression_caught(self):
        cfg = {"alpha_factors": [
            {"name": "OK", "expr": "close / open"},
            {"name": "BAD", "expr": "__import__('os').system('id')"},
        ]}
        issues = validate_config("factors_alpha158.yaml", cfg)
        assert any("BAD" in i and "非法" in i for i in issues)

    def test_duplicate_factor_name_caught(self):
        cfg = {"alpha_factors": [
            {"name": "X", "expr": "close"}, {"name": "X", "expr": "open"}]}
        issues = validate_config("factors_alpha158.yaml", cfg)
        assert any("重复" in i for i in issues)

    def test_bad_holdings_types_caught(self):
        cfg = {"holdings": [{"code": "600519", "shares": -5, "cost": "abc"}]}
        issues = validate_config("holdings.yaml", cfg)
        assert len(issues) >= 2

    def test_unregistered_file_returns_empty(self):
        assert validate_config("unknown_file.yaml", {"whatever": 1}) == []


@pytest.mark.unit
class TestFileLevelApi:
    def test_missing_file(self, tmp_path):
        issues = validate_config_file(tmp_path / "nope.yaml")
        assert issues and "不存在" in issues[0]

    def test_yaml_syntax_error(self, tmp_path):
        p = tmp_path / "scheduler.yaml"
        p.write_text("jobs: [\n  - id: x\n    type: 'unclosed", encoding="utf-8")
        issues = validate_config_file(p)
        assert issues and "YAML 语法错误" in issues[0]

    def test_strict_mode_raises(self, tmp_path):
        p = tmp_path / "scheduler.yaml"
        p.write_text(yaml.safe_dump({"jobs": [{"type": "bogus_type"}]}),
                     encoding="utf-8")
        with pytest.raises(ConfigError):
            validate_config_file(p, strict=True)

    def test_validate_all_on_clean_dir(self, tmp_path):
        (tmp_path / "scheduler.yaml").write_text(
            yaml.safe_dump({"jobs": [{"id": "a", "type": "screener"}]}),
            encoding="utf-8")
        assert validate_all(tmp_path) == {}


@pytest.mark.unit
class TestConfigValidateEndpoint:
    def test_endpoint_reports_ok(self):
        from fastapi.testclient import TestClient

        import src.api.main as api_main
        r = TestClient(api_main.app).get("/api/config/validate")
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is True and body["total_issues"] == 0
