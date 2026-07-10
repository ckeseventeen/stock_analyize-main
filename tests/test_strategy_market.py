"""
tests/test_strategy_market.py — 策略市场单元测试

覆盖：打包/校验/发布/列表/导入 全链路（tmp 目录隔离）
"""
from __future__ import annotations

import json
import os
import sys

import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.services import screening_service as svc
from src.services import strategy_market as market


@pytest.fixture
def sample_body() -> dict:
    return {
        "name": "测试策略",
        "conditions": [
            {"type": "exclude_st"},
            {"type": "market_cap", "min": 50, "max": 500},
        ],
        "output": {"sort_by": "总市值(亿)", "limit": 20},
    }


@pytest.fixture
def tmp_config(tmp_path, sample_body):
    p = tmp_path / "screen_config.yaml"
    p.write_text(
        yaml.safe_dump({"strategies": {"s1": sample_body}}, allow_unicode=True),
        encoding="utf-8",
    )
    return p


@pytest.mark.unit
class TestPackageBuild:
    def test_build_and_validate_roundtrip(self, sample_body):
        pkg = market.build_package("s1", sample_body, author="tester",
                                   version="1.2.3", tags=["技术面"])
        assert market.validate_package(pkg) == []
        assert pkg["name"] == "测试策略"
        assert pkg["checksum"].startswith("sha256:")

    def test_checksum_stable_regardless_of_key_order(self, sample_body):
        reordered = {k: sample_body[k] for k in reversed(list(sample_body))}
        assert market.compute_checksum(sample_body) == market.compute_checksum(reordered)

    def test_tampered_strategy_fails_checksum(self, sample_body):
        pkg = market.build_package("s1", sample_body)
        pkg["strategy"]["conditions"].append({"type": "exclude_st"})
        errors = market.validate_package(pkg)
        assert any("checksum" in e for e in errors)

    def test_invalid_id_rejected(self, sample_body):
        pkg = market.build_package("bad id with spaces", sample_body)
        assert any("id 非法" in e for e in market.validate_package(pkg))

    def test_invalid_version_rejected(self, sample_body):
        pkg = market.build_package("s1", sample_body, version="v1-beta")
        assert any("version 非法" in e for e in market.validate_package(pkg))

    def test_unknown_condition_rejected(self):
        body = {"name": "x", "conditions": [{"type": "hacked_condition"}]}
        pkg = market.build_package("s1", body)
        assert any("hacked_condition" in e for e in market.validate_package(pkg))

    def test_future_schema_version_rejected(self, sample_body):
        pkg = market.build_package("s1", sample_body)
        pkg["schema_version"] = 999
        assert any("schema_version" in e for e in market.validate_package(pkg))


@pytest.mark.unit
class TestPublishAndList:
    def test_publish_writes_file(self, tmp_path, tmp_config):
        mdir = tmp_path / "market"
        ok, msg, path = market.publish_strategy(
            "s1", author="tester", version="1.0.0",
            market_dir=mdir, config_path=tmp_config,
        )
        assert ok, msg
        assert path.exists()
        assert path.name == "s1@1.0.0.json"

    def test_publish_missing_strategy(self, tmp_path, tmp_config):
        ok, msg, path = market.publish_strategy(
            "nonexistent", market_dir=tmp_path / "market", config_path=tmp_config,
        )
        assert not ok
        assert path is None

    def test_list_packages(self, tmp_path, tmp_config):
        mdir = tmp_path / "market"
        market.publish_strategy("s1", market_dir=mdir, config_path=tmp_config)
        entries = market.list_packages(mdir)
        assert len(entries) == 1
        assert entries[0].valid
        assert entries[0].package["id"] == "s1"

    def test_list_reports_corrupt_file(self, tmp_path):
        mdir = tmp_path / "market"
        mdir.mkdir()
        (mdir / "broken.json").write_text("{not valid json", encoding="utf-8")
        entries = market.list_packages(mdir)
        assert len(entries) == 1
        assert not entries[0].valid

    def test_list_empty_dir(self, tmp_path):
        assert market.list_packages(tmp_path / "nonexistent") == []


@pytest.mark.unit
class TestImport:
    def test_import_roundtrip(self, tmp_path, tmp_config, sample_body):
        pkg = market.build_package("shared", sample_body, author="alice")
        ok, msg = market.import_package(pkg, config_path=tmp_config)
        assert ok, msg
        imported = svc.get_strategy("shared", tmp_config)
        assert imported["name"] == "测试策略"
        assert imported["_market_meta"]["author"] == "alice"
        # 条件原样保留
        assert imported["conditions"] == sample_body["conditions"]

    def test_import_conflict_requires_overwrite(self, tmp_config, sample_body):
        pkg = market.build_package("s1", sample_body)
        ok, msg = market.import_package(pkg, config_path=tmp_config)
        assert not ok
        assert "已存在" in msg
        ok2, _ = market.import_package(pkg, overwrite=True, config_path=tmp_config)
        assert ok2

    def test_import_with_rename(self, tmp_config, sample_body):
        pkg = market.build_package("shared", sample_body)
        ok, _ = market.import_package(pkg, new_sid="my_local_name",
                                      config_path=tmp_config)
        assert ok
        assert "my_local_name" in svc.load_all_strategies(tmp_config)

    def test_import_invalid_package_rejected(self, tmp_config):
        ok, msg = market.import_package({"kind": "wrong"}, config_path=tmp_config)
        assert not ok

    def test_load_package_text(self, sample_body):
        pkg = market.build_package("s1", sample_body)
        loaded, errors = market.load_package_text(json.dumps(pkg, ensure_ascii=False))
        assert errors == []
        assert loaded["id"] == "s1"

        bad, errors2 = market.load_package_text("{broken")
        assert bad is None
        assert errors2
