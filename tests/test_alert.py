"""
tests/test_alert.py — 告警通道与去重单元测试

覆盖：
  - AlertStateStore 冷却窗口与持久化
  - ConsoleChannel 始终返回 True
  - ServerChan/Bark/PushPlus 的 HTTP payload 正确性
  - dispatch() 带去重的分发
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# 将项目根目录加入 sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.automation.alert import (
    AlertEvent,
    AlertStateStore,
    BarkChannel,
    ConsoleChannel,
    PushPlusChannel,
    ServerChanChannel,
    build_channels,
    dispatch,
)

# ========================
# 夹具
# ========================

@pytest.fixture
def sample_event() -> AlertEvent:
    """构造通用测试事件"""
    return AlertEvent(
        title="贵州茅台 价格预警",
        body="当前价 1450，低于阈值 1500",
        event_key="600519:price_below_1500:2026-04-15",
        stock_code="600519",
        stock_name="贵州茅台",
        event_type="price_below",
    )


@pytest.fixture
def tmp_state_path(tmp_path: Path) -> Path:
    """临时状态文件路径（隔离测试）"""
    return tmp_path / "alert_state.json"


# ========================
# AlertStateStore
# ========================

@pytest.mark.unit
class TestAlertStateStore:
    def test_initial_not_fired(self, tmp_state_path):
        """首次查询应返回 False"""
        store = AlertStateStore(tmp_state_path)
        assert store.was_fired("key1", cooldown_hours=24) is False

    def test_mark_and_check(self, tmp_state_path):
        """mark 后立即查询应返回 True"""
        store = AlertStateStore(tmp_state_path)
        store.mark_fired("key1")
        assert store.was_fired("key1", cooldown_hours=24) is True

    def test_persistence_across_instances(self, tmp_state_path):
        """状态应持久化到文件，新实例能读出"""
        store1 = AlertStateStore(tmp_state_path)
        store1.mark_fired("key_persist")
        # 新实例重新加载
        store2 = AlertStateStore(tmp_state_path)
        assert store2.was_fired("key_persist", cooldown_hours=24) is True

    def test_cooldown_zero_always_fires(self, tmp_state_path):
        """冷却时间为 0 时，即使刚 mark 也应视为未触发"""
        store = AlertStateStore(tmp_state_path)
        store.mark_fired("key0")
        assert store.was_fired("key0", cooldown_hours=0) is False

    def test_corrupted_file_resets(self, tmp_state_path):
        """状态文件损坏时应自动回退为空状态"""
        tmp_state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_state_path.write_text("not a json", encoding="utf-8")
        store = AlertStateStore(tmp_state_path)
        # 不抛异常，视为空
        assert store.was_fired("any", cooldown_hours=24) is False


# ========================
# ConsoleChannel
# ========================

@pytest.mark.unit
class TestConsoleChannel:
    def test_send_success(self, sample_event):
        """控制台通道应始终返回 True"""
        ch = ConsoleChannel({"enable": True})
        assert ch.send(sample_event) is True

    def test_disabled_channel_skips(self, sample_event):
        """禁用时 send 应返回 False"""
        ch = ConsoleChannel({"enable": False})
        assert ch.send(sample_event) is False


# ========================
# HTTP 通道（mock requests）
# ========================

@pytest.mark.unit
class TestServerChanChannel:
    def test_send_posts_correct_payload(self, sample_event):
        """应向 Server酱端点 POST title + desp"""
        ch = ServerChanChannel({"enable": True, "sendkey": "SCT_TEST_KEY"})

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"code": 0, "message": "ok"}
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()

        with patch("src.automation.alert.base.requests.post", return_value=mock_resp) as mock_post:
            ok = ch.send(sample_event)
            assert ok is True
            # 验证 URL 正确
            args, kwargs = mock_post.call_args
            assert "sctapi.ftqq.com/SCT_TEST_KEY.send" in args[0]
            # 验证 payload
            data = kwargs["data"]
            assert "title" in data and "desp" in data
            assert sample_event.stock_code in data["desp"]

    def test_env_var_overrides_config(self, sample_event, monkeypatch):
        """环境变量应优先于 YAML sendkey"""
        monkeypatch.setenv("SERVERCHAN_KEY", "ENV_KEY")
        ch = ServerChanChannel({"enable": True, "sendkey": "YAML_KEY"})
        assert ch.sendkey == "ENV_KEY"

    def test_missing_key_disables_channel(self):
        """无 sendkey 时应自动禁用"""
        ch = ServerChanChannel({"enable": True, "sendkey": ""})
        assert ch.enabled is False

    def test_api_error_returns_false(self, sample_event):
        """API 返回 code != 0 时应返回 False"""
        ch = ServerChanChannel({"enable": True, "sendkey": "K"})
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"code": 40001, "message": "bad key"}
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()

        with patch("src.automation.alert.base.requests.post", return_value=mock_resp):
            assert ch.send(sample_event) is False


@pytest.mark.unit
class TestBarkChannel:
    def test_send_posts_json(self, sample_event):
        ch = BarkChannel({"enable": True, "key": "BARK_TEST", "group": "测试"})
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"code": 200, "message": "success"}
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()

        with patch("src.automation.alert.base.requests.post", return_value=mock_resp) as mock_post:
            assert ch.send(sample_event) is True
            _, kwargs = mock_post.call_args
            payload = kwargs["json"]
            assert payload["device_key"] == "BARK_TEST"
            assert payload["group"] == "测试"
            assert payload["title"] == sample_event.title


@pytest.mark.unit
class TestPushPlusChannel:
    def test_send_html_template(self, sample_event):
        ch = PushPlusChannel({"enable": True, "token": "PP_TEST", "template": "html"})
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"code": 200, "msg": "ok"}
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()

        with patch("src.automation.alert.base.requests.post", return_value=mock_resp) as mock_post:
            assert ch.send(sample_event) is True
            _, kwargs = mock_post.call_args
            payload = kwargs["json"]
            assert payload["token"] == "PP_TEST"
            assert payload["template"] == "html"
            assert "<p>" in payload["content"]  # HTML 格式


# ========================
# 重试机制
# ========================

@pytest.mark.unit
class TestRetry:
    def test_retry_on_exception(self, sample_event):
        """HTTP 异常时应按 max_retries 重试"""
        ch = ServerChanChannel({"enable": True, "sendkey": "K"})
        ch.max_retries = 3

        call_count = {"n": 0}

        def raising_post(*args, **kwargs):
            call_count["n"] += 1
            raise ConnectionError("network down")

        with patch("src.automation.alert.base.requests.post", side_effect=raising_post):
            ok = ch.send(sample_event)
            assert ok is False
            assert call_count["n"] == 3  # 重试 3 次


# ========================
# build_channels + dispatch
# ========================

@pytest.mark.unit
class TestBuildAndDispatch:
    def test_build_only_enabled(self):
        """只构建启用的通道"""
        cfg = {
            "channels": {
                "console": {"enable": True},
                "serverchan": {"enable": False, "sendkey": "x"},
                "bark": {"enable": True, "key": "K"},
            }
        }
        channels = build_channels(cfg)
        names = {c.name for c in channels}
        assert "console" in names
        assert "bark" in names
        assert "serverchan" not in names

    def test_build_empty_config_fallback_console(self):
        """空配置应兜底加入 console"""
        channels = build_channels({})
        assert len(channels) == 1
        assert channels[0].name == "console"

    def test_dispatch_deduplicates(self, sample_event, tmp_state_path):
        """同一事件在冷却期内应被去重"""
        store = AlertStateStore(tmp_state_path)
        console = ConsoleChannel({"enable": True})

        # 第一次应推送
        r1 = dispatch(sample_event, [console], store, cooldown_hours=24)
        assert r1.get("console") is True

        # 第二次应被跳过（返回空 dict）
        r2 = dispatch(sample_event, [console], store, cooldown_hours=24)
        assert r2 == {}

    def test_dispatch_records_only_on_success(self, sample_event, tmp_state_path):
        """全部通道失败时不应记录冷却（允许下次重试）"""
        store = AlertStateStore(tmp_state_path)
        failing = ConsoleChannel({"enable": True})
        failing._send_impl = MagicMock(side_effect=Exception("fail"))

        result = dispatch(sample_event, [failing], store, cooldown_hours=24)
        assert result.get("console") is False
        # 未记录冷却
        assert store.was_fired(sample_event.event_key, 24) is False
