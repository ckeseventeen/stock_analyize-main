"""
src/analysis/screening/proxy_policy.py — 进程级代理策略（自适应探测）

从 data_provider 提取。本机东财可达性随环境漂移（有时直连通、有时只有
代理通），硬编码任一策略都会在另一种局面把数据源全打挂，故启动时探测一次。
"""
from __future__ import annotations

import os

from src.utils.logger import get_logger

logger = get_logger("proxy_policy")

# 进程级：代理策略只决定一次
_PROXY_CLEARED = False


# 代理探测用的轻量东财端点（~1KB 响应）
_PROXY_PROBE_URL = (
    "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    "?secid=1.600519&fields1=f1&fields2=f51,f53&klt=101&fqt=1&lmt=1"
)


def _probe_em(direct: bool, timeout: float = 4.0) -> bool:
    """探测东财是否可达。direct=True 强制绕过系统代理"""
    try:
        import requests
        s = requests.Session()
        if direct:
            s.trust_env = False
        r = s.get(_PROXY_PROBE_URL, timeout=timeout)
        return r.status_code == 200
    except Exception as e:
        logger.debug(f"_probe_em 忽略异常: {type(e).__name__}: {e}")
        return False


def _ensure_no_proxy_disable():
    """
    一次性决定本进程的代理策略（自适应探测，只执行一次）。

    背景：本机东财可达性随环境漂移——有时代理挂了直连正常（历史用户报告
    "不走代理时 akshare 正常"），有时反爬封直连只能走代理。硬编码任一策略
    都会在另一种局面下把数据源全打挂，因此启动时各探测一次：

      直连通 → 全局禁用代理（env + 注册表 + requests 三层，见下）
      直连不通但代理通 → 保留系统代理
      两路都不通 → 禁用代理（快速失败优于 ProxyError 吃满超时；熔断器接管）

    环境变量覆盖：STOCK_ANALYZE_KEEP_PROXY=1 强制保留代理并跳过探测。

    禁用需要三层防线（缺一不可，历史教训）：
      1. 清空代理 env 变量 —— 只防 env 来源
      2. 重写 urllib.request.getproxies —— 只防 import 晚于本函数的库；
         requests.utils 在 import 时就绑定了 getproxies，事后覆盖无效
      3. patch requests.Session.__init__ 置 trust_env=False —— 真正生效的
         一层：Session.__init__ 每次都把 trust_env 重置为 True（实例属性），
         必须在 __init__ 后改回 False，requests 才会跳过 env/注册表代理
    """
    global _PROXY_CLEARED
    if _PROXY_CLEARED:
        return
    if os.environ.get("STOCK_ANALYZE_KEEP_PROXY") == "1":
        _PROXY_CLEARED = True
        logger.info("STOCK_ANALYZE_KEEP_PROXY=1，保留系统代理设置")
        return

    if _probe_em(direct=True):
        logger.info("代理探测：东财直连可达 → 全局禁用代理")
    elif _probe_em(direct=False):
        logger.info("代理探测：东财直连被拒但系统代理可达 → 保留系统代理")
        _PROXY_CLEARED = True
        return
    else:
        logger.warning(
            "代理探测：东财直连与系统代理均不可达 → 禁用代理快速失败"
            "（数据将走 Sina/pytdx/Baostock 降级链）"
        )

    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
              "ALL_PROXY", "all_proxy"):
        os.environ.pop(k, None)
    os.environ["NO_PROXY"] = "*"

    # 防线 2：禁用注册表代理（对 import 晚于此处的库有效）
    try:
        import urllib.request
        urllib.request.getproxies = lambda: {}
        logger.debug("已重写 urllib.request.getproxies 以禁用注册表代理")
    except Exception as e:
        logger.warning(f"重写 urllib.request.getproxies 失败: {e}")

    # 防线 3：requests.Session 实例级 trust_env=False（对 requests 真正生效的一层）
    try:
        import requests

        _orig_init = requests.Session.__init__
        if not getattr(_orig_init, "_no_proxy_patched", False):
            def _patched_init(self, *args, **kwargs):
                _orig_init(self, *args, **kwargs)
                self.trust_env = False

            _patched_init._no_proxy_patched = True
            requests.Session.__init__ = _patched_init
            logger.debug("已 patch requests.Session 强制 trust_env=False")
    except Exception as e:
        logger.warning(f"patch requests.Session 失败: {e}")

    _PROXY_CLEARED = True
    logger.debug("代理策略已固定（一次探测，全进程生效）")
