#!/usr/bin/env python3
"""
scripts/vps_preflight.py — VPS 部署前置体检

在目标 VPS 上直接运行（无需先装项目依赖，只用标准库）：

    python3 scripts/vps_preflight.py

检查四件决定"能不能用"的事：
  1. 内存/磁盘/CPU 是否够（本项目实测：Web 常驻 ~62MB，全市场筛选峰值 ~342MB）
  2. **国内行情数据源的可达性** —— 海外 VPS 最大的坑：所有数据源
     （东财/新浪/问财/Baostock/通达信）都在国内，海外 IP 可能被限流或直接不通
  3. 时区是否为 Asia/Shanghai（调度器按 A 股交易时段触发）
  4. Python 版本

退出码：0=可部署，1=有硬性阻塞项
"""
from __future__ import annotations

import os
import platform
import shutil
import socket
import ssl
import sys
import time
import urllib.request

# 本项目实测的资源需求（见 docs 或 README 部署章节）
MEM_MIN_MB = 700          # 低于此值：全市场筛选会 OOM
MEM_RECOMMEND_MB = 1024
DISK_MIN_MB = 2048        # 依赖 ~320MB + 基础镜像 + 缓存增长余量

# 数据源探测目标：(标签, 类型, 地址, 说明)
HTTP_TARGETS = [
    ("东财 行情/K线", "https://push2his.eastmoney.com/api/qt/stock/kline/get"
     "?secid=1.600519&fields1=f1&fields2=f51&klt=101&fqt=1&lmt=1",
     "主数据源，被封则降级 Sina/pytdx"),
    ("新浪 全市场行情", "https://hq.sinajs.cn/list=sh600519",
     "全A股票全集来源（5500+ 只）"),
    ("东财 另类数据", "https://datacenter-web.eastmoney.com/api/data/v1/get"
     "?reportName=RPT_DAILYBILLBOARD_DETAILSNEW&pageSize=1&columns=ALL",
     "龙虎榜/解禁/业绩预告等"),
    ("Baostock", "http://www.baostock.com", "最终兜底源（慢但稳）"),
]
TCP_TARGETS = [
    ("通达信行情服务器", "119.147.212.81", 7709, "pytdx K线兜底，海外常被拒"),
    ("通达信备用节点", "114.80.80.222", 7709, "同上"),
]


def _c(txt: str, color: str) -> str:
    if not sys.stdout.isatty():
        return txt
    codes = {"g": "32", "y": "33", "r": "31", "b": "1"}
    return f"\033[{codes.get(color, '0')}m{txt}\033[0m"


def check_system() -> list[str]:
    """内存/磁盘/CPU/Python/时区，返回阻塞项列表"""
    blockers = []
    print(_c("\n[1/4] 系统资源", "b"))

    mem_mb = None
    try:
        # Linux
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    mem_mb = int(line.split()[1]) // 1024
                    break
    except OSError:
        try:
            mem_mb = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") // (1024**2)
        except (ValueError, OSError, AttributeError):
            pass

    if mem_mb is None:
        print("  内存      : 无法探测（非 Linux？）")
    elif mem_mb < MEM_MIN_MB:
        print(f"  内存      : {_c(f'{mem_mb} MB —— 不足', 'r')} "
              f"(需 ≥{MEM_MIN_MB}MB，全市场筛选峰值 342MB)")
        blockers.append(f"内存仅 {mem_mb}MB，低于 {MEM_MIN_MB}MB 硬性下限")
    elif mem_mb < MEM_RECOMMEND_MB:
        print(f"  内存      : {_c(f'{mem_mb} MB —— 勉强', 'y')} "
              f"(建议 ≥{MEM_RECOMMEND_MB}MB；须加 swap 且别开进程池筛选)")
    else:
        print(f"  内存      : {_c(f'{mem_mb} MB —— 充足', 'g')}")

    free_mb = shutil.disk_usage(".").free // (1024**2)
    if free_mb < DISK_MIN_MB:
        print(f"  磁盘可用  : {_c(f'{free_mb} MB —— 不足', 'r')} (需 ≥{DISK_MIN_MB}MB)")
        blockers.append(f"磁盘可用仅 {free_mb}MB")
    else:
        print(f"  磁盘可用  : {_c(f'{free_mb} MB', 'g')} (依赖约需 320MB)")

    cpus = os.cpu_count() or 1
    note = "（1 核：全市场筛选约 20-30 分钟，建议只用线程池）" if cpus <= 1 else ""
    print(f"  CPU 核数  : {cpus} {note}")

    v = sys.version_info
    if v < (3, 9):
        print(f"  Python    : {_c(f'{v.major}.{v.minor} —— 过低', 'r')} (需 ≥3.9)")
        blockers.append(f"Python {v.major}.{v.minor} < 3.9")
    else:
        print(f"  Python    : {_c(f'{v.major}.{v.minor}.{v.micro}', 'g')}")

    tz = os.environ.get("TZ") or time.tzname[0]
    tz_ok = "CST" in str(time.tzname) or "Asia/Shanghai" in str(tz)
    print(f"  时区      : {tz} {'' if tz_ok else _c('← 建议设为 Asia/Shanghai（调度器按A股时段触发）', 'y')}")
    print(f"  平台      : {platform.system()} {platform.release()}")
    return blockers


def check_http(label: str, url: str, note: str, timeout: float = 8.0) -> tuple[bool, str]:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn",
    })
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            body = r.read(2048)
            ms = int((time.time() - t0) * 1000)
            if r.status == 200 and body:
                return True, f"{ms}ms"
            return False, f"HTTP {r.status}"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:40]}"


def check_tcp(host: str, port: int, timeout: float = 6.0) -> tuple[bool, str]:
    t0 = time.time()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, f"{int((time.time() - t0) * 1000)}ms"
    except Exception as e:
        return False, f"{type(e).__name__}"


def check_datasources() -> list[str]:
    """数据源可达性——海外 VPS 的头号风险"""
    print(_c("\n[2/4] 国内行情数据源可达性（海外 VPS 头号风险）", "b"))
    results = {}
    for label, url, note in HTTP_TARGETS:
        ok, detail = check_http(label, url, note)
        results[label] = ok
        mark = _c("✓", "g") if ok else _c("✗", "r")
        print(f"  {mark} {label:16s} {detail:32s} {note}")
    for label, host, port, note in TCP_TARGETS:
        ok, detail = check_tcp(host, port)
        results[label] = ok
        mark = _c("✓", "g") if ok else _c("✗", "r")
        print(f"  {mark} {label:16s} {detail:32s} {note}")

    blockers = []
    reachable = sum(1 for v in results.values() if v)
    if reachable == 0:
        blockers.append("所有行情数据源都不可达——该 VPS 无法获取行情，部署无意义")
    elif not any(results.get(k) for k in ("东财 行情/K线", "新浪 全市场行情")):
        blockers.append("东财与新浪均不可达——无法获取全市场行情，筛选功能不可用")
    print(f"\n  可达 {reachable}/{len(results)} 个数据源")
    return blockers


def check_proxy_env() -> None:
    print(_c("\n[3/4] 代理环境", "b"))
    proxies = {k: v for k, v in os.environ.items()
               if k.lower() in ("http_proxy", "https_proxy", "all_proxy")}
    if proxies:
        print(f"  检测到代理环境变量: {list(proxies)}")
        print("  提示：项目启动时会自动探测直连/代理择优；若确需保留代理，"
              "设 STOCK_ANALYZE_KEEP_PROXY=1")
    else:
        print("  无代理环境变量（项目会自动探测直连可达性）")


def print_recommendation(blockers: list[str]) -> None:
    print(_c("\n[4/4] 结论", "b"))
    if blockers:
        print(_c("  ✗ 不建议直接部署，存在阻塞项：", "r"))
        for b in blockers:
            print(f"    - {b}")
    else:
        print(_c("  ✓ 基本满足部署条件", "g"))
    print("""
  部署建议（$10/年 级别 VPS）：
    · 只起 webui + scheduler 两个进程；docker-compose.yml 里的 mysql
      服务本项目**从未使用**，务必删掉（白吃 400MB+ 内存）
    · 内存 <1GB 时：加 1-2GB swap，且筛选不要开 use_processes（进程池会翻倍内存）
    · 首次拉取全市场数据较慢（冷缓存下分钟级），建议先用 scheduler 预热缓存
    · 数据源若大面积不可达，优先换**香港/日本/新加坡**节点，或改用国内轻量云
""")


def main() -> int:
    print(_c("=" * 68, "b"))
    print(_c("  stock_analyize — VPS 部署前置体检", "b"))
    print(_c("=" * 68, "b"))
    blockers = check_system()
    blockers += check_datasources()
    check_proxy_env()
    print_recommendation(blockers)
    return 1 if blockers else 0


if __name__ == "__main__":
    sys.exit(main())
