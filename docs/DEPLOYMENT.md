# 部署指南

本文档记录实测的资源需求、常见故障与排查方法。所有数字来自本机实测，
不是估算。

---

## 一、资源需求（实测）

| 项 | 实测值 | 说明 |
|---|---|---|
| Python 依赖磁盘 | **317 MB** | scipy 118 + pandas 66 + numpy 42 + matplotlib 37 为大头 |
| Web 服务常驻内存 | **62 MB** | 仅 uvicorn + FastAPI，冷启动后 |
| 全市场筛选峰值内存 | **342 MB** | 5000 只 spot + K线缓存 + 线程池 |
| 全市场筛选耗时 | **约 9 分钟**（16 核） | 1 核机器上约 20-30 分钟 |
| ML 建数据集 | **约 20 分钟**（300 只） | 训练本身仅数秒 |
| 缓存目录 | 约 10-30 MB | 行情缓存 + ML 数据集另计（20+ MB） |

### 最低配置建议

- **内存 ≥ 1 GB**。512 MB 会在全市场筛选时 OOM（峰值 342 MB + 系统开销）；
  若只用个股分析不跑筛选，512 MB 勉强可行但仍建议加 swap
- **磁盘 ≥ 2 GB**
- **CPU** 1 核可用，但筛选慢 2-3 倍；筛选**不要**开 `use_processes`
  （进程池会让内存翻倍）
- **时区设为 `Asia/Shanghai`**，否则调度器的 A 股交易时段判断会错

### 部署前体检

```bash
python3 scripts/vps_preflight.py
```

纯标准库实现，无需先装依赖。会检查内存/磁盘/CPU/Python版本/时区，
并逐个探测行情数据源可达性。退出码 0 = 可部署，1 = 有阻塞项。

---

## 二、Docker 部署

```bash
docker compose up -d          # 启动 app / scheduler / webui 三服务
docker compose logs -f webui  # 看日志
```

三个服务共用同一镜像，靠 command 区分角色：

| 服务 | 作用 | 备注 |
|---|---|---|
| `webui` | FastAPI + SPA，暴露 8600 端口 | 设了 `SCHEDULER_DISABLED=1`，避免与 scheduler 双启动 |
| `scheduler` | APScheduler 长驻，按 cron 跑监控/抓取/重训 | 唯一开启调度的容器 |
| `app` | CLI 批量分析 | 按需运行，非常驻 |

> **本项目为纯文件存储**（YAML 配置 + pickle/parquet 缓存），**不需要数据库**。
> 历史上 docker-compose 里曾定义 mysql 服务但代码从未引用，已移除——
> 在 1GB 级 VPS 上它会白吃 400MB+ 导致 OOM。

---

## 三、海外 VPS 的最大风险：数据源可达性

**所有行情数据源都在国内**（东财、新浪、同花顺问财、Baostock、通达信）。
海外 IP 访问时可能被限流甚至完全不通——这是海外部署的头号风险，
且**无法通过加配置解决**。

部署前务必用 `scripts/vps_preflight.py` 实测。如果大面积不可达：

1. 优先换 **香港 / 日本 / 新加坡** 节点（延迟低、通常不被墙）
2. 或改用国内轻量云（阿里云/腾讯云学生机等）
3. 或在国内机器上跑调度器预热缓存，海外机器只做展示

### 已知的数据源特性

| 数据源 | 特性 | 应对 |
|---|---|---|
| 东财 `push2*`（行情/K线） | **动态 IP 限流**，同一段代码可能先成功后被封 | 熔断器自动冷却；降级 Sina/pytdx |
| 东财 `datacenter-web`（另类数据） | 通常比 push2 宽松 | — |
| 同花顺问财 | 分页不稳定，可能返回半份数据 | 质量校验拦截；且依赖 V8（见下） |
| Baostock | 最稳但最慢（串行 5500 只约 20 分钟） | 有 300 秒时间预算保护，可用 `STOCK_ANALYZE_BAOSTOCK_BUDGET` 调整 |
| pytdx（通达信） | 不限频、并发安全，约 22ms/只 | 全市场批量取数的主力 |

---

## 四、环境变量

| 变量 | 默认 | 作用 |
|---|---|---|
| `SCHEDULER_DISABLED` | — | 设 `1` 时进程内不启动调度器（webui/app 容器用） |
| `SCHEDULER_ENABLED` | — | 设 `true` 时启用调度器（scheduler 容器用） |
| `STOCK_ANALYZE_KEEP_PROXY` | — | 设 `1` 保留系统代理；默认会自适应探测直连/代理择优 |
| `STOCK_ANALYZE_ENABLE_V8` | — | 设 `1` 完全不装 V8 守卫，直通（见下节） |
| `STOCK_ANALYZE_DISABLE_V8` | — | 设 `1` 跳过探测直接硬禁用 V8（已知会崩的环境） |
| `STOCK_ANALYZE_BAOSTOCK_BUDGET` | `300` | Baostock 全A兜底的时间预算（秒） |
| `STOCK_ANALYZE_TTL_SCALE` | `1.0` | 缓存 TTL 整体缩放，调试时可设 `0.5` |
| `SCREENER_STRICT` | — | 设 `1` 时未知筛选条件直接抛错（CI 用） |

---

## 五、故障排查

### 服务毫无征兆退出、所有功能不可用

日志尾部若出现：

```
[FATAL:partition_address_space.cc] Check failed: !IsConfigurablePoolInitialized()
```

这是 **py_mini_racer(V8) 初始化 FATAL 中止**——不是异常，`try/except` 拦不住，
进程被当场杀死。akshare **自身依赖** mini-racer（用于部分数据源的 JS 解密）。

项目已内置守卫（`src/core/v8_guard.py`）。它**不是**一律禁用 V8——那样会误伤
美股：`ak.stock_us_daily`（新浪）必须靠 V8 解密，而东财美股接口在国内网络常年
不通，新浪一断美股就彻底没数据源。

而 V8 是否 FATAL **因环境而异**，所以守卫采用**隔离子进程懒探测**：首次真正
用到 V8 时，先起一个独立子进程初始化一次 V8——

- 子进程正常退出 → 本机 V8 安全，放行真实 `MiniRacer`（美股、问财照常可用）
- 子进程被 FATAL 杀死（非零退出码）→ 后续实例化抛可捕获异常，走多源降级链，
  主进程不受影响

探测结果按 (解释器路径, mini-racer 版本) 落盘缓存 7 天，只有第一次付 ~1-2s
子进程启动成本；从不碰 V8 的进程完全不付费。

确认守卫状态与探测结论：

```bash
curl http://localhost:8600/api/health
# {"status":"ok","v8_guard":{"applied":true,"mode":"lazy",
#  "probe":{"probed":true,"safe":true,"reason":"子进程 V8 初始化成功（缓存）"}}}
```

`probe.safe` 为 `false` 说明本机 V8 确实会崩，此时问财与新浪美股不可用属预期行为。

**这是强信号而非证明**：子进程能初始化，不代表主进程在任意并发时序下都安全。
若线上仍观测到 FATAL，设 `STOCK_ANALYZE_DISABLE_V8=1` 硬禁用（跳过探测）。
反之若确信本机 V8 无恙、想省掉探测开销，设 `STOCK_ANALYZE_ENABLE_V8=1` 直通。

### 美股查不到 / 港美股按名称搜不到

美股行情走「新浪（需 V8）→ 东财（105./106./107. 前缀轮询）」。若两者都失败：

1. 先看 `/api/health` 的 `v8_guard.probe.safe`——`false` 则新浪这条路是断的
2. 再看东财是否可达（国内直连常被拒、走代理也常不通）：
   `curl 'https://63.push2his.eastmoney.com/api/qt/stock/kline/get?secid=105.AAPL&klt=101&fqt=1&fields1=f1&fields2=f51,f52,f53,f54,f55,f56'`

港美股的**搜索池只来自关注列表配置**（`config/us_stocks.yaml` 等），不是全市场
名称表——没配进去的标的按名称搜不到，但直接输代码仍可分析。

### 筛选结果为 0

先分清两类：

- **正常**：底背离/突破/金叉等技术触发条件只在"当天出现信号"时命中，
  多数交易日 0 命中是预期结果。换个交易日再跑
- **故障**：筛选结果的 `warnings` 里会有 `⚠️ 行情数据源退化：...` 明确提示

验证链路是否正常：跑基本面型策略（通常命中数十只）

```bash
python -c "import sys;sys.path.insert(0,'.');import src;from src.services.screening_service import run_screening;r=run_screening(['quality_value'],scope_keys=['all']);print(len(r.df), r.warnings)"
```

### 配置改了不生效

配置有加载期校验，先自查：

```bash
curl http://localhost:8600/api/config/validate
```

未知的条件类型、未知的 job type、非法的因子表达式、重复的 job id
都会被明确报出，而不是静默失效。

### ML 训练没有产出模型

```bash
curl http://localhost:8600/api/ml/status
```

若 `trained: false`，看日志里是否有"沪深 300 指数为空"——标签依赖指数数据，
指数拉不到则数据集为空、训练无从进行。指数有多源降级（新浪 → pytdx →
Baostock → 沪深300ETF 代理）且历史数据长期缓存，正常不会全挂。

---

## 六、日志

- 位置：`logs/stock_analyzer.log`，按天轮转保留 30 天
- 关键日志前缀：`[market_monitor]` `[ml_retrain]` `[screener]` `[fallback]`
- 数据源降级会打 WARNING，退化采用会打 ERROR，便于 grep：

```bash
grep -E "退化|降级|熔断|所有数据源" logs/stock_analyzer.log | tail -20
```
