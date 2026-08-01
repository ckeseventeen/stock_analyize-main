# 多市场量化投研分析平台

集估值分析、技术筛选、因子分析、回测验证、自动化监控于一体的一站式工具，
支持 **A 股 / 港股 / 美股** 三市场，提供 Web 界面（FastAPI + 单页应用）、CLI 批量处理与 APScheduler 长驻调度器三种运行模式。

## 功能概览

| 功能模块 | 说明 | 运行方式 |
|----------|------|---------|
| 📈 **估值分析** | TTM 净利润推演 → PE/PS 三情景目标价 → 历史分位数 → 四格估值图 | CLI / Web |
| 🔍 **股票筛选** | **16 条**策略、**43 种**可组合条件，支持 **AND/OR 嵌套逻辑**与 **TopN 排序**（动量轮动等相对筛选），两阶段过滤（向量化 Spot + OHLCV 深度验证） | CLI / Web |
| 🔬 **因子分析** | **Alpha158 表达式因子引擎**（31 个 QLib 标准因子 + 15 个时序算子）、IC/ICIR/分层回测/衰减曲线、FCF 现金流评分 | Web |
| 📊 **策略回测** | Backtrader 事件回测 + **NumPy 向量化引擎**（快 1-2 个数量级）+ 多策略对比 | Web |
| 🎛️ **参数寻优** | 网格 / 随机 / **optuna 贝叶斯(TPE)** + **Walk-Forward** 过拟合检测（IS vs OOS） | Web |
| 🤖 **ML 选股** | LightGBM 截面模型预测 20 日超额收益，Purged 时序交叉验证防泄漏，55 特征 | Web / 调度器 |
| 🕐 **分钟线** | 1m/5m/15m/30m/60m 多周期 K 线（东财主源 + pytdx 兜底） | Web |
| 🧭 **另类数据** | 龙虎榜 / 北向资金 / 融资融券 / 大宗交易 / 解禁 / 股东人数 / 分红 / 概念成分（9 源） | Web |
| 📚 **基本面深度** | 三大报表 / 财务指标 / 业绩预告 / 业绩快报（6 源） | Web |
| 🔔 **价格预警** | 价格阈值 / 涨跌幅 / 相对成本 / 均线突破，推送至手机 | 调度器 |
| 📅 **财报监控** | A/港/美三市场未来 30 天披露日历 + 业绩预告 | 调度器 |
| 🌐 **资讯抓取** | 财经新闻 / 公司公告 / 股东持仓 / 研报评级 | CLI / 调度器 |
| 🖥️ **Web 界面** | 单页应用：个股/市场/策略/回测/持仓/交易 6 视图 + **38 个** REST 端点 | Web |
| 🩺 **持仓体检** | 四维健康分（卖出信号/集中度/盈亏结构/大盘环境）+ 问题清单 | Web |
| 💹 **模拟交易** | 本地模拟盘（A 股费用模型：佣金/印花税/过户费），券商实盘适配层预留 | Web |
| 📰 **AI 周报** | 持仓体检 + 筛选结果 → Claude 深度解读 → md/html/pdf 导出 | Web |
| 🏪 **策略市场** | 策略打包（sha256 校验）→ 发布/导入/分享 | Web |
| ⏰ **定时任务** | APScheduler 长驻进程，YAML 驱动 cron（含 ML 月度重训） | 调度器 |
| 🐳 **Docker 部署** | app / scheduler / webui 三服务容器栈（纯文件存储，无需数据库） | Docker |

### 工程特性

- **故障可见**：数据获取返回 `DataResult`（OK/DEGRADED/EMPTY/FAILED），"确实没数据"与"拿数据失败"在类型层面区分，退化原因直达前端
- **多源降级**：统一的 `FallbackChain` 声明式降级链（数据源 + 质量校验 + 熔断），东财直连 → akshare → 问财 → Baostock
- **数据质量门禁**：全A行情表须过"行数/市值有效率/基准蓝筹"三道校验，残缺数据不会毒害缓存
- **架构守卫**：**0 循环依赖**，依赖图单向 DAG；分层与文件规模约束由测试强制（`tests/test_architecture.py`）
- **配置校验**：11 个 YAML 加载期校验，写错条件类型/job 类型/因子表达式会明确报错而非静默失效
- **747 个测试**，覆盖率 63%

---

## 快速开始

### 方式一：Web 界面（推荐日常使用）

```bash
# 安装依赖
pip install -r requirements.txt

# Web 界面 → http://localhost:8600（API 文档在 /docs）
python -m uvicorn src.api.main:app --port 8600
```

### 方式二：CLI 批量模式

```bash
# 估值分析
python main.py                         # 全市场
python main.py --market a              # 仅 A 股
python main.py --market all            # A + 港股 + 美股

# 股票筛选
python main.py --screen

# 数据抓取
python main.py --scrape news           # 仅新闻
python main.py --scrape all            # 新闻+公告+持仓+研报

# 监控（单次运行）
python main.py --monitor price         # 价格预警检查
python main.py --monitor earnings      # 财报披露检查
```

### 方式三：调度器长驻模式

```bash
# 长驻后台，自动按 cron 执行监控和抓取
python -m src.automation.scheduler
```

---

## 项目结构

```
stock_analyize-main/
│
├── main.py                          # CLI 调度入口（估值/筛选/监控/抓取）
│
├── config/                          # YAML 驱动全部业务逻辑（加载期有 schema 校验）
│   ├── stocks/                      # 股票池配置（按市场分）
│   ├── screen_config.yaml           # 16 条筛选策略（支持 AND/OR 嵌套逻辑）
│   ├── factors.yaml                 # 因子引擎 profile
│   ├── factors_alpha158.yaml        # 31 个 Alpha158 表达式因子
│   ├── holdings.yaml                # 持仓记录（持仓监控/体检数据源）
│   ├── scheduler.yaml               # 12 个定时任务（含 ML 月度重训）
│   ├── alerts.yaml                  # 告警通道 Server酱/Bark/PushPlus/Webhook
│   ├── price_alerts.yaml            # 价格预警规则
│   ├── earnings_monitor.yaml        # 财报监控 watchlist
│   ├── scraper.yaml                 # 4 类抓取器配置
│   ├── indicators.yaml              # 技术指标参数配置
│   └── backtest_presets.yaml        # 回测预设
│
├── src/
│   ├── core/                        # 核心层（不依赖任何业务模块）
│   │   ├── analyzer.py             # TTM 估值 + 分位数 + 目标价推演
│   │   ├── data_health.py          # ★ 数据健康度契约 DataResult/HealthStatus
│   │   ├── fallback.py             # ★ 统一多源降级链 FallbackChain
│   │   ├── cache_policy.py         # ★ 缓存 TTL 单点真相（按数据变化频率分档）
│   │   ├── config_validation.py    # ★ YAML 加载期校验（11 个配置）
│   │   ├── v8_guard.py             # ★ V8 崩溃守卫（子进程探测，崩才拦）
│   │   ├── columns.py              # 中英文列名映射（跨层数据契约）
│   │   ├── config_io.py            # YAML 读写 + 路径常量（唯一权威）
│   │   ├── market_registry.py      # 市场注册中心（加市场只改一处）
│   │   ├── plugin.py / settings.py / visualizer.py
│   │
│   ├── analysis/                    # 分析引擎
│   │   ├── technical/              # 技术指标（MACD/RSI/KDJ/布林带/均线 + 背离检测）
│   │   ├── factor/                 # 因子分析
│   │   │   ├── engine.py           # 因子引擎（YAML 驱动注册）
│   │   │   ├── expression.py       # ★ Alpha158 表达式引擎（AST 白名单 + 15 算子）
│   │   │   ├── ic_analysis.py      # ★ IC/ICIR/分层回测/衰减曲线
│   │   │   ├── fcf_analyzer.py     # 自由现金流评分（5 维度, 满分 100）
│   │   │   └── momentum.py / quality.py / valuation.py / technical.py
│   │   └── screening/              # 股票筛选引擎
│   │       ├── screener.py         # 两阶段过滤（向量化 Pass1 + 线程/进程池 Pass2）
│   │       ├── conditions_pkg/     # ★ 43 种条件（按范式分包）
│   │       │   ├── base.py         #   契约 + RankingCondition + CompositeCondition
│   │       │   ├── fundamental.py  #   基本面/快照条件
│   │       │   ├── entry.py        #   买入向（形态/突破/背离）
│   │       │   ├── exit.py         #   卖出向（止损/超买/死叉）
│   │       │   ├── factor.py       #   因子/排序类（ML打分/动量排名/低波动/股息）
│   │       │   └── meta.py         #   分类表与方向推断
│   │       ├── conditions.py       # 向后兼容 re-export shim
│   │       ├── config_schema.py    # YAML 策略解析（支持嵌套 logic 节点）
│   │       ├── data_provider.py    # 行情数据提供层（spot + 板块）
│   │       ├── kline_mixin.py      # K 线获取能力（日/周/月 + 分钟线 + 批量预取）
│   │       ├── spot_quality.py     # ★ 全A行情质量校验（三道门）
│   │       ├── circuit_breaker.py  # ★ 接口熔断器
│   │       └── proxy_policy.py     # ★ 代理自适应探测
│   │
│   ├── data/                        # 数据获取层
│   │   ├── fetchers.py             # A/HK/US 三市场数据获取器
│   │   ├── providers/
│   │   │   ├── eastmoney_spot.py   # ★ 东财全A直连（分页并发，5888 只/1.5 秒）
│   │   │   ├── alternative_data.py # ★ 另类数据 9 源
│   │   │   ├── fundamental_data.py # ★ 基本面深度 6 源
│   │   │   ├── baostock_provider.py / pytdx_provider.py / wencai_provider.py
│   │   │   ├── cache_manager.py    # 磁盘缓存（TTL + LRU + 容量上限）
│   │   │   ├── earnings_fetcher.py / index_kline.py
│   │   └── scrapers/               # 资讯抓取器（新闻/公告/持仓/研报）
│   │
│   ├── strategy/backtest/           # 回测模块
│   │   ├── runner.py               # Backtrader Cerebro 封装（事件驱动）
│   │   ├── vector_engine.py        # ★ NumPy 向量化回测（快 1-2 个数量级）
│   │   ├── optimizer.py            # ★ 参数寻优（grid/random/optuna）+ Walk-Forward
│   │   ├── report.py / compare.py  # 报告生成 + 多策略对比
│   │   └── ma_crossover.py / factor_strategy.py / rule_based.py / screener_rule.py
│   │
│   ├── ml/                          # 机器学习（LightGBM 截面模型）
│   │   ├── dataset_builder.py      # 数据集构建（55 特征 + 未来 20 日超额收益标签）
│   │   ├── trainer.py              # 训练 + Purged 时序 CV（防标签泄漏）
│   │   ├── predictor.py            # 推理
│   │   └── cli.py                  # 命令行入口
│   │
│   ├── notify/                      # ★ 告警通道（横切能力，不依赖业务层）
│   │   ├── base.py / state.py      # AlertChannel 基类 + 去重冷却
│   │   └── serverchan.py / bark.py / pushplus.py / webhook.py / console.py
│   │
│   ├── monitors/                    # ★ 监控业务（与调度解耦）
│   │   ├── buy_sell_alerts.py      # 买卖信号预警
│   │   ├── earnings_monitor.py     # 财报披露监控
│   │   └── holding_monitor.py      # 持仓预警
│   │
│   ├── automation/                  # 调度编排（只管"何时跑"）
│   │   ├── scheduler.py            # APScheduler 入口（YAML 驱动 cron）
│   │   └── scheduler_manager.py    # 调度管理器
│   │
│   ├── services/                    # 无头服务层（API 与调度器共用的唯一入口）
│   │   ├── screening_service.py    # 策略 CRUD（含新建）+ 条件 schema + 筛选执行
│   │   ├── backtest_service.py     # 回测 + 参数寻优
│   │   ├── market_data_service.py  # ★ 另类/基本面数据
│   │   ├── ml_service.py           # ★ ML 训练编排与状态
│   │   ├── factor_service.py       # ★ 因子清单与体检
│   │   ├── market_monitor_service.py # 市场情绪六维面板
│   │   ├── valuation_service.py / stock_service.py / portfolio_service.py
│   │   └── alert_service.py / strategy_market.py
│   │
│   ├── portfolio/                   # 持仓域（卖出引擎/大盘状态/组合诊断）
│   ├── trading/                     # 交易域（模拟盘 + 券商适配层）
│   ├── report/                      # AI 周报（Claude + 模板降级 + 导出）
│   │
│   ├── api/                         # Web 界面（FastAPI + SPA）
│   │   ├── main.py                 # REST API（38 端点，/docs 自动文档）
│   │   └── static/index.html       # 单页前端（6 视图）
│   │
│   └── utils/                       # 工具（日志/配置解析/异常/名称解析/文件锁）
│
├── scripts/vps_preflight.py         # ★ VPS 部署前置体检（纯标准库）
├── tests/                           # 747 个测试（含架构守卫与分层约束）
├── output/                          # 估值图 / 筛选 CSV / 回测历史 / 抓取数据
├── cache/                           # ML 数据集与模型、行情缓存
│
├── .github/workflows/test.yml       # GitHub Actions CI
├── pyproject.toml                   # 项目元数据 + mypy/ruff/pytest 配置
├── Dockerfile                       # Docker 构建
└── docker-compose.yml               # 三容器部署（app / scheduler / webui）
```

> ★ 标记为近期新增/重构的模块。依赖方向严格单向：
> `api → services → analysis/strategy/ml → data → core/utils`，
> `notify` 与 `monitors` 为横切/业务层，**无循环依赖**（由 `tests/test_architecture.py` 强制）。

---

## 三种运行模式详解

### Web 界面（FastAPI + 单页应用）

```bash
python -m uvicorn src.api.main:app --port 8600
```

打开 http://localhost:8600，左栏 5 个视图：

| 视图 | 功能 |
|------|------|
| 🎯 个股 | 代码/名称搜索联想 → 估值、目标价、策略买卖点、通用信号一次出 |
| 🧠 策略 | 策略管理（改名/复制/删除/条件编辑）+ 全市场筛选 + 按策略回测（含回撤与资金曲线） |
| 🧪 回测 | 多策略对比：KPI 表 + 收益曲线 |
| 🩺 持仓体检 | 四维健康分 + 问题清单 + 持仓明细 |
| 💹 模拟交易 | 本地模拟盘：下单/撤单/持仓/订单流水（A 股费用模型） |

REST API 文档：http://localhost:8600/docs（所有能力均可编程调用）。

### CLI 批量模式

```bash
# 估值分析 — 输出四格估值图 PNG
python main.py --market a            # A 股 → output/reports/
python main.py --market all          # 全市场

# 股票筛选 — 输出 CSV
python main.py --screen               # 默认策略 → output/screens/
python main.py --screen --screen-config ./config/my.yaml  # 自定义

# 价格预警 — 单次检查
python main.py --monitor price

# 财报监控 — 单次检查
python main.py --monitor earnings

# 数据抓取 — 输出 CSV
python main.py --scrape news          # 仅新闻
python main.py --scrape all           # 全部 4 类
```

### 调度器长驻模式

```bash
python -m src.automation.scheduler
```

默认配置 (`config/scheduler.yaml`)：

| 任务 | 触发时间 | 说明 |
|------|---------|------|
| 价格预警 | 交易日 9:30-15:00 每 5 分钟 | 实时监控 |
| 财报监控 | 每日 08:30 | 检查未来 30 天披露 |
| 公告抓取 | 盘后 18:00 | 当日公告 |
| 持仓抓取 | 盘后 18:10 | 股东持仓 |
| 研报抓取 | 盘前 09:00 | 券商研报 |
| 每日筛选 | 收盘 15:05 | 自动跑筛选 + 推送摘要 |

---

## 编写筛选策略

策略全部由 `config/screen_config.yaml` 驱动，也可在 Web 界面「策略中心」
点「➕ 新建策略」后用可视化编辑器配置。

### 基本结构

```yaml
strategies:
  my_strategy:
    name: 我的策略
    description: 一句话说明这个策略在赌什么
    conditions:                      # 买入侧：默认 AND 组合
      - type: market_cap             # 基本面条件（Pass1 向量化，快）
        min: 100                     # 单位：亿元
        max: 5000
      - type: price_above_ma         # 技术条件（Pass2 逐只算 K 线）
        ma_period: 60
    backtest:                        # 可选：配了才能回测
      default_stock: "600519"
      days_back: 750
      position_size: 0.9
      sell_conditions:
        - type: trailing_stop
          drawdown_pct: 10
```

### 嵌套逻辑（AND / OR）

用 `logic` + `conditions` 表达组合节点，可任意嵌套。下例是
"基本面达标 **且**（超跌 **或** 底背离 **或** 金叉）"：

```yaml
conditions:
  - type: market_cap
    min: 100
  - logic: any                       # all/and · any/or · none/not
    conditions:
      - type: rsi_oversold
        threshold: 32
      - type: daily_macd_divergence
      - type: ma_gold_cross
```

### 排序型条件（取前 N 名）

普通条件只回答"通过/不通过"，无法表达"全市场最强的 20 只"。排序型条件
（`momentum_rank` / `ml_top_k`）会在筛选结束后做全局排序截断：

```yaml
conditions:
  - type: momentum_rank
    period: 60                       # 按 60 日涨幅排序
    top_k: 20                        # 取最强 20 只
    skip_recent_days: 5              # 跳过最近 5 日，规避短期反转
```

### 内置策略一览（16 条）

| 类型 | 策略 |
|------|------|
| 价值 | 质量价值白马、红利价值、低估值底背离反转 |
| 成长/动量 | 成长动量多头、动量轮动TOP20、周日线多周期共振 |
| 突破 | 质量箱体突破、双均线趋势跟踪 |
| 反转 | 超跌质优反弹、均值回归（布林下轨）、日线底背离反弹 |
| 因子 | 低波动质优、ML增强选股 |
| 其他 | 小市值活跃金叉、北向流入低估值、多信号择一入场（嵌套示例） |

> **0 命中不等于故障**：底背离/突破/金叉等技术触发条件只在"当天出现信号"
> 时命中，多数交易日为 0 属正常，换个交易日再跑即可。真正的数据故障会在
> 筛选结果的 `warnings` 里给出明确提示（如"⚠️ 行情数据源退化"）。
> 想验证链路是否正常，跑基本面型策略 `quality_value`（通常命中数十只）。

---

## 告警通道配置

1. 复制 `.env.example` → `.env`，填入密钥：
   - **Server酱**（微信推送）：`https://sct.ftqq.com/`
   - **Bark**（iOS 推送）：Bark APP 设备 key
   - **PushPlus**（微信推送）：`http://www.pushplus.plus/`

2. 编辑 `config/alerts.yaml` 启用对应通道：

```yaml
channels:
  serverchan:
    enable: true
  bark:
    enable: true
  pushplus:
    enable: false
  console:
    enable: true     # 本地开发兜底，写入 logs/alerts.log
```

---

## 技术指标配置

编辑 `config/indicators.yaml`，支持多 profile 切换：

```yaml
active_profile: "default"
profiles:
  default:
    macd: {fast: 12, slow: 26, signal: 9}
    rsi: {period: 14}
    kdj: {n: 9, m1: 3, m2: 3}
    bollinger: {period: 20, std_dev: 2.0}
    moving_averages: {periods: [5, 10, 20, 60, 120, 250]}
```

---

## 数据源

| 市场 | 实时行情 | 财务数据 | 历史估值 | 披露日历 |
|------|----------|----------|----------|----------|
| A 股 | **东财直连** → akshare → 问财 → Baostock | akshare | akshare(百度) | akshare 预告/报告/预约披露 |
| 港股 | akshare(东方财富/新浪) | akshare(东方财富) | akshare(百度) | akshare 财报推导 |
| 美股 | akshare(东方财富/新浪) / yfinance | akshare(东方财富) | akshare(百度) | yfinance Ticker.calendar |

**K 线**：akshare(东财) → akshare(新浪) → pytdx 直连 → Baostock，其中 pytdx
不限频、并发安全，是全市场批量取数的主力（约 22ms/只）。

### 降级与质量保障

数据源不稳定是本项目最大的现实约束（东财对 IP 有**动态限流**，问财分页会
返回半份数据）。因此：

- 全A行情必须通过 `spot_quality` 三道校验才被采用——**行数 ≥4000**、
  **总市值有效率 ≥80%**、**必含基准蓝筹**。历史事故：问财返回 2649/5300 只
  且 97.9% 市值为 NaN、缺全部蓝筹，被当作正常数据缓存 12 小时，导致所有
  指数成分股筛选恒 0 命中
- 全链路失败时返回**最完整的一份**并标记退化，原因经 `warnings` 直达前端，
  用户能分清"数据源出问题"与"策略当天无信号"
- 熔断器：接口连续失败进入冷却窗口，避免每次请求都空等超时
- 代理策略**自适应探测**（直连/代理择优），可用 `STOCK_ANALYZE_KEEP_PROXY=1` 固定

> 百度接口不支持市销率(PS)历史数据，配置 `valuation: ps` 时历史估值使用市净率(PB)替代。

---

## ML 选股模型

**单一截面模型**（非一股一模型）：所有股票的所有交易日拉平成一张表，
`code` 排除在特征外，标签是**未来 20 日相对沪深300 的超额收益**。

```bash
# 训练（首次约 20 分钟，主要耗在建数据集；训练本身仅数秒）
python -m src.ml.cli train

# 或从 Web 界面「回测」页点「🔧 训练 ML 模型」
```

| 项 | 说明 |
|---|---|
| 特征 | 55 个 = 24 手写（动量/波动/均线偏离/RSI/MACD/估值）+ 31 个 Alpha158 |
| 标签 | `y_excess_ret_20d`（未来 20 交易日超额收益 %） |
| 验证 | `PurgedWalkForwardSplit`（purge 20 天 + embargo 5 天，防标签区间重叠泄漏） |
| 评估 | 横截面 IC（Spearman）；参考线：>0.03 可用，>0.05 较好 |
| 重训 | `scheduler.yaml` 的 `ml_retrain` job，每月 1 日自动执行 |

模型产出后，`ml_top_k` 条件即可用于筛选（见策略 `ml_enhanced`）。
**注意**：IC 会随市场风格切换而衰减，训练日志里的分折 IC 若出现负值，
说明近期模型有效性下降，应考虑重训或调整特征。

---

## 测试与代码质量

```bash
# 运行全部测试
pytest tests/ -v

# 代码检查
ruff check src/ tests/

# 类型检查
mypy src/ --ignore-missing-imports

# 安装 pre-commit 钩子（自动检查每次提交）
pre-commit install
```

### 测试覆盖（747 个用例，覆盖率 63%）

| 领域 | 测试文件 | 关注点 |
|------|---------|-------|
| 服务层 | test_screening/valuation/backtest/stock_service.py | 策略 CRUD、条件 schema、买卖点扫描 |
| API | test_api.py | 端点契约、策略管理、模拟盘下单 |
| 交易 | test_trading.py | 费用模型、限价撮合、状态持久化 |
| 组合 | test_portfolio.py / test_diagnostics.py | 卖出引擎、四维诊断 |
| 报告 | test_report.py | 上下文组装、LLM 降级、导出 |
| **架构** | test_architecture.py | **依赖图无环、分层方向、模块位置、文件规模预算** |
| **分层** | test_service_layering.py | **API 只依赖 services（防越层腐化）** |
| 数据健康 | test_data_health.py | EMPTY 与 FAILED 可区分、降级链语义 |
| 数据质量 | test_screener.py | 全A行情三道门校验（含两个线上事故值回归） |
| 稳定性 | test_v8_guard.py | V8 守卫双向契约：会崩必拦、不崩必放行（两次事故回归） |
| 因子 | test_expression_factor.py | 表达式 AST 白名单安全、IC/分层/衰减 |
| 回测 | test_vector_engine.py / test_optimizer.py | 无未来函数、成交口径、寻优与 Walk-Forward |
| 条件 | test_composite_conditions.py | AND/OR 嵌套语义、两轮筛选配合 |
| 配置 | test_config_validation.py | 用真实配置错误驱动（未知条件/非法表达式） |
| 引擎 | test_screener/factors/technical/divergence.py | 筛选、因子、指标、背离 |
| 监控 | test_alert/price_monitor/earnings_monitor/scheduler.py | 通道、去重、cron |

---------|-------|-------|
| test_analyzer.py | 16 | TTM 计算、估值推演、bug 回归 |
| test_alert.py | 18 | 4 通道推送、去重、重试 |
| test_price_monitor.py | 17 | 规则评估、cooldown、mock 行情 |
| test_earnings_monitor.py | 10 | 披露日历、业绩预告解析 |
| test_scraper.py | 14 | 4 类抓取器、增量检测 |
| test_scheduler.py | 14 | Job 注册、cron 解析 |
| test_screener.py | — | 筛选器测试 |
| test_factors.py | — | 因子测试 |
| test_technical.py | — | 技术指标测试 |
| test_divergence.py | — | 背离检测测试 |

---

## Docker 部署

```bash
# 构建镜像
docker build -t stock-analyize .

# CLI 模式
docker run --rm -v $(pwd)/output:/app/output stock-analyize

# 完整服务栈（分析 + 调度 + Web + 数据库）
docker-compose up
```

docker-compose 默认启动：
- `app` — 主分析服务（main.py）
- `scheduler` — APScheduler 长驻调度
- `webui` — Web 界面（FastAPI + SPA，端口 8600）
- `db` — MySQL 数据库

---

## 技术栈

| 类别 | 技术 |
|------|------|
| 数据源 | akshare, Baostock, pytdx, yfinance |
| 数据处理 | pandas, numpy, scipy |
| 技术分析 | ta (MACD/RSI/布林带), 手工实现 KDJ |
| 回测 | backtrader (事件驱动) + NumPy 向量化引擎 |
| Web | FastAPI + 原生单页前端（ECharts 图表） |
| 可视化 | matplotlib (静态), ECharts (交互) |
| 调度 | APScheduler (cron/interval) |
| 告警 | Server酱 / Bark / PushPlus / Webhook(HMAC) |
| 机器学习 | LightGBM (截面模型), optuna (参数寻优) |
| 测试 | pytest (747 用例), ruff (代码规范), mypy (类型检查) |
| CI/CD | GitHub Actions, pre-commit |
| 部署 | Docker, docker-compose |

---

## 许可证

MIT License
