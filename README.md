# 多市场量化投研分析平台

集估值分析、技术筛选、因子分析、回测验证、自动化监控于一体的一站式工具，
支持 **A 股 / 港股 / 美股** 三市场，提供 CLI 批量处理、Streamlit Web 交互界面与 APScheduler 长驻调度器三种运行模式。

## 功能概览

| 功能模块 | 说明 | 运行方式 |
|----------|------|---------|
| 📈 **估值分析** | TTM 净利润推演 → PE/PS 三情景目标价 → 历史分位数 → 四格估值图 | CLI / Web |
| 🔍 **股票筛选** | 15 个预设策略、28 种可组合条件，两阶段过滤（向量化 Spot + OHLCV 深度验证） | CLI / Web |
| 🔬 **因子分析** | FCF 自由现金流评分、动量因子、质量因子、估值因子、技术因子 | Web |
| 📊 **策略回测** | Backtrader 回测引擎 + 多策略对比 + 历史回测持久化 | Web |
| 🔔 **价格预警** | 价格阈值 / 涨跌幅 / 相对成本 / 均线突破，推送至手机 | 调度器 |
| 📅 **财报监控** | A/港/美三市场未来 30 天披露日历 + 业绩预告 | 调度器 |
| 🌐 **资讯抓取** | 财经新闻 / 公司公告 / 股东持仓 / 研报评级 | CLI / 调度器 |
| 🖥️ **Web 界面** | 12 个功能页面，覆盖全部操作（Streamlit） | Web |
| ⏰ **定时任务** | APScheduler 长驻进程，自动周期执行监控与抓取 | 调度器 |
| 🐳 **Docker 部署** | app / scheduler / webui / db 四服务容器栈 | Docker |

---

## 快速开始

### 方式一：Web 交互界面（推荐日常使用）

```bash
# 安装依赖
pip install -r requirements.txt

# 启动 Web 服务 → 自动打开浏览器 http://localhost:8501
streamlit run src/web/app.py

# 或者使用一键脚本（自动创建虚拟环境 + 安装依赖）
# Windows:
run.bat --gui
# Linux/macOS:
./run.sh --gui
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
├── config/                          # YAML 驱动全部业务逻辑
│   ├── stocks/                      # 股票池配置（按市场分）
│   │   ├── a_stock.yaml            # A 股（科技/消费/高端制造）
│   │   ├── hk_stock.yaml           # 港股（科技巨头/消费）
│   │   └── us_stock.yaml           # 美股（科技巨头/消费）
│   ├── screen_config.yaml           # 15 个筛选策略定义
│   ├── scheduler.yaml               # 定时任务（价格/财报/抓取/每日筛选）
│   ├── alerts.yaml                  # 告警通道 Server酱/Bark/PushPlus
│   ├── price_alerts.yaml            # 价格预警规则
│   ├── earnings_monitor.yaml        # 财报监控 watchlist
│   ├── scraper.yaml                 # 4 类抓取器配置
│   ├── factors.yaml                 # 因子引擎配置
│   ├── indicators.yaml              # 技术指标参数配置
│   └── backtest_presets.yaml        # 回测预设
│
├── src/
│   ├── core/                        # 核心引擎
│   │   ├── analyzer.py             # TTM 估值 + 分位数 + 目标价推演
│   │   ├── data_fetcher.py         # A/HK/US 三市场数据获取器
│   │   └── visualizer.py           # 四格估值图 matplotlib
│   │
│   ├── analysis/                    # 分析引擎
│   │   ├── technical/              # 技术指标（MACD/RSI/KDJ/布林带/均线）
│   │   │   ├── indicators.py       # 指标计算器
│   │   │   └── divergence.py       # MACD 底背离/顶背离检测
│   │   ├── factor/                 # 因子分析
│   │   │   ├── engine.py           # 因子引擎（YAML 驱动注册）
│   │   │   ├── fcf_analyzer.py     # 自由现金流评分（5 维度, 满分 100）
│   │   │   ├── momentum.py         # 动量因子
│   │   │   ├── quality.py          # 质量因子
│   │   │   ├── valuation.py        # 估值因子
│   │   │   └── technical.py        # 技术因子
│   │   └── screening/              # 股票筛选引擎
│   │       ├── screener.py         # 两阶段过滤（向量化 + 线程池）
│   │       ├── conditions.py       # 28 种筛选条件
│   │       ├── config_schema.py    # YAML 策略解析器
│   │       └── data_provider.py    # 筛选数据提供层
│   │
│   ├── data/                        # 数据获取层
│   │   ├── providers/              # 数据源（baostock + 缓存）
│   │   │   ├── baostock_provider.py
│   │   │   ├── cache_manager.py    # 磁盘缓存（TTL + LRU + 200MB 上限）
│   │   │   └── earnings_fetcher.py # 财报日历获取器
│   │   ├── scrapers/              # 资讯抓取器
│   │   │   ├── base.py             # 抓取器基类
│   │   │   ├── news_scraper.py     # 财经新闻
│   │   │   ├── announcement_scraper.py # 公司公告
│   │   │   ├── holdings_scraper.py # 股东持仓
│   │   │   └── research_scraper.py # 研报评级
│   │   └── fcf_data_fetcher.py     # 自由现金流数据获取器
│   │
│   ├── strategy/backtest/           # 回测模块
│   │   ├── runner.py               # Backtrader Cerebro 封装
│   │   ├── report.py               # 报告生成 + CSV/JSON 持久化 + 多策略对比
│   │   ├── base_strategy.py        # 策略基类
│   │   ├── ma_crossover.py         # 双均线策略
│   │   ├── factor_strategy.py      # 因子轮动策略
│   │   ├── rule_based.py           # 规则引擎策略
│   │   └── screener_rule.py        # 筛选器条件桥接回测
│   │
│   ├── automation/                  # 自动化
│   │   ├── scheduler.py            # APScheduler 入口（YAML 驱动 cron）
│   │   ├── scheduler_manager.py    # 调度管理器
│   │   ├── alert/                  # 告警通道
│   │   │   ├── base.py             # AlertChannel 基类
│   │   │   ├── serverchan.py       # Server酱（微信）
│   │   │   ├── bark.py             # Bark（iOS）
│   │   │   ├── pushplus.py         # PushPlus（微信）
│   │   │   ├── console.py          # 控制台
│   │   │   └── state.py            # 告警去重 + 冷却
│   │   └── monitor/                # 监控器
│   │       ├── base.py             # BaseMonitor
│   │       ├── price_monitor.py    # 价格预警（4 种规则类型）
│   │       └── earnings_monitor.py # 财报披露监控
│   │
│   ├── web/                         # Streamlit Web 前端
│   │   ├── app.py                  # 入口 + 首页仪表盘
│   │   ├── utils.py                # 配置 CRUD + 组件工具
│   │   └── pages/                  # 12 个功能页面
│   │       ├── 1_估值分析.py        # 四格估值分析
│   │       ├── 2_策略配置.py        # 筛选策略可视化编辑
│   │       ├── 3_股票筛选.py        # 一键执行筛选
│   │       ├── 4_策略回测.py        # 回测 + 多策略对比
│   │       ├── 5_价格预警.py        # 预警规则管理
│   │       ├── 6_财报披露.py        # 三市场披露日历
│   │       ├── 7_资讯抓取.py        # 4 类数据抓取
│   │       ├── 8_告警历史.py        # 告警事件查看
│   │       ├── 9_配置管理.py        # 全局配置管理
│   │       ├── 10_调度管理.py       # APScheduler 状态
│   │       ├── 11_FCF分析.py        # 自由现金流仪表盘
│   │       └── 12_关注标的.py       # 关注列表管理
│   │
│   └── utils/                       # 工具模块
│       ├── logger.py                # 日志（按天轮转保留30天）
│       ├── config_parser.py         # 通用配置解析
│       ├── exception_handler.py     # 异常处理
│       └── name_resolver.py         # 股票名称解析
│
├── tests/                           # 89+ 单元测试（pytest）
├── output/                          # 自动生成的输出
│   ├── reports/                     # 估值分析 PNG 图
│   ├── screens/                     # 筛选结果 CSV
│   ├── backtest_history/            # 回测历史记录
│   └── scrapers/                    # 抓取数据 CSV
│
├── .streamlit/config.toml           # Streamlit 主题配置
├── .github/workflows/test.yml       # GitHub Actions CI
├── .pre-commit-config.yaml          # 代码规范 pre-commit 钩子
├── pyproject.toml                   # 项目元数据 + mypy/ruff/pytest 配置
├── Dockerfile                       # Docker 构建
└── docker-compose.yml               # 四容器部署
```

---

## 三种运行模式详解

### Web 交互界面（12 个功能页面）

```bash
streamlit run src/web/app.py
```

打开 http://localhost:8501 后，侧边栏分为 3 组：

**📊 分析与筛选**
| 页面 | 功能 |
|------|------|
| 1_估值分析 | 选股 → 拉取财务数据 → 生成四格估值图 |
| 2_策略配置 | 可视化编辑筛选策略参数 |
| 3_股票筛选 | 一键执行策略筛选，结果可排序导出 |
| 4_策略回测 | Backtrader 回测 + 多策略绩效对比 |

**🔔 预警与监控**
| 页面 | 功能 |
|------|------|
| 5_价格预警 | 添加/编辑/删除预警规则 |
| 6_财报披露 | A/港/美股未来 30 天披露日历 |
| 7_资讯抓取 | 新闻/公告/持仓/研报 4 类抓取 |
| 8_告警历史 | 已触发事件 + 冷却状态 |

**⚙️ 系统管理**
| 页面 | 功能 |
|------|------|
| 9_配置管理 | 全局配置、指标参数、回测预设 |
| 10_调度管理 | APScheduler 状态监控 |
| 11_FCF分析 | 自由现金流多标的横向对比 |
| 12_关注标的 | 多市场关注列表增删改查 |

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
| A 股 | akshare(东方财富) / Baostock | akshare | akshare(百度) | akshare 预告/报告/预约披露 |
| 港股 | akshare(东方财富) | akshare(东方财富) | akshare(百度) | akshare 财报推导 |
| 美股 | akshare(东方财富) / yfinance | akshare(东方财富) | akshare(百度) | yfinance Ticker.calendar |

> 百度接口不支持市销率(PS)历史数据，配置 `valuation: ps` 时历史估值使用市净率(PB)替代。

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

### 测试覆盖

| 测试文件 | 用例数 | 关注点 |
|---------|-------|-------|
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
- `webui` — Streamlit 前端（端口 8501）
- `db` — MySQL 数据库

---

## 技术栈

| 类别 | 技术 |
|------|------|
| 数据源 | akshare, Baostock, pytdx, yfinance |
| 数据处理 | pandas, numpy, scipy |
| 技术分析 | ta (MACD/RSI/布林带), 手工实现 KDJ |
| 回测 | backtrader |
| 可视化 | matplotlib (静态), plotly (交互), Streamlit (Web) |
| 调度 | APScheduler (cron/interval) |
| 告警 | Server酱 / Bark / PushPlus |
| 测试 | pytest (89+ 用例), ruff (代码规范), mypy (类型检查) |
| CI/CD | GitHub Actions, pre-commit |
| 部署 | Docker, docker-compose |

---

## 许可证

MIT License
