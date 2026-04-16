# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# -------- 估值分析 --------
python main.py --market a        # A股
python main.py --market hk       # 港股
python main.py --market us       # 美股
python main.py --market all      # 全部市场（默认）

# -------- 股票筛选 --------
python main.py --screen                                    # 默认筛选配置
python main.py --screen --screen-config ./config/my.yaml   # 自定义配置

# -------- 价格 / 财报监控（推送告警）--------
python main.py --monitor price                             # 单次执行价格预警
python main.py --monitor earnings                          # 单次执行财报披露监控
python main.py --monitor price --monitor-config ./config/price_alerts.yaml

# -------- 数据抓取（新闻/公告/持仓/研报）--------
python main.py --scrape news         # 只抓新闻
python main.py --scrape all          # 全部 4 类

# -------- Streamlit 前端（6 个页面）--------
streamlit run src/visualization/gui/streamlit_app.py --server.port=8501

# -------- APScheduler 长驻调度器 --------
python -m src.automation.scheduler                         # 默认 config/scheduler.yaml
python -m src.automation.scheduler --config ./my.yaml

# -------- 测试（需加 -o "addopts=" 跳过 pyproject.toml 中的 cov 配置，除非已安装 pytest-cov）--------
pytest -o "addopts=" tests/ -v
pytest -o "addopts=" tests/test_analyzer.py::TestBugFixes -v

# -------- 代码检查 --------
ruff check src/ tests/
ruff format src/ tests/

# -------- Docker --------
docker-compose up    # 启动 app / scheduler / webui / db 四个服务
```

## Architecture

Multi-market stock valuation analysis platform supporting A-shares, HK, and US stocks. Core capabilities:

1. **估值分析**：4 格估值图（营收利润、历史分位、目标价、汇总表）
2. **股票筛选**：二阶段条件组合筛选（快速 spot 过滤 + 深度 OHLCV 分析）
3. **价格预警**：阈值 / 涨跌幅 / 均线突破规则，推送至 Server酱 / Bark / PushPlus
4. **财报披露监控**：跟踪 A/港/美未来 30 天披露日历 + 业绩预告
5. **数据抓取**：财经新闻 / 公司公告 / 股东持仓 / 研报评级 4 类
6. **Streamlit 前端**：6 页面（估值分析 / 筛选 / 预警 / 财报 / 抓取 / 告警历史）
7. **APScheduler 调度器**：长驻进程按 cron 触发所有监控与抓取任务

### Data Flow

```
YAML config (config/*.yaml)
    ├─→ DataFetcher ─→ Analyzer ─→ Visualizer ─→ PNG (output/)
    ├─→ Scraper ─→ CSV (output/scraper/)
    ├─→ Monitor ─→ AlertChannel ─→ HTTP push
    └─→ Streamlit ─→ Browser UI (8501)
```

### Class Hierarchy

- **`data_fetcher.py`**:
  - `BaseDataFetcher` (ABC) — 统一接口 + 上下文管理器协议
  - `AStockDataFetcher` — A股，pytdx 实时行情 + akshare 财务/估值
  - `AkshareDataFetcher` — 港股/美股通用基类，封装共性逻辑
    - `HKStockDataFetcher` — 港股钩子（akshare 东方财富+百度）
    - `USStockDataFetcher` — 美股钩子（akshare 东方财富+百度）

- **`analyzer.py`**:
  - `BaseAnalyzer` (ABC) — TTM 计算、估值推演、历史分位数
  - `AStockAnalyzer` — A股财务数据清洗
  - `InternationalStockAnalyzer` — 港股/美股共用（别名 `HKStockAnalyzer`/`USStockAnalyzer`）

- **`visualizer.py`**: Single `Visualizer` class for all markets, produces matplotlib 4-grid charts

- **`src/automation/alert/`** — 告警通道层：
  - `AlertChannel` (ABC) + `AlertEvent` dataclass
  - `ConsoleChannel` / `ServerChanChannel` / `BarkChannel` / `PushPlusChannel`
  - `AlertStateStore` — JSON 去重存储（cooldown 窗口）
  - `CHANNEL_REGISTRY` / `build_channels(config)` / `dispatch(event, channels, store)`

- **`src/automation/monitor/`** — 监控器：
  - `BaseMonitor` (ABC) — 统一 run() 流程（采集→去重→推送→落盘 CSV）
  - `PriceMonitor` — 价格预警（price_below/above, pct_change_daily, pct_from_cost, ma_break）
  - `EarningsMonitor` — 财报披露 + 业绩预告

- **`src/automation/scheduler.py`** — APScheduler BlockingScheduler：
  - `build_scheduler(config)` → 注册所有 jobs
  - `JOB_BUILDERS` 字典（price_monitor / earnings_monitor / scraper）
  - 支持 cron 与 interval 触发器，SIGTERM 优雅停止

- **`src/data/scraper/`** — 抓取器：
  - `BaseScraper` (ABC) — fetch / fetch_new（基于 seen.json 增量）/ save_csv / filter_by_keywords
  - `NewsScraper` / `AnnouncementScraper` / `HoldingsScraper` / `ResearchScraper`
  - `SCRAPER_REGISTRY` / `build_scrapers(config)` / `run_all(config, output_dir)`

- **`src/data/fetcher/earnings_fetcher.py`** — `EarningsFetcher`：统一 A/HK/US 披露日历

### Market Registration

`main.py` has a `MARKET_MAPPING` dict — adding a new market requires only adding an entry here plus the corresponding fetcher/analyzer classes and YAML config.

### Data Sources

| Market | Real-time | Financials | Historical Valuation | Earnings Calendar |
|--------|-----------|------------|---------------------|-------------------|
| A-shares | pytdx (TdxHq) | akshare | akshare (Baidu finance) | akshare `stock_yjyg_em`/`stock_yysj_em` |
| HK | akshare (东方财富 daily) | akshare (东方财富) | akshare (Baidu finance) | akshare `stock_financial_hk_analysis_indicator_em` 推导 |
| US | akshare (东方财富 daily) | akshare (东方财富) | akshare (Baidu finance) | yfinance `Ticker.calendar` |

> Note: Baidu finance API has no PS (Price-to-Sales) indicator. When `valuation: ps` is configured, historical valuation data uses PB (Price-to-Book) as a substitute.

### Config Files (config/)

| File | Purpose |
|------|---------|
| `a_stock.yaml` / `hk_stock.yaml` / `us_stock.yaml` | 各市场关注股票池 + 估值档位 |
| `screen_config.yaml` | 股票筛选条件组合 |
| `alerts.yaml` | 告警通道开关与密钥（支持环境变量覆盖） |
| `price_alerts.yaml` | 价格预警规则 |
| `earnings_monitor.yaml` | 财报监控 watchlist + 阈值 |
| `scraper.yaml` | 4 类抓取器配置 |
| `scheduler.yaml` | APScheduler Job 定义 |

### Environment Variables

敏感密钥从 `.env` 读取（复制 `.env.example`），`alerts.yaml` 留空即自动 fallback 到 env：

- `SERVERCHAN_KEY` — Server 酱 sendkey
- `BARK_KEY` — Bark 设备 key
- `PUSHPLUS_TOKEN` — PushPlus token

### Refactored Modules (src/)

- `src/analysis/` — factor analysis (valuation, momentum, quality, technical), technical indicators, divergence detection
- `src/screener/` — composable stock screening engine (two-stage: fast spot filter + deep OHLCV analysis)
- `src/data/fetcher/` — market-specific fetchers, cache manager, earnings fetcher
- `src/data/scraper/` — 4 类抓取器（news/announcement/holdings/research）
- `src/data/storage/` **[stub]** — SQLAlchemy ORM（未实装）
- `src/strategy/backtest/` — backtrader-based backtesting framework
- `src/automation/alert/` — 4 推送通道 + 去重 state
- `src/automation/monitor/` — price / earnings 监控器
- `src/automation/scheduler.py` — APScheduler 长驻任务
- `src/report/` **[stub]** — PDF/PPTX report generation（未实装）
- `src/visualization/gui/` — Streamlit 多页面前端（6 个页面）

## Key Conventions

- Python 3.9+, line length 120, double quotes, ruff for linting/formatting
- All code comments and docstrings in Chinese
- Stock pool configuration is YAML-driven: per-stock `valuation` type (pe/ps) and threshold ranges (`pe_range`, `ps_range`)
- Config validation: `validate_stock_config()` in main.py checks required fields before processing
- Isolation: single stock or market failures are caught and logged without crashing the full run
- Resource management: DataFetcher supports `with` statement for safe cleanup (especially TDX connections)
- matplotlib is configured with `Agg` backend and Chinese font stack at import time (top of `main.py` only; Streamlit pages 单独配置)
- Test markers: `@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.slow`
- 告警事件 `event_key` 由 `股票代码:规则类型:日期` 组成，cooldown 窗口内相同 key 只推一次
- 抓取器增量检测：`fetch_new()` 基于 `.cache/scraper/seen_{name}.json` 做 hash diff
