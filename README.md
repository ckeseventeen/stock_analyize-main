# 多市场股票分析平台

集估值分析、条件筛选、价格预警、财报披露监控、财经资讯抓取于一体的一站式工具，
支持 A 股 / 港股 / 美股三市场，提供 CLI、Streamlit 前端与 APScheduler 长驻调度器。

## 功能概览

- **估值分析**：四格估值图（营收利润、历史分位、目标价、汇总表）
- **股票筛选**：基于 YAML 条件组合的两阶段筛选（快速 spot 过滤 + 深度 OHLCV）
- **价格预警**：支持价格阈值 / 涨跌幅 / 相对成本 / 均线突破，推送至手机
- **财报披露监控**：跟踪 A/港/美未来 30 天披露日历 + 业绩预告
- **数据抓取**：财经新闻 / 公司公告 / 股东持仓 / 研报评级 4 类
- **Streamlit 前端**：6 页面（估值分析 / 筛选 / 预警 / 财报 / 抓取 / 告警历史）
- **APScheduler 调度器**：长驻进程自动周期执行监控与抓取
- **Docker 一键部署**：app / scheduler / webui / db 四服务栈

## 快速开始

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. （可选）配置告警密钥
cp .env.example .env
# 编辑 .env 填入 SERVERCHAN_KEY / BARK_KEY / PUSHPLUS_TOKEN

# 3. 运行估值分析
python main.py --market a
```

## 项目结构

```
stock_analyize/
├── main.py                     # 主入口（估值/筛选/监控/抓取四种模式）
├── data_fetcher.py             # A/HK/US 三市场数据获取器
├── analyzer.py                 # 估值分析器（TTM、分位数、目标价）
├── visualizer.py               # 四格图可视化
├── config/
│   ├── a_stock.yaml            # A 股股票池
│   ├── hk_stock.yaml           # 港股股票池
│   ├── us_stock.yaml           # 美股股票池
│   ├── screen_config.yaml      # 股票筛选条件
│   ├── alerts.yaml             # 告警通道开关与密钥
│   ├── price_alerts.yaml       # 价格预警规则
│   ├── earnings_monitor.yaml   # 财报监控 watchlist
│   ├── scraper.yaml            # 4 类抓取器配置
│   └── scheduler.yaml          # 定时任务定义
├── src/
│   ├── analysis/               # 因子分析、技术指标、背离检测
│   ├── screener/               # 股票筛选引擎
│   ├── data/
│   │   ├── fetcher/            # 实时行情、财务、披露日历
│   │   └── scraper/            # 新闻/公告/持仓/研报抓取
│   ├── strategy/backtest/      # backtrader 回测
│   ├── automation/
│   │   ├── alert/              # 4 推送通道（Server酱/Bark/PushPlus/Console）
│   │   ├── monitor/            # 价格 / 财报监控器
│   │   └── scheduler.py        # APScheduler 长驻任务
│   └── visualization/gui/      # Streamlit 多页面前端
├── tests/                      # 89+ 单元测试（pytest）
├── output/                     # 分析图 / 事件 CSV
├── cache/                      # 数据缓存 + 告警 state
└── logs/                       # 运行日志
```

## 使用方式

### 1. 估值分析

```bash
python main.py                    # 全部市场
python main.py --market a         # A 股
python main.py --market hk        # 港股
python main.py --market us        # 美股
```

输出路径：`output/a股/`、`output/港股/`、`output/美股/`

### 2. 股票筛选

```bash
python main.py --screen                                    # 默认配置
python main.py --screen --screen-config ./config/my.yaml   # 自定义
```

输出：`output/screen_result.csv`

### 3. 价格预警

编辑 `config/price_alerts.yaml` 添加规则，然后：

```bash
python main.py --monitor price
```

支持的规则类型：
- `price_below` / `price_above` — 绝对价格阈值
- `pct_change_daily` — 当日涨跌幅超 ±X%
- `pct_from_cost` — 相对成本价涨跌超 X%
- `ma_break` — 跌破 / 突破 N 日均线

### 4. 财报披露监控

```bash
python main.py --monitor earnings
```

- 未来 3 天内披露 → 推送「披露提醒」
- 业绩预告（yjyg）→ 推送「预告事件」含同比幅度
- 输出 `output/earnings_calendar.csv`

### 5. 数据抓取

```bash
python main.py --scrape news         # 仅新闻
python main.py --scrape all          # 新闻+公告+持仓+研报
```

输出路径：`output/scraper/{type}_{YYYYMMDD}.csv`

### 6. Streamlit 前端

```bash
streamlit run src/visualization/gui/streamlit_app.py --server.port=8501
```

浏览器打开 http://localhost:8501，侧边栏 6 个页面：
1. 📈 估值分析 — 选股 → 跑 analyzer → 展示 4 格图
2. 🔍 股票筛选 — 一键跑 screener，结果表可下载 CSV
3. 🔔 价格预警 — 表单增删规则，直接写回 YAML（保留注释）
4. 📅 财报披露 — 三市场 Tab + 汇总柱状图
5. 🌐 资讯抓取 — 4 Tab 分别触发 scraper
6. 📜 告警历史 — 查看已触发事件 + cooldown 状态

### 7. APScheduler 调度器

```bash
python -m src.automation.scheduler                  # 默认 config/scheduler.yaml
python -m src.automation.scheduler --config ./my.yaml
```

默认配置：
- 价格预警：交易日 9-11, 13-15 时段每 5 分钟
- 财报监控：每天 08:30
- 新闻抓取：每 30 分钟
- 公告/持仓/研报：每天盘后 18:00 / 盘前 09:00

## 告警通道配置

复制 `.env.example` → `.env`，填入真实密钥：

```bash
SERVERCHAN_KEY=SCT...         # https://sct.ftqq.com/
BARK_KEY=XxXxXxXxXx           # iOS Bark APP 设备 key
PUSHPLUS_TOKEN=32位字符        # http://www.pushplus.plus/
```

编辑 `config/alerts.yaml` 启用对应通道：

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

## 数据源

| 市场 | 实时行情 | 财务数据 | 历史估值 | 披露日历 |
|------|----------|----------|----------|----------|
| A 股 | 通达信(pytdx) | akshare | akshare(百度) | akshare 预告/报告/预约披露 |
| 港股 | akshare(东方财富) | akshare(东方财富) | akshare(百度) | akshare 财报推导 |
| 美股 | akshare(东方财富) | akshare(东方财富) | akshare(百度) | yfinance Ticker.calendar |

> 注意：百度接口不支持市销率(PS)历史数据，配置 `valuation: ps` 时历史估值使用市净率(PB)替代。

## 测试

```bash
# 运行全部测试（89+ 用例）
pytest -o "addopts=" tests/ -v

# 指定测试类
pytest -o "addopts=" tests/test_analyzer.py::TestBugFixes -v

# 代码检查与格式化
ruff check src/ tests/
ruff format src/ tests/
```

## Docker

```bash
# 单独分析
docker build -t stock-analyize .
docker run --rm -v $(pwd)/output:/app/output stock-analyize

# 完整栈（分析 + 定时调度 + Web前端 + MySQL）
docker-compose up
```

docker-compose 默认启动：
- `app` — 主分析服务
- `scheduler` — APScheduler 长驻
- `webui` — Streamlit 前端 (8501)
- `db` — MySQL

## 测试覆盖

本项目附带 89+ 单元测试覆盖各核心模块：

| 测试文件 | 用例数 | 关注点 |
|---------|-------|-------|
| `test_analyzer.py` | 16 | TTM 计算、估值推演、bug 回归 |
| `test_alert.py` | 18 | 4 通道推送、去重、重试 |
| `test_price_monitor.py` | 17 | 规则评估、cooldown、mock 行情 |
| `test_earnings_monitor.py` | 10 | 披露日历、业绩预告解析 |
| `test_scraper.py` | 14 | 4 类抓取器、增量检测、关键词过滤 |
| `test_scheduler.py` | 14 | Job 注册、cron 解析、工厂闭包 |

## 许可证

MIT License
