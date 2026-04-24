import matplotlib
# matplotlib 全局配置：无头模式 + 中文字体栈（需在 import pyplot 之前设置）
matplotlib.use('Agg')
matplotlib.rcParams['font.sans-serif'] = ['Hiragino Sans GB', 'PingFang HK', 'Heiti TC', 'STHeiti', 'Arial Unicode MS', 'DejaVu Sans']
matplotlib.rcParams['axes.unicode_minus'] = False

import yaml
import matplotlib.pyplot as plt
import pandas as pd
import argparse
import os
import time
from data_fetcher import AStockDataFetcher, HKStockDataFetcher, USStockDataFetcher
from analyzer import AStockAnalyzer, HKStockAnalyzer, USStockAnalyzer
from visualizer import Visualizer
from src.utils.logger import setup_logger

# 全局日志初始化
logger = setup_logger()

# 市场映射表：添加新市场只需在此增加条目 + 对应 fetcher/analyzer 类 + YAML 配置文件
MARKET_MAPPING = {
    "a": {
        "name": "A股",
        "config_path": "./config/a_stock.yaml",
        "fetcher_class": AStockDataFetcher,
        "analyzer_class": AStockAnalyzer
    },
    "hk": {
        "name": "港股",
        "config_path": "./config/hk_stock.yaml",
        "fetcher_class": HKStockDataFetcher,
        "analyzer_class": HKStockAnalyzer
    },
    "us": {
        "name": "美股",
        "config_path": "./config/us_stock.yaml",
        "fetcher_class": USStockDataFetcher,
        "analyzer_class": USStockAnalyzer
    }
}


def load_config(file_path: str) -> dict:
    """
    加载指定市场的配置文件
    :param file_path: 配置文件完整路径
    :return: 配置字典
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        logger.debug(f"配置文件加载成功: {file_path}")
        return config
    except Exception as e:
        logger.error(f"配置文件加载失败 [{file_path}]: {e}", exc_info=True)
        return {}


def validate_stock_config(stock: dict, market_name: str, category_name: str) -> bool:
    """
    校验单只股票配置的完整性
    必填字段：code, name
    估值字段：valuation(pe/ps) + 对应的 pe_range 或 ps_range（3个数值的列表）
    :return: True 表示配置有效
    """
    code = stock.get("code")
    name = stock.get("name")
    if not code or not name:
        logger.warning(f"【{market_name}-{category_name}】股票配置缺少 code 或 name，跳过: {stock}")
        return False

    val_type = stock.get("valuation", "pe").lower()
    if val_type not in ("pe", "ps"):
        logger.warning(f"【{market_name}-{name}】估值类型 '{val_type}' 无效，仅支持 pe/ps，跳过")
        return False

    range_key = f"{val_type}_range"
    val_range = stock.get(range_key, [])
    if not isinstance(val_range, list) or len(val_range) != 3:
        logger.warning(f"【{market_name}-{name}】{range_key} 配置缺失或格式错误（需3个数值），跳过")
        return False

    return True


def process_single_market(market_code: str):
    """
    处理单个市场的全量分析流程（独立隔离，单个市场异常不影响其他市场）
    :param market_code: 市场编码 a/hk/us
    """
    # 1. 校验市场编码
    if market_code not in MARKET_MAPPING:
        logger.error(f"不支持的市场编码: {market_code}，可选值: {list(MARKET_MAPPING.keys())}")
        return

    market_info = MARKET_MAPPING[market_code]
    market_name = market_info["name"]
    config_path = market_info["config_path"]
    fetcher_class = market_info["fetcher_class"]
    analyzer_class = market_info["analyzer_class"]

    # 2. 加载市场配置
    market_config = load_config(config_path)
    if not market_config:
        logger.error(f"【{market_name}】配置加载失败，跳过该市场")
        return

    # 3. 校验市场全局开关
    if not market_config.get("enable", True):
        logger.info(f"【{market_name}】已在配置中关闭，跳过该市场")
        return

    # 4. 初始化该市场专属数据获取器（使用 with 语句确保资源安全释放）
    try:
        fetcher = fetcher_class()
        logger.info(f"【{market_name}】数据获取器初始化成功")
    except Exception as e:
        logger.error(f"【{market_name}】数据获取器初始化失败: {e}", exc_info=True)
        return

    with fetcher:
        # 5. 获取输出目录，自动创建
        output_dir = market_config.get("output_dir", f"./output/{market_name}")
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"【{market_name}】输出目录: {output_dir}")

        # 6. 遍历该市场所有板块
        category_list = market_config.get("categories", {})
        if not category_list:
            logger.warning(f"【{market_name}】未找到任何板块配置，跳过该市场")
            return

        for category_key, category_data in category_list.items():
            if category_data is None:
                logger.warning(f"【{market_name}】板块 '{category_key}' 配置为空（检查YAML缩进），跳过")
                continue
            category_name = category_data.get("name", "未分类")
            logger.info(f"【{market_name}】========== 开始处理板块: {category_name} ==========")

            stock_list = category_data.get("stocks", [])
            if not stock_list:
                logger.warning(f"【{market_name}-{category_name}】板块下无股票配置，跳过")
                continue

            # 7. 遍历板块内所有股票（单只股票异常不影响其他股票）
            for stock in stock_list:
                # 配置完整性校验：缺少必填字段或格式错误时跳过
                if not validate_stock_config(stock, market_name, category_name):
                    continue

                code = stock["code"]
                name = stock["name"]

                # 注入扩展信息，供下游分析、可视化使用
                stock["market_name"] = market_name
                stock["category_name"] = category_name

                logger.info(f"【{market_name}-{category_name}】启动数据拉取与分析: {name} - {code} ...")

                try:
                    # 1. 网络IO拉取数据（调用该市场专属 fetcher，接口统一）
                    fin_df = fetcher.get_financial_abstract(code)
                    val_type = stock.get('valuation', 'pe')
                    hist_val_df = fetcher.get_historical_valuation(code, val_type)
                    market_data = fetcher.get_current_market_data(code)

                    # 2. 内存计算与特征提取（调用该市场专属 analyzer）
                    analyzer = analyzer_class(fin_df, hist_val_df, market_data, stock)
                    analysis_result = analyzer.process()

                    if not analysis_result:
                        logger.error(f"【{market_name}-{name}】分析结果为空，跳过可视化")
                        time.sleep(1)
                        continue

                    # 3. 视图渲染
                    visualizer = Visualizer(analysis_result, stock)
                    fig = visualizer.plot()

                    # 4. 结果落盘（按市场独立目录存储）
                    output_file = f"[{category_name}]_{name}_{code}_四格估值分析.png"
                    output_full_path = os.path.join(output_dir, output_file)
                    fig.savefig(output_full_path, dpi=300, bbox_inches='tight')
                    plt.close(fig)

                    logger.info(f"【{market_name}-{category_name}】生成成功 -> {output_full_path}")

                except Exception as e:
                    logger.error(f"【{market_name}-{category_name}】分析异常 [{name}-{code}]: {e}", exc_info=True)
                    # 单只股票异常，继续处理下一只，不中断流程
                finally:
                    # akshare 请求间隔，防止限流
                    time.sleep(2)

        logger.info(f"【{market_name}】全量板块处理完成，所有结果已保存至: {output_dir}")


def run_screener(args, strategy_ids: list[str] | None = None):
    """
    运行股票筛选模式
    :param args: argparse 命名空间
    :param strategy_ids: 可选，指定要运行的策略 ID 列表
    """
    from src.screener import StockScreener

    config_path = args.screen_config
    logger.info(f"========== 股票筛选系统启动，配置文件: {config_path}，策略: {strategy_ids} ==========")

    screener = StockScreener()
    results = screener.run_from_config(config_path, strategy_ids=strategy_ids)

    if results.empty:
        logger.info("筛选结果为空，未找到符合条件的股票")
    else:
        # 控制台输出结果表格
        print("\n" + "=" * 80)
        print("  股票筛选结果")
        print("=" * 80)
        # 格式化数值列
        pd.set_option("display.max_rows", 200)
        pd.set_option("display.max_columns", 20)
        pd.set_option("display.width", 120)
        pd.set_option("display.unicode.east_asian_width", True)
        print(results.to_string(index=False))
        print(f"\n共 {len(results)} 只股票符合筛选条件")
        print("=" * 80)

        # 保存CSV
        output_dir = "./output"
        os.makedirs(output_dir, exist_ok=True)
        csv_path = os.path.join(output_dir, "screen_result.csv")
        results.to_csv(csv_path, index=False, encoding="utf-8-sig")
        logger.info(f"筛选结果已保存至: {csv_path}")

    logger.info("========== 股票筛选系统执行完成 ==========")


def run_monitor(args):
    """
    运行监控任务（价格预警 / 财报披露）
    :param args: argparse 命名空间，需包含 monitor_type 字段
    """
    mtype = args.monitor_type
    logger.info(f"========== 监控任务启动：{mtype} ==========")

    # 统一构建告警通道（读取 config/alerts.yaml）
    from src.automation.alert import AlertStateStore, build_channels
    alerts_cfg = load_config(args.alerts_config)
    channels = build_channels(alerts_cfg)
    store = AlertStateStore()

    if mtype == "price":
        from src.automation.monitor.price_monitor import PriceMonitor
        cfg = load_config(args.monitor_config or "./config/price_alerts.yaml")
        rules = cfg.get("rules", []) or []
        monitor = PriceMonitor(
            rules=rules,
            channels=channels,
            state_store=store,
            cooldown_hours=int(cfg.get("default_cooldown_hours", 24)),
        )
        stats = monitor.run()
        logger.info(f"价格预警执行完成: 总 {stats['total']} / 已推送 {stats['sent']} / 跳过 {stats['skipped']}")

    elif mtype == "earnings":
        from src.automation.monitor.earnings_monitor import EarningsMonitor
        cfg = load_config(args.monitor_config or "./config/earnings_monitor.yaml")
        monitor = EarningsMonitor(
            watchlist=cfg.get("watchlist", {}) or {},
            days_ahead=int(cfg.get("days_ahead", 30)),
            remind_days_ahead=int(cfg.get("remind_days_ahead", 3)),
            track_forecasts=bool(cfg.get("track_forecasts", True)),
            channels=channels,
            state_store=store,
            cooldown_hours=int(cfg.get("cooldown_hours", 72)),
        )
        stats = monitor.run()
        logger.info(f"财报监控执行完成: 总 {stats['total']} / 已推送 {stats['sent']} / 跳过 {stats['skipped']}")

    else:
        logger.error(f"未知监控类型: {mtype}")

    logger.info(f"========== 监控任务完成：{mtype} ==========")


def run_scrape(args):
    """
    运行数据抓取任务（新闻/公告/持仓/研报/全部）
    :param args: argparse 命名空间，需包含 scrape_type 字段
    """
    stype = args.scrape_type
    logger.info(f"========== 数据抓取任务启动：{stype} ==========")

    from src.data.scraper import (
        AnnouncementScraper, HoldingsScraper, NewsScraper, ResearchScraper, run_all,
    )

    cfg = load_config(args.scraper_config)
    output_dir = args.output_dir

    if stype == "all":
        results = run_all(cfg, output_dir=output_dir)
        for name, df in results.items():
            logger.info(f"  [{name}] {len(df)} 条")
    else:
        # 单独抓取某一种
        SINGLE_CLS = {
            "news": NewsScraper,
            "announcements": AnnouncementScraper,
            "holdings": HoldingsScraper,
            "research": ResearchScraper,
        }
        cls = SINGLE_CLS.get(stype)
        if not cls:
            logger.error(f"未知抓取类型: {stype}")
            return
        section = (cfg.get(stype) or {}).copy()
        section.pop("enable", None)
        scraper = cls(**section)
        df = scraper.fetch()
        scraper.save_csv(df, output_dir)
        logger.info(f"[{stype}] 抓取完成，共 {len(df)} 条")

    logger.info(f"========== 数据抓取任务完成：{stype} ==========")


def main():
    # 命令行参数解析
    # 设计保留扁平结构（兼容老命令），同时新增 --monitor / --scrape 两个子任务开关
    parser = argparse.ArgumentParser(description="三市场股票分析平台（估值/筛选/预警/监控/抓取）")

    # ---- 估值分析（原有）----
    parser.add_argument(
        "--market", type=str, default="all", choices=["a", "hk", "us", "all"],
        help="指定要运行的市场：a=A股，hk=港股，us=美股，all=全量运行"
    )

    # ---- 股票筛选（原有）----
    parser.add_argument(
        "--screen", action="store_true",
        help="启动股票筛选模式（根据条件组合筛选A股）"
    )
    parser.add_argument(
        "--screen-config", type=str, default="./config/screen_config.yaml",
        help="筛选条件配置文件路径（默认 ./config/screen_config.yaml）"
    )

    # ---- 监控任务（新增）----
    parser.add_argument(
        "--monitor", dest="monitor_type", type=str, choices=["price", "earnings"],
        help="启动监控任务：price=价格预警, earnings=财报披露监控"
    )
    parser.add_argument(
        "--monitor-config", type=str, default=None,
        help="监控任务配置文件路径（默认 price→price_alerts.yaml, earnings→earnings_monitor.yaml）"
    )
    parser.add_argument(
        "--alerts-config", type=str, default="./config/alerts.yaml",
        help="告警通道配置文件路径（默认 ./config/alerts.yaml）"
    )

    # ---- 数据抓取（新增）----
    parser.add_argument(
        "--scrape", dest="scrape_type", type=str,
        choices=["news", "announcements", "holdings", "research", "all"],
        help="启动抓取任务：news/announcements/holdings/research/all"
    )
    parser.add_argument(
        "--scraper-config", type=str, default="./config/scraper.yaml",
        help="抓取配置文件路径（默认 ./config/scraper.yaml）"
    )
    parser.add_argument(
        "--output-dir", type=str, default="./output",
        help="输出根目录（默认 ./output）"
    )

    args = parser.parse_args()

    # 分发：优先级  监控 > 抓取 > 筛选 > 估值分析
    if args.monitor_type:
        run_monitor(args)

    if args.scrape_type:
        run_scrape(args)

    if args.screen:
        # 如果是命令行运行，目前只支持传一个策略 ID（可选）
        strategy_ids = None
        if hasattr(args, "strategy_id") and args.strategy_id:
            strategy_ids = [args.strategy_id]
        run_screener(args, strategy_ids=strategy_ids)

    # 如果没有任何特殊模式，则执行默认的估值分析模式
    if not (args.monitor_type or args.scrape_type or args.screen):
        # 估值分析模式（默认）
        if args.market == "all":
            run_markets = list(MARKET_MAPPING.keys())
        else:
            run_markets = [args.market]

        logger.info(f"========== 估值分析系统启动，待运行市场: {run_markets} ==========")

        for market_code in run_markets:
            process_single_market(market_code)
            time.sleep(3)

        logger.info("========== 估值分析系统全部任务执行完成 ==========")


if __name__ == "__main__":
    main()