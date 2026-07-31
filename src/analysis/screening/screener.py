"""
src/screener/screener.py — 股票筛选引擎

两轮过滤架构：
  1. 第一轮（快速）：基于实时行情数据在内存中过滤（市值/PE/PB等）—— 向量化
  2. 第二轮（精筛）：对候选股拉取K线数据，检测技术条件（底背离/RSI等）—— session 复用 + 线程池并行

性能优化（相比最初版本）：
  - Pass1 `apply(axis=1)` → pd.Series 向量化运算（50-200x 提速）
  - Pass2 单次 Baostock login 复用（省掉每股 2×(login+logout)=~1s 开销）
  - Pass2 ThreadPoolExecutor 并发执行 CPU 密集的指标计算
  - 去掉默认 0.3s sleep（Baostock 不限频）
"""
import math
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

import pandas as pd

from src.analysis.screening.conditions import BaseCondition, ExcludeRiskCondition
from src.analysis.screening.config_schema import parse_screen_config
from src.analysis.screening.data_provider import ScreenerDataProvider
from src.utils.logger import get_logger

logger = get_logger("screener")


# ============================================================================
# Pass2 单股评估（模块级：线程池与进程池共用；Windows spawn 需要顶层可 pickle）
# ============================================================================

# 日线基线 bar 数；交易日 → 日历天换算系数（365/245 ≈ 1.49，取 1.6 留安全余量）
_MIN_DAILY_BARS = 60
_CALENDAR_PER_TRADING_DAY = 1.6
# 周线：1 根 bar ≈ 7 日历天，×1.1 余量；默认拉取深度下限保持 3 年（1095 天）
_MIN_WEEKLY_DAYS = 365 * 3


def required_daily_bars(conditions: list) -> int:
    """
    一组条件所需的最小日线 bar 数（取各条件 required_bars() 的最大值）。

    修复背景：get_daily_ohlcv 默认 days_back=120 是**日历天**（约 81 根交易 bar），
    而 price_above_ma(120) / multi_ma_bull(ma250) 等长窗口条件需要 120/250 根 bar，
    数据永远不够长 → len(df) < 窗口 → 条件恒 False → 筛选恒 0 命中。
    """
    need = _MIN_DAILY_BARS
    for cond in conditions:
        if getattr(cond, "ohlcv_period", "daily") != "daily":
            continue
        try:
            need = max(need, int(cond.required_bars()))
        except Exception:
            pass
    return need


def daily_days_back_for(conditions: list) -> int:
    """条件所需日线 bar 数 → get_daily_ohlcv 的 days_back（日历天数）"""
    return int(required_daily_bars(conditions) * _CALENDAR_PER_TRADING_DAY) + 10


def weekly_days_back_for(conditions: list) -> int:
    """
    条件所需周线 bar 数 → get_weekly_ohlcv 的 days_back（日历天数）。

    与日线同类的隐患：周线固定拉 3 年 ≈ 150 根 bar，若周线条件的
    lookback 配置超过 ~130 根同样会恒 False。此处按需放大，下限保持 3 年。
    """
    need_bars = 0
    for cond in conditions:
        if getattr(cond, "ohlcv_period", "daily") != "weekly":
            continue
        try:
            need_bars = max(need_bars, int(cond.required_bars()))
        except Exception:
            pass
    return max(_MIN_WEEKLY_DAYS, int(need_bars * 7 * 1.1) + 30)


def _evaluate_row(
    provider: ScreenerDataProvider,
    row: pd.Series,
    ohlcv_conditions: list,
    need_weekly: bool,
    need_daily: bool,
    daily_days_back: int = 120,
    weekly_days_back: int = _MIN_WEEKLY_DAYS,
) -> tuple[bool, str, str, str, float | None]:
    """
    对单只候选股拉 K 线并逐条件评估。

    Args:
        daily_days_back / weekly_days_back:
            日/周线回看的日历天数（须与 Phase A 预取一致才能命中缓存）

    Returns:
        (passed, name, code, reason, ml_score)
        reason ∈ {"ok", "no_code", "no_data", "cond_failed:<name>", "error:<msg>"}
        ml_score: ml_top_k 条件算出的预测分（无则 None）
    """
    code = str(row.get("代码", "")).strip()
    name = str(row.get("名称", "")).strip()
    if not code:
        return False, name, code, "no_code", None

    try:
        weekly_df = (provider.get_weekly_ohlcv(code, days_back=weekly_days_back)
                     if need_weekly else pd.DataFrame())
        daily_df = (provider.get_daily_ohlcv(code, days_back=daily_days_back)
                    if need_daily else pd.DataFrame())
    except Exception as e:
        logger.debug(f"{code} K线获取异常: {e}")
        return False, name, code, f"error:{type(e).__name__}", None

    # 区分：K 线数据为空（拉取失败） vs 条件未通过
    if (need_weekly and (weekly_df is None or weekly_df.empty)) or \
       (need_daily and (daily_df is None or daily_df.empty)):
        return False, name, code, "no_data", None

    ml_score: float | None = None
    for cond in ohlcv_conditions:
        ohlcv_df = weekly_df if cond.ohlcv_period == "weekly" else daily_df
        try:
            if not cond.evaluate_full(row, ohlcv_df):
                return False, name, code, f"cond_failed:{cond.name}", ml_score
            # B 路径：捕获 ml_top_k 条件计算出的预测分
            if getattr(cond, "name", "") == "ml_top_k":
                score = row.get("_ml_score")
                if score is not None and pd.notna(score):
                    ml_score = float(score)
        except Exception as e:
            logger.debug(f"{code} 条件 {cond.name} 异常: {e}")
            return False, name, code, f"error:{cond.name}:{type(e).__name__}", ml_score

    return True, name, code, "ok", ml_score


# 进程池 worker 的每进程 provider（initializer 中构造，避免每 chunk 重建）
_PROC_PROVIDER: ScreenerDataProvider | None = None


def _proc_pool_init(cache_dir: str) -> None:
    global _PROC_PROVIDER
    _PROC_PROVIDER = ScreenerDataProvider(cache_dir=cache_dir)


def _proc_eval_chunk(
    payload: list[tuple],          # [(idx, row_dict), ...]
    ohlcv_conditions: list,
    need_weekly: bool,
    need_daily: bool,
    daily_days_back: int = 120,
    weekly_days_back: int = _MIN_WEEKLY_DAYS,
) -> list[tuple]:
    """进程池任务单元：评估一个 chunk 的候选股，返回 [(idx, passed, name, code, reason, ml_score)]"""
    provider = _PROC_PROVIDER or ScreenerDataProvider()
    out = []
    for idx, row_dict in payload:
        row = pd.Series(row_dict)
        passed, name, code, reason, ml_score = _evaluate_row(
            provider, row, ohlcv_conditions, need_weekly, need_daily,
            daily_days_back=daily_days_back, weekly_days_back=weekly_days_back,
        )
        out.append((idx, passed, name, code, reason, ml_score))
    return out


class StockScreener:
    """
    可组合条件的A股筛选器

    使用示例:
        from src.analysis.screening.conditions import MarketCapCondition, WeeklyMACDBottomDivergenceCondition

        screener = StockScreener()
        screener.add_condition(MarketCapCondition(min_cap=50, max_cap=500))
        screener.add_condition(WeeklyMACDBottomDivergenceCondition())

        results = screener.run()
        print(results)

    Args:
        data_provider: 数据提供者（默认新建）
        request_delay: 每只股票请求后 sleep 时长；默认 0，Baostock 实际无限频
        max_workers: Pass2 并行 worker 数。=1 走串行（最稳），>1 开线程池并发
                     指标计算（MACD/RSI/背离）并行；K 线拉取仍在 worker 串行以免 Baostock
                     竞争全局 socket
        use_processes: Pass2 改用进程池（绕过 GIL，CPU 密集条件多时提速明显）。
                       K 线已由 Phase A 预取到磁盘缓存，子进程直接读缓存无网络竞争。
                       候选股 < _PROC_MIN_CANDIDATES 时自动退回线程池（进程启动开销不划算）
    """

    # 进程池启用的最小候选股数（低于此值 spawn 开销 > 收益）
    _PROC_MIN_CANDIDATES = 16

    def __init__(
        self,
        data_provider: ScreenerDataProvider | None = None,
        request_delay: float = 0.0,
        max_workers: int = 8,
        use_processes: bool = False,
    ):
        self._provider = data_provider or ScreenerDataProvider()
        self._conditions: list[BaseCondition] = []
        self._request_delay: float = float(request_delay)
        self._max_workers: int = max(1, int(max_workers))
        self._use_processes: bool = bool(use_processes)
        # B 路径：ML 预测分缓存 code -> score，用于 ml_top_k 条件的最终排名
        self._ml_scores: dict[str, float] = {}
        # 本次运行的数据质量警告（数据源退化时非空，由服务层透传前端）
        self.data_warning: str = ""

    def add_condition(self, condition: BaseCondition) -> "StockScreener":
        """链式添加筛选条件（AND组合）"""
        self._conditions.append(condition)
        logger.debug(f"添加筛选条件: {condition}")
        return self

    def run(self, sort_by: str = "总市值", ascending: bool = False, limit: int = 100,
            stock_scope: set[str] | None = None, auto_exclude_risk: bool = True) -> pd.DataFrame:
        """
        执行筛选

        Args:
            sort_by: 结果排序列
            ascending: 是否升序
            limit: 最大返回数量
            stock_scope: 可选的股票代码范围集合（板块/指数过滤），None 表示不过滤
            auto_exclude_risk: 是否自动注入全局风险排除条件（排除ST/退市股），默认开启

        Returns:
            筛选结果 DataFrame，包含: 代码, 名称, 最新价, 总市值(亿), 市盈率, 市净率 等
        """
        if not self._conditions:
            logger.warning("未添加任何筛选条件，返回空结果")
            return pd.DataFrame()

        # 自动注入全局默认排除条件（ST/退市股），避免每个策略重复配置
        if auto_exclude_risk:
            has_risk_exclude = any(
                c.name in ("exclude_risk", "exclude_st", "exclude_delisting_risk")
                for c in self._conditions
            )
            if not has_risk_exclude:
                self._conditions.insert(0, ExcludeRiskCondition(strict=True))
                logger.debug("自动注入全局风险排除条件（排除ST/退市股）")

        # 第一轮：获取全A实时行情并做内存过滤
        all_stocks = self._provider.get_all_a_shares()
        if all_stocks.empty:
            logger.error("获取全A股数据失败，筛选终止")
            self.data_warning = "全A行情数据获取失败（所有数据源均不可用），无法筛选"
            return pd.DataFrame()
        # 数据源退化时记录警告，供服务层透传前端——否则用户只看到"0 只符合"，
        # 会误以为是策略太严，实际是行情表残缺（历史事故：2649/5300 只且缺全部蓝筹）
        if all_stocks.attrs.get("degraded"):
            self.data_warning = (
                f"⚠️ 行情数据源退化：{all_stocks.attrs.get('degrade_reason', '')}"
                f"（当前仅 {len(all_stocks)} 只）。筛选结果偏少属数据问题，"
                f"建议稍后重试或检查网络/代理"
            )
            logger.warning(self.data_warning)

        # 板块/指数范围过滤
        if stock_scope is not None:
            before = len(all_stocks)
            code_col = "代码" if "代码" in all_stocks.columns else "code"
            all_stocks = all_stocks[
                all_stocks[code_col].astype(str).isin(stock_scope)
            ].copy()
            logger.info(f"板块/指数范围过滤: {before} → {len(all_stocks)} 只")

        logger.info(f"第一轮筛选开始，全A共 {len(all_stocks)} 只股票")
        candidates = self._pass1_spot_filter(all_stocks)
        logger.info(f"第一轮筛选完成，{len(all_stocks)} → {len(candidates)} 只候选")

        # 第二轮：对候选股逐只做OHLCV条件检测
        ohlcv_conditions = [c for c in self._conditions if c.requires_ohlcv]
        if ohlcv_conditions:
            logger.info(f"第二轮筛选开始，{len(candidates)} 只候选，{len(ohlcv_conditions)} 个K线条件")
            candidates = self._pass2_ohlcv_filter(candidates, ohlcv_conditions)
            logger.info(f"第二轮筛选完成，剩余 {len(candidates)} 只")
        else:
            logger.info("无K线条件，跳过第二轮筛选")

        if candidates.empty:
            logger.info("筛选结果为空")
            return pd.DataFrame()

        # B 路径：若包含 ml_top_k 条件，做 top-K 截断（按 ml_score 排序）
        ml_top_k_cond = next(
            (c for c in self._conditions if getattr(c, "name", "") == "ml_top_k"),
            None,
        )
        if ml_top_k_cond is not None and self._ml_scores:
            code_col = "代码" if "代码" in candidates.columns else "code"
            candidates = candidates.copy()
            candidates["ml_score"] = candidates[code_col].astype(str).map(self._ml_scores)
            # 按 ml_score 降序取 top_k
            top_k = int(getattr(ml_top_k_cond, "top_k", 50))
            before = len(candidates)
            candidates = candidates.sort_values("ml_score", ascending=False, na_position="last").head(top_k)
            logger.info(f"ml_top_k 截断: {before} → {len(candidates)} (top_k={top_k})")

        # 整理输出列
        result = self._format_output(candidates)

        # 如果有 ML 分，把它带到输出列（便于查看）
        if "ml_score" in candidates.columns and "ml_score" not in result.columns:
            code_col = "代码" if "代码" in result.columns else "code"
            code_to_score = dict(zip(candidates[
                "代码" if "代码" in candidates.columns else "code"
            ].astype(str), candidates["ml_score"]))
            result["ml_score"] = result[code_col].astype(str).map(code_to_score)

        # 排序和截断
        if sort_by in result.columns:
            result = result.sort_values(sort_by, ascending=ascending)
        else:
            # 兜底：sort_by 列不存在（如数据源退化无总市值列）时，
            # 退化到 代码 列排序并提示，避免静默无序
            fallback_col = next(
                (c for c in ("代码", "code", "最新价") if c in result.columns),
                None,
            )
            if fallback_col is not None:
                logger.warning(
                    f"排序列 '{sort_by}' 不存在，退化到 '{fallback_col}'"
                )
                result = result.sort_values(fallback_col, ascending=ascending)
            else:
                logger.warning(f"排序列 '{sort_by}' 不存在，且无可用兜底列，跳过排序")
        result = result.head(limit).reset_index(drop=True)

        logger.info(f"筛选完成，共 {len(result)} 只股票符合条件")
        return result

    def run_from_config(self, config_path: str, strategy_ids: list[str] | None = None,
                        stock_scope: set[str] | None = None) -> pd.DataFrame:
        """
        从配置文件加载条件并运行完整筛选。

        Args:
            config_path: YAML路径
            strategy_ids: 可选，指定要运行的策略 ID 列表
            stock_scope: 可选的股票代码范围集合（板块/指数过滤）
        """
        conditions, output_config = parse_screen_config(config_path, strategy_ids=strategy_ids)
        for cond in conditions:
            self.add_condition(cond)

        sort_by = output_config.get("sort_by", "总市值(亿)")
        limit = output_config.get("limit", 100)
        ascending = output_config.get("ascending", False)

        return self.run(sort_by=sort_by, ascending=ascending, limit=limit, stock_scope=stock_scope)

    def _pass1_spot_filter(self, all_stocks: pd.DataFrame) -> pd.DataFrame:
        """
        第一轮：Spot条件内存过滤（向量化）

        每个条件通过 `evaluate_vectorized(df) -> pd.Series[bool]` 直接对整个 DataFrame 批处理，
        比原 `apply(axis=1)` 快 50-200 倍（5000 行从 ~20 秒降到 <0.5 秒）。
        """
        spot_conditions = [c for c in self._conditions if not c.requires_ohlcv]
        if not spot_conditions:
            return all_stocks

        t0 = time.perf_counter()
        mask = pd.Series(True, index=all_stocks.index)
        for cond in spot_conditions:
            try:
                cond_mask = cond.evaluate_vectorized(all_stocks)
                # 保险：未对齐时强制对齐
                if not isinstance(cond_mask, pd.Series) or not cond_mask.index.equals(all_stocks.index):
                    cond_mask = all_stocks.apply(lambda row, _c=cond: _c.evaluate_spot(row), axis=1)
            except Exception as e:
                logger.warning(f"条件 [{cond.name}] 向量化失败，降级 apply: {e}")
                cond_mask = all_stocks.apply(lambda row, _c=cond: _c.evaluate_spot(row), axis=1)
            cond_mask = cond_mask.astype(bool).fillna(False)
            passed = int(cond_mask.sum())
            logger.debug(f"  条件 [{cond.name}]: 通过 {passed} 只")
            mask &= cond_mask

        elapsed = time.perf_counter() - t0
        logger.info(f"第一轮向量化耗时 {elapsed:.2f}s（全市场 {len(all_stocks)} 只）")
        return all_stocks[mask].copy()

    def _evaluate_one(self, idx, row, ohlcv_conditions, need_weekly, need_daily,
                      daily_days_back: int = 120,
                      weekly_days_back: int = _MIN_WEEKLY_DAYS):
        """
        单只候选股评估（线程池任务单元）。委托模块级 _evaluate_row（与进程池共用）。

        B18 修复：返回值含 reason 字段，区分"数据拉取失败" vs "条件未通过"。
        Returns:
            (idx, passed: bool, name, code, reason: str)
              reason ∈ {"ok", "no_data", "cond_failed:<name>", "error:<msg>"}
        """
        passed, name, code, reason, ml_score = _evaluate_row(
            self._provider, row, ohlcv_conditions, need_weekly, need_daily,
            daily_days_back=daily_days_back, weekly_days_back=weekly_days_back,
        )
        if ml_score is not None:
            self._ml_scores[code] = ml_score
        if self._request_delay > 0:
            time.sleep(self._request_delay)
        return idx, passed, name, code, reason

    def _pass2_ohlcv_filter(self, candidates: pd.DataFrame,
                             ohlcv_conditions: list[BaseCondition]) -> pd.DataFrame:
        """
        第二轮：对候选股拉 K 线并检测技术条件。

        优化点：
          1. **批量并发预取 K 线**（akshare 线程安全，8 worker 并发 → 6-8x 提速）
             之后条件评估阶段全命中本地缓存，几乎零 I/O
          2. 线程池并发评估：指标计算（MACD/RSI/背离）并行
          3. Baostock 兜底：akshare 拉失败的 code 用 Baostock session 补拉（串行但复用连接）
        """
        need_weekly = any(c.ohlcv_period == "weekly" for c in ohlcv_conditions)
        need_daily = any(c.ohlcv_period == "daily" for c in ohlcv_conditions)

        total = len(candidates)
        if total == 0:
            return candidates

        # 按条件的最长回看窗口计算拉取深度（修复：MA120/MA250 等长窗口条件
        # 在默认 120 日历天≈81 根 bar 下永远数据不足 → 恒 False → 筛选恒 0；
        # 周线同理，按需在 3 年下限之上放大）
        daily_days_back = daily_days_back_for(ohlcv_conditions)
        weekly_days_back = weekly_days_back_for(ohlcv_conditions)
        if need_daily:
            logger.info(
                f"  日线拉取深度: {required_daily_bars(ohlcv_conditions)} bar "
                f"→ {daily_days_back} 日历天（按条件最长窗口自动推断）"
            )

        # --- Phase A: 批量并发预取 K 线（真正的加速点） ---
        codes = [str(r.get("代码", "")).strip() for _, r in candidates.iterrows()]
        codes = [c for c in codes if c]

        t_prefetch = time.perf_counter()
        if need_weekly:
            stat = self._provider.prefetch_ohlcv_batch(
                codes, period="weekly", days_back=weekly_days_back,
                max_workers=self._max_workers,
            )
            logger.info(f"  周线预取: {stat}")
        if need_daily:
            stat = self._provider.prefetch_ohlcv_batch(
                codes, period="daily", days_back=daily_days_back,
                max_workers=self._max_workers,
            )
            logger.info(f"  日线预取: {stat}")
        logger.info(f"  Phase A（预取）耗时 {time.perf_counter() - t_prefetch:.1f}s")

        # --- Phase B: 条件评估（全命中缓存，纯 CPU + 少量磁盘 I/O）---
        t0 = time.perf_counter()
        passed_indices: list = []
        done = 0

        # B18: 统计失败原因分布
        reason_counter: dict[str, int] = {}

        workers = min(self._max_workers, total)

        # 进程池路径：CPU 密集条件多时绕过 GIL（Phase A 已把 K 线预取到磁盘缓存，
        # 子进程读同一缓存目录，无重复网络请求）。失败自动降级线程池。
        if self._use_processes and workers > 1 and total >= self._PROC_MIN_CANDIDATES:
            try:
                passed_indices, reason_counter = self._pass2_eval_processes(
                    candidates, ohlcv_conditions, need_weekly, need_daily, workers,
                    daily_days_back=daily_days_back,
                    weekly_days_back=weekly_days_back,
                )
                elapsed = time.perf_counter() - t0
                logger.info(f"  Phase B（进程池评估）耗时 {elapsed:.1f}s "
                            f"(平均 {elapsed / max(total, 1) * 1000:.1f}ms/只)")
                logger.info(f"第二轮完成：{total} 只 → {len(passed_indices)} 只")
                no_data = reason_counter.get("no_data", 0)
                if no_data > 0:
                    logger.warning(
                        f"  其中 {no_data} 只因 K 线数据为空被剔除"
                        f"（可能数据源失败或新股，建议复查）"
                    )
                return candidates.loc[passed_indices].copy()
            except Exception as e:
                logger.warning(f"进程池评估失败（{type(e).__name__}: {e}），降级线程池")
                passed_indices, reason_counter = [], {}

        if workers <= 1:
            # 串行路径
            for idx, row in candidates.iterrows():
                idx_, ok, name, code, reason = self._evaluate_one(
                    idx, row, ohlcv_conditions, need_weekly, need_daily,
                    daily_days_back, weekly_days_back,
                )
                done += 1
                reason_counter[reason] = reason_counter.get(reason, 0) + 1
                if ok:
                    passed_indices.append(idx_)
                    logger.info(f"  ✓ {name}({code}) 通过所有K线条件")
                if done == 1 or done % 100 == 0 or done == total:
                    elapsed = time.perf_counter() - t0
                    rate = done / elapsed if elapsed > 0 else 0
                    eta = (total - done) / rate if rate > 0 else 0
                    logger.info(f"  Phase B 评估进度: {done}/{total} "
                                f"速率 {rate:.1f}/s ETA {eta:.0f}s")
        else:
            logger.info(f"  Phase B 并行评估: workers={workers}")
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(self._evaluate_one, idx, row,
                                ohlcv_conditions, need_weekly, need_daily,
                                daily_days_back, weekly_days_back): idx
                    for idx, row in candidates.iterrows()
                }
                for fut in as_completed(futures):
                    idx_, ok, name, code, reason = fut.result()
                    done += 1
                    reason_counter[reason] = reason_counter.get(reason, 0) + 1
                    if ok:
                        passed_indices.append(idx_)
                        logger.info(f"  ✓ {name}({code}) 通过所有K线条件")
                    if done == 1 or done % 100 == 0 or done == total:
                        elapsed = time.perf_counter() - t0
                        rate = done / elapsed if elapsed > 0 else 0
                        eta = (total - done) / rate if rate > 0 else 0
                        logger.info(f"  Phase B 评估进度: {done}/{total} "
                                    f"速率 {rate:.1f}/s ETA {eta:.0f}s")

        elapsed = time.perf_counter() - t0
        logger.info(f"  Phase B（评估）耗时 {elapsed:.1f}s "
                    f"(平均 {elapsed / max(total, 1) * 1000:.1f}ms/只)")
        logger.info(f"第二轮完成：{total} 只 → {len(passed_indices)} 只")
        # B18：在日志显示失败原因分布，方便诊断"是数据问题还是条件太严"
        no_data_count = reason_counter.get("no_data", 0)
        if no_data_count > 0:
            logger.warning(
                f"  其中 {no_data_count} 只因 K 线数据为空被剔除"
                f"（可能数据源失败或新股，建议复查）"
            )
        return candidates.loc[passed_indices].copy()

    def _pass2_eval_processes(
        self,
        candidates: pd.DataFrame,
        ohlcv_conditions: list[BaseCondition],
        need_weekly: bool,
        need_daily: bool,
        workers: int,
        daily_days_back: int = 120,
        weekly_days_back: int = _MIN_WEEKLY_DAYS,
    ) -> tuple[list, dict[str, int]]:
        """
        Pass2 进程池评估：候选股按 chunk 分发到子进程（减少 IPC 次数）。

        Returns:
            (passed_indices, reason_counter)
        """
        total = len(candidates)
        rows_payload = [(idx, row.to_dict()) for idx, row in candidates.iterrows()]
        chunk_size = max(1, math.ceil(total / (workers * 4)))
        chunks = [rows_payload[i:i + chunk_size]
                  for i in range(0, total, chunk_size)]
        logger.info(f"  Phase B 进程池评估: workers={workers}, "
                    f"chunks={len(chunks)} (每块 {chunk_size} 只)")

        passed_indices: list = []
        reason_counter: dict[str, int] = {}
        done = 0
        t0 = time.perf_counter()
        with ProcessPoolExecutor(
            max_workers=workers,
            initializer=_proc_pool_init,
            initargs=(self._provider.cache_dir,),
        ) as pool:
            futures = [
                pool.submit(_proc_eval_chunk, chunk, ohlcv_conditions,
                            need_weekly, need_daily, daily_days_back,
                            weekly_days_back)
                for chunk in chunks
            ]
            for fut in as_completed(futures):
                for idx, ok, name, code, reason, ml_score in fut.result():
                    done += 1
                    reason_counter[reason] = reason_counter.get(reason, 0) + 1
                    if ml_score is not None:
                        self._ml_scores[code] = ml_score
                    if ok:
                        passed_indices.append(idx)
                        logger.info(f"  ✓ {name}({code}) 通过所有K线条件")
                elapsed = time.perf_counter() - t0
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                logger.info(f"  Phase B 评估进度: {done}/{total} "
                            f"速率 {rate:.1f}/s ETA {eta:.0f}s")
        return passed_indices, reason_counter

    @staticmethod
    def _format_output(df: pd.DataFrame) -> pd.DataFrame:
        """整理输出列：重命名 + 选取 + 市值转亿元"""
        # 源列名 → 目标列名映射
        rename_map = {
            "代码": "代码",
            "名称": "名称",
            "最新价": "最新价",
            "涨跌幅": "涨跌幅(%)",
            "总市值": "总市值(亿)",
            "流通市值": "流通市值(亿)",
            "市盈率-动态": "PE(动态)",
            "市净率": "PB",
            "换手率": "换手率(%)",
            "振幅": "振幅(%)",
        }

        # 仅保留源数据中存在的列，一步 rename
        available = {k: v for k, v in rename_map.items() if k in df.columns}
        result = df[list(available.keys())].rename(columns=available).copy()

        # 市值转亿元
        for col in ("总市值(亿)", "流通市值(亿)"):
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce") / 1e8

        return result
