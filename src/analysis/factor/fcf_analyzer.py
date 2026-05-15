import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("fcf_analyzer")


class FCFAnalyzer:
    """
    FCF (Free Cash Flow) 分析计算与评分逻辑

    B4 修复：构造时检查输入频率（年度 vs 季度），季度数据自动注解 annualized=False
    并在 FCF Yield 计算时按需 × 4，避免低估 4 倍。
    """

    def __init__(self, df: pd.DataFrame, market_cap: float, *, annualized: bool | None = None):
        """
        Args:
            df: 需要包含列 operating_cash_flow, capex, revenue, net_profit
            market_cap: 当前总市值
            annualized: 输入数据是否已年化（年度 / TTM）。None 时自动检测：
                - 索引为 DatetimeIndex 且相邻间隔 < 200 天 → 季度数据
                - 否则视为年度
                FCF Yield 计算在季度数据上会自动 × 4 年化。
        """
        self.df = df.copy()
        self.market_cap = market_cap
        self.scores = {}
        self.summary = {}
        self.annualized = self._detect_annualized() if annualized is None else bool(annualized)
        if not self.annualized:
            logger.info(
                "FCFAnalyzer 检测到输入为季度数据，FCF Yield 将按 × 4 年化"
            )

    def _detect_annualized(self) -> bool:
        """根据时间索引间隔判断输入是否已年化（年度/TTM）"""
        if self.df.empty:
            return True
        idx = self.df.index
        if not isinstance(idx, pd.DatetimeIndex) or len(idx) < 2:
            # 没有可解析的日期索引，按"已年化"的安全假设处理（保留旧行为）
            return True
        try:
            sorted_idx = idx.sort_values()
            deltas = sorted_idx.to_series().diff().dropna()
            median_days = deltas.dt.days.median()
            # < 200 天的中位间隔强烈暗示季度数据
            return bool(median_days >= 200)
        except Exception:
            return True

    def calculate_metrics(self) -> pd.DataFrame:
        if self.df.empty:
            return pd.DataFrame()

        # FCF = 经营现金流净额 - 资本支出
        self.df['fcf'] = self.df['operating_cash_flow'] - self.df['capex']

        # FCF利润率 = FCF / 营业收入 * 100%
        self.df['fcf_margin'] = (self.df['fcf'] / self.df['revenue'].replace(0, pd.NA)) * 100
        self.df['fcf_margin'] = self.df['fcf_margin'].fillna(0)

        # FCF vs 净利润比值
        self.df['fcf_net_profit_ratio'] = self.df['fcf'] / self.df['net_profit'].replace(0, pd.NA)
        self.df['fcf_net_profit_ratio'] = self.df['fcf_net_profit_ratio'].fillna(0)

        # 整理时间格式，按时间正序
        self.df = self.df.sort_index(ascending=True)

        return self.df

    def generate_scorecard(self) -> dict:
        """
        计算 5 项评分（各20分，满分100分）及结论。
        """
        if self.df.empty:
            return {}

        latest = self.df.iloc[-1]
        fcf = latest['fcf']
        net_profit = latest['net_profit']
        fcf_margin = latest['fcf_margin']

        # 1. FCF 绝对值水平 (20)
        score_absolute = 20 if fcf > 0 else 0

        # 2. FCF vs 净利润质量 (20)
        if fcf > net_profit and fcf > 0:
            score_quality = 20
        elif 0 < fcf <= net_profit:
            score_quality = 10
        else:
            score_quality = 0

        # 3. FCF 利润率水平 (20)
        if fcf_margin >= 20:
            score_margin = 20
        elif fcf_margin >= 10:
            score_margin = 15
        elif fcf_margin >= 5:
            score_margin = 10
        elif fcf_margin > 0:
            score_margin = 5
        else:
            score_margin = 0

        # 4. FCF 增长趋势 (20)
        score_growth = 10 # 默认分
        if len(self.df) >= 2:
            prev_fcf = self.df.iloc[-2]['fcf']
            # B15 修复：prev_fcf 过小时（< 总资产 0.1% 的经验阈值，这里用绝对值 1e6 元 = 100 万兜底），
            # 增长率会爆炸（如 prev=1 → 增长 99 倍），评分失真。改为：
            #   - prev_fcf < 1e6（百万级以下，对上市公司基本是噪音）：用绝对差额而非比率判断
            #   - 其他场景按原逻辑
            MIN_PREV_FCF = 1e6  # 100 万人民币/美元，过滤掉数据噪音
            if prev_fcf > MIN_PREV_FCF:
                growth = (fcf - prev_fcf) / prev_fcf
                if growth > 0.10:
                    score_growth = 20
                elif growth > 0:
                    score_growth = 15
                else:
                    score_growth = 5
            elif prev_fcf > 0:
                # 微小正值的过去 FCF：只看当前是否显著正
                score_growth = 20 if fcf > MIN_PREV_FCF else (15 if fcf > prev_fcf else 5)
            else:
                # prev_fcf <= 0
                if fcf > 0:
                    score_growth = 20 # 扭亏为盈
                else:
                    score_growth = 0  # 持续为负

        # 5. FCF Yield 性价比 (20)
        # B4 修复：根据 self.annualized 自动决定是否 × 4 年化
        if self.market_cap > 0:
            annual_fcf = fcf if self.annualized else fcf * 4
            fcf_yield = (annual_fcf / self.market_cap) * 100
        else:
            fcf_yield = 0.0

        if fcf_yield >= 5:
            score_yield = 20
        elif fcf_yield >= 3:
            score_yield = 15
        elif fcf_yield >= 1:
            score_yield = 10
        elif fcf_yield > 0:
            score_yield = 5
        else:
            score_yield = 0

        total_score = score_absolute + score_quality + score_margin + score_growth + score_yield

        # 评级
        if total_score >= 80:
            rating = "优"
            judgement = "公司拥有卓越的自由现金流创造能力，是现金牛，值得从FCF角度重点关注。"
        elif total_score >= 60:
            rating = "良"
            judgement = "公司现金流状况良好，能够覆盖日常经营与资本开支，具备一定投资价值。"
        elif total_score >= 40:
            rating = "中"
            judgement = "公司自由现金流表现平庸，可能需要依赖外部融资，需结合业务增速进一步评估。"
        else:
            rating = "差"
            judgement = "公司存在严重的自由现金流缺口，需警惕财务风险和流动性危机。"

        # 寻找主要风险点 (得分最低的项)
        risk_map = {
            "自由现金流为负，主业造血能力不足": score_absolute,
            "净利润含金量低，纸面富贵": score_quality,
            "FCF利润率低下，经营效率或行业地位较弱": score_margin,
            "现金流出现恶化或持续衰退趋势": score_growth,
            "当前估值较高，FCF Yield 回报率缺乏吸引力": score_yield
        }
        main_risk = min(risk_map, key=risk_map.get) if total_score < 100 else "无明显FCF层面风险"

        self.scores = {
            "absolute": score_absolute,
            "quality": score_quality,
            "margin": score_margin,
            "growth": score_growth,
            "yield": score_yield,
            "total": total_score
        }

        self.summary = {
            "current_fcf_yield": fcf_yield,
            "rating": rating,
            "judgement": judgement,
            "main_risk": main_risk
        }

        return {
            "scores": self.scores,
            "summary": self.summary
        }
