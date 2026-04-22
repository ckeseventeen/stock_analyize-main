# 注意：matplotlib 字体和后端配置已在 main.py 入口处统一设置，此处不再重复
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from src.utils.logger import setup_logger

logger = setup_logger()


class Visualizer:
    def __init__(self, analysis_result, stock_config):
        self.result = analysis_result
        self.config = stock_config
        self.market_name = self.result.get("market_name", self.config.get("market_name", "A股"))
        self.stock_name = self.config.get("name", "未知股票")
        self.code = self.config.get("code", "未知代码")
        self.category_name = self.config.get("category_name", "未分类")

        self.val_type = self.config.get('valuation', 'pe').lower()
        if self.val_type == 'pe':
            self.val_range = self.config.get('pe_range', [0, 0, 0])
            self.current_val = self.result.get('current_pe', np.nan)
            self.val_name = 'PE'
            self.val_full_name = 'PE (TTM)'
        else:
            self.val_range = self.config.get('ps_range', [0, 0, 0])
            self.current_val = self.result.get('current_ps', np.nan)
            self.val_name = 'PS'
            self.val_full_name = 'PS (TTM)'

        self.scenarios = self.result.get('scenarios', [0, 0, 0])
        self.price = self.result.get('price', 0)

    def plot(self):
        logger.info(f"【{self.market_name}-{self.stock_name}】开始生成财务与估值分析图谱")

        fig, axes = plt.subplots(2, 2, figsize=(18, 13), dpi=100)
        fig.suptitle(
            f'[{self.category_name}] {self.stock_name} ({self.code}) 财务与估值分析图谱',
            fontsize=20, fontweight='bold', y=0.98
        )

        self._plot_revenue_profit(axes[0, 0])
        self._plot_hist_valuation(axes[0, 1])
        self._plot_scenario(axes[1, 0])
        self._plot_summary_table(axes[1, 1])

        plt.tight_layout(rect=[0, 0, 1, 0.95])
        logger.info(f"【{self.market_name}-{self.stock_name}】财务与估值分析图谱生成完成")
        return fig

    def _plot_revenue_profit(self, ax):
        """左上：近五年年度营收与净利润 + 毛利率双Y轴"""
        ax.set_title('左上：近五年年度营收与净利润', fontsize=13, fontweight='bold', color='#555555')
        annual_df = self.result.get('annual_df', None)

        if annual_df is None or annual_df.empty:
            ax.text(0.5, 0.5, '无年度财务数据', ha='center', va='center', fontsize=14, transform=ax.transAxes)
            return

        years = annual_df.index.year.astype(str).tolist()
        x = np.arange(len(years))
        width = 0.35

        # 营业总收入和归母净利润（转为亿元）
        revenue = annual_df.get('营业总收入', pd.Series(dtype=float))
        net_profit = annual_df.get('归母净利润', pd.Series(dtype=float))
        gross_margin = annual_df.get('毛利率', pd.Series(dtype=float))

        rev_vals = (revenue / 1e8).tolist() if not revenue.empty else [0] * len(years)
        np_vals = (net_profit / 1e8).tolist() if not net_profit.empty else [0] * len(years)
        gm_vals = (gross_margin * 100).tolist() if not gross_margin.empty else []

        bars1 = ax.bar(x - width / 2, rev_vals, width, label='营业总收入(亿元)', color='#7BA3CC', alpha=0.85)
        bars2 = ax.bar(x + width / 2, np_vals, width, label='归母净利润(亿元)', color='#A8D8A8', alpha=0.85)

        ax.set_ylabel('金额 (亿元)', fontsize=11)
        ax.set_xticks(x)
        ax.set_xticklabels(years, fontsize=11)
        ax.legend(loc='upper left', fontsize=9)
        ax.grid(axis='y', alpha=0.2)

        # 右侧Y轴：毛利率折线
        if gm_vals:
            ax2 = ax.twinx()
            ax2.plot(x, gm_vals, color='#E74C3C', marker='o', linewidth=2, markersize=6, label='毛利率(%)')
            ax2.set_ylabel('毛利率 (%)', fontsize=11)
            ax2.legend(loc='upper right', fontsize=9)

    def _plot_hist_valuation(self, ax):
        """右上：历史估值走势 + 当前值 + 50%中位线"""
        hist_percentile = self.result.get('hist_percentile', 0)
        ax.set_title(
            f'右上：历史 {self.val_name} 走势 (当前所处分位: {hist_percentile:.1f}%)',
            fontsize=13, fontweight='bold', color='#555555'
        )

        hist_val = self.result.get('hist_val', None)
        val_col = 'pe_ttm' if self.val_type == 'pe' else 'ps_ttm'

        if hist_val is None or hist_val.empty or val_col not in hist_val.columns:
            ax.text(0.5, 0.5, '无历史估值数据', ha='center', va='center', fontsize=14, transform=ax.transAxes)
            return

        hist_series = pd.to_numeric(hist_val[val_col], errors='coerce').dropna()
        if hist_series.empty:
            ax.text(0.5, 0.5, '无历史估值数据', ha='center', va='center', fontsize=14, transform=ax.transAxes)
            return

        ax.plot(hist_series.index, hist_series.values, color='#3B7DD8', linewidth=1.2)

        # 当前值横线
        if not np.isnan(self.current_val):
            ax.axhline(y=self.current_val, color='#E74C3C', linestyle='--', linewidth=1.8,
                        label=f'当前值: {self.current_val:.2f}')

        # 50%中位数线
        median_val = hist_series.median()
        ax.axhline(y=median_val, color='#888888', linestyle=':', linewidth=1.5,
                    label=f'50%中位: {median_val:.2f}')

        ax.set_ylabel(f'{self.val_full_name}', fontsize=11)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.tick_params(axis='x', rotation=30)
        ax.legend(loc='upper right', fontsize=9, framealpha=0.9)
        ax.grid(alpha=0.2)

    def _plot_scenario(self, ax):
        """左下：不同情景假设下的估值推演"""
        ax.set_title('左下：不同情景假设下的估值推演', fontsize=13, fontweight='bold', color='#555555')

        labels = [
            f'保守\n({self.val_name}={self.val_range[0]})',
            f'中性\n({self.val_name}={self.val_range[1]})',
            f'乐观\n({self.val_name}={self.val_range[2]})'
        ]
        colors = ['#5DAE8B', '#F0C75E', '#E8835A']

        # 检测配置是否合理：乐观目标价 < 当前股价*0.5 说明 PE 区间配置偏低
        max_scenario = max(self.scenarios) if self.scenarios else 0
        config_warning = False
        if self.price > 0 and max_scenario > 0 and max_scenario < self.price * 0.5:
            config_warning = True

        bars = ax.bar(labels, self.scenarios, color=colors, width=0.55, edgecolor='white', linewidth=1.5)

        # 当前股价横线
        ax.axhline(y=self.price, color='#333333', linestyle='--', linewidth=2,
                    label=f'当前股价: {self.price:.2f}元')

        # 柱顶标注
        for bar, val in zip(bars, self.scenarios):
            y_offset = max(max_scenario, self.price) * 0.015
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + y_offset,
                    f'{val:.2f}', ha='center', fontsize=12, fontweight='bold')

        ax.set_ylabel('推演目标股价 (元)', fontsize=11)
        ax.legend(fontsize=11, loc='upper left')
        ax.grid(axis='y', alpha=0.2)

        # 配置不合理警告
        if config_warning:
            ax.text(0.5, 0.92, f'⚠ {self.val_name}区间偏低，当前{self.val_name}={self.current_val:.1f}，建议调整配置',
                    ha='center', va='top', fontsize=9, color='#CC0000', style='italic',
                    transform=ax.transAxes,
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='#FFF3CD', alpha=0.9))

    def _plot_summary_table(self, ax):
        """右下：核心指标综合总览表格"""
        ax.set_title('右下：核心指标综合总览', fontsize=13, fontweight='bold', color='#555555')
        ax.axis('off')

        annual_df = self.result.get('annual_df', None)
        hist_percentile = self.result.get('hist_percentile', 0)
        ttm_revenue = self.result.get('ttm_revenue', 0)
        ttm_net_profit = self.result.get('ttm_net_profit', 0)

        # 最新年报年度
        if annual_df is not None and not annual_df.empty:
            latest_year = str(annual_df.index[-1].year)
            latest_annual = annual_df.iloc[-1]
            rev_val = latest_annual.get('营业总收入', 0) / 1e8
            np_val = latest_annual.get('归母净利润', 0) / 1e8
            gm_val = latest_annual.get('毛利率', 0) * 100
        else:
            latest_year = '-'
            rev_val = ttm_revenue / 1e8
            np_val = ttm_net_profit / 1e8
            gm_val = 0

        table_data = [
            ['财报年度', latest_year, '最新年度数据依据'],
            ['营业总收入 (亿元)', f'{rev_val:.2f}', '年度总计'],
            ['归母净利润 (亿元)', f'{np_val:.2f}', '年度总计'],
            ['毛利率 (%)', f'{gm_val:.2f}%', '(营业收入-营业成本)/营业收入'],
            ['当前股价 (元)', f'{self.price:.2f}', '实时动态行情'],
            [f'当前 {self.val_full_name}', f'{self.current_val:.2f}', '基于最新滚动四个季度'],
            [f'历史 {self.val_name} 分位数', f'{hist_percentile:.1f}%', '处于过去历史排位'],
        ]

        col_labels = ['关键指标', '最新数据', '备注解析']

        table = ax.table(
            cellText=table_data,
            colLabels=col_labels,
            loc='center',
            cellLoc='center',
            colWidths=[0.32, 0.22, 0.46]
        )

        table.auto_set_font_size(False)
        table.set_fontsize(11)
        table.scale(1.0, 1.8)

        # 表头样式
        for j in range(len(col_labels)):
            cell = table[0, j]
            cell.set_facecolor('#4A4A4A')
            cell.set_text_props(color='white', fontweight='bold')

        # 数据行交替色
        for i in range(1, len(table_data) + 1):
            for j in range(len(col_labels)):
                cell = table[i, j]
                cell.set_facecolor('#F9F9F9' if i % 2 == 1 else '#EEEEEE')
                cell.set_edgecolor('#CCCCCC')
