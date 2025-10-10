"""
キャンペーンパフォーマンス比較分析モジュール
高パフォーマンス vs 低パフォーマンスキャンペーンの構造的な差異を分析
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple
import statistics


@dataclass
class CampaignMetrics:
    """キャンペーンの主要指標"""
    campaign_name: str
    cpa: float
    roas: float
    conversion_rate: float
    click_rate: float
    impressions: int
    clicks: int
    conversions: int
    cost: float


@dataclass
class MetricComparison:
    """指標の比較結果"""
    metric_name: str
    high_perf_avg: float
    low_perf_avg: float
    difference_pct: float  # 相対差異（%）
    is_significant: bool  # 20%以上の差異があるか


@dataclass
class ComparativeAnalysis:
    """比較分析結果"""
    high_performers: List[CampaignMetrics]
    low_performers: List[CampaignMetrics]
    significant_differences: List[MetricComparison]
    analysis_summary: Dict[str, Any]
    skipped: bool = False
    skip_reason: Optional[str] = None


class ComparativeAnalyzer:
    """キャンペーンパフォーマンス比較分析クラス"""
    
    def __init__(self, min_campaigns_per_group: int = 3):
        """
        Args:
            min_campaigns_per_group: 各グループに必要な最低キャンペーン数
        """
        self.min_campaigns_per_group = min_campaigns_per_group
        self.significance_threshold = 0.20  # 20%の差異で有意とする
    
    def analyze(self, campaigns_data: List[Dict[str, Any]]) -> ComparativeAnalysis:
        """
        キャンペーンデータを分析し、高/低パフォーマンスグループを比較
        
        Args:
            campaigns_data: キャンペーンデータのリスト
            
        Returns:
            ComparativeAnalysis: 比較分析結果
        """
        # データ検証
        if len(campaigns_data) < self.min_campaigns_per_group * 2:
            return ComparativeAnalysis(
                high_performers=[],
                low_performers=[],
                significant_differences=[],
                analysis_summary={},
                skipped=True,
                skip_reason=f"キャンペーン数が不足しています（必要: {self.min_campaigns_per_group * 2}以上、実際: {len(campaigns_data)}）"
            )
        
        # CPA基準でソート（CPAが低い = 高パフォーマンス）
        valid_campaigns = [c for c in campaigns_data if c.get('cpa', 0) > 0]
        if len(valid_campaigns) < self.min_campaigns_per_group * 2:
            return ComparativeAnalysis(
                high_performers=[],
                low_performers=[],
                significant_differences=[],
                analysis_summary={},
                skipped=True,
                skip_reason=f"有効なCPAを持つキャンペーン数が不足しています"
            )
        
        sorted_campaigns = sorted(valid_campaigns, key=lambda x: x.get('cpa', float('inf')))
        
        # 上位20%と下位20%を抽出
        total_count = len(sorted_campaigns)
        top_20_pct_count = max(self.min_campaigns_per_group, int(total_count * 0.2))
        bottom_20_pct_count = max(self.min_campaigns_per_group, int(total_count * 0.2))
        
        high_perf_campaigns = sorted_campaigns[:top_20_pct_count]
        low_perf_campaigns = sorted_campaigns[-bottom_20_pct_count:]
        
        # メトリクスオブジェクトに変換
        high_performers = [self._to_campaign_metrics(c) for c in high_perf_campaigns]
        low_performers = [self._to_campaign_metrics(c) for c in low_perf_campaigns]
        
        # 指標を比較
        comparisons = self._compare_metrics(high_performers, low_performers)
        
        # 有意な差異のみ抽出
        significant_differences = [c for c in comparisons if c.is_significant]
        
        # サマリー生成
        analysis_summary = self._generate_summary(
            high_performers, low_performers, significant_differences
        )
        
        return ComparativeAnalysis(
            high_performers=high_performers,
            low_performers=low_performers,
            significant_differences=significant_differences,
            analysis_summary=analysis_summary,
            skipped=False
        )
    
    def _to_campaign_metrics(self, campaign_data: Dict[str, Any]) -> CampaignMetrics:
        """キャンペーンデータをCampaignMetricsオブジェクトに変換"""
        return CampaignMetrics(
            campaign_name=campaign_data.get('campaign_name', 'Unknown'),
            cpa=campaign_data.get('cpa', 0.0),
            roas=campaign_data.get('roas', 0.0),
            conversion_rate=campaign_data.get('conversion_rate', 0.0),
            click_rate=campaign_data.get('click_rate', 0.0),
            impressions=campaign_data.get('impressions', 0),
            clicks=campaign_data.get('clicks', 0),
            conversions=campaign_data.get('conversions', 0),
            cost=campaign_data.get('cost', 0.0)
        )
    
    def _compare_metrics(
        self, 
        high_performers: List[CampaignMetrics], 
        low_performers: List[CampaignMetrics]
    ) -> List[MetricComparison]:
        """各指標を比較し、相対差異を計算"""
        comparisons = []
        
        # 比較する指標の定義
        metrics_to_compare = [
            ('CPA', 'cpa', False),  # 低い方が良い
            ('ROAS', 'roas', True),  # 高い方が良い
            ('コンバージョン率', 'conversion_rate', True),
            ('クリック率', 'click_rate', True),
            ('平均費用', 'cost', False),
            ('平均コンバージョン数', 'conversions', True),
            ('平均クリック数', 'clicks', True),
        ]
        
        for display_name, attr_name, higher_is_better in metrics_to_compare:
            high_values = [getattr(c, attr_name) for c in high_performers]
            low_values = [getattr(c, attr_name) for c in low_performers]
            
            # 平均値を計算
            high_avg = statistics.mean(high_values) if high_values else 0
            low_avg = statistics.mean(low_values) if low_values else 0
            
            # 相対差異を計算（ゼロ除算を回避）
            if low_avg != 0:
                if higher_is_better:
                    # 高い方が良い指標: (高 - 低) / 低
                    diff_pct = ((high_avg - low_avg) / abs(low_avg)) * 100
                else:
                    # 低い方が良い指標: (低 - 高) / 低
                    diff_pct = ((low_avg - high_avg) / abs(low_avg)) * 100
            else:
                diff_pct = 0.0
            
            is_significant = abs(diff_pct) >= (self.significance_threshold * 100)
            
            comparisons.append(MetricComparison(
                metric_name=display_name,
                high_perf_avg=high_avg,
                low_perf_avg=low_avg,
                difference_pct=diff_pct,
                is_significant=is_significant
            ))
        
        return comparisons
    
    def _generate_summary(
        self,
        high_performers: List[CampaignMetrics],
        low_performers: List[CampaignMetrics],
        significant_differences: List[MetricComparison]
    ) -> Dict[str, Any]:
        """分析サマリーを生成"""
        return {
            'high_performer_count': len(high_performers),
            'low_performer_count': len(low_performers),
            'significant_difference_count': len(significant_differences),
            'high_perf_avg_cpa': statistics.mean([c.cpa for c in high_performers]),
            'low_perf_avg_cpa': statistics.mean([c.cpa for c in low_performers]),
            'high_perf_avg_roas': statistics.mean([c.roas for c in high_performers]),
            'low_perf_avg_roas': statistics.mean([c.roas for c in low_performers]),
            'most_significant_differences': sorted(
                significant_differences, 
                key=lambda x: abs(x.difference_pct), 
                reverse=True
            )[:5]  # 上位5つの差異
        }
    
    def generate_ai_prompt(self, analysis: ComparativeAnalysis) -> str:
        """AI分析用のプロンプトを生成"""
        if analysis.skipped:
            return ""
        
        prompt = f"""以下のキャンペーン比較分析結果に基づき、高パフォーマンスと低パフォーマンスキャンペーンの差異要因を分析してください。

## 高パフォーマンスキャンペーン（CPA上位20%）
- キャンペーン数: {len(analysis.high_performers)}
- 平均CPA: ¥{analysis.analysis_summary['high_perf_avg_cpa']:,.0f}
- 平均ROAS: {analysis.analysis_summary['high_perf_avg_roas']:.2f}

## 低パフォーマンスキャンペーン（CPA下位20%）
- キャンペーン数: {len(analysis.low_performers)}
- 平均CPA: ¥{analysis.analysis_summary['low_perf_avg_cpa']:,.0f}
- 平均ROAS: {analysis.analysis_summary['low_perf_avg_roas']:.2f}

## 有意な差異（20%以上）
"""
        
        for diff in analysis.significant_differences:
            prompt += f"""
- {diff.metric_name}:
  * 高パフォーマンス: {diff.high_perf_avg:,.2f}
  * 低パフォーマンス: {diff.low_perf_avg:,.2f}
  * 差異: {diff.difference_pct:+.1f}%
"""
        
        prompt += """
以下の形式で分析してください：

1. **主要な差異要因**（最も影響が大きい3つ）
2. **なぜこの差が生まれているのか**（構造的・戦略的な観点から）
3. **低パフォーマンスキャンペーンが改善すべき点**（具体的に）

※事実に基づき、データから確実に言えることのみを記載してください。
"""
        
        return prompt


def create_comparative_analyzer(min_campaigns: int = 3) -> ComparativeAnalyzer:
    """ComparativeAnalyzerのファクトリー関数"""
    return ComparativeAnalyzer(min_campaigns_per_group=min_campaigns)