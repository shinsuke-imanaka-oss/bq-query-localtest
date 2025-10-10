"""
アクション提案自動生成モジュール
比較分析結果に基づき、優先度付きの具体的施策を提案
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from enum import Enum


class Priority(Enum):
    """優先度"""
    HIGH = "高"
    MEDIUM = "中"
    LOW = "低"


@dataclass
class ActionItem:
    """アクション提案項目"""
    title: str
    description: str
    priority: Priority
    expected_impact: str
    validation_method: str
    category: str  # 予算最適化、ターゲティング改善、クリエイティブ改善など


@dataclass
class ActionRecommendations:
    """アクション提案セット"""
    actions: List[ActionItem]
    summary: str
    high_priority_count: int
    medium_priority_count: int
    low_priority_count: int


class ActionRecommender:
    """アクション提案生成クラス"""
    
    # インパクト閾値
    HIGH_IMPACT_THRESHOLD = 0.20  # 20%以上
    MEDIUM_IMPACT_THRESHOLD = 0.10  # 10-20%
    
    def __init__(self):
        pass
    
    def generate_recommendations(
        self,
        comparative_analysis: Any,  # ComparativeAnalysis
        overall_metrics: Optional[Dict[str, Any]] = None
    ) -> ActionRecommendations:
        """
        比較分析結果に基づきアクション提案を生成
        
        Args:
            comparative_analysis: 比較分析結果
            overall_metrics: 全体指標（オプション）
            
        Returns:
            ActionRecommendations: アクション提案セット
        """
        if comparative_analysis.skipped:
            return ActionRecommendations(
                actions=[],
                summary="キャンペーン数が不足しているため、アクション提案を生成できません。",
                high_priority_count=0,
                medium_priority_count=0,
                low_priority_count=0
            )
        
        actions = []
        
        # 有意な差異に基づいてアクションを生成
        for diff in comparative_analysis.significant_differences:
            action = self._generate_action_from_difference(diff)
            if action:
                actions.append(action)
        
        # 全体指標に基づく追加提案
        if overall_metrics:
            additional_actions = self._generate_overall_actions(overall_metrics)
            actions.extend(additional_actions)
        
        # 優先度でソート
        actions.sort(key=lambda x: (
            0 if x.priority == Priority.HIGH else 
            1 if x.priority == Priority.MEDIUM else 2
        ))
        
        # カウント
        high_count = sum(1 for a in actions if a.priority == Priority.HIGH)
        medium_count = sum(1 for a in actions if a.priority == Priority.MEDIUM)
        low_count = sum(1 for a in actions if a.priority == Priority.LOW)
        
        # サマリー生成
        summary = self._generate_summary(actions, high_count, medium_count, low_count)
        
        return ActionRecommendations(
            actions=actions,
            summary=summary,
            high_priority_count=high_count,
            medium_priority_count=medium_count,
            low_priority_count=low_count
        )
    
    def _generate_action_from_difference(
        self, 
        difference: Any  # MetricComparison
    ) -> Optional[ActionItem]:
        """指標の差異からアクション提案を生成"""
        metric_name = difference.metric_name
        diff_pct = abs(difference.difference_pct)
        
        # 優先度を決定
        if diff_pct >= self.HIGH_IMPACT_THRESHOLD * 100:
            priority = Priority.HIGH
            expected_impact = f"{diff_pct:.0f}%の改善が見込まれます"
        elif diff_pct >= self.MEDIUM_IMPACT_THRESHOLD * 100:
            priority = Priority.MEDIUM
            expected_impact = f"{diff_pct:.0f}%の改善が見込まれます"
        else:
            priority = Priority.LOW
            expected_impact = f"{diff_pct:.0f}%の改善が見込まれます"
        
        # 指標ごとのアクション提案マッピング
        action_mapping = {
            'CPA': ActionItem(
                title="低パフォーマンスキャンペーンのCPA改善",
                description=f"高パフォーマンスキャンペーンと比較してCPAが{diff_pct:.1f}%高くなっています。ターゲティングの見直し、入札戦略の最適化、品質スコアの向上を検討してください。",
                priority=priority,
                expected_impact=expected_impact,
                validation_method="2週間後のCPA推移をモニタリング",
                category="予算最適化"
            ),
            'ROAS': ActionItem(
                title="ROASの向上施策",
                description=f"高パフォーマンスキャンペーンと比較してROASが{diff_pct:.1f}%低くなっています。コンバージョン価値の高いターゲット層への注力、除外キーワードの設定を検討してください。",
                priority=priority,
                expected_impact=expected_impact,
                validation_method="1ヶ月後のROAS推移を測定",
                category="収益最適化"
            ),
            'コンバージョン率': ActionItem(
                title="コンバージョン率改善",
                description=f"高パフォーマンスキャンペーンと比較してコンバージョン率が{diff_pct:.1f}%低くなっています。ランディングページの最適化、広告文とLPの整合性向上を検討してください。",
                priority=priority,
                expected_impact=expected_impact,
                validation_method="A/Bテストによる効果測定（2週間）",
                category="クリエイティブ改善"
            ),
            'クリック率': ActionItem(
                title="クリック率向上施策",
                description=f"高パフォーマンスキャンペーンと比較してクリック率が{diff_pct:.1f}%低くなっています。広告文の見直し、訴求ポイントの強化、広告表示オプションの追加を検討してください。",
                priority=priority,
                expected_impact=expected_impact,
                validation_method="1週間ごとのCTR推移をモニタリング",
                category="クリエイティブ改善"
            ),
            '平均費用': ActionItem(
                title="予算配分の最適化",
                description=f"キャンペーン間で平均費用に{diff_pct:.1f}%の差があります。高パフォーマンスキャンペーンへの予算シフト、低パフォーマンスキャンペーンの予算削減を検討してください。",
                priority=priority,
                expected_impact=expected_impact,
                validation_method="予算変更後の全体ROASを比較",
                category="予算最適化"
            ),
        }
        
        return action_mapping.get(metric_name)
    
    def _generate_overall_actions(
        self, 
        overall_metrics: Dict[str, Any]
    ) -> List[ActionItem]:
        """全体指標に基づく追加アクション提案"""
        actions = []
        
        # 予算執行率に基づく提案
        if 'budget_usage_pct' in overall_metrics:
            usage_pct = overall_metrics['budget_usage_pct']
            if usage_pct < 50:
                actions.append(ActionItem(
                    title="予算執行率の改善",
                    description=f"現在の予算執行率は{usage_pct:.1f}%です。入札単価の見直し、配信量の拡大、新規キャンペーンの追加を検討してください。",
                    priority=Priority.MEDIUM,
                    expected_impact="予算の有効活用により機会損失を削減",
                    validation_method="週次で予算執行率を確認",
                    category="予算最適化"
                ))
            elif usage_pct > 90:
                actions.append(ActionItem(
                    title="予算超過リスクの管理",
                    description=f"現在の予算執行率は{usage_pct:.1f}%です。予算の追加確保、または低パフォーマンスキャンペーンの停止を検討してください。",
                    priority=Priority.HIGH,
                    expected_impact="予算超過を防ぎつつ効率的な配信を維持",
                    validation_method="日次で予算残高を確認",
                    category="予算最適化"
                ))
        
        # 全体ROASに基づく提案
        if 'overall_roas' in overall_metrics:
            roas = overall_metrics['overall_roas']
            target_roas = overall_metrics.get('target_roas', 0)
            if target_roas > 0 and roas < target_roas:
                gap_pct = ((target_roas - roas) / target_roas) * 100
                actions.append(ActionItem(
                    title="目標ROAS達成のための総合施策",
                    description=f"現在のROASは目標を{gap_pct:.1f}%下回っています。高ROAS商品へのフォーカス、除外設定の強化、自動入札戦略の見直しを包括的に実施してください。",
                    priority=Priority.HIGH if gap_pct > 20 else Priority.MEDIUM,
                    expected_impact=f"ROAS {gap_pct:.1f}%改善を目指す",
                    validation_method="月次でROAS推移を評価",
                    category="収益最適化"
                ))
        
        return actions
    
    def _generate_summary(
        self,
        actions: List[ActionItem],
        high_count: int,
        medium_count: int,
        low_count: int
    ) -> str:
        """アクション提案のサマリーを生成"""
        if not actions:
            return "現時点で提案できる具体的なアクションはありません。"
        
        summary = f"""分析結果に基づき、{len(actions)}件のアクション提案を生成しました。

- 高優先度: {high_count}件（即座に実施を推奨）
- 中優先度: {medium_count}件（2週間以内の実施を推奨）
- 低優先度: {low_count}件（状況に応じて実施を検討）

優先度の高いアクションから順次実施し、効果を測定しながら次のアクションに進むことを推奨します。"""
        
        return summary
    
    def generate_ai_prompt(
        self, 
        recommendations: ActionRecommendations,
        comparative_analysis: Any
    ) -> str:
        """AI分析用のプロンプトを生成"""
        if not recommendations.actions:
            return ""
        
        prompt = f"""以下のアクション提案について、実施の際の注意点と期待される相乗効果を分析してください。

## 提案されたアクション
"""
        
        for i, action in enumerate(recommendations.actions[:5], 1):  # 上位5件
            prompt += f"""
{i}. {action.title}（優先度: {action.priority.value}）
   - 内容: {action.description}
   - 期待効果: {action.expected_impact}
   - カテゴリ: {action.category}
"""
        
        prompt += """
以下の観点で分析してください：

1. **実施順序の推奨**（どのアクションから始めるべきか）
2. **相乗効果が期待できる組み合わせ**
3. **実施時の注意点**（リスクや考慮事項）
4. **短期・中期・長期の効果見込み**

※実務的な観点から、実行可能性を重視した分析をお願いします。
"""
        
        return prompt


def create_action_recommender() -> ActionRecommender:
    """ActionRecommenderのファクトリー関数"""
    return ActionRecommender()