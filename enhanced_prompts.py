# enhanced_prompts.py
"""
高度化されたプロンプトシステム
- Gemini用SQL生成プロンプトの強化
- Claude用分析・洞察プロンプトの高度化
- コンテキスト認識の向上
- 業界知識の組み込み
"""

import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

# =========================================================================
# 高度化されたSQL生成プロンプト (Gemini用)
# =========================================================================

ENHANCED_PROMPT_DEFINITIONS = {
    "campaign": {
        "description": "キャンペーン分析 - 包括的なパフォーマンス評価",
        "template": """
# あなたは10年以上の経験を持つデジタルマーケティング分析の専門家です。

## 分析依頼
{user_input}

## データソース
テーブル: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`

## 利用可能な列
### 基本指標
- Date: 日付 (YYYY-MM-DD形式)
- Impressions: インプレッション数
- Clicks: クリック数  
- Conversions: コンバージョン数
- AllConversions: 全コンバージョン数

### コスト指標
- Cost: 基本コスト
- CostIncludingFees: 手数料込みコスト (分析には常にこちらを使用)

### メディア・キャンペーン情報
- ServiceNameJA_Media: メディア名 (Google広告、Facebook広告など)
- ServiceNameJA: サービス名
- AccountName: アカウント名
- CampaignName: キャンペーン名
- PromotionName: プロモーション名

### その他
- DayOfWeekJA: 曜日 (月、火、水...)
- VideoViews: 動画視聴数
- ConversionValue: コンバージョン価値
- AllConversionValue: 全コンバージョン価値

## 必須KPI計算式
以下のKPIは必ず正確な式で計算してください:
- **CTR** = Clicks / Impressions * 100 (%)
- **CPA** = CostIncludingFees / Conversions (円)
- **CPC** = CostIncludingFees / Clicks (円)  
- **CVR** = Conversions / Clicks * 100 (%)
- **ROAS** = ConversionValue / CostIncludingFees

## 分析の観点
### パフォーマンス評価基準
- CTR: 1-3%が一般的、5%以上は優秀
- CPA: 業界により異なるが、目標値との比較が重要
- CVR: 1-5%が一般的、用途により大きく変動
- ROAS: 2.0以上が望ましい (200%以上の投資回収)

### 注意すべきデータ品質
- Impressions = 0 の場合、CTR計算不可
- Clicks = 0 の場合、CVR・CPA計算不可  
- 週末と平日のパフォーマンス差を考慮
- メディア別の特性差を考慮

## SQL出力要件
1. **効率的なクエリ**: 必要な列のみ選択
2. **適切なソート**: パフォーマンス順または時系列順
3. **NULL値処理**: SAFE_DIVIDE()を使用
4. **見やすい結果**: 適切な列名・順序
5. **実用的な行数**: 通常10-50行程度に制限

## 出力形式
実行可能なBigQuery SQLのみを返してください。説明や```sql```は不要です。

{context_info}
""",
        "context_enhancement": True
    },
    
    "time_series": {
        "description": "時系列分析 - トレンドと季節性の解析",
        "template": """
# あなたは時系列データ分析のエキスパートです。

## 分析依頼
{user_input}

## 専門知識の適用
### デジタルマーケティングの時系列パターン
- **月曜効果**: 週初めはパフォーマンスが低い傾向
- **週末パターン**: BtoC商材は土日が高い、BtoBは平日が高い
- **月末月初効果**: 給与日前後でユーザー行動が変化
- **季節性**: 年末年始、GW、夏季休暇での変動
- **イベント効果**: セール期間、新商品発売の影響

### 推奨分析手法
- 週別・月別での集計とトレンド分析
- 曜日別パフォーマンス比較
- 前年同期比、前期比の計算
- 移動平均によるノイズ除去

## データソース
テーブル: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`

## 時系列分析のベストプラクティス
1. **適切な期間設定**: 季節性を考慮した期間選択
2. **異常値の扱い**: 外れ値の影響を考慮
3. **欠損日の処理**: 祝日・システム停止日の考慮
4. **比較基準**: 前期比、前年同期比など

## 出力形式
実行可能なBigQuery SQLのみを返してください。

{context_info}
"""
    },
    
    "advanced_kpi": {
        "description": "高度KPI分析 - 複合指標と効率性評価",
        "template": """
# あなたは高度なマーケティング効率分析の専門家です。

## 分析依頼  
{user_input}

## 高度KPI計算式
以下の高度な指標も考慮してください:

### 効率性指標
- **Cost Per Mille (CPM)** = CostIncludingFees / Impressions * 1000
- **Engagement Rate** = (Clicks + VideoViews) / Impressions * 100
- **Value Per Click** = ConversionValue / Clicks
- **Profit Margin** = (ConversionValue - CostIncludingFees) / ConversionValue * 100

### パフォーマンス指標
- **Quality Score推定** = CTR * CVR * 100 (簡易版)
- **Efficiency Index** = (Conversions * ConversionValue) / CostIncludingFees
- **Market Share** = 各キャンペーンのImpressions / 総Impressions * 100

### リスク指標
- **Cost Concentration** = 上位20%キャンペーンのコスト割合
- **Performance Volatility** = 日別パフォーマンスの標準偏差

## 業界ベンチマーク
- E-commerce平均CTR: 2.0%、CVR: 2.5%
- BtoB平均CTR: 1.5%、CVR: 3.0%  
- アプリ系平均CTR: 2.5%、CVR: 8.0%

## データソース
テーブル: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`

## 出力形式
実行可能なBigQuery SQLのみを返してください。

{context_info}
"""
    }
}

# =========================================================================
# 高度化された分析コメントプロンプト (Claude用)
# =========================================================================

ENHANCED_CLAUDE_PROMPT_TEMPLATE = """
あなたは10年以上の経験を持つデジタルマーケティング分析の専門家です。
以下のデータに基づいて、戦略的な洞察と具体的なアクションプランを提供してください。

## データ分析結果
{data_sample}

## 可視化設定
{chart_settings}

## 分析観点
### 1. パフォーマンス評価
- 業界ベンチマークとの比較
- KPIの相互関係分析
- トレンドの方向性と持続可能性

### 2. 問題点の特定
- パフォーマンス低下の要因
- 機会損失の可能性
- リソース配分の効率性

### 3. 改善提案
- 具体的な最適化施策
- 予算再配分の提案
- A/Bテストの提案

## 出力形式
以下の構造で回答してください:

### 📊 **主要な発見**
（最も重要な3つのポイント）

### 🎯 **戦略的示唆**
（ビジネス戦略への影響）

### 🚀 **具体的なアクション**
（すぐに実行できる改善策）

### ⚠️ **注意点**
（リスクや考慮事項）

{context_enhancement}
"""

CLAUDE_INSIGHT_PROMPT_TEMPLATE = """
あなたは戦略コンサルタントレベルの分析力を持つマーケティング専門家です。
以下のデータから、経営層が求める洞察を導き出してください。

## データ概要
{data_summary}

## 分析コンテキスト
{analysis_context}

## 求められる洞察レベル
### C-level向け洞察
- ROI・収益性への影響
- 競合優位性の評価
- 成長機会の特定
- リスク要因の洗い出し

### マネージャー向けアクション
- チーム・リソースの最適化
- 予算配分の見直し
- KPI改善の具体策
- 優先順位の明確化

### オペレーター向け実行策
- 日々の運用改善点
- 設定変更の提案
- 監視すべき指標
- 緊急対応が必要な項目

## 出力要件
- 根拠に基づく具体的な数値
- 実行可能性の高い提案
- 期待される効果の定量化
- タイムラインの明示

{industry_knowledge}
"""

# =========================================================================
# コンテキスト強化機能
# =========================================================================

class PromptContextEnhancer:
    """プロンプトのコンテキストを強化するクラス"""
    
    def __init__(self):
        self.analysis_history = []
        self.data_characteristics = {}
        self.user_preferences = {}
    
    def enhance_sql_prompt(self, base_prompt: str, user_input: str, context: Dict[str, Any] = None) -> str:
        """SQL生成プロンプトのコンテキスト強化"""
        context = context or {}
        
        # コンテキスト情報の構築
        context_info = self._build_sql_context(user_input, context)
        
        # プロンプトテンプレートの選択と強化
        enhanced_prompt = base_prompt.format(
            user_input=user_input,
            context_info=context_info
        )
        
        return enhanced_prompt
    
    def enhance_claude_prompt(self, base_prompt: str, data_sample: str, chart_settings: str, context: Dict[str, Any] = None) -> str:
        """Claude分析プロンプトのコンテキスト強化"""
        context = context or {}
        
        # 分析履歴の活用
        context_enhancement = self._build_claude_context(context)
        
        # 業界知識の追加
        industry_knowledge = self._get_industry_insights(context)
        
        enhanced_prompt = base_prompt.format(
            data_sample=data_sample,
            chart_settings=chart_settings,
            context_enhancement=context_enhancement
        )
        
        # 高度な分析プロンプトの場合
        if "insight" in base_prompt.lower():
            enhanced_prompt = CLAUDE_INSIGHT_PROMPT_TEMPLATE.format(
                data_summary=data_sample[:500] + "...",
                analysis_context=context_enhancement,
                industry_knowledge=industry_knowledge
            )
        
        return enhanced_prompt
    
    def _build_sql_context(self, user_input: str, context: Dict[str, Any]) -> str:
        """SQL生成用のコンテキスト情報構築"""
        context_parts = []
        
        # 過去の分析パターンから学習
        if self.analysis_history:
            recent_patterns = self._analyze_recent_patterns()
            if recent_patterns:
                context_parts.append(f"## 最近の分析パターン\n{recent_patterns}")
        
        # データ特性の考慮
        if context.get("data_period"):
            context_parts.append(f"## 分析期間\n{context['data_period']}")
        
        # ユーザーの意図分析
        intent = self._analyze_user_intent(user_input)
        if intent:
            context_parts.append(f"## 分析意図\n{intent}")
        
        # パフォーマンス最適化のヒント
        optimization_tips = self._get_optimization_tips(user_input)
        if optimization_tips:
            context_parts.append(f"## 最適化のポイント\n{optimization_tips}")
        
        return "\n\n".join(context_parts) if context_parts else ""
    
    def _build_claude_context(self, context: Dict[str, Any]) -> str:
        """Claude分析用のコンテキスト情報構築"""
        context_parts = []
        
        # 分析の背景・目的
        if context.get("analysis_goal"):
            context_parts.append(f"### 分析目的\n{context['analysis_goal']}")
        
        # 過去の分析結果との関連
        if self.analysis_history:
            previous_insights = self._extract_previous_insights()
            if previous_insights:
                context_parts.append(f"### 過去の分析結果\n{previous_insights}")
        
        # データの特徴・制約
        data_constraints = self._identify_data_constraints(context)
        if data_constraints:
            context_parts.append(f"### データの特徴・制約\n{data_constraints}")
        
        # ビジネスコンテキスト
        business_context = self._get_business_context(context)
        if business_context:
            context_parts.append(f"### ビジネスコンテキスト\n{business_context}")
        
        return "\n\n".join(context_parts) if context_parts else ""
    
    def _analyze_user_intent(self, user_input: str) -> str:
        """ユーザー入力から分析意図を推定"""
        user_lower = user_input.lower()
        
        # 比較分析の意図
        if any(word in user_lower for word in ["比較", "対比", "違い", "差", "vs"]):
            return "比較分析: 複数の要素を比較して相対的なパフォーマンスを評価"
        
        # トレンド分析の意図
        elif any(word in user_lower for word in ["推移", "変化", "トレンド", "傾向", "時系列"]):
            return "トレンド分析: 時間経過に伴う変化パターンの把握"
        
        # 原因分析の意図
        elif any(word in user_lower for word in ["原因", "要因", "理由", "なぜ", "影響"]):
            return "原因分析: パフォーマンス変動の背景要因の特定"
        
        # 最適化の意図
        elif any(word in user_lower for word in ["改善", "最適化", "効率", "向上", "削減"]):
            return "最適化分析: パフォーマンス改善のための施策検討"
        
        # 探索的分析の意図
        elif any(word in user_lower for word in ["全体", "概要", "サマリー", "把握"]):
            return "探索的分析: データ全体の概観把握とパターン発見"
        
        return ""
    
    def _get_optimization_tips(self, user_input: str) -> str:
        """SQLパフォーマンス最適化のヒント"""
        tips = []
        
        user_lower = user_input.lower()
        
        # 大量データの処理が予想される場合
        if any(word in user_lower for word in ["全て", "全部", "すべて", "全期間"]):
            tips.append("• 大量データの場合はLIMIT句で結果を制限することを検討")
            tips.append("• 必要に応じてWHERE句で期間を絞り込み")
        
        # 集計処理の場合
        if any(word in user_lower for word in ["合計", "平均", "集計", "グループ"]):
            tips.append("• GROUP BY使用時は適切なインデックスを活用")
            tips.append("• HAVING句よりWHERE句での事前フィルタリングを優先")
        
        # 複雑な分析の場合
        if any(word in user_lower for word in ["詳細", "複雑", "多角的", "相関"]):
            tips.append("• 複雑な分析は段階的にクエリを構築")
            tips.append("• サブクエリよりもWITH句の使用を推奨")
        
        return "\n".join(tips) if tips else ""
    
    def _get_industry_insights(self, context: Dict[str, Any]) -> str:
        """業界固有の知識・インサイト"""
        insights = []
        
        # デジタル広告業界の一般的パターン
        insights.append("### 業界ベンチマーク")
        insights.append("- 検索広告CTR: 2-5%、ディスプレイ広告CTR: 0.5-1%")
        insights.append("- EC業界CVR: 1-3%、BtoBサービス: 2-5%")
        insights.append("- ROAS目標値: 4.0以上（400%）が理想的")
        
        insights.append("### 季節性パターン")
        insights.append("- Q4（10-12月）: EC・小売業は最高パフォーマンス期")
        insights.append("- 8月・1月: 多くの業界で活動が低下")
        insights.append("- 月末・月初: BtoB商材でコンバージョン率が向上")
        
        insights.append("### 最適化のベストプラクティス")
        insights.append("- CPA最適化: CVRよりもCPCの改善が効果的")
        insights.append("- ROAS改善: ターゲティング精度 > 入札戦略")
        insights.append("- 予算配分: 80-20法則（上位20%キャンペーンに注力）")
        
        return "\n".join(insights)
    
    def _analyze_recent_patterns(self) -> str:
        """最近の分析パターンを分析"""
        if len(self.analysis_history) < 2:
            return ""
        
        recent = self.analysis_history[-3:]  # 直近3回の分析
        patterns = []
        
        # よく使われる指標パターン
        common_metrics = self._extract_common_metrics(recent)
        if common_metrics:
            patterns.append(f"頻用指標: {', '.join(common_metrics)}")
        
        # 分析の焦点パターン
        focus_areas = self._extract_focus_areas(recent)
        if focus_areas:
            patterns.append(f"関心領域: {', '.join(focus_areas)}")
        
        return " | ".join(patterns) if patterns else ""
    
    def _extract_common_metrics(self, recent_analyses: List[Dict]) -> List[str]:
        """最近の分析でよく使われた指標を抽出"""
        metric_count = {}
        
        for analysis in recent_analyses:
            sql = analysis.get("sql", "").lower()
            
            # 指標の出現回数をカウント
            metrics = ["ctr", "cpa", "cpc", "cvr", "roas", "cost", "clicks", "conversions"]
            for metric in metrics:
                if metric in sql:
                    metric_count[metric] = metric_count.get(metric, 0) + 1
        
        # 2回以上使われた指標を返す
        return [metric.upper() for metric, count in metric_count.items() if count >= 2]
    
    def _extract_focus_areas(self, recent_analyses: List[Dict]) -> List[str]:
        """最近の分析の焦点領域を抽出"""
        focus_areas = []
        
        for analysis in recent_analyses:
            request = analysis.get("user_input", "").lower()
            
            if any(word in request for word in ["キャンペーン", "campaign"]):
                focus_areas.append("キャンペーン分析")
            if any(word in request for word in ["メディア", "media"]):
                focus_areas.append("メディア比較")
            if any(word in request for word in ["時系列", "推移", "トレンド"]):
                focus_areas.append("時系列分析")
            if any(word in request for word in ["デバイス", "device"]):
                focus_areas.append("デバイス分析")
        
        return list(set(focus_areas))  # 重複除去
    
    def _identify_data_constraints(self, context: Dict[str, Any]) -> str:
        """データの制約・特徴を特定"""
        constraints = []
        
        # データ期間の制約
        if context.get("date_range"):
            constraints.append(f"分析期間: {context['date_range']}")
        
        # データボリュームの制約
        if context.get("row_count"):
            row_count = context["row_count"]
            if row_count > 100000:
                constraints.append("大量データ: サンプリングまたは期間限定を推奨")
            elif row_count < 1000:
                constraints.append("少量データ: 統計的有意性に注意")
        
        # NULL値・品質の問題
        if context.get("data_quality_issues"):
            constraints.append("データ品質: 一部の指標でNULL値や異常値を検出")
        
        return " | ".join(constraints) if constraints else ""
    
    def _get_business_context(self, context: Dict[str, Any]) -> str:
        """ビジネスコンテキストの推定"""
        business_info = []
        
        # 業界・事業タイプの推定
        if context.get("campaign_types"):
            campaign_types = context["campaign_types"]
            if any("EC" in ct or "通販" in ct for ct in campaign_types):
                business_info.append("事業タイプ: Eコマース・通販系")
            elif any("BtoB" in ct or "法人" in ct for ct in campaign_types):
                business_info.append("事業タイプ: BtoBサービス系")
        
        # 事業規模の推定
        if context.get("monthly_spend"):
            monthly_spend = context["monthly_spend"]
            if monthly_spend > 10000000:  # 1000万円以上
                business_info.append("事業規模: 大規模（月額1000万円以上）")
            elif monthly_spend > 1000000:  # 100万円以上
                business_info.append("事業規模: 中規模（月額100-1000万円）")
            else:
                business_info.append("事業規模: 小規模（月額100万円未満）")
        
        return " | ".join(business_info) if business_info else ""
    
    def add_analysis_to_history(self, analysis_data: Dict[str, Any]):
        """分析履歴への追加"""
        self.analysis_history.append({
            "timestamp": datetime.now(),
            "user_input": analysis_data.get("user_input", ""),
            "sql": analysis_data.get("sql", ""),
            "row_count": analysis_data.get("row_count", 0),
            "metrics_used": analysis_data.get("metrics_used", [])
        })
        
        # 履歴の上限管理（メモリ節約）
        if len(self.analysis_history) > 20:
            self.analysis_history = self.analysis_history[-20:]

# =========================================================================
# 強化されたプロンプト選択ロジック
# =========================================================================

def select_enhanced_prompt(user_input: str, context: Dict[str, Any] = None) -> Dict[str, str]:
    """ユーザー入力から最適な強化プロンプトを選択"""
    user_lower = user_input.lower()
    context = context or {}
    
    # 時系列分析の判定
    if any(keyword in user_lower for keyword in ["推移", "変化", "トレンド", "時系列", "日別", "週別", "月別"]):
        return ENHANCED_PROMPT_DEFINITIONS["time_series"]
    
    # 高度KPI分析の判定
    elif any(keyword in user_lower for keyword in ["効率", "最適化", "roas", "roi", "コスト効率", "パフォーマンス"]):
        return ENHANCED_PROMPT_DEFINITIONS["advanced_kpi"]
    
    # デフォルトはキャンペーン分析
    else:
        return ENHANCED_PROMPT_DEFINITIONS["campaign"]

def generate_enhanced_sql_prompt(user_input: str, context: Dict[str, Any] = None) -> str:
    """強化されたSQL生成プロンプトの作成"""
    enhancer = PromptContextEnhancer()
    
    # 適切なプロンプトテンプレートを選択
    prompt_template = select_enhanced_prompt(user_input, context)
    
    # コンテキストを強化してプロンプトを生成
    enhanced_prompt = enhancer.enhance_sql_prompt(
        prompt_template["template"], 
        user_input, 
        context
    )
    
    return enhanced_prompt

def generate_enhanced_claude_prompt(data_sample: str, chart_settings: str, context: Dict[str, Any] = None) -> str:
    """強化されたClaude分析プロンプトの作成"""
    enhancer = PromptContextEnhancer()
    
    # 基本プロンプトまたは高度プロンプトの選択
    base_prompt = ENHANCED_CLAUDE_PROMPT_TEMPLATE
    
    # 高度な洞察が必要な場合の判定
    if context and context.get("analysis_depth") == "strategic":
        base_prompt = CLAUDE_INSIGHT_PROMPT_TEMPLATE
    
    # コンテキストを強化してプロンプトを生成
    enhanced_prompt = enhancer.enhance_claude_prompt(
        base_prompt, 
        data_sample, 
        chart_settings, 
        context
    )
    
    return enhanced_prompt

# =========================================================================
# レガシー関数の互換性維持
# =========================================================================

def select_best_prompt(user_input: str) -> dict:
    """既存コードとの互換性のための関数"""
    enhanced_prompt = select_enhanced_prompt(user_input)
    
    return {
        "description": enhanced_prompt["description"],
        "template": enhanced_prompt["template"]
    }

# MODIFY_SQL_TEMPLATE の強化版
ENHANCED_MODIFY_SQL_TEMPLATE = """
# あなたはBigQuery SQLの最適化専門家です。

## 修正対象SQL
```sql
{original_sql}
```

## 修正指示
{modification_instruction}

## 修正時の考慮点
### データ品質
- NULL値の適切な処理（SAFE_DIVIDE使用）
- 外れ値の影響を最小化
- 重複データの除外

### パフォーマンス最適化
- 必要な列のみ選択
- 適切なWHERE句での絞り込み
- LIMITでの結果制限

### 分析品質
- 業界ベンチマークを考慮した指標計算
- 統計的有意性の確保
- 解釈しやすい結果の出力

## 出力要件
実行可能なBigQuery SQLのみを返してください。説明は不要です。
"""

# Claude用コメントプロンプトのテンプレート更新
ENHANCED_CLAUDE_COMMENT_PROMPT_TEMPLATE = ENHANCED_CLAUDE_PROMPT_TEMPLATE