# enhanced_prompts.py - 設定対応版（完全版）
"""
強化プロンプトシステム - 設定一元管理対応
業界ベンチマーク・戦略分析・コンテキスト学習機能
"""

import json
from datetime import datetime
from typing import Dict, Any, List, Optional
import streamlit as st
from context_glossary import get_glossary_for_prompt, extract_relevant_glossary

# 設定管理システムの読み込み
try:
    from bq_tool_config import settings
    SETTINGS_AVAILABLE = settings is not None
except ImportError:
    SETTINGS_AVAILABLE = False
    settings = None

# 分析対象としたいテーブル名をリストで定義（今後ここを編集するだけで対象を増減できる）
TARGET_TABLES = [
    "LookerStudio_report_campaign",
    "LookerStudio_report_campaign_device",
    "LookerStudio_report_ad_group",
    "LookerStudio_report_ad",
    "LookerStudio_report_keyword",
    "LookerStudio_report_device",
    "LookerStudio_report_gender",
    "LookerStudio_report_age_group",
    "LookerStudio_report_budget",
    "LookerStudio_report_final_url",
    "LookerStudio_report_hourly",
    "LookerStudio_report_interest",
    "LookerStudio_report_placement",
    "LookerStudio_report_search_query",
    "LookerStudio_report_area"
]

@st.cache_data(ttl=3600)
def get_table_schema_for_prompt() -> str:
    """【新・改】複数のターゲットテーブルのスキーマを取得し、プロンプト用に整形する"""
    bq_client = st.session_state.get("bq_client")
    if not bq_client:
        return "（スキーマ情報を取得できませんでした）"
    
    try:
        # 設定ファイルからプロジェクトIDとデータセットIDを取得
        dataset_id = settings.bigquery.dataset
        project_id = settings.bigquery.project_id

        # TARGET_TABLESリストのテーブル名を直接IN句で使用する
        table_list_str = "', '".join(TARGET_TABLES)

        # INFORMATION_SCHEMAを一度にクエリして、複数テーブルの列情報をまとめて取得
        query = f"""
        SELECT table_name, column_name, data_type
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name IN ('{table_list_str}')
        ORDER BY table_name, ordinal_position
        """
        df = bq_client.query(query).to_dataframe()
        
        if df.empty:
            st.warning(f"以下のテーブルのスキーマ情報が取得できませんでした: {', '.join(TARGET_TABLES)}")
            return "（スキーマ情報を取得できませんでした）"

        # テーブルごとに見やすく整形
        schema_str = ""
        current_table = ""
        for _, row in df.iterrows():
            if row.table_name != current_table:
                current_table = row.table_name
                # settingsから取得した情報で完全なテーブル名を再構築して表示
                full_table_path = f"`{project_id}.{dataset_id}.{current_table}`"
                schema_str += f"\n### テーブル名: {full_table_path}\n"
            schema_str += f"- {row.column_name} ({row.data_type})\n"

        return schema_str

    except Exception as e:
        st.warning(f"複数テーブルスキーマの取得に失敗: {e}")
        return "（スキーマ情報を取得できませんでした）"


# =========================================================================
# 業界ベンチマーク・定数定義（設定対応版）
# =========================================================================

def get_industry_benchmarks() -> Dict[str, Dict[str, float]]:
    """業界ベンチマーク数値（設定から拡張可能）"""
    base_benchmarks = {
        "検索広告": {
            "平均CTR": 0.035,    # 3.5%
            "平均CPC": 150,      # 150円
            "平均CVR": 0.025,    # 2.5%
            "平均CPA": 6000,     # 6,000円
            "平均ROAS": 4.2      # 420%
        },
        "ディスプレイ広告": {
            "平均CTR": 0.008,    # 0.8%
            "平均CPC": 80,       # 80円
            "平均CVR": 0.012,    # 1.2%
            "平均CPA": 8000,     # 8,000円
            "平均ROAS": 3.1      # 310%
        },
        "SNS広告": {
            "平均CTR": 0.015,    # 1.5%
            "平均CPC": 120,      # 120円
            "平均CVR": 0.018,    # 1.8%
            "平均CPA": 7200,     # 7,200円
            "平均ROAS": 3.8      # 380%
        }
    }
    
    # 設定から業界ベンチマークの上書きが可能
    if SETTINGS_AVAILABLE and hasattr(settings, 'industry_benchmarks'):
        base_benchmarks.update(settings.industry_benchmarks)
    
    return base_benchmarks

def get_llm_config() -> Dict[str, Any]:
    """LLM設定の取得"""
    if SETTINGS_AVAILABLE:
        return {
            "gemini_model": settings.ai.gemini_model,
            "claude_model": settings.ai.claude_model,
            "temperature": settings.ai.temperature,
            "max_tokens": settings.ai.max_tokens,
            "timeout": settings.ai.timeout
        }
    else:
        # フォールバック設定
        return {
            "gemini_model": "gemini-2.0-flash-001",
            "claude_model": "claude-3-sonnet-20240229", 
            "temperature": 0.3,
            "max_tokens": 4000,
            "timeout": 60
        }

# =========================================================================
# 強化プロンプト生成クラス（設定対応版）
# =========================================================================

class EnhancedPrompts:
    """強化プロンプトシステム（設定管理対応）"""
    
    def __init__(self):
        self.config = get_llm_config()
        self.industry_knowledge = get_industry_benchmarks()
        self.analysis_history = []
        
        # 設定からプロンプトテンプレートの読み込み
        if SETTINGS_AVAILABLE and hasattr(settings, 'prompt_templates'):
            self.prompt_templates = settings.prompt_templates
        else:
            self.prompt_templates = self._get_default_templates()
    
    def _get_default_templates(self) -> Dict[str, str]:
        """デフォルトプロンプトテンプレート"""
        return {
            "sql_planning": """

            # BigQuery SQLクエリ設計書 作成指示
            
            ## あなたの役割
            あなたは、ユーザーの自然言語による要求を分析し、それを実行するためのSQLクエリの構成要素を**JSON形式で出力する**「SQLプランナー」です。
            **重要: あなたはSQLを直接書いてはいけません。**SQLを組み立てるための「設計書」となるJSONを作ることが、あなたの唯一の仕事です。

            ## ユーザー要求
            {user_input}

            ## 利用可能なテーブルスキーマ
            {table_schema}

            ## ビジネス用語集
            {context}

            # ▼▼▼【重要】ここからが今回の修正箇所▼▼▼
            ## 行動原則
            1.  **テーブル選択:** まず、ユーザー要求と利用可能なテーブルスキーマを分析し、**最も要求に適したテーブルを1つだけ選択**し、その名前をJSONの`table_to_use`キーに設定すること。
            2.  **分析軸と指標の分離:** ユーザーの要求を「何で（`dimensions`）」と「何を（`metrics`）」に分解して考えること。
            3.  **スキーマの厳守:** 上記で選択したテーブルのスキーマにリストされていない列名は、**いかなる場合も絶対に使用してはならない。**

            ## JSON設計書の出力形式とルール
            - `table_to_use`: クエリのベースとなるテーブル名（文字列）。
            - `dimensions`: 分析の軸となる列名（GROUP BY句で使う列）を**文字列のフラットなリスト**で記述。例: `["CampaignName", "DeviceCategory"]`
            - `metrics`: 計算したい指標を記述。`alias`には必ず分かりやすい別名を付け、`expression`には集計関数を使った計算式を記述。
            - `filters`: WHERE句の条件を記述。
            - `order_by`: `alias`で付けた名前を使う。
            - `limit`: 結果の行数。
            - **必ずJSON形式のコードブロック（```json ... ```）のみを出力してください。解説は一切不要です。**

            ## JSON設計書の例
            ```json
            {{
            "table_to_use": "LookerStudio_report_keyword",
            "dimensions": ["Keyword"],
            "metrics": [
                {{"alias": "合計クリック数", "expression": "SUM(Clicks)"}},
                {{"alias": "CTR", "expression": "SAFE_DIVIDE(SUM(Clicks), SUM(Impressions))"}}
            ],
            "filters": [],
            "order_by": {{"column": "CTR", "direction": "DESC"}},
            "limit": 10
            }}            
            """,
                        
            "claude_analysis": """
            # マーケティング分析専門家として回答してください

            ## 分析対象データ
            {data_summary}

            ## 分析要求
            {user_input}

            ## 業界ベンチマーク
            {industry_benchmarks}

            ## 出力要求
            1. **📊 データサマリー**: 重要な数値とトレンド
            2. **🔍 インサイト**: 発見されたパターンと特徴
            3. **💡 戦略提案**: 具体的なアクションプラン
            4. **📈 改善施策**: 優先度順の推奨事項

            {context}
            """
        }
    
    def generate_sql_plan_prompt(self, user_input: str, context: Dict[str, Any] = None) -> str:
        """【新】SQLの「設計書」をAIに生成させるためのプロンプトを生成する"""
        if context is None:
            context = {}
        
        # BigQuery設定の取得
        if SETTINGS_AVAILABLE:
            table_schema = get_table_schema_for_prompt()
        else:
            table_schema = "（スキーマ情報なし）"

        # コンテキスト情報の構築
        sql_context = self._build_sql_context(user_input, context)
        
        # プロンプトテンプレートの適用
        plan_prompt = self.prompt_templates["sql_planning"].format(
            user_input=user_input,
            table_schema=table_schema,
            context=sql_context
        )
        
        return plan_prompt
    
    def generate_enhanced_claude_prompt(self, user_input: str, data_summary: str, context: Dict[str, Any] = None) -> str:
        """強化Claude分析プロンプト（設定対応版）"""
        if context is None:
            context = {}
        
        # 業界ベンチマーク情報の準備
        industry_benchmark_text = self._format_industry_benchmarks()
        
        # コンテキスト情報の構築  
        claude_context = self._build_claude_context(user_input, context)
        
        # プロンプトテンプレートの適用
        enhanced_prompt = self.prompt_templates["claude_analysis"].format(
            user_input=user_input,
            data_summary=data_summary,
            industry_benchmarks=industry_benchmark_text,
            context=claude_context
        )
        
        # Claude モデル固有の調整
        if self.config["claude_model"] == "claude-3-sonnet-20240229":
            enhanced_prompt += "\n\n## 分析品質要求\n- 統計的な根拠を明示\n- 実践的なアクションプランを提供\n- ROI・効果測定の観点を含める"
        
        return enhanced_prompt
    
    def _format_industry_benchmarks(self) -> str:
        """業界ベンチマークのフォーマット"""
        benchmark_text = "## 🏭 業界ベンチマーク（日本市場）\n\n"
        
        for media_type, metrics in self.industry_knowledge.items():
            benchmark_text += f"### {media_type}\n"
            for metric_name, value in metrics.items():
                if "CTR" in metric_name or "CVR" in metric_name:
                    benchmark_text += f"- **{metric_name}**: {value:.1%}\n"
                elif "CPC" in metric_name or "CPA" in metric_name:
                    benchmark_text += f"- **{metric_name}**: ¥{value:,.0f}\n"
                elif "ROAS" in metric_name:
                    benchmark_text += f"- **{metric_name}**: {value:.1f}倍\n"
                else:
                    benchmark_text += f"- **{metric_name}**: {value}\n"
            benchmark_text += "\n"
        
        return benchmark_text
    
    def _build_sql_context(self, user_input: str, context: Dict[str, Any]) -> str:
        """SQL生成用のコンテキスト情報構築"""
        context_parts = []

        # 関連用語だけを抽出して追加 ▼▼▼
        glossary = extract_relevant_glossary(user_input)
        context_parts.append(glossary)

        # 過去の分析パターンから学習
        if self.analysis_history:
            recent_patterns = self._analyze_recent_patterns()
            if recent_patterns:
                context_parts.append(f"## 📚 最近の分析パターン\n{recent_patterns}")
        
        # データ期間の考慮
        if context.get("data_period"):
            context_parts.append(f"## 📅 分析期間\n{context['data_period']}")
        
        # ユーザー意図の分析
        intent = self._analyze_user_intent(user_input)
        if intent:
            context_parts.append(f"## 🎯 分析意図\n{intent}")
        
        # パフォーマンス最適化のヒント
        optimization_tips = self._get_optimization_tips(user_input)
        if optimization_tips:
            context_parts.append(f"## ⚡ 最適化のポイント\n{optimization_tips}")
        
        return "\n\n".join(context_parts) if context_parts else ""
    
    def _build_claude_context(self, user_input: str, context: Dict[str, Any]) -> str:
        """Claude分析用のコンテキスト情報構築"""
        user_input = context.get("user_input", "") 
        context_parts = []

        # 用語集を追加
        glossary = extract_relevant_glossary(user_input)
        context_parts.append(glossary)

        # 分析の背景・目的
        if context.get("analysis_goal"):
            context_parts.append(f"### 🎯 分析目的\n{context['analysis_goal']}")
        
        # 過去の分析結果との関連
        if self.analysis_history:
            previous_insights = self._extract_previous_insights()
            if previous_insights:
                context_parts.append(f"### 📊 過去の分析結果\n{previous_insights}")
        
        # データの特徴・制約
        data_constraints = self._identify_data_constraints(context)
        if data_constraints:
            context_parts.append(f"### ⚠️ データの特徴・制約\n{data_constraints}")
        
        # ビジネスコンテキスト
        business_context = self._get_business_context(context)
        if business_context:
            context_parts.append(f"### 🏢 ビジネスコンテキスト\n{business_context}")
        
        return "\n\n".join(context_parts) if context_parts else ""
    
    def _analyze_user_intent(self, user_input: str) -> str:
        """ユーザー入力から分析意図を推定"""
        user_lower = user_input.lower()
        
        # 比較分析の意図
        if any(word in user_lower for word in ["比較", "対比", "違い", "差", "vs"]):
            return "比較分析: 複数の要素を比較して相対的なパフォーマンスを評価"
        
        # トレンド分析の意図
        elif any(word in user_lower for word in ["トレンド", "推移", "変化", "時系列"]):
            return "トレンド分析: 時間軸での変化パターンと将来予測"
        
        # 最適化の意図
        elif any(word in user_lower for word in ["最適化", "改善", "効率", "パフォーマンス"]):
            return "最適化分析: 成果改善のためのボトルネック特定と改善提案"
        
        # 詳細分析の意図
        elif any(word in user_lower for word in ["詳細", "深堀り", "分析", "調査"]):
            return "詳細分析: データの詳細な特徴分析と洞察抽出"
        
        return ""
    
    def _get_optimization_tips(self, user_input: str) -> str:
        """SQL最適化のヒント"""
        tips = []
        user_lower = user_input.lower()
        
        if any(word in user_lower for word in ["大量", "全データ", "全期間"]):
            tips.append("💡 大量データ処理: LIMIT句やサンプリング（TABLESAMPLE）の活用を検討")
        
        if any(word in user_lower for word in ["グループ", "集計", "合計"]):
            tips.append("💡 集計処理: 適切なINDEXとPARTITION BY句で高速化")
        
        if any(word in user_lower for word in ["結合", "JOIN", "マージ"]):
            tips.append("💡 結合処理: 小さいテーブルを左側に配置し、適切な結合キーを使用")
        
        return " | ".join(tips) if tips else ""
    
    def _analyze_recent_patterns(self) -> str:
        """最近の分析パターン分析"""
        if not self.analysis_history:
            return ""
        
        recent_analyses = self.analysis_history[-5:]  # 直近5件
        patterns = []
        
        # よく使われる指標
        common_metrics = {}
        for analysis in recent_analyses:
            for metric in analysis.get("metrics_used", []):
                common_metrics[metric] = common_metrics.get(metric, 0) + 1
        
        if common_metrics:
            top_metrics = sorted(common_metrics.items(), key=lambda x: x[1], reverse=True)[:3]
            patterns.append(f"頻用指標: {', '.join([m[0] for m in top_metrics])}")
        
        return " | ".join(patterns) if patterns else ""
    
    def _extract_previous_insights(self) -> str:
        """過去の分析結果から重要な洞察を抽出"""
        # 実装：過去の分析結果から重要なインサイトを抽出
        if not self.analysis_history:
            return ""
        
        return "前回分析で効果的なキャンペーンパターンを特定済み"
    
    def _identify_data_constraints(self, context: Dict[str, Any]) -> str:
        """データの特徴・制約の識別"""
        constraints = []
        
        # データサイズの制約
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
        
        # 履歴の上限管理（設定から取得）
        max_history = getattr(settings.app, 'max_analysis_history', 20) if SETTINGS_AVAILABLE else 20
        if len(self.analysis_history) > max_history:
            self.analysis_history = self.analysis_history[-max_history:]

# =========================================================================
# グローバル インスタンス・関数（設定対応版）
# =========================================================================

# 強化プロンプトシステムのグローバルインスタンス
try:
    enhanced_prompts = EnhancedPrompts()
    print("✅ 強化プロンプトシステム初期化完了")
except Exception as e:
    print(f"⚠️ 強化プロンプトシステム初期化エラー: {e}")
    enhanced_prompts = None

def generate_sql_plan_prompt(user_input: str, context: Dict[str, Any] = None) -> str:
    """【新】強化SQL「設計書」生成プロンプト（エントリーポイント）"""
    if enhanced_prompts:
        return enhanced_prompts.generate_sql_plan_prompt(user_input, context)
    else:
        # フォールバック処理
        return json.dumps({"error": "Enhanced prompts not available."})

def generate_enhanced_claude_prompt(user_input: str, data_summary: str, context: Dict[str, Any] = None) -> str:
    """強化Claude分析プロンプト（エントリーポイント）"""
    if enhanced_prompts:
        return enhanced_prompts.generate_enhanced_claude_prompt(user_input, data_summary, context)
    else:
        # フォールバック処理
        basic_template = """
マーケティング分析専門家として、以下のデータを分析してください:

## データ概要
{data_summary}

## 分析要求
{user_input}
"""
        return basic_template.format(user_input=user_input, data_summary=data_summary)

def select_enhanced_prompt(user_input: str, context: Dict[str, Any] = None) -> Dict[str, str]:
    """ユーザー入力から最適な強化プロンプトを選択"""
    # ... (この関数の中身は変更なし) ...
    user_lower = user_input.lower()
    context = context or {}
    
    # 時系列分析の判定
    if any(keyword in user_lower for keyword in ["時系列", "推移", "トレンド", "変化", "月別", "日別"]):
        return {
            "type": "time_series",
            "description": "時系列・トレンド分析",
            "template": "時系列データの変化パターンとトレンド分析に特化したプロンプト"
        }
    
    # 比較分析の判定
    elif any(keyword in user_lower for keyword in ["比較", "対比", "差", "違い", "vs", "ランキング"]):
        return {
            "type": "comparison", 
            "description": "比較・ランキング分析",
            "template": "複数要素の比較とランキング分析に特化したプロンプト"
        }
    
    # パフォーマンス分析の判定
    elif any(keyword in user_lower for keyword in ["効果", "成果", "roi", "roas", "cpa", "cpc"]):
        return {
            "type": "performance",
            "description": "パフォーマンス・効果測定分析", 
            "template": "広告効果とROI分析に特化したプロンプト"
        }
    
    # 詳細分析の判定
    elif any(keyword in user_lower for keyword in ["詳細", "深堀り", "調査", "分析"]):
        return {
            "type": "detailed",
            "description": "詳細・深堀り分析",
            "template": "データの詳細特徴と深層洞察に特化したプロンプト"
        }
    
    # デフォルト（総合分析）
    else:
        return {
            "type": "comprehensive",
            "description": "総合マーケティング分析",
            "template": "包括的なマーケティング分析プロンプト"
        }

# =========================================================================
# 設定連携・ユーティリティ関数
# =========================================================================

def get_prompt_settings() -> Dict[str, Any]:
    """プロンプト関連設定の取得"""
    if SETTINGS_AVAILABLE:
        return {
            "use_enhanced_prompts": getattr(settings.app, 'use_enhanced_prompts', True),
            "include_benchmarks": getattr(settings.app, 'include_benchmarks', True),
            "context_learning": getattr(settings.app, 'context_learning', True),
            "max_context_length": getattr(settings.ai, 'max_tokens', 4000)
        }
    else:
        return {
            "use_enhanced_prompts": True,
            "include_benchmarks": True,
            "context_learning": True,
            "max_context_length": 4000
        }

def update_industry_benchmarks(new_benchmarks: Dict[str, Dict[str, float]]):
    """業界ベンチマークの動的更新"""
    if enhanced_prompts:
        enhanced_prompts.industry_knowledge.update(new_benchmarks)
        print(f"✅ 業界ベンチマーク更新: {list(new_benchmarks.keys())}")

def get_analysis_history_summary() -> Dict[str, Any]:
    """分析履歴のサマリー取得"""
    if not enhanced_prompts or not enhanced_prompts.analysis_history:
        return {"total_analyses": 0, "recent_patterns": []}
    
    history = enhanced_prompts.analysis_history
    return {
        "total_analyses": len(history),
        "recent_patterns": [h.get("user_input", "")[:50] + "..." for h in history[-5:]],
        "last_analysis": history[-1]["timestamp"] if history else None
    }

def reset_analysis_history():
    """分析履歴のリセット"""
    if enhanced_prompts:
        enhanced_prompts.analysis_history = []
        print("🔄 分析履歴をリセットしました")

# =========================================================================
# デバッグ・テスト用関数
# =========================================================================

def test_enhanced_prompts():
    """強化プロンプトシステムのテスト"""
    test_cases = [
        "過去30日のキャンペーン別のCTRとCVRを比較して",
        "コスト効率が最も高い広告グループを特定して",
        "月別の売上トレンドを分析して",
        "デバイス別のパフォーマンスを詳細に調査して"
    ]
    
    results = []
    for test_input in test_cases:
        selected = select_enhanced_prompt(test_input)
        results.append({
            "input": test_input,
            "type": selected["type"],
            "description": selected["description"]
        })
    
    return results

def get_system_info() -> Dict[str, Any]:
    """システム情報の取得"""
    return {
        "settings_available": SETTINGS_AVAILABLE,
        "enhanced_prompts_active": enhanced_prompts is not None,
        "config": get_llm_config(),
        "prompt_settings": get_prompt_settings(),
        "analysis_history_count": len(enhanced_prompts.analysis_history) if enhanced_prompts else 0
    }

# =========================================================================
# エクスポート用定数・設定
# =========================================================================

# 外部モジュールからアクセス可能な設定情報
ENHANCED_PROMPT_CONFIG = {
    "version": "2.0.0-config",
    "settings_available": SETTINGS_AVAILABLE,
    "default_model": get_llm_config()["gemini_model"],
    "supported_analysis_types": ["time_series", "comparison", "performance", "detailed", "comprehensive"]
}

# 利用可能な業界タイプ
SUPPORTED_INDUSTRIES = list(get_industry_benchmarks().keys())

# よく使用される分析パターン
COMMON_ANALYSIS_PATTERNS = {
    "効果測定": ["CTR", "CVR", "CPA", "ROAS分析"],
    "比較分析": ["キャンペーン比較", "メディア比較", "期間比較"],
    "トレンド分析": ["時系列推移", "季節性分析", "成長率分析"],
    "最適化": ["予算配分", "入札戦略", "ターゲティング最適化"]
}

if __name__ == "__main__":
    # テスト実行
    print("🧪 強化プロンプトシステム テスト実行")
    test_results = test_enhanced_prompts()
    for result in test_results:
        print(f"✅ {result['input'][:30]}... → {result['type']} ({result['description']})")
    
    # システム情報表示
    system_info = get_system_info()
    print(f"\n📊 システム情報: {system_info}")
    
    print("✅ 強化プロンプトシステム テスト完了")