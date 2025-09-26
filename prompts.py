# prompts.py - 最小版
"""
基本プロンプトシステム
"""

from typing import Dict, Any

# 分析レシピ定義
ANALYSIS_RECIPES = {
    "自由入力": "自由にSQLクエリや分析内容を入力してください",
    "TOP10キャンペーン": "コストが高いキャンペーンTOP10を表示して、CTRとCVRも含めて分析してください",
    "コスト効率分析": "CPAが最も低い効率的なキャンペーンを特定して、その特徴を分析してください",
    "時系列トレンド": "過去30日間のクリック数とコストの推移をグラフで表示してください",
    "メディア比較": "メディア別のパフォーマンス（CTR、CVR、CPA）を比較分析してください",
    "デバイス分析": "デバイス別の効果（PC、モバイル、タブレット）を比較してください",
    "今月サマリー": "今月のキャンペーン全体のサマリー（総コスト、総クリック、平均CPA）を表示してください"
}

# プロンプト定義
PROMPT_DEFINITIONS = {
    "basic_sql": {
        "description": "基本SQL生成",
        "template": """
以下の要求に基づいて、BigQueryで実行可能なSQLクエリを生成してください：

{user_input}

テーブル: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`

重要な列:
- Date: 日付
- CampaignName: キャンペーン名  
- ServiceNameJA_Media: メディア名
- Clicks: クリック数
- Impressions: インプレッション数
- CostIncludingFees: コスト（手数料込み）
- Conversions: コンバージョン数

SQLのみを出力してください。説明は不要です。
"""
    }
}

def select_best_prompt(user_input: str) -> Dict[str, Any]:
    """最適なプロンプトを選択"""
    return PROMPT_DEFINITIONS["basic_sql"]

def get_optimized_bigquery_template(user_input: str) -> str:
    """最適化されたBigQueryテンプレートを取得"""
    prompt = select_best_prompt(user_input)
    return prompt["template"].format(user_input=user_input)