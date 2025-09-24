# prompts.py
"""
AIへの指示（プロンプト）を管理するファイル
- 自然言語の指示から最適なSQL生成プロンプトを選択
- AIコメント生成用のプロンプトを定義
- 深掘り分析ナビゲーション用のプロンプトを定義
"""

# =========================================================================
# SQL生成プロンプト (Gemini用)
# =========================================================================
PROMPT_DEFINITIONS = {
    "campaign": {
        "description": "キャンペーン単位での広告実績を分析",
        "template": """
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}
# 分析対象: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`
# カラム: Impressions, Clicks, CostIncludingFees, Conversions, ServiceNameJA, PromotionName, AccountName, CampaignName, Date, DayOfWeekJA, AllConversions, Cost, VideoViews, ConversionValue, AllConversionValue, ServiceNameJA_Media
# 指標: CTR=Clicks/Impressions, CPA=CostIncludingFees/Conversions, CPC=CostIncludingFees/Clicks, CVR=Conversions/Clicks
# ルール: ユーザーの指示に最も関連性の高い指標を選択してSQLを生成してください。CostはCostIncludingFeesを、ConversionsはConversionsを使用してください。
# 出力: 実行可能な BigQuery SQL だけ返す（説明なし）
"""
    },
    "age_group": {
        "description": "年齢区分ごとの広告パフォーマンス分析",
        "template": """
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}
# 分析対象: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_age_group`
# カラム: Impressions, Clicks, CostIncludingFees, Conversions, ServiceNameJA, CampaignName, Date, AgeRange, DayOfWeekJA, AllConversions, Cost, VideoViews, ConversionValue, AllConversionValue, ServiceNameJA_Media
# 指標: CTR=Clicks/Impressions, CPA=CostIncludingFees/Conversions, CPC=CostIncludingFees/Clicks, CVR=Conversions/Clicks
# ルール: ユーザーの指示に最も関連性の高い指標を選択してSQLを生成してください。CostはCostIncludingFeesを、ConversionsはConversionsを使用してください。
# 出力: 実行可能な BigQuery SQL だけ返す（説明なし）
"""
    },
    "gender": {
        "description": "性別ごとの広告パフォーマンス分析",
        "template": """
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}
# 分析対象: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_gender`
# カラム: Impressions, Clicks, CostIncludingFees, Conversions, ServiceNameJA, CampaignName, Date, UnifiedGenderJA, DayOfWeekJA, AllConversions, Cost, VideoViews, ConversionValue, AllConversionValue, ServiceNameJA_Media
# 指標: CTR=Clicks/Impressions, CPA=CostIncludingFees/Conversions, CPC=CostIncludingFees/Clicks, CVR=Conversions/Clicks
# ルール: ユーザーの指示に最も関連性の高い指標を選択してSQLを生成してください。CostはCostIncludingFeesを、ConversionsはConversionsを使用してください。
# 出力: 実行可能な BigQuery SQL だけ返す（説明なし）
"""
    },
    "campaign_device": {
        "description": "デバイス別の広告パフォーマンス分析",
        "template": """
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}
# 分析対象: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign_device`
# カラム: Impressions, Clicks, CostIncludingFees, Conversions, ServiceNameJA, CampaignName, Date, DeviceCategory, DayOfWeekJA, AllConversions, Cost, VideoViews, ConversionValue, AllConversionValue, ServiceNameJA_Media
# 指標: CTR=Clicks/Impressions, CPA=CostIncludingFees/Conversions, CPC=CostIncludingFees/Clicks, CVR=Conversions/Clicks
# ルール: ユーザーの指示に最も関連性の高い指標を選択してSQLを生成してください。CostはCostIncludingFeesを、ConversionsはConversionsを使用してください。
# 出力: 実行可能な BigQuery SQL だけ返す（説明なし）
"""
    },
    "keyword": {
        "description": "キーワード別の広告パフォーマンス分析",
        "template": """
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}
# 分析対象: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_keyword`
# カラム: Impressions, Clicks, CostIncludingFees, Conversions, ServiceNameJA, CampaignName, Date, Keyword, MatchType, DayOfWeekJA, AllConversions, Cost, VideoViews, ConversionValue, AllConversionValue, ServiceNameJA_Media
# 指標: CTR=Clicks/Impressions, CPA=CostIncludingFees/Conversions, CPC=CostIncludingFees/Clicks, CVR=Conversions/Clicks
# ルール: ユーザーの指示に最も関連性の高い指標を選択してSQLを生成してください。CostはCostIncludingFeesを、ConversionsはConversionsを使用してください。
# 出力: 実行可能な BigQuery SQL だけ返す（説明なし）
"""
    },
    "area": {
        "description": "地域別の広告パフォーマンス分析",
        "template": """
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}
# 分析対象: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_geo`
# カラム: Impressions, Clicks, CostIncludingFees, Conversions, ServiceNameJA, CampaignName, Date, PrefectureNameJA, DayOfWeekJA, AllConversions, Cost, VideoViews, ConversionValue, AllConversionValue, ServiceNameJA_Media
# 指標: CTR=Clicks/Impressions, CPA=CostIncludingFees/Conversions, CPC=CostIncludingFees/Clicks, CVR=Conversions/Clicks
# ルール: ユーザーの指示に最も関連性の高い指標を選択してSQLを生成してください。CostはCostIncludingFeesを、ConversionsはConversionsを使用してください。
# 出力: 実行可能な BigQuery SQL だけ返す（説明なし）
"""
    },
    "interest": {
        "description": "興味・関心カテゴリ別の広告パフォーマンス分析",
        "template": """
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}
# 分析対象: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_interest`
# カラム: Impressions, Clicks, CostIncludingFees, Conversions, ServiceNameJA, CampaignName, Date, InterestCategory, DayOfWeekJA, AllConversions, Cost, VideoViews, ConversionValue, AllConversionValue, ServiceNameJA_Media
# 指標: CTR=Clicks/Impressions, CPA=CostIncludingFees/Conversions, CPC=CostIncludingFees/Clicks, CVR=Conversions/Clicks
# ルール: ユーザーの指示に最も関連性の高い指標を選択してSQLを生成してください。CostはCostIncludingFeesを、ConversionsはConversionsを使用してください。
# 出力: 実行可能な BigQuery SQL だけ返す（説明なし）
"""
    },
    "creative_text": {
        "description": "テキスト広告クリエイティブの分析",
        "template": """
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}
# 分析対象: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_ad`
# カラム: Impressions, Clicks, CostIncludingFees, Conversions, ServiceNameJA, CampaignName, Date, AdName, Headline, Description, AdTypeJA, DayOfWeekJA, AllConversions, Cost, VideoViews, ConversionValue, AllConversionValue, ServiceNameJA_Media
# 指標: CTR=Clicks/Impressions, CPA=CostIncludingFees/Conversions, CPC=CostIncludingFees/Clicks, CVR=Conversions/Clicks
# ルール: ユーザーの指示に最も関連性の高い指標を選択してSQLを生成してください。CostはCostIncludingFeesを、ConversionsはConversionsを使用してください。テキスト広告の場合はAdTypeJA='テキスト'でフィルターしてください。
# 出力: 実行可能な BigQuery SQL だけ返す（説明なし）
"""
    },
    "creative_display": {
        "description": "ディスプレイ広告クリエイティブの分析",
        "template": """
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}
# 分析対象: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_ad`
# カラム: Impressions, Clicks, CostIncludingFees, Conversions, ServiceNameJA, CampaignName, Date, AdName, AssetURL, AdTypeJA, DayOfWeekJA, AllConversions, Cost, VideoViews, ConversionValue, AllConversionValue, ServiceNameJA_Media
# 指標: CTR=Clicks/Impressions, CPA=CostIncludingFees/Conversions, CPC=CostIncludingFees/Clicks, CVR=Conversions/Clicks
# ルール: ユーザーの指示に最も関連性の高い指標を選択してSQLを生成してください。CostはCostIncludingFeesを、ConversionsはConversionsを使用してください。ディスプレイ広告の場合はAdTypeJA='ディスプレイ'でフィルターしてください。
# 出力: 実行可能な BigQuery SQL だけ返す（説明なし）
"""
    },
    "placement": {
        "description": "プレースメント別の広告パフォーマンス分析",
        "template": """
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}
# 分析対象: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_placement`
# カラム: Impressions, Clicks, CostIncludingFees, Conversions, PlacementJA, ServiceNameJA, CampaignName, Date, DayOfWeekJA, AllConversions, Cost, VideoViews, ConversionValue, AllConversionValue, ServiceNameJA_Media
# 指標: CTR=Clicks/Impressions, CPA=CostIncludingFees/Conversions, CPC=CostIncludingFees/Clicks, CVR=Conversions/Clicks
# ルール: ユーザーの指示に最も関連性の高い指標を選択してSQLを生成してください。CostはCostIncludingFeesを、ConversionsはConversionsを使用してください。
# 出力: 実行可能な BigQuery SQL だけ返す（説明なし）
"""
    },
    "hourly": {
        "description": "時間帯ごとのパフォーマンス分析",
        "template": """
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}
# 分析対象: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_hourly`
# カラム: Impressions, Clicks, CostIncludingFees, Conversions, HourOfDay, ServiceNameJA, CampaignName, Date, DayOfWeekJA, AllConversions, Cost, VideoViews, ConversionValue, AllConversionValue, ServiceNameJA_Media
# 指標: CTR=Clicks/Impressions, CPA=CostIncludingFees/Conversions, CPC=CostIncludingFees/Clicks, CVR=Conversions/Clicks
# ルール: ユーザーの指示に最も関連性の高い指標を選択してSQLを生成してください。CostはCostIncludingFeesを、ConversionsはConversionsを使用してください。
# 出力: 実行可能な BigQuery SQL だけ返す（説明なし）
"""
    }
}

# =========================================================================
# SQL修正プロンプト
# =========================================================================
MODIFY_SQL_TEMPLATE = """
# あなたはGoogle BigQueryの専門家です。
# 以下のSQLを、指示に基づいて修正し、実行可能なBigQuery SQLだけを返してください。
# 
# 元のSQL:
# {original_sql}
#
# 修正指示:
# {modification_instruction}
#
# 出力:
"""

# =========================================================================
# 汎用SQLテンプレート
# =========================================================================
GENERAL_SQL_TEMPLATE = """
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}

{prompt_description}

{prompt_template}
"""

# =========================================================================
# AIコメント生成プロンプト (Claude用)
# =========================================================================
CLAUDE_COMMENT_PROMPT_TEMPLATE = """
あなたは優秀なデータアナリストです。
以下のデータサンプルとグラフ設定に基づき、広告運用担当者向けの分析レポートを作成してください。

- 重要な傾向や特筆すべき点を3つ以内で箇条書きで分かりやすく解説してください。
- グラフから読み取れる示唆を具体的に述べてください。

[データサンプル]
{data_sample}

[グラフ設定]
{chart_settings}

分析レポート:
"""

# =========================================================================
# 深掘り分析ナビゲーションプロンプト (Claude用)
# =========================================================================
CLAUDE_NEXT_ACTION_PROMPT_TEMPLATE = """
あなたは優秀なデータアナリストです。
以下の分析結果とユーザーの分析履歴に基づき、次に実行すべき深掘り分析を3つ提案してください。

提案は以下のフォーマットで作成してください。
提案1: [タイトル]
- [説明]
提案2: [タイトル]
- [説明]
提案3: [タイトル]
- [説明]

[直前の分析結果のサマリー]
{last_analysis_summary}

[ユーザーの分析履歴]
{analysis_history}

次にすべき深掘り分析の提案:
"""

# =========================================================================
# 共通ロジック
# =========================================================================
def select_best_prompt(user_input: str) -> dict:
    """ユーザーの指示から最適なプロンプト定義を選択する"""
    user_input = user_input.lower()

    # --- 評価の優先順位 1: 最も具体的なキーワード ---
    if any(keyword in user_input for keyword in ["広告クリエイティブ", "クリエイティブ"]):
        # 「クリエイティブ」というキーワードがテキストとディスプレイの両方で使われる可能性があるため、
        # より具体的なキーワード（例: 「テキスト」「ディスプレイ」）を優先
        if any(keyword in user_input for keyword in ["テキスト", "見出し", "説明文"]):
            return PROMPT_DEFINITIONS["creative_text"]
        elif any(keyword in user_input for keyword in ["ディスプレイ", "画像", "バナー", "アセット"]):
            return PROMPT_DEFINITIONS["creative_display"]
        else:
            # どちらか特定できない場合は、テキスト広告をデフォルトとして返す
            return PROMPT_DEFINITIONS["creative_text"]

    # --- 評価の優先順位 2: 特定のディメンションキーワード ---
    if any(keyword in user_input for keyword in ["キーワード", "query"]):
        return PROMPT_DEFINITIONS["keyword"]

    if any(keyword in user_input for keyword in ["地域", "県", "市区町村"]):
        return PROMPT_DEFINITIONS["area"]

    # 「スマホ」や「PC」など、より口語的な表現を追加
    if any(keyword in user_input for keyword in ["デバイス", "端末", "スマートフォン", "スマホ", "PC", "モバイル", "タブレット"]):
        return PROMPT_DEFINITIONS["campaign_device"]

    if any(keyword in user_input for keyword in ["性別", "男女", "男性", "女性"]):
        return PROMPT_DEFINITIONS["gender"]

    if any(keyword in user_input for keyword in ["興味", "関心", "オーディエンス"]):
        return PROMPT_DEFINITIONS["interest"]

    # 「掲載面」や「配信先」といった関連キーワードを追加
    if any(keyword in user_input for keyword in ["流入元", "プレースメント", "掲載面", "配信先"]):
        return PROMPT_DEFINITIONS["placement"]

    if any(keyword in user_input for keyword in ["年齢", "age", "ターゲット"]):
        return PROMPT_DEFINITIONS["age_group"]

    if any(keyword in user_input for keyword in ["時間", "hour", "時刻", "時間帯"]):
        return PROMPT_DEFINITIONS["hourly"]

    # --- 評価の優先順位 3: 階層的なキーワード ---
    # 最も汎用的な「キャンペーン」を最後に評価
    if any(keyword in user_input for keyword in ["キャンペーン", "広告", "予算", "成果"]):
        return PROMPT_DEFINITIONS["campaign"]

    # 一致しない場合は、最も汎用的なプロンプトを返す
    return PROMPT_DEFINITIONS["campaign"]

# =========================================================================
# プロンプトシステムのテスト・デバッグ用関数
# =========================================================================
def test_prompt_selection():
    """プロンプト選択のテスト"""
    test_cases = [
        "キャンペーン別のCPA分析",
        "年齢層ごとの効果測定",
        "性別別のパフォーマンス",
        "デバイス別のCTR比較",
        "キーワードの効果分析",
        "地域別の成果確認",
        "テキストクリエイティブの分析",
        "ディスプレイ広告の効果",
        "時間帯別の配信効果"
    ]
    
    print("プロンプト選択テスト結果:")
    for test_case in test_cases:
        selected = select_best_prompt(test_case)
        print(f"入力: {test_case}")
        print(f"選択: {selected['description']}")
        print("---")

if __name__ == "__main__":
    # テスト実行
    test_prompt_selection()