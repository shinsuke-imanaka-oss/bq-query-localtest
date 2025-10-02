# ▼▼▼【このコードで全文を置き換え】▼▼▼
# insight_miner.py

import streamlit as st
import pandas as pd
from typing import Optional, Dict

# --- 依存モジュールのインポート ---
try:
    from bq_tool_config import settings
    SETTINGS_AVAILABLE = True
except ImportError:
    st.error("設定ファイル(bq_tool_config.py)が見つかりません。")
    SETTINGS_AVAILABLE = False
    settings = None

# --- 分析ロジック ---

def analyze_dimension(bq_client, dimension_jp: str, table_name: str, dimension_en: str, target_kpi_en: str) -> Optional[pd.DataFrame]:
    """
    指定された単一のテーブルとディメンション（要因）に基づいてKPIを分析する汎用関数
    """
    # 【修正点2】クエリ内のテーブル名をバッククォートで囲むように修正
    query = f"""
    SELECT
        `{dimension_en}` AS factor,
        SUM(CostIncludingFees) AS cost,
        SUM(Impressions) AS impressions,
        SUM(Clicks) AS clicks,
        SUM(Conversions) AS conversions
    FROM `{table_name}`
    WHERE `{dimension_en}` IS NOT NULL
    GROUP BY factor
    HAVING SUM(CostIncludingFees) > 100
    """
    try:
        df = bq_client.query(query).to_dataframe()
        if df.empty:
            return None

        df['cpa'] = df['cost'] / df['conversions']
        df['cvr'] = df['conversions'] / df['clicks']
        df['ctr'] = df['clicks'] / df['impressions']
        # cpcの計算を追加
        df['cpc'] = df['cost'] / df['clicks']
        df['dimension'] = dimension_jp

        return df[['dimension', 'factor', target_kpi_en]]

    except Exception as e:
        st.warning(f"分析エラー ({dimension_jp}): {e}")
        return None

def find_key_drivers_safe(bq_client, target_kpi_en: str) -> Optional[pd.DataFrame]:
    """
    複数のテーブルを個別に分析し、結果を統合して貢献度を算出する（設定対応版）
    """
    if not SETTINGS_AVAILABLE:
        st.error("設定が読み込まれていないため、分析を実行できません。")
        return None

    # 【修正点3】設定オブジェクトから動的にテーブル名を生成
    ANALYSIS_MAP = {
        "デバイス": (settings.bigquery.get_full_table_name("campaign_device"), "DeviceCategory"),
        "広告グループ": (settings.bigquery.get_full_table_name("ad_group"), "AdGroupName_unified"),
        "メディア": (settings.bigquery.get_full_table_name("campaign"), "ServiceNameJA_Media"),
        "キャンペーン": (settings.bigquery.get_full_table_name("campaign"), "CampaignName"),
        "性別": (settings.bigquery.get_full_table_name("gender"), "UnifiedGenderJA"),
        "年齢": (settings.bigquery.get_full_table_name("age_group"), "AgeRange"),
        "地域": (settings.bigquery.get_full_table_name("area"), "RegionJA"),
        "曜日": (settings.bigquery.get_full_table_name("campaign"), "DayOfWeekJA"), 
        "テキスト": (settings.bigquery.get_full_table_name("ad"), "Headline"),
        "ディスプレイ": (settings.bigquery.get_full_table_name("ad"), "AdName"),
        #"キーワード": (settings.bigquery.get_full_table_name("keyword"), "Keyword"),
        "時間": (settings.bigquery.get_full_table_name("hourly"), "HourOfDay")
    }

    all_results = []
    with st.spinner("各要因を個別に分析中..."):
        for dim_jp, (table, dim_en) in ANALYSIS_MAP.items():
            result_df = analyze_dimension(bq_client, dim_jp, table, dim_en, target_kpi_en)
            if result_df is not None:
                all_results.append(result_df)

    if not all_results:
        st.error("分析可能なデータが見つかりませんでした。")
        return None

    combined_df = pd.concat(all_results, ignore_index=True)

    overall_avg_kpi = combined_df[target_kpi_en].mean()
    combined_df['contribution'] = combined_df[target_kpi_en] - overall_avg_kpi

    if target_kpi_en in ['cpa', 'cost', 'cpc']: # 低い方が良い指標にcpcを追加
        combined_df['type'] = combined_df['contribution'].apply(lambda x: 'ポジティブ要因 (低い)' if x < 0 else 'ネガティブ要因 (高い)')
        return combined_df.sort_values(by='contribution', ascending=True)
    else:
        combined_df['type'] = combined_df['contribution'].apply(lambda x: 'ポジティブ要因 (高い)' if x > 0 else 'ネガティブ要因 (低い)')
        return combined_df.sort_values(by='contribution', ascending=False)


# --- AIによるインサイト生成 (修正なし) ---
def generate_ai_insight(drivers_df: pd.DataFrame, gemini_model) -> str:
    # (この関数の中身は変更ありません)
    if not gemini_model:
        return "AIモデルが利用できません。"
    prompt = f"""
    あなたは経験豊富なデータサイエンティストです。
    以下のKPI変動要因の分析結果を解釈し、マーケティング担当者向けに
    「サマリー」「発見されたインサイト」「推奨アクション」を箇条書きで分かりやすく説明してください。
    特に、最も影響の大きいポジティブ要因とネガティブ要因に焦点を当ててください。

    # KPI変動要因 分析結果
    {drivers_df.to_string()}
    """
    try:
        with st.spinner("AIが分析結果を解釈し、インサイトを抽出中..."):
            response = gemini_model.generate_content(prompt)
            return response.text
    except Exception as e:
        return f"AIインサイト生成中にエラーが発生: {e}"

# --- メイン実行関数 (修正なし) ---
def run_insight_analysis():
    # (この関数の中身は変更ありません)
    st.header("🧠 自動インサイト分析")
    st.markdown("AIがデータの深層を分析し、KPIに影響を与えている「隠れた要因」を自動で発見します。")

    bq_client = st.session_state.get("bq_client")
    gemini_model = st.session_state.get("gemini_model")
    if not bq_client or not gemini_model:
        st.error("この機能を利用するには、BigQueryとGeminiの両方に接続してください。")
        return

    kpi_options_map = {'CPA': 'cpa', 'CVR': 'cvr', 'CTR': 'ctr', 'CPC': 'cpc'}
    target_kpi_ja = st.selectbox("分析の主軸となるKPIを選択してください", options=list(kpi_options_map.keys()))
    target_kpi_en = kpi_options_map[target_kpi_ja]

    if st.button("🔍 要因分析を実行する", type="primary"):
        key_drivers_df = find_key_drivers_safe(bq_client, target_kpi_en=target_kpi_en)

        if key_drivers_df is None:
            return

        ai_summary = generate_ai_insight(key_drivers_df, gemini_model)

        st.subheader("🤖 AIによる分析レポート")
        st.info(ai_summary)

        with st.expander("📝 要因分析の詳細データ"):
            st.dataframe(key_drivers_df, use_container_width=True)