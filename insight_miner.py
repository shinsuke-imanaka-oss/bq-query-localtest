# insight_miner.py - 日付範囲対応版（既存コードに最小限の修正）

import streamlit as st
import pandas as pd
from typing import Optional, Dict
from datetime import date

# --- 依存モジュールのインポート ---
try:
    from bq_tool_config import settings
    SETTINGS_AVAILABLE = True
except ImportError:
    st.error("設定ファイル(bq_tool_config.py)が見つかりません。")
    SETTINGS_AVAILABLE = False
    settings = None

# --- 分析ロジック（日付対応版） ---

def analyze_dimension(
    bq_client, 
    dimension_jp: str, 
    table_name: str, 
    dimension_en: str, 
    target_kpi_en: str,
    start_date: date = None,
    end_date: date = None
) -> Optional[pd.DataFrame]:
    """
    指定された単一のテーブルとディメンション（要因）に基づいてKPIを分析する汎用関数（日付対応版）
    
    Args:
        bq_client: BigQueryクライアント
        dimension_jp: ディメンション名（日本語）
        table_name: テーブル名
        dimension_en: ディメンション名（英語カラム名）
        target_kpi_en: ターゲットKPI
        start_date: 開始日（Noneの場合は全期間）
        end_date: 終了日（Noneの場合は全期間）
    
    Returns:
        分析結果のDataFrame
    """
    # WHERE句の構築
    where_clauses = [f"`{dimension_en}` IS NOT NULL"]
    
    if start_date:
        where_clauses.append(f"Date >= '{start_date}'")
    if end_date:
        where_clauses.append(f"Date <= '{end_date}'")
    
    where_clause = "WHERE " + " AND ".join(where_clauses)
    
    query = f"""
    SELECT
        `{dimension_en}` AS factor,
        SUM(CostIncludingFees) AS cost,
        SUM(Impressions) AS impressions,
        SUM(Clicks) AS clicks,
        SUM(Conversions) AS conversions
    FROM `{table_name}`
    {where_clause}
    GROUP BY factor
    HAVING SUM(CostIncludingFees) > 100
    """
    
    try:
        df = bq_client.query(query).to_dataframe()
        if df.empty:
            return None

        # KPI計算
        df['cpa'] = df['cost'] / df['conversions']
        df['cvr'] = df['conversions'] / df['clicks']
        df['ctr'] = df['clicks'] / df['impressions']
        df['cpc'] = df['cost'] / df['clicks']
        df['dimension'] = dimension_jp

        return df[['dimension', 'factor', target_kpi_en]]

    except Exception as e:
        st.warning(f"分析エラー ({dimension_jp}): {e}")
        return None


def find_key_drivers_safe(
    bq_client, 
    target_kpi_en: str,
    start_date: date = None,
    end_date: date = None
) -> Optional[pd.DataFrame]:
    """
    複数のテーブルを個別に分析し、結果を統合して貢献度を算出する（日付対応版）
    
    Args:
        bq_client: BigQueryクライアント
        target_kpi_en: ターゲットKPI ('cvr', 'ctr', 'cpa', 'cpc')
        start_date: 開始日（Noneの場合は全期間）
        end_date: 終了日（Noneの場合は全期間）
    
    Returns:
        要因分析結果のDataFrame
    """
    if not SETTINGS_AVAILABLE:
        st.error("設定が読み込まれていないため、分析を実行できません。")
        return None

    # 設定オブジェクトから動的にテーブル名を生成
    ANALYSIS_MAP = {
        "デバイス": (settings.bigquery.get_full_table_name("campaign_device"), "DeviceCategory"),
        "メディア": (settings.bigquery.get_full_table_name("campaign"), "ServiceNameJA_Media"),
        "キャンペーン": (settings.bigquery.get_full_table_name("campaign"), "CampaignName"),
        "年齢": (settings.bigquery.get_full_table_name("age_group"), "AgeRange"),
        "性別": (settings.bigquery.get_full_table_name("gender"), "UnifiedGenderJA"),
        "地域": (settings.bigquery.get_full_table_name("area"), "RegionJA"),
        "時間": (settings.bigquery.get_full_table_name("hourly"), "HourOfDay"),
        "キーワード": (settings.bigquery.get_full_table_name("keyword"), "Keyword"),
        "広告グループ": (settings.bigquery.get_full_table_name("ad_group"), "AdGroupName_unified"),
        "検索ワード": (settings.bigquery.get_full_table_name("search_query"), "Query"),
        "広告": (settings.bigquery.get_full_table_name("ad"), "Headline")
    }

    all_results = []
    
    for dimension_jp, (table_name, dimension_en) in ANALYSIS_MAP.items():
        try:  # ← ここを追加
            result = analyze_dimension(
                bq_client, 
                dimension_jp, 
                table_name, 
                dimension_en, 
                'cvr',  # デフォルトのKPI
                start_date,
                end_date
            )
            if result is not None and not result.empty:  # ← 空チェック追加
                all_results.append(result)
        except Exception as e:  # ← ここを追加
            print(f"分析エラー ({dimension_jp}): {e}")
            # エラーが出ても続行
            continue

    if not all_results:
        st.warning("要因分析のデータが取得できませんでした。")
        return None

    # 結果を統合
    combined_df = pd.concat(all_results, ignore_index=True)
    
    if combined_df.empty:
        return None

    # 貢献度の計算
    kpi_mean = combined_df[target_kpi_en].mean()
    combined_df['contribution'] = combined_df[target_kpi_en] - kpi_mean
    combined_df['effect_category'] = combined_df['contribution'].apply(
        lambda x: 'ポジティブ要因 (高い)' if x > 0 else 'ネガティブ要因 (低い)'
    )
    
    return combined_df.sort_values(by='contribution', ascending=False)


# --- AIによるインサイト生成 ---

def generate_ai_insight(drivers_df: pd.DataFrame, gemini_model) -> str:
    """要因分析結果をAIに解釈させる"""
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


# --- メイン実行関数 ---

def run_insight_analysis():
    """インサイト分析のメインフローを実行する"""
    st.header("🧠 自動インサイト分析")
    st.markdown("AIがデータの深層を分析し、KPIに影響を与えている「隠れた要因」を自動で発見します。")

    bq_client = st.session_state.get("bq_client")
    gemini_model = st.session_state.get("gemini_model")
    
    if not bq_client or not gemini_model:
        st.error("この機能を利用するには、BigQueryとGeminiの両方に接続してください。")
        return

    kpi_options_map = {'CPA': 'cpa', 'CVR': 'cvr', 'CTR': 'ctr', 'CPC': 'cpc'}
    target_kpi_ja = st.selectbox(
        "分析の主軸となるKPIを選択してください", 
        options=list(kpi_options_map.keys())
    )
    target_kpi_en = kpi_options_map[target_kpi_ja]

    if st.button("🔍 要因分析を実行する", type="primary"):
        # 日付範囲なし = 全期間
        key_drivers_df = find_key_drivers_safe(bq_client, target_kpi_en=target_kpi_en)

        if key_drivers_df is None:
            return

        ai_summary = generate_ai_insight(key_drivers_df, gemini_model)

        st.subheader("🤖 AIによる分析レポート")
        st.info(ai_summary)

        with st.expander("📝 要因分析の詳細データ"):
            st.dataframe(key_drivers_df, use_container_width=True)