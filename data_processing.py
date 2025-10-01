# data_processing.py

import streamlit as st
import pandas as pd
from typing import Optional, List

@st.cache_data
def process_uploaded_csv(uploaded_file) -> Optional[pd.DataFrame]:
    """
    REQ-A1-01, REQ-A1-02 (新形式対応)
    アップロードされたCSVファイルを処理し、検証する。
    """
    if uploaded_file is None:
        return None
    try:
        df = pd.read_csv(uploaded_file)
        
        # 新しいCSV形式の必須列を検証
        required_columns = ['analysis_target_column', 'analyzed_text', 'tag']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.error(f"❌エラー: CSVに必須列がありません: {', '.join(missing_columns)}")
            return None

        # analysis_target_columnが空でないか、かつ全て同じ値かを確認
        if df['analysis_target_column'].isnull().any() or df['analysis_target_column'].nunique() != 1:
            st.error("❌エラー: 'analysis_target_column' 列は、全ての行で同じ有効な列名を持つ必要があります。")
            return None

        st.success(f"✅ タグCSVの読み込みに成功しました。(結合キー: {df['analysis_target_column'].iloc[0]})")
        return df

    except Exception as e:
        st.error(f"❌エラー: CSVファイルの読み込み中にエラーが発生しました: {e}")
        return None

@st.cache_data
def merge_data_with_tags(main_df: pd.DataFrame, tag_df: pd.DataFrame) -> pd.DataFrame:
    """
    REQ-A1-03, NFR-03 (新形式対応)
    メインのDataFrameにタグ情報を動的に結合する。
    """
    if tag_df is None or main_df is None:
        return main_df

    # --- 結合キーの特定 ---
    # CSVの 'analysis_target_column' から、BigQuery側の結合キーとなる列名を取得
    join_key_column = tag_df['analysis_target_column'].iloc[0]

    # BigQueryデータフレーム(main_df)に、その列が存在するか確認
    if join_key_column not in main_df.columns:
        st.warning(f"⚠️ タグ結合スキップ: 分析結果のデータに、CSVで指定された列 '{join_key_column}' が見つかりません。")
        return main_df

    # --- データクレンジング ---
    # 結合前に、両方のキーから前後の空白を削除し、文字列に統一する
    main_df[join_key_column] = main_df[join_key_column].astype(str).str.strip()
    tag_df['analyzed_text'] = tag_df['analyzed_text'].astype(str).str.strip()
    
    # --- 複数タグの集約処理 ---
    # 'analyzed_text' でグループ化し、各グループの 'tag' をカンマ区切りの文字列に結合する
    tags_grouped = tag_df.groupby('analyzed_text')['tag'].apply(lambda x: ', '.join(x)).reset_index()

    # --- デバッグ情報表示 ---
    with st.expander(f"🔍 タグ結合のデバッグ情報 (キー: '{join_key_column}')"):
        st.write("BigQuery側のキー（先頭5件）:", main_df[join_key_column].head().tolist())
        st.write("CSV側の集約済みキー（先頭5件）:", tags_grouped['analyzed_text'].head().tolist())
        st.write("集約されたタグの例:", tags_grouped['tag'].head().tolist())
        
        bq_keys = set(main_df[join_key_column])
        csv_keys = set(tags_grouped['analyzed_text'])
        matching_keys_count = len(bq_keys.intersection(csv_keys))
        
        if matching_keys_count > 0:
            st.success(f"✅ {matching_keys_count}件のキーが一致しました。")
        else:
            st.warning("⚠️ 一致するキーが見つかりませんでした。")

    # --- 結合処理 ---
    tag_df_renamed = tags_grouped.rename(columns={'analyzed_text': join_key_column})
    merged_df = pd.merge(main_df, tag_df_renamed, on=join_key_column, how='left')
    
    return merged_df

def filter_data_by_tags(df: pd.DataFrame, selected_tags: List[str]) -> pd.DataFrame:
    """
    REQ-A2-02 (複数タグ対応版)
    選択されたタグでDataFrameをフィルタリングする
    """
    if not selected_tags or 'tag' not in df.columns:
        return df
    
    # 'tag' 列に、選択されたタグのいずれかが「含まれている」行を抽出する
    # カンマ区切りの文字列に対応するため、.str.contains() を使用
    # na=False は、タグがない(NaN)行を無視するための設定
    condition = df['tag'].str.contains('|'.join(selected_tags), na=False)
    return df[condition]