# context_glossary.py の全量を以下に置き換えてください

import pandas as pd
import streamlit as st
from pathlib import Path

# キャッシュを使って、アプリ起動時に一度だけCSVを読み込む
@st.cache_data(ttl=3600)
def load_glossary_from_csv() -> dict:
    """glossary.csvから用語集を読み込み、辞書形式に変換する"""
    glossary_path = Path("glossary.csv")
    if not glossary_path.exists():
        st.warning("⚠️ glossary.csv が見つかりません。")
        return {}
    
    df = pd.read_csv(glossary_path).fillna('') # NaNを空文字に変換
    
    # DataFrameを辞書形式に変換
    glossary_dict = {}
    for _, row in df.iterrows():
        term = row['term']
        glossary_dict[term] = {
            "full_name": row['full_name'],
            "definition": row['definition'],
            "calculation": row['calculation'],
            "analysis_point": row['analysis_point']
        }
    return glossary_dict

def get_glossary_for_prompt() -> str:
    """プロンプトに埋め込むための整形済み用語集テキストを生成する"""
    glossary_dict = load_glossary_from_csv()
    if not glossary_dict:
        return ""

    prompt_text = "## 📖 ビジネス用語集\n"
    prompt_text += "ユーザーの指示を解釈する際は、以下の定義を最優先で参考にしてください。\n\n"

    for term, data in glossary_dict.items():
        prompt_text += f"### {term} ({data.get('full_name', '')})\n"
        if data.get('definition'):
            prompt_text += f"- **定義**: {data['definition']}\n"
        if data.get('calculation'):
            prompt_text += f"- **計算式**: `{data['calculation']}`\n"
        if data.get('analysis_point'):
            prompt_text += f"- **分析のポイント**: {data['analysis_point']}\n"
        prompt_text += "\n"
        
    return prompt_text

def extract_relevant_glossary(user_input: str) -> str:
    """ユーザー入力に関連する用語だけを抽出してプロンプト用テキストを生成する"""
    glossary_dict = load_glossary_from_csv()
    if not glossary_dict or not user_input:
        return ""

    # ユーザーの入力テキストに、用語集のキーワードが含まれているかチェック
    relevant_terms = {term: data for term, data in glossary_dict.items() if term in user_input}
    
    if not relevant_terms:
        return "" # 関連用語がなければ何も返さない

    prompt_text = "## 📖 関連ビジネス用語\n"
    prompt_text += "ユーザーの指示を解釈する際に、以下の定義を参考にしてください。\n\n"

    # 検出された関連用語だけをプロンプト用に整形
    for term, data in relevant_terms.items():
        prompt_text += f"### {term} ({data.get('full_name', '')})\n"
        if data.get('definition'):
            prompt_text += f"- **定義**: {data['definition']}\n"
        if data.get('calculation'):
            prompt_text += f"- **計算式**: `{data['calculation']}`\n"
        if data.get('analysis_point'):
            prompt_text += f"- **分析のポイント**: {data['analysis_point']}\n"
        prompt_text += "\n"
        
    return prompt_text