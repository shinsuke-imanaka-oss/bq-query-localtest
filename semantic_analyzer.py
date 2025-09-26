# semantic_analyzer.py (最終診断版)
import streamlit as st
from typing import List, Dict, Optional
import numpy as np
import pandas as pd

try:
    from bq_tool_config import settings
    SETTINGS_AVAILABLE = settings is not None
except ImportError:
    SETTINGS_AVAILABLE = False
    settings = None

try:
    from google.cloud import aiplatform
    AIPLATFORM_AVAILABLE = True
except ImportError:
    AIPLATFORM_AVAILABLE = False

@st.cache_data(show_spinner="AIがテキストをベクトル化しています...")
def generate_embeddings(texts: List[str]) -> Optional[Dict[str, List[float]]]:
    if not AIPLATFORM_AVAILABLE:
        st.error("❌ `google-cloud-aiplatform`ライブラリが見つかりません。")
        return None
    
    if SETTINGS_AVAILABLE:
        project_id = settings.bigquery.project_id
        location = settings.bigquery.location
    else:
        project_id = "vorn-digi-mktg-poc-635a"
        location = "asia-northeast1"

    if not project_id or not location:
        st.error("❌ GCPのプロジェクトIDとロケーションが設定されていません。")
        return None

    try:
        print("[semantic_analyzer] TRYブロック: API呼び出しを開始します。")
        aiplatform.init(project=project_id, location=location)
        model = aiplatform.TextEmbeddingModel.from_pretrained("text-embedding-004")
        instances = [{"content": text} for text in texts]
        embeddings = model.get_embeddings(instances=instances)
        
        embedding_dict = { text: embedding.values for text, embedding in zip(texts, embeddings) }
        print("[semantic_analyzer] TRYブロック: API呼び出し成功。")
        return embedding_dict

    except Exception as e:
        print(f"[semantic_analyzer] EXCEPTブロック: エラーを捕捉しました。エラーをraiseします。")
        print(f"[semantic_analyzer] 捕捉したエラーのタイプ: {type(e).__name__}")
        print(f"[semantic_analyzer] 捕捉したエラーのメッセージ: {e}")
        # ★★★★★ ここでエラーを呼び出し元に報告します ★★★★★
        raise e

def find_similar_texts(query_text: str, embeddings_dict: Dict[str, List[float]], top_n: int = 5) -> Optional[pd.DataFrame]:
    # (この関数は変更ありません)
    if query_text not in embeddings_dict:
        st.error(f"基準テキスト '{query_text}' のベクトルが見つかりません。")
        return None
    try:
        query_vector = np.array(embeddings_dict[query_text])
        texts = list(embeddings_dict.keys())
        vectors = np.array(list(embeddings_dict.values()))
        norm_query = query_vector / np.linalg.norm(query_vector)
        norm_vectors = vectors / np.linalg.norm(vectors, axis=1, keepdims=True)
        similarities = np.dot(norm_vectors, norm_query)
        results_df = pd.DataFrame({"text": texts, "similarity": similarities})
        results_df = results_df[results_df["text"] != query_text]
        top_results = results_df.sort_values(by="similarity", ascending=False).head(top_n)
        return top_results
    except Exception as e:
        st.error(f"類似性計算中にエラーが発生しました: {e}")
        return None