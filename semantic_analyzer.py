# semantic_analyzer.py (最終解決版)
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

# --- ライブラリのインポート ---
# TextEmbeddingModel に依存しない、より低レベルな部品をインポートします
try:
    from google.cloud import aiplatform
    from google.protobuf import json_format
    from google.protobuf.struct_pb2 import Value
    AIPLATFORM_AVAILABLE = True
except ImportError:
    AIPLATFORM_AVAILABLE = False

@st.cache_data(show_spinner="AIがテキストをベクトル化しています...")
def generate_embeddings(texts: List[str]) -> Optional[Dict[str, List[float]]]:
    """
    TextEmbeddingModel を使わずに、Vertex AI Endpointを直接呼び出してベクトルを生成する。
    """
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
        # APIクライアントを初期化
        client_options = {"api_endpoint": f"{location}-aiplatform.googleapis.com"}
        client = aiplatform.gapic.PredictionServiceClient(client_options=client_options)

        # エンドポイントのパスを構築
        endpoint = f"projects/{project_id}/locations/{location}/publishers/google/models/text-embedding-004"
        
        # APIが要求する形式でインスタンスを作成
        instances = [json_format.ParseDict({"content": text}, Value()) for text in texts]
        
        # APIにリクエストを送信
        response = client.predict(endpoint=endpoint, instances=instances)
        
        # 結果を「テキスト: ベクトル」の辞書形式に整形
        embedding_dict = {}
        for i, prediction in enumerate(response.predictions):
            vector = [v for v in prediction['embeddings']['values']]
            embedding_dict[texts[i]] = vector
            
        return embedding_dict

    except Exception as e:
        st.error(f"エンベディング生成中にエラーが発生しました: {e}")
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