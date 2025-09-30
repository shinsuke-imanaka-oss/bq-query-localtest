# semantic_analyzer.py

import streamlit as st
from typing import List, Dict, Optional
import numpy as np
import pandas as pd
# ▼▼▼【ここからが追加・修正箇所です】▼▼▼
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE
import plotly.express as px
import hdbscan
# ▲▲▲【追加・修正はここまでです】▲▲▲

try:
    from bq_tool_config import settings
    SETTINGS_AVAILABLE = settings is not None
except ImportError:
    SETTINGS_AVAILABLE = False
    settings = None

# --- ライブラリのインポート ---
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
        client_options = {"api_endpoint": f"{location}-aiplatform.googleapis.com"}
        client = aiplatform.gapic.PredictionServiceClient(client_options=client_options)
        endpoint = f"projects/{project_id}/locations/{location}/publishers/google/models/text-embedding-004"
        instances = [json_format.ParseDict({"content": text}, Value()) for text in texts]
        response = client.predict(endpoint=endpoint, instances=instances)
        embedding_dict = {}
        for i, prediction in enumerate(response.predictions):
            vector = [v for v in prediction['embeddings']['values']]
            embedding_dict[texts[i]] = vector
        return embedding_dict
    except Exception as e:
        st.error(f"エンベディング生成中にエラーが発生しました: {e}")
        raise e

def find_similar_texts(query_text: str, embeddings_dict: Dict[str, List[float]], top_n: int = 5) -> Optional[pd.DataFrame]:
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

def group_texts_by_meaning(texts: List[str], min_cluster_size: int = 3) -> Optional[pd.DataFrame]:
    """
    テキストリストをHDBSCANを用いて意味に基づいてクラスタリングする。
    """
    st.info(f"⚙️ {len(texts)}件のテキストからベクトルを生成しています...")
    embeddings_dict = generate_embeddings(texts)
    if not embeddings_dict:
        st.error("ベクトルの生成に失敗しました。")
        return None

    df = pd.DataFrame(embeddings_dict.items(), columns=['text', 'vector'])
    vectors = np.array(df['vector'].tolist())

    st.info(f"⚙️ HDBSCANで最適なグループを自動判定しています (最小グループサイズ: {min_cluster_size})...")

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=1,
        metric='euclidean',
        cluster_selection_method='eom'
    )
    df['cluster'] = clusterer.fit_predict(vectors)

    num_clusters_found = len(df[df['cluster'] != -1]['cluster'].unique())
    num_noise_points = len(df[df['cluster'] == -1])
    st.success(f"✅ グルーピング完了！ {num_clusters_found}個の主要グループと{num_noise_points}個の分類外テキストが発見されました。")

    return df[['text', 'cluster']]

def extract_tags_for_cluster(grouped_df: pd.DataFrame, model) -> Dict[int, List[str]]:
    """
    各クラスタから複数の特徴的なタグを抽出する。
    """
    if not model:
        st.warning("⚠️ AIモデルが利用できないため、タグ抽出はスキップします。")
        return {}

    st.info("🤖 AIが各グループの特徴タグを抽出しています...")
    cluster_tags = {}
    for cluster_id in sorted(grouped_df['cluster'].unique()):
        if cluster_id == -1: continue # ノイズはタグ抽出の対象外
        sample_texts = grouped_df[grouped_df['cluster'] == cluster_id]['text'].sample(min(10, len(grouped_df[grouped_df['cluster'] == cluster_id]))).tolist()
        texts_for_prompt = "\n- ".join(sample_texts)
        prompt = f"""
        # 指示
        あなたは優秀なマーケティングアナリストです。
        以下の広告文のリストから、共通する訴求ポイントや特徴を分析し、最大5個の「#」で始まる簡潔なタグを抽出してください。
        # 広告文リスト
        - {texts_for_prompt}
        # 出力形式（この形式を厳守してください）
        - 必ず「#」から始まるタグのみを改行区切りで出力してください。
        - 解説や挨拶は一切不要です。
        # 出力例
        #送料無料
        #期間限定セール
        #初心者向け
        """
        try:
            response = model.generate_content(prompt)
            tags = [tag.strip() for tag in response.text.strip().split('\n') if tag.strip()]
            cluster_tags[cluster_id] = tags
        except Exception as e:
            cluster_tags[cluster_id] = [f"タグ抽出エラー: {e}"]
    return cluster_tags

def reduce_dimensions_for_visualization(embeddings_dict: Dict[str, List[float]]) -> Optional[pd.DataFrame]:
    """
    可視化のためにベクトルをt-SNEで2次元に削減する。
    """
    if not embeddings_dict:
        return None

    texts = list(embeddings_dict.keys())
    vectors = np.array(list(embeddings_dict.values()))

    st.info("🎨 グラフ表示のために次元削減を実行中...")
    # データ数が少ない場合のperplexityの調整
    perplexity_value = min(30, len(texts) - 1)
    if perplexity_value <= 0:
        st.warning("データ数が少なすぎるため、可視化マップは生成できません。")
        return None

    tsne = TSNE(n_components=2, random_state=42, perplexity=perplexity_value)
    reduced_vectors = tsne.fit_transform(vectors)

    vis_df = pd.DataFrame(reduced_vectors, columns=['x', 'y'])
    vis_df['text'] = texts

    return vis_df