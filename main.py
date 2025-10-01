# main.py - 設定管理対応版
"""
デジタルマーケティング分析アプリケーション - メインファイル
設定一元管理システム対応
BigQuery + AI(Gemini/Claude) による広告データ分析プラットフォーム
"""

import sys
import streamlit as st
from dotenv import load_dotenv  # <-- 1. この行を追加
load_dotenv()  # <-- 2. この行を追加
st.warning(f"🐍 Streamlitが使用中のPython: {sys.executable}")
import pandas as pd
import os
import traceback
from datetime import datetime as dt, date
from typing import Dict, List, Optional, Any
import diagnostics
from error_handler import handle_error_with_ai
# from troubleshooter import display_troubleshooting_guide


# =========================================================================
# ページ設定（最初に実行）
# =========================================================================

st.set_page_config(
    page_title="🚀 AIデジタルマーケティング分析",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================================================================
# 設定管理システムの初期化
# =========================================================================

try:
    from bq_tool_config import settings, show_config_panel
    SETTINGS_AVAILABLE = settings is not None
    CONFIG_UI_AVAILABLE = True
    if SETTINGS_AVAILABLE:
        st.success("✅ 設定管理システム初期化完了")
        
        # 設定の検証
        validation_result = settings.get_validation_status()
        if not validation_result["valid"]:
            st.error("❌ 設定エラーが検出されました:")
            for error in validation_result["errors"]:
                st.error(f"- {error}")
            st.stop()
        if validation_result["warnings"]:
            st.warning("⚠️ 設定に関する警告:")
            for warning in validation_result["warnings"]:
                st.warning(f"- {warning}")
    else:
        st.error("❌ 設定管理システムの初期化に失敗しました")
        SETTINGS_AVAILABLE = False
        
except ImportError as e:
    st.error(f"設定管理システムが見つかりません: {e}")
    SETTINGS_AVAILABLE = False
    CONFIG_UI_AVAILABLE = False
    settings = None

    # フォールバック関数
    def show_config_panel():
        st.error("設定管理システムが利用できません")

# =========================================================================
# インポート状況管理
# =========================================================================

IMPORT_STATUS = {
    "config.settings": SETTINGS_AVAILABLE,
    "google.cloud.bigquery": False,
    "google.oauth2.service_account": False,
    "google.generativeai": False,
    "anthropic": False,
    "prompts": False,
    "enhanced_prompts": False,
    "ui_main": False,
    "ui_features": False,
    "analysis_controller": False,
    "error_handler": False,
    "data_quality_checker": False,
    "looker_handler": False
}

# =========================================================================
# 必須ライブラリのインポート（設定対応版）
# =========================================================================

# Google Cloud & AI
try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    IMPORT_STATUS["google.cloud.bigquery"] = True
    IMPORT_STATUS["google.oauth2.service_account"] = True
    print("✅ Google Cloud ライブラリ インポート成功")
except ImportError as e:
    print(f"❌ Google Cloud ライブラリ インポートエラー: {e}")
    st.error("❌ Google Cloud ライブラリが見つかりません。`pip install google-cloud-bigquery` を実行してください。")

try:
    import google.generativeai as genai
    IMPORT_STATUS["google.generativeai"] = True
    print("✅ Gemini ライブラリ インポート成功")
except ImportError as e:
    print(f"❌ Gemini ライブラリ インポートエラー: {e}")
    st.error("❌ Gemini ライブラリが見つかりません。`pip install google-generativeai` を実行してください。")

try:
    import anthropic
    IMPORT_STATUS["anthropic"] = True
    print("✅ Claude ライブラリ インポート成功")
except ImportError as e:
    print(f"❌ Claude ライブラリ インポートエラー: {e}")
    st.error("❌ Claude ライブラリが見つかりません。`pip install anthropic` を実行してください。")

# =========================================================================
# プロンプトシステム（設定対応版）
# =========================================================================

# 基本プロンプトシステム
try:
    from prompts import (
        select_best_prompt, get_optimized_bigquery_template,
        ANALYSIS_RECIPES, PROMPT_DEFINITIONS
    )
    IMPORT_STATUS["prompts"] = True
    print("✅ prompts.py インポート成功")
except ImportError as e:
    print(f"⚠️ prompts.py インポートエラー: {e}")
    ANALYSIS_RECIPES = {"自由入力": "自由にSQLクエリや分析内容を入力してください"}
    PROMPT_DEFINITIONS = {}

# 強化プロンプトシステム
try:
    from enhanced_prompts import generate_sql_plan_prompt, generate_enhanced_claude_prompt
    IMPORT_STATUS["enhanced_prompts"] = True
    print("✅ enhanced_prompts.py インポート成功")
except ImportError as e:
    print(f"⚠️ enhanced_prompts.py インポートエラー: {e}")


# =========================================================================
# UI・分析制御システム
# =========================================================================

# UI システム
try:
    from ui_main import show_analysis_workbench, show_manual_sql_interface
    IMPORT_STATUS["ui_main"] = True
    print("✅ ui_main.py インポート成功")
except ImportError as e:
    print(f"⚠️ ui_main.py インポートエラー: {e}")
# UI機能拡張
try:
    from ui_features import (
        show_analysis_summary_panel,
        show_data_quality_panel, 
        show_error_history,
        show_usage_statistics,
        show_quick_reanalysis
    )
    IMPORT_STATUS["ui_features"] = True
    print("✅ ui_features.py インポート成功")
except ImportError as e:
    print(f"⚠️ ui_features.py インポートエラー: {e}")
    IMPORT_STATUS["ui_features"] = False

# 分析制御
try:
    from analysis_controller import run_analysis_flow, execute_sql_query
    IMPORT_STATUS["analysis_controller"] = True
    print("✅ analysis_controller.py インポート成功")
except ImportError as e:
    print(f"⚠️ analysis_controller.py インポートエラー: {e}")

# エラーハンドリング
try:
    from error_handler import handle_error_with_ai
    IMPORT_STATUS["error_handler"] = True
    print("✅ error_handler.py インポート成功")
except ImportError as e:
    print(f"⚠️ error_handler.py インポートエラー: {e}")
    def handle_error_with_ai(*args, **kwargs):
        st.info("📊 エラーハンドリング機能は一時的に利用できません")

# データ品質チェック
try:
    from data_quality_checker import check_data_quality
    IMPORT_STATUS["data_quality_checker"] = True
    print("✅ data_quality_checker.py インポート成功")
except ImportError as e:
    print(f"⚠️ data_quality_checker.py インポートエラー: {e}")
    def check_data_quality(*args, **kwargs):
        st.info("📊 データ品質チェック機能は一時的に利用できません")

# Looker連携
try:
    from looker_handler import show_looker_studio_integration, show_filter_ui
    from dashboard_analyzer import SHEET_ANALYSIS_QUERIES 
    IMPORT_STATUS["looker_handler"] = True
    print("✅ looker_handler.py インポート成功")
except ImportError as e:
    print(f"⚠️ looker_handler.py インポートエラー: {e}")
    IMPORT_STATUS["looker_handler"] = False

# =========================================================================
# セッション状態管理（設定対応版）
# =========================================================================

def ensure_session_state():
    """セッション状態の確実な初期化（設定対応版）"""
    if not SETTINGS_AVAILABLE:
        # フォールバック設定
        defaults = {
            "usage_stats": {"total_analyses": 0, "error_count": 0, "enhanced_usage": 0, "avg_execution_time": 0.0},
            "error_history": [],
            "analysis_history": [],
            "filter_settings": {"start_date": dt.now().date(), "end_date": dt.now().date(), "media": [], "campaigns": []},
            "last_analysis_result": None,
            "last_sql": "",
            "last_user_input": "",
            "auto_claude_analysis": True,
            "view_mode": "📊 ダッシュボード表示",
            "accessibility_settings": {"high_contrast": False, "large_text": False, "reduced_motion": False}
        }
    else:
        # 設定から初期値を取得
        defaults = {
            "usage_stats": {"total_analyses": 0, "error_count": 0, "enhanced_usage": 0, "avg_execution_time": 0.0},
            "error_history": [],
            "analysis_history": [],
            "filter_settings": {"start_date": dt.now().date(), "end_date": dt.now().date(), "media": [], "campaigns": []},
            "last_analysis_result": None,
            "last_sql": "",
            "last_user_input": "",
            "debug_mode": settings.app.debug_mode,
            "auto_claude_analysis": settings.app.auto_claude_analysis,
            "view_mode": "📊 ダッシュボード表示",
            "accessibility_settings": {"high_contrast": False, "large_text": False, "reduced_motion": False}
        }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

# =========================================================================
# APIクライアント設定・認証（設定対応版）
# =========================================================================

def setup_bigquery_client():
    """BigQueryクライアントのセットアップ（修正版）"""
    try:
            # 設定からプロジェクトIDを取得
        if SETTINGS_AVAILABLE and settings.bigquery.project_id:
            project_id = settings.bigquery.project_id
            location = settings.bigquery.location
        else:
            project_id = None
            location = "US"
        
        # Streamlit Secretsから認証情報を取得
        if "gcp_service_account" in st.secrets:
            credentials_info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            if not project_id:
                project_id = credentials_info.get("project_id")
            client = bigquery.Client(credentials=credentials, project=project_id, location=location)
                
            # ✅ 重要: セッション状態に保存
            st.session_state.bq_client = client
            st.success(f"✅ BigQuery接続成功 (Secrets) - プロジェクト: {project_id}")
            return client
            
        # 環境変数から認証
        elif "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
            client = bigquery.Client(project=project_id, location=location)
        
            # ✅ 重要: セッション状態に保存
            st.session_state.bq_client = client
            st.success(f"✅ BigQuery接続成功 (環境変数) - プロジェクト: {client.project}")
            return client
            
        # デフォルト認証
        else:
            client = bigquery.Client(project=project_id, location=location)
            
            # ✅ 重要: セッション状態に保存
            st.session_state.bq_client = client
            st.success(f"✅ BigQuery接続成功 (デフォルト) - プロジェクト: {client.project}")
            return client
    except Exception as e:
        #handle_error_with_ai(e, st.session_state.get("gemini_model"), {"operation": "BigQueryクライアントのセットアップ"})
        #if 'bq_client' in st.session_state:
        #    st.session_state.bq_client = None
        #return None            
        raise e


def setup_gemini_client():
    """Gemini APIクライアントのセットアップ（設定対応版）"""
    try:
        if SETTINGS_AVAILABLE:
            api_key = settings.get_api_key("gemini")
            model_name = settings.ai.gemini_model
        else:
            api_key = st.secrets.get("GEMINI_API_KEY") or st.secrets.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
            model_name = "gemini-1.5-pro"
            
        # ▼▼▼【重要】APIキーがない場合のエラー処理を修正 ▼▼▼
        if not api_key:
            st.error("❌ Gemini API キーが見つかりません。")
            st.markdown("💡 `secrets.toml` または環境変数に `GOOGLE_API_KEY` を設定してください。設定後、このボタンを再度クリックしてください。")
            return None # エラーを発生させずにNoneを返す
            
        genai.configure(api_key=api_key)
            
        if SETTINGS_AVAILABLE:
            generation_config = {"temperature": settings.ai.temperature, "max_output_tokens": settings.ai.max_tokens}
        else:
            generation_config = {"temperature": 0.3, "max_output_tokens": 4000}
            
        model = genai.GenerativeModel(model_name, generation_config=generation_config)
        st.success(f"✅ Gemini API 接続成功 - モデル: {model_name}")
        return model
    except Exception as e:
        # 予期せぬエラーはAIエラーハンドラで処理
        handle_error_with_ai(e, None, {"operation": "Geminiクライアントのセットアップ"})
        return None
        

def setup_claude_client():
    """Claude APIクライアントのセットアップ（設定対応版）"""
    try:
        # 設定からAPIキーとモデルを取得
        if SETTINGS_AVAILABLE:
            api_key = settings.get_api_key("claude")
            model_name = settings.ai.claude_model
        else:
            api_key = st.secrets.get("CLAUDE_API_KEY") or st.secrets.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
            model_name = "claude-3-sonnet-20240229"
        
        if not api_key:
            st.error("❌ Claude API キーが設定されていません")
            st.markdown("💡 `.env` ファイルまたはStreamlit Secretsで `ANTHROPIC_API_KEY` を設定してください")
            return None, None
            
        client = anthropic.Anthropic(api_key=api_key)
        st.success(f"✅ Claude API 接続成功 - モデル: {model_name}")
        return client, model_name
    except Exception as e:
        #handle_error_with_ai(e, st.session_state.get("gemini_model"), {"operation": "Claudeクライアントのセットアップ"})
        #return None, None
        raise e


# =========================================================================
# システム状態表示（設定対応版）
# =========================================================================

def show_system_status():
    """システム状態の表示（設定対応版）"""
    with st.expander("🔧 システム状態", expanded=False):
        st.markdown("### 📦 モジュール読み込み状況")
        for module_name, status in IMPORT_STATUS.items():
            st.markdown(f"{'✅' if status else '❌'} **{module_name}**")
        
        st.markdown("---")
        st.markdown("### 🔑 API接続状況")
        st.markdown(f"**BigQuery**: {'✅ 接続済み' if st.session_state.get('bq_client') else '❌ 未接続'}")
        st.markdown(f"**Gemini**: {'✅ 接続済み' if st.session_state.get('gemini_model') else '❌ 未接続'}")
        st.markdown(f"**Claude**: {'✅ 接続済み' if st.session_state.get('claude_client') else '❌ 未接続'}")
        
        if SETTINGS_AVAILABLE:
            st.markdown("---")
            st.markdown("### ⚙️ 設定情報")
            st.markdown(f"**Geminiモデル**: {settings.ai.gemini_model}")
            st.markdown(f"**Claudeモデル**: {settings.ai.claude_model}")
            st.markdown(f"**Temperature**: {settings.ai.temperature}")
            st.markdown(f"**BigQueryプロジェクト**: {settings.bigquery.project_id or '未設定'}")
            st.markdown(f"**デバッグモード**: {'✅ 有効' if settings.app.debug_mode else '❌ 無効'}")

def show_settings_panel():
    """設定パネル表示"""
    if not SETTINGS_AVAILABLE:
        st.warning("⚠️ 設定管理システムが利用できません")
        return
    
    with st.expander("⚙️ 設定管理", expanded=False):
        st.markdown("### 📋 現在の設定")
        
        # LLM設定
        st.markdown("**🤖 LLM設定**")
        st.code(f"""
Geminiモデル: {settings.ai.gemini_model}
Claudeモデル: {settings.ai.claude_model}
Temperature: {settings.ai.temperature}
最大トークン数: {settings.ai.max_tokens}
        """)
        
        # BigQuery設定
        st.markdown("**📊 BigQuery設定**")
        st.code(f"""
プロジェクトID: {settings.bigquery.project_id or '未設定'}
データセット: {settings.bigquery.dataset}
テーブルプレフィックス: {settings.bigquery.table_prefix}
ロケーション: {settings.bigquery.location}
        """)
        
        # 設定再読み込みボタン
        if st.button("🔄 設定を再読み込み", help="環境変数や設定ファイルの変更を反映します"):
            try:
                settings.reload_settings()
                st.success("✅ 設定を再読み込みしました")
                st.rerun()
            except Exception as e:
                st.error(f"❌ 設定再読み込みエラー: {e}")

# =========================================================================
# メイン表示モード（設定対応版）
# =========================================================================

# main.py のshow_dashboard_mode関数を以下に置き換え

def show_dashboard_mode():
    """ダッシュボード表示モード（Looker Studio機能復活版）"""
    st.header("📊 Looker Studio ダッシュボード")
    
    # BigQueryクライアントの確認
    bq_client = st.session_state.get("bq_client")
    if not bq_client:
        st.warning("⚠️ BigQuery接続が必要です")
        st.info("サイドバーで「🔄 BigQuery接続」をクリックしてください")
        return
    
    # Looker連携機能の確認
    if IMPORT_STATUS.get("looker_handler", False):
        try:
            from looker_handler import show_looker_studio_integration, show_filter_ui
            
            # サイドバーでフィルターUI表示
            with st.sidebar:
                st.markdown("### 📊 Looker Studio フィルター")
                show_filter_ui(bq_client)
            
            # メインエリアでLooker Studio表示
            show_looker_studio_integration(
                bq_client=bq_client,
                model=st.session_state.get("gemini_model"),
                key_prefix="dashboard",
                sheet_analysis_queries=SHEET_ANALYSIS_QUERIES
            )
            
        except ImportError as e:
            st.error(f"❌ Looker機能のインポートエラー: {e}")
            show_fallback_dashboard()
        except Exception as e:
            st.error(f"❌ Looker機能のエラー: {e}")
            show_fallback_dashboard()
    else:
        show_fallback_dashboard()

def show_fallback_dashboard():
    """Looker機能が使用できない場合のフォールバック"""
    st.info("📊 Looker Studio連携機能は準備中です")
    
    # 基本的なダッシュボード代替機能
    st.markdown("### 📈 基本分析ダッシュボード")
    
    # 分析統計表示
    if "usage_stats" in st.session_state:
        stats = st.session_state.usage_stats
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("総分析数", stats.get("total_analyses", 0))
        with col2:
            st.metric("成功分析", stats.get("total_analyses", 0) - stats.get("error_count", 0))
        with col3:
            st.metric("高品質分析", stats.get("enhanced_usage", 0))
    
    # 最新分析結果表示
    if st.session_state.get("last_analysis_result") is not None:
        st.markdown("### 📊 最新の分析結果")
        df = st.session_state.last_analysis_result
        st.dataframe(df.head(10), width='stretch')
        
        # 基本統計
        if len(df.select_dtypes(include=['number']).columns) > 0:
            st.markdown("### 📈 基本統計")
            st.write(df.describe())
    
    st.info("💡 完全なLooker Studio機能を有効にするには、looker_handler.pyの設定を確認してください")

def show_semantic_search_ui():
    """セマンティック分析のUIを表示する"""
    st.markdown("---")
    st.subheader("🧠 類似キャンペーン検索")
    
    # BigQueryから全キャンペーン名を取得（キャッシュを活用）
    @st.cache_data(ttl=3600)
    def get_all_campaign_names(_bq_client): # bq_client -> _bq_client に変更
        if not _bq_client: # bq_client -> _bq_client に変更
            return []
        try:
            query = "SELECT DISTINCT CampaignName FROM `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign` WHERE CampaignName IS NOT NULL"
            df = _bq_client.query(query).to_dataframe() # bq_client -> _bq_client に変更
            return df["CampaignName"].tolist()
        except Exception as e:
            st.warning(f"キャンペーン名の取得に失敗: {e}")
            return []

    bq_client = st.session_state.get("bq_client")
    all_campaigns = get_all_campaign_names(bq_client)

    if not all_campaigns:
        st.warning("分析対象のキャンペーン名が取得できませんでした。BigQueryに接続してください。")
        return

    # 検索UI
    target_campaign = st.selectbox("基準となるキャンペーンを選択してください", options=all_campaigns)

    if st.button("類似キャンペーンを検索する", width='stretch'):
        if target_campaign:
            from semantic_analyzer import generate_embeddings, find_similar_texts
            
            # 全キャンペーン名のベクトルを生成（キャッシュが効く）
            all_embeddings = generate_embeddings(all_campaigns)
            
            if all_embeddings:
                # 類似キャンペーンを検索
                similar_campaigns_df = find_similar_texts(target_campaign, all_embeddings, top_n=5)
                
            if similar_campaigns_df is not None:
                st.success("類似キャンペーンが見つかりました:")
                # st.dataframe を以下のように変更
                st.dataframe(
                    similar_campaigns_df,
                    column_config={
                        "text": st.column_config.TextColumn("キャンペーン名", width="large"),
                        "similarity": st.column_config.ProgressColumn(
                            "関連性スコア",
                            help="基準テキストとの意味的な近さ（1に近いほど関連性が高い）",
                            format="%.3f",
                            min_value=0,
                            max_value=1,
                        ),
                    },
                    use_container_width=True,
                    hide_index=True
                )
        else:
            st.error("基準となるキャンペーンを選択してください。")

def show_auto_grouping_ui():
    """セマンティック分析による自動グルーピングUI (結果保持機能付き)"""
    st.markdown("---")
    st.subheader("🧠 広告クリエイティブ自動グルーピング")
    st.markdown("AIがデータの密度に基づき、意味的に自然なグループを自動で発見します。")

    # --- 1. 過去データのインポート（参照）機能 ---
    with st.expander("📂 過去のタグデータをアップロードして参照する"):
        uploaded_file = st.file_uploader(
            "保存したタグ情報CSVをアップロードしてください",
            type="csv",
            key="grouping_csv_uploader", # keyを追加してウィジェットを区別
            help="以前にこのアプリでダウンロードした `semantic_tags_...csv` ファイルを選択してください。"
        )
        if uploaded_file is not None:
            try:
                past_tags_df = pd.read_csv(uploaded_file)
                if 'ad_text' in past_tags_df.columns:
                    past_tags_df.rename(columns={'ad_text': 'analyzed_text'}, inplace=True)
                required_columns = ['analyzed_text', 'cluster_id', 'tag']
                if not all(col in past_tags_df.columns for col in required_columns):
                     st.error(f"❌ ファイルの形式が正しくありません。{', '.join(required_columns)} が含まれるCSVをアップロードしてください。")
                else:
                    st.success(f"✅ {len(past_tags_df)}件の過去のタグ情報を読み込みました。")
                    st.dataframe(past_tags_df, use_container_width=True)
            except Exception as e:
                st.error(f"ファイルの読み込みに失敗しました: {e}")

    st.markdown("---")

    bq_client = st.session_state.get("bq_client")
    if not bq_client:
        st.warning("BigQueryに接続してください。")
        return

    # --- 分析対象の定義 ---
    ANALYSIS_TARGETS = {
        "広告文 (Headline)": ("LookerStudio_report_ad", "Headline"),
        "キーワード": ("LookerStudio_report_keyword", "Keyword"),
        "広告グループ名": ("LookerStudio_report_ad_group", "AdGroupName_unified"),
        "キャンペーン名": ("LookerStudio_report_campaign", "CampaignName"),
    }

    selected_target_label = st.selectbox(
        "分析対象のテキスト列を選択してください",
        options=list(ANALYSIS_TARGETS.keys()),
        index=0
    )
    table_name, selected_column = ANALYSIS_TARGETS[selected_target_label]

    @st.cache_data(ttl=3600)
    def get_text_data(_bq_client, table_name, column_name):
        query = f"""
        SELECT DISTINCT {column_name} AS text_data
        FROM `vorn-digi-mktg-poc-635a.toki_air.{table_name}`
        WHERE {column_name} IS NOT NULL AND LENGTH({column_name}) > 5
        LIMIT 500
        """
        try:
            df = _bq_client.query(query).to_dataframe()
            return df["text_data"].tolist()
        except Exception as e:
            st.error(f"「{column_name}」列のデータ取得に失敗: {e}")
            return []

    ad_texts = get_text_data(bq_client, table_name, selected_column)
    if not ad_texts:
        st.info(f"分析対象のデータ（{selected_column}）がありません。")
        return

    min_cluster_size = st.slider(
        "グループの最小サイズ",
        min_value=2, max_value=10, value=3,
        help="何個以上の広告文が集まったら一つのグループと見なすかを設定します。"
    )

    # ▼▼▼【ここからが今回の修正箇所です】▼▼▼

    # --- 実行ボタンが押された時の処理 ---
    if st.button("🚀 グルーピング実行", type="primary"):
        with st.spinner("AIによるグルーピングを実行中です..."):
            from semantic_analyzer import group_texts_by_meaning, extract_tags_for_cluster, reduce_dimensions_for_visualization, generate_embeddings
            import pandas as pd

            grouped_df = group_texts_by_meaning(ad_texts, min_cluster_size=min_cluster_size)

            if grouped_df is not None:
                grouped_df_for_tags = grouped_df[grouped_df['cluster'] != -1].copy()
                gemini_model = st.session_state.get("gemini_model")
                cluster_tags = extract_tags_for_cluster(grouped_df_for_tags, gemini_model)
                cluster_themes = {cluster_id: ", ".join(tags) for cluster_id, tags in cluster_tags.items()}
                cluster_themes[-1] = "ノイズ / 分類外"

                embeddings_dict = generate_embeddings(grouped_df['text'].tolist())
                vis_df = None
                if embeddings_dict:
                    vis_df_raw = reduce_dimensions_for_visualization(embeddings_dict)
                    if vis_df_raw is not None:
                        vis_df = pd.merge(vis_df_raw, grouped_df, on='text')
                        vis_df['theme'] = vis_df['cluster'].map(cluster_themes)

                # 結果をセッション状態に保存
                st.session_state.grouping_results = {
                    "grouped_df": grouped_df,
                    "cluster_themes": cluster_themes,
                    "vis_df": vis_df,
                    "cluster_tags": cluster_tags,
                    "selected_column": selected_column # 分析対象列も保存
                }
            else:
                st.session_state.grouping_results = None
                st.error("グルーピングに失敗しました。")

    # --- セッション状態に保存された結果があれば表示する ---
    if "grouping_results" in st.session_state and st.session_state.grouping_results:
        import plotly.express as px
        import pandas as pd

        # セッションから結果を読み込む
        results = st.session_state.grouping_results
        grouped_df = results["grouped_df"]
        cluster_themes = results["cluster_themes"]
        vis_df = results["vis_df"]
        cluster_tags = results["cluster_tags"]
        selected_column = results["selected_column"]

        st.subheader("📊 グルーピング結果")

        # 可視化
        if vis_df is not None:
            fig = px.scatter(
                vis_df, x='x', y='y', color='theme', hover_name='text',
                title='広告クリエイティブの概念マップ', labels={'color': 'グループテーマ'},
                color_discrete_map={"ノイズ / 分類外": "lightgrey"}
            )
            fig.update_layout(legend_title_text='<b>概念グループ</b>')
            st.plotly_chart(fig, use_container_width=True)

        # 各クラスタの詳細
        for cluster_id in sorted(grouped_df['cluster'].unique()):
            if cluster_id == -1: continue
            theme_name = cluster_themes.get(cluster_id, f"グループ {cluster_id + 1}")
            with st.expander(f"**{theme_name}** ({len(grouped_df[grouped_df['cluster'] == cluster_id])}件)"):
                st.dataframe(grouped_df[grouped_df['cluster'] == cluster_id][['text']], use_container_width=True)

        noise_df = grouped_df[grouped_df['cluster'] == -1]
        if not noise_df.empty:
            with st.expander(f"**ノイズ / 分類外** ({len(noise_df)}件)"):
                st.dataframe(noise_df[['text']], use_container_width=True)
        
        # ダウンロード機能
        st.markdown("---")
        st.subheader("💾 今回の分析結果を保存")
        
        tags_to_save = []
        for cluster_id, tags in cluster_tags.items():
            texts_in_cluster = grouped_df[grouped_df['cluster'] == cluster_id]['text']
            for text in texts_in_cluster:
                for tag in tags:
                    tags_to_save.append({
                        "analyzed_text": text,
                        "cluster_id": cluster_id,
                        "tag": tag,
                        "analysis_timestamp": pd.Timestamp.now(tz="Asia/Tokyo").isoformat()
                    })
        save_df = pd.DataFrame(tags_to_save)

        if not save_df.empty:
            save_df['analysis_target_column'] = selected_column
            new_column_order = ['analysis_target_column', 'analyzed_text', 'cluster_id', 'tag', 'analysis_timestamp']
            save_df = save_df[new_column_order]

        st.download_button(
            label="📥 このタグ付け結果をCSVでダウンロード",
            data=save_df.to_csv(index=False, encoding='utf-8-sig'),
            file_name=f"semantic_tags_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
            mime='text/csv',
        )

def show_ai_mode():
    """AI分析モード"""
    if IMPORT_STATUS["ui_main"]:
        # セッション状態から必要な情報を取得
        gemini_model = st.session_state.get("gemini_model")
        claude_client = st.session_state.get("claude_client")
        claude_model_name = st.session_state.get("claude_model_name")
        
        # SHEET_ANALYSIS_QUERIESの取得
        sheet_analysis_queries = globals().get("SHEET_ANALYSIS_QUERIES", {})
        
        show_analysis_workbench(gemini_model, claude_client, claude_model_name, sheet_analysis_queries)

        # セマンティック分析が有効な場合のみ、検索UIを表示
        if st.session_state.get("use_semantic_analysis", False):
            show_semantic_search_ui() # 既存の類似検索UI
            show_auto_grouping_ui()   # 新しく追加するグルーピングUI
    else:
        st.error("❌ AI分析機能が利用できません")

def show_manual_mode():
    """手動SQL実行モード"""
    if IMPORT_STATUS["ui_main"]:
        show_manual_sql_interface()
    else:
        st.error("❌ 手動SQL機能が利用できません")

def show_monitoring_dashboard():
    """使用状況やエラー履歴を表示する監視ダッシュボード"""
    st.header("📈 監視ダッシュボード")

    if st.button("🔄 最新の情報に更新"):
        st.rerun()

    st.subheader("📊 使用状況サマリー")
    
    # st.session_stateから統計情報を取得
    stats = st.session_state.get("usage_stats", {})
    total_analyses = stats.get("total_analyses", 0)
    error_count = stats.get("error_count", 0)
    enhanced_usage = stats.get("enhanced_usage", 0)
    
    success_rate = ((total_analyses - error_count) / total_analyses * 100) if total_analyses > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("総分析回数", f"{total_analyses} 回")
    with col2:
        st.metric("エラー回数", f"{error_count} 回")
    with col3:
        st.metric("成功率", f"{success_rate:.1f} %")
    with col4:
        st.metric("高品質分析", f"{enhanced_usage} 回")

    st.subheader("⚠️ エラー履歴")
    
    error_history = st.session_state.get("error_history", [])
    if not error_history:
        st.success("✅ これまでに記録されたエラーはありません。")
    else:
        # 直近5件のエラーを表示
        with st.expander(f"直近のエラー履歴 ({len(error_history)}件)"):
            for i, error_info in enumerate(reversed(error_history[-5:])):
                st.error(f"**エラー #{len(error_history)-i}:** {error_info.get('timestamp')}")
                st.code(error_info.get('error_message', '詳細不明'), language='text')

def show_environment_debug_page():
    """
    アプリケーションの実行環境を徹底的に自己診断するためのデバッグページ
    """
    st.header("🔬 環境自己診断レポート")

    st.subheader("1. Python実行環境")
    st.markdown("現在このStreamlitアプリを動かしているPythonの実行ファイルです。")
    st.code(sys.executable, language="text")

    st.subheader("2. ライブラリ検索パス")
    st.markdown("Pythonがライブラリを探しに行くフォルダの一覧です。この中に`venv\\Lib\\site-packages`が含まれているか確認してください。")
    st.json(sys.path)

    st.subheader("3. 実際にインストールされているライブラリ")
    st.markdown("上記のPython環境に、現在インストールされているライブラリの一覧です。")
    st.info("「`pip freeze`の結果をここに出力」ボタンを押してください。")

    if st.button("`pip freeze`の結果をここに出力"):
        import subprocess
        try:
            with st.spinner("ライブラリ一覧を取得中..."):
                # sys.executable を使って、現在実行中のPythonでpipを実行する
                result = subprocess.run(
                    [sys.executable, "-m", "pip", "freeze"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                st.code(result.stdout, language="text")
        except Exception as e:
            st.error(f"pip freezeの実行に失敗しました: {e}")
            st.code(traceback.format_exc())

    st.subheader("4. `google-cloud-aiplatform` の直接インポートテスト")
    st.markdown("このボタンを押すと、このアプリが `google-cloud-aiplatform` を直接インポートできるか試します。")
    if st.button("インポートテスト実行"):
        try:
            with st.spinner("インポートを試みています..."):
                from google.cloud import aiplatform
                st.success("✅ `from google.cloud import aiplatform` のインポートに成功しました！")
                st.write(f"インポートされたモジュールの場所: `{aiplatform.__file__}`")
        except Exception as e:
            st.error("❌ インポートに失敗しました。")
            st.code(traceback.format_exc())

def show_glossary_ui():
    """サイドバーに用語集を表示するUI"""
    with st.sidebar.expander("📖 ビジネス用語集"):
        try:
            # 必要なライブラリをここでインポート
            import pandas as pd
            from pathlib import Path

            glossary_path = Path("glossary.csv")
            if glossary_path.exists():
                df = pd.read_csv(glossary_path)
                # hide_index=True でDataFrameのインデックス（0, 1, 2...）を非表示にする
                st.dataframe(df, hide_index=True)
                st.caption("この用語集は `glossary.csv` を編集することで更新できます。")
            else:
                st.info("`glossary.csv` が見つかりません。")
        except Exception as e:
            st.error(f"用語集の表示中にエラーが発生: {e}")

# =========================================================================
# メイン処理
# =========================================================================

def main():
    """メイン処理関数（設定対応版）"""
    
    # ヘッダー表示
    st.title("🚀 AIデジタルマーケティング分析プラットフォーム")
    
    if SETTINGS_AVAILABLE:
        st.success("✅ 設定管理システム稼働中")
    else:
        st.warning("⚠️ フォールバックモードで動作中（設定管理システム未使用）")
    
    # セッション状態初期化
    ensure_session_state()
    
    # システム状態・設定パネル
    col1, col2 = st.columns([3, 1])
    with col2:
        show_system_status()
        show_settings_panel()
    
    # サイドバー設定
    with st.sidebar:
        st.header("🎛️ システム制御")
        
        # 表示モード選択
        view_options = ["📊 ダッシュボード表示", "🤖 AI分析", "⚙️ 手動SQL実行", "🩺 システム診断", "📈 監視ダッシュボード", "🔬 環境デバッグ"]
        st.session_state.view_mode = st.selectbox(
            "表示モード選択",
            view_options,
            index=view_options.index(st.session_state.get("view_mode", "📊 ダッシュボード表示"))
        )
        
        st.markdown("---")
        
        # API接続設定
        st.markdown("### 🔌 API接続")
        
        # BigQuery接続
        if st.button("🔄 BigQuery接続", width='stretch'):
            try: # ← try を追加
                with st.spinner("BigQuery接続中..."):
                    bq_client = setup_bigquery_client()
                    if bq_client:
                        st.session_state.bq_client = bq_client
            except Exception as e: # ← except を追加
                handle_error_with_ai(e, st.session_state.get("gemini_model"), {"operation": "BigQuery接続ボタン"})
        
        # Gemini接続
        if st.button("🔄 Gemini接続", width='stretch'):
            try: # ← try を追加
                with st.spinner("Gemini API接続中..."):
                    gemini_model = setup_gemini_client()
                    if gemini_model:
                        st.session_state.gemini_model = gemini_model
            except Exception as e: # ← except を追加
                handle_error_with_ai(e, None, {"operation": "Gemini接続ボタン"})

        # Claude接続
        if st.button("🔄 Claude接続", width='stretch'):
            try: # ← try を追加
                with st.spinner("Claude API接続中..."):
                    claude_client, claude_model_name = setup_claude_client()
                    if claude_client and claude_model_name:
                        st.session_state.claude_client = claude_client
                        st.session_state.claude_model_name = claude_model_name
            except Exception as e: # ← except を追加
                handle_error_with_ai(e, st.session_state.get("gemini_model"), {"operation": "Claude接続ボタン"})

        st.markdown("---")

        if st.button("⚙️ システム設定", width='stretch'):
            st.session_state.show_config_panel = True
            st.rerun()
        
        # 用語集表示UIを呼び出す
        show_glossary_ui()

        # --- ↓↓↓ ここからが追加するコードです ↓↓↓ ---
        st.markdown("---")
        st.markdown("### 🧠 AI拡張機能")

        # セマンティック分析のオン/オフトグル
        st.session_state.use_semantic_analysis = st.toggle(
            "セマンティック分析を有効にする",
            value=st.session_state.get("use_semantic_analysis", False), # デフォルトはオフ
            help="オンにすると、キャンペーン名や広告文の意味的な類似性分析などが可能になりますが、処理に時間がかかる場合があります。"
        )
        
        # デバッグ設定
        # Streamlitのkeyを使ってウィジェットとセッション状態を直接紐付ける
        st.checkbox(
            "🐛 デバッグモード",
            key="debug_mode",
            help="オンにすると、エラー発生時にAIの内部的な応答やセッション状態などの詳細情報が表示されます。"
        )

        if st.session_state.debug_mode:
            st.markdown("**🔍 デバッグ情報**")
            
            # 簡易サマリー表示
            st.json({
                "セッション状態キー数": len(st.session_state.keys()),
                "設定システム": "✅ 利用可能" if SETTINGS_AVAILABLE else "❌ 利用不可",
                "最後の分析": st.session_state.get("last_user_input", "なし")[:50] + "..."
            })
            
            # セッション状態の全内容表示機能を追加
            with st.expander("セッション状態（st.session_state）の全内容を表示"):
                st.json(st.session_state.to_dict(), expanded=False)


    # 🔧 設定パネル表示処理を追加
    if st.session_state.get("show_config_panel", False):
        if CONFIG_UI_AVAILABLE:
            show_config_panel()
        else:
            st.error("設定管理システムが利用できません")
    
        if st.button("❌ 設定パネルを閉じる"):
            st.session_state.show_config_panel = False
            st.rerun()
        return

    # メインコンテンツ表示
    with col1:
        try:
            # ▼▼▼【重要】ここからが修正箇所 ▼▼▼
            # どのモードよりも先に、修正案レビュー画面を表示するかを最優先でチェック
            if st.session_state.get("show_fix_review"):
                from ui_main import show_sql_fix_review_ui
                show_sql_fix_review_ui()
            
            # 修正案レビュー画面を表示しない場合に、通常のモード別画面を表示
            elif st.session_state.view_mode == "📊 ダッシュボード表示":
                show_dashboard_mode()
            elif st.session_state.view_mode in ["🤖 AI分析", "⚙️ 手動SQL実行"]:
                show_ai_mode()
            elif st.session_state.view_mode == "🩺 システム診断":
                diagnostics.run_all_checks(
                    settings=settings,
                    bq_client=st.session_state.get("bq_client"),
                    gemini_model=st.session_state.get("gemini_model"),
                    claude_client=st.session_state.get("claude_client")
                )      
            elif st.session_state.view_mode == "📈 監視ダッシュボード":
                show_monitoring_dashboard()
            elif st.session_state.view_mode == "🔬 環境デバッグ":
                show_environment_debug_page()
            # ▲▲▲ 修正ここまで ▲▲▲

        except Exception as e:
            st.error(f"❌ 表示モードエラー: {str(e)}")
            if st.session_state.debug_mode:
                st.code(traceback.format_exc())
    
    # フッター情報
    st.markdown("---")
    footer_col1, footer_col2, footer_col3 = st.columns(3)
    with footer_col1:
        st.markdown("**🚀 Version**: 2.0.0-config")
    with footer_col2:
        if SETTINGS_AVAILABLE:
            st.markdown(f"**🤖 Models**: {settings.ai.gemini_model}, {settings.ai.claude_model}")
        else:
            st.markdown("**🤖 Models**: Default")
    with footer_col3:
        st.markdown(f"**⏰ Last Update**: {dt.now().strftime('%Y-%m-%d %H:%M')}")

# =========================================================================
# アプリケーション実行
# =========================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ アプリケーションエラー: {str(e)}")
        st.markdown("### 🔧 トラブルシューティング")
        st.markdown("""
        1. **設定ファイル確認**: `.env` ファイルと `config/` ディレクトリが正しく配置されているか
        2. **API キー設定**: 環境変数またはStreamlit Secretsで API キーが設定されているか  
        3. **依存関係**: 必要なライブラリがインストールされているか
        4. **権限確認**: BigQuery等のサービスへのアクセス権限があるか
        """)
        
        if st.checkbox("🐛 詳細なエラー情報を表示"):
            st.code(traceback.format_exc())

def show_glossary_ui():
    """サイドバーに用語集を表示するUI"""
    with st.sidebar.expander("📖 ビジネス用語集"):
        try:
            # 必要なライブラリをここでインポート
            import pandas as pd
            from pathlib import Path

            glossary_path = Path("glossary.csv")
            if glossary_path.exists():
                df = pd.read_csv(glossary_path)
                # hide_index=True でDataFrameのインデックス（0, 1, 2...）を非表示にする
                st.dataframe(df, hide_index=True)
                st.caption("この用語集は `glossary.csv` を編集することで更新できます。")
            else:
                st.info("`glossary.csv` が見つかりません。")
        except Exception as e:
            st.error(f"用語集の表示中にエラーが発生: {e}")