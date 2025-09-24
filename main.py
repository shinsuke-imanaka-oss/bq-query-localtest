# main.py - 完全版（全機能統合・エラーハンドリング強化）
"""
🚀 デジタルマーケティング分析プラットフォーム - メインエントリーポイント
- BigQuery × AI（Gemini + Claude）統合分析システム
- 3つの表示モード: ダッシュボード・ワークスペース・AIアシスタント
- 完全なエラーハンドリングとフォールバック機能
- セッション管理・履歴機能・フィルタリング統合
"""

import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import traceback

# Google Cloud & AI関連
from google.cloud import bigquery
from google.oauth2 import service_account
import google.generativeai as genai
import anthropic

# 可視化
import plotly.express as px
import plotly.graph_objects as go

# =========================================================================
# ページ設定・初期化
# =========================================================================

st.set_page_config(
    page_title="🚀 デジタルマーケティング分析プラットフォーム",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': """
        # 🚀 デジタルマーケティング分析プラットフォーム
        
        **AI駆動の次世代データ分析システム**
        
        - 🤖 **Gemini**: 自然言語→SQL変換
        - 🧠 **Claude**: データ解釈・戦略提案
        - 📊 **BigQuery**: 高性能データ処理
        - 📈 **Looker Studio**: 統合ダッシュボード
        """
    }
)

# =========================================================================
# 分割ファイルからのインポート（堅牢なエラーハンドリング）
# =========================================================================

# インポート状況追跡用辞書
IMPORT_STATUS = {
    "prompts": False,
    "enhanced_prompts": False,
    "ui_main": False,
    "ui_features": False,
    "looker_handler": False,
    "analysis_controller": False,
    "analysis_logic": False,
    "error_handler": False,
    "data_quality_checker": False
}

# 基本プロンプトシステム（最優先）
try:
    from prompts import *
    IMPORT_STATUS["prompts"] = True
    print("✅ prompts.py インポート成功")
except ImportError as e:
    print(f"⚠️ prompts.py インポートエラー: {e}")
    # フォールバック：基本関数定義
    def select_best_prompt(user_input: str) -> dict:
        return {
            "description": "基本分析",
            "template": f"以下の分析を実行してください: {user_input}",
            "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign"
        }
    
    GENERAL_SQL_TEMPLATE = "SELECT * FROM `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign` LIMIT 10"
    MODIFY_SQL_TEMPLATE = "SQLを修正してください: {original_sql}\n指示: {modification_instruction}"
    CLAUDE_COMMENT_PROMPT_TEMPLATE = "以下のデータを分析してください: {data_sample}"

# 強化プロンプトシステム
try:
    from enhanced_prompts import *
    IMPORT_STATUS["enhanced_prompts"] = True
    print("✅ enhanced_prompts.py インポート成功")
except ImportError as e:
    print(f"⚠️ enhanced_prompts.py インポートエラー: {e}")
    # フォールバック関数
    def generate_enhanced_sql_prompt(*args, **kwargs):
        return select_best_prompt(args[0] if args else "基本分析")
    def generate_enhanced_claude_prompt(*args, **kwargs):
        return "以下のデータを詳細に分析してください"

# メインUI
try:
    from ui_main import show_analysis_workbench, initialize_main_session_state
    IMPORT_STATUS["ui_main"] = True
    print("✅ ui_main.py インポート成功")
except ImportError as e:
    print(f"❌ ui_main.py インポートエラー: {e}")
    st.error("❌ ui_main.py が見つかりません。アプリケーションを正常に動作させるには必要です。")
    st.stop()

# UI機能パネル
try:
    from ui_features import (
        initialize_analysis_tracking,
        log_analysis_usage,
        add_error_to_history,
        show_analysis_summary_panel,
        show_data_quality_panel
    )
    IMPORT_STATUS["ui_features"] = True
    print("✅ ui_features.py インポート成功")
except ImportError as e:
    print(f"⚠️ ui_features.py インポートエラー: {e}")
    # フォールバック関数
    def initialize_analysis_tracking():
        if "usage_stats" not in st.session_state:
            st.session_state.usage_stats = {"total_analyses": 0, "error_count": 0, "success_count": 0}
    
    def log_analysis_usage(user_input: str, system_type: str, execution_time: float = 0, error: bool = False):
        if "usage_stats" not in st.session_state:
            initialize_analysis_tracking()
        st.session_state.usage_stats["total_analyses"] += 1
        if error:
            st.session_state.usage_stats["error_count"] += 1
        else:
            st.session_state.usage_stats["success_count"] += 1
    
    def add_error_to_history(error_message: str, error_category: str = "一般エラー", solutions: List[str] = None):
        if "error_history" not in st.session_state:
            st.session_state.error_history = []
        st.session_state.error_history.append({
            "timestamp": datetime.now(),
            "message": error_message,
            "category": error_category,
            "solutions": solutions or []
        })
    
    def show_analysis_summary_panel():
        st.info("📊 分析サマリー機能は一時的に利用できません")
    
    def show_data_quality_panel():
        st.info("🔍 データ品質チェック機能は一時的に利用できません")

# Looker Studio統合
try:
    from looker_handler import (
        show_looker_studio_integration,
        show_filter_ui, # show_filter_ui をインポートリストに追加
        init_filters
    )
    IMPORT_STATUS["looker_handler"] = True
    print("✅ looker_handler.py インポート成功")
except ImportError as e:
    print(f"⚠️ looker_handler.py インポートエラー: {e}")
    # フォールバック関数
    def show_looker_studio_integration(*args, **kwargs):
        st.error("❌ Looker Studio統合機能が利用できません")
    
    def init_filters():
        if "filters" not in st.session_state:
            st.session_state.filters = {
                "sheet": "メディア",
                "start_date": datetime.now().date() - timedelta(days=30),
                "end_date": datetime.now().date(),
                "media": [],
                "campaigns": []
            }

# 分析制御
try:
    from analysis_controller import (
        run_analysis_flow,
        execute_sql_with_enhanced_handling,
        show_manual_sql_input
    )
    IMPORT_STATUS["analysis_controller"] = True
    print("✅ analysis_controller.py インポート成功")
except ImportError as e:
    print(f"⚠️ analysis_controller.py インポートエラー: {e}")
    # フォールバック関数
    def run_analysis_flow(*args, **kwargs):
        st.error("❌ 分析制御機能が利用できません")
        return False
    
    def execute_sql_with_enhanced_handling(client, sql):
        try:
            return client.query(sql).to_dataframe()
        except Exception as e:
            st.error(f"SQL実行エラー: {e}")
            return None

# エラーハンドリング
try:
    from error_handler import EnhancedErrorHandler, show_enhanced_error_message
    IMPORT_STATUS["error_handler"] = True
    print("✅ error_handler.py インポート成功")
except ImportError as e:
    print(f"⚠️ error_handler.py インポートエラー: {e}")
    def show_enhanced_error_message(error_message: str, error_type: str = "一般エラー"):
        st.error(f"❌ {error_type}: {error_message}")

# データ品質チェック
try:
    from data_quality_checker import generate_quality_report as run_comprehensive_data_quality_check
    IMPORT_STATUS["data_quality_checker"] = True
    print("✅ data_quality_checker.py インポート成功")
except ImportError as e:
    print(f"⚠️ data_quality_checker.py インポートエラー: {e}")
    def run_comprehensive_data_quality_check(*args, **kwargs):
        st.info("📊 データ品質チェック機能は一時的に利用できません")

# =========================================================================
# APIクライアント設定・認証
# =========================================================================

def setup_bigquery_client():
    """BigQueryクライアントのセットアップ（改良版）"""
    try:
        # 認証方法の優先順位:
        # 1. Streamlit Secrets
        # 2. 環境変数
        # 3. デフォルト認証
        
        if "gcp_service_account" in st.secrets:
            # Streamlit Secrets使用
            credentials_info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            project_id = credentials_info.get("project_id")
            client = bigquery.Client(credentials=credentials, project=project_id)
            st.success(f"✅ BigQuery接続成功 (Secrets) - プロジェクト: {project_id}")
            return client
        
        elif "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
            # サービスアカウントファイル使用
            client = bigquery.Client()
            st.success(f"✅ BigQuery接続成功 (環境変数) - プロジェクト: {client.project}")
            return client
        
        else:
            # デフォルト認証試行
            client = bigquery.Client()
            st.success(f"✅ BigQuery接続成功 (デフォルト) - プロジェクト: {client.project}")
            return client
            
    except Exception as e:
        st.error(f"❌ BigQuery接続エラー: {str(e)}")
        
        # 詳細なエラー情報とソリューション
        with st.expander("🔧 BigQuery接続設定ヘルプ"):
            st.markdown("""
            ## 📋 BigQuery接続設定方法
            
            ### 方法1: Streamlit Secrets (推奨)
            `.streamlit/secrets.toml` に以下を追加:
            ```toml
            [gcp_service_account]
            type = "service_account"
            project_id = "your-project-id"
            private_key_id = "..."
            private_key = "..."
            client_email = "..."
            # ... その他のサービスアカウント情報
            ```
            
            ### 方法2: 環境変数
            ```bash
            export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
            ```
            
            ### 方法3: Google Cloud Shell / Colab
            デフォルト認証が自動的に適用されます。
            """)
        
        return None

def setup_gemini_client():
    """Geminiクライアントのセットアップ（改良版）"""
    try:
        # API Key取得の優先順位
        if "GEMINI_API_KEY" in st.secrets:
            api_key = st.secrets["GEMINI_API_KEY"]
        elif "GOOGLE_API_KEY" in st.secrets:
            api_key = st.secrets["GOOGLE_API_KEY"]
        elif "GEMINI_API_KEY" in os.environ:
            api_key = os.environ["GEMINI_API_KEY"]
        elif "GOOGLE_API_KEY" in os.environ:
            api_key = os.environ["GOOGLE_API_KEY"]
        else:
            raise ValueError("Gemini API Keyが設定されていません")
        
        # Gemini設定
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-2.0-flash-001")
        
        st.success("✅ Gemini API接続成功")
        return model
        
    except Exception as e:
        st.error(f"❌ Gemini API接続エラー: {str(e)}")
        
        with st.expander("🔧 Gemini API設定ヘルプ"):
            st.markdown("""
            ## 🤖 Gemini API設定方法
            
            ### Streamlit Secrets設定
            `.streamlit/secrets.toml` に以下を追加:
            ```toml
            GEMINI_API_KEY = "your-gemini-api-key"
            ```
            
            ### 環境変数設定
            ```bash
            export GEMINI_API_KEY="your-gemini-api-key"
            ```
            
            ### API Key取得方法
            1. [Google AI Studio](https://makersuite.google.com/app/apikey) にアクセス
            2. 「Create API Key」をクリック
            3. 生成されたキーをコピーして設定
            """)
        
        return None

def setup_claude_client():
    """Claudeクライアントのセットアップ（改良版）"""
    try:
        # API Key取得の優先順位
        if "ANTHROPIC_API_KEY" in st.secrets:
            api_key = st.secrets["ANTHROPIC_API_KEY"]
        elif "ANTHROPIC_API_KEY" in os.environ:
            api_key = os.environ["ANTHROPIC_API_KEY"]
        else:
            raise ValueError("Claude API Keyが設定されていません")
        
        client = anthropic.Anthropic(api_key=api_key)
        model_name = "claude-3-sonnet-20240229"
        
        # 接続テスト
        test_response = client.messages.create(
            model=model_name,
            max_tokens=10,
            messages=[{"role": "user", "content": "Hello"}]
        )
        
        st.success("✅ Claude API接続成功")
        return client, model_name
        
    except Exception as e:
        st.error(f"❌ Claude API接続エラー: {str(e)}")
        
        with st.expander("🔧 Claude API設定ヘルプ"):
            st.markdown("""
            ## 🧠 Claude API設定方法
            
            ### Streamlit Secrets設定
            `.streamlit/secrets.toml` に以下を追加:
            ```toml
            ANTHROPIC_API_KEY = "your-claude-api-key"
            ```
            
            ### 環境変数設定
            ```bash
            export ANTHROPIC_API_KEY="your-claude-api-key"
            ```
            
            ### API Key取得方法
            1. [Anthropic Console](https://console.anthropic.com/) にアクセス
            2. アカウント作成・ログイン
            3. API Keysページで新しいキーを生成
            """)
        
        return None, None

def setup_clients():
    """全APIクライアントのセットアップ"""
    success_count = 0
    
    # BigQuery
    bq_client = setup_bigquery_client()
    if bq_client:
        st.session_state.bq_client = bq_client
        success_count += 1
    
    # Gemini
    gemini_model = setup_gemini_client()
    if gemini_model:
        st.session_state.gemini_model = gemini_model
        success_count += 1
    
    # Claude
    claude_client, claude_model_name = setup_claude_client()
    if claude_client:
        st.session_state.claude_client = claude_client
        st.session_state.claude_model_name = claude_model_name
        success_count += 1
    
    # 成功状況の表示
    if success_count == 3:
        st.success("🎉 全APIクライアントの接続に成功しました！")
        return True
    elif success_count >= 1:
        st.warning(f"⚠️ {success_count}/3個のAPIクライアントが接続されました。一部機能が制限されます。")
        return True
    else:
        st.error("❌ APIクライアントの接続に失敗しました。設定を確認してください。")
        return False

# =========================================================================
# セッション状態初期化
# =========================================================================

def initialize_session_state():
    """セッション状態の包括的初期化"""
    
    # 基本セッション状態
    basic_states = {
        "view_mode": "📊 ダッシュボード表示",
        "show_filters": True,
        "apply_streamlit_filters": True,
        "debug_mode": False,
        "filters_changed": False,
        "use_enhanced_prompts": True,
        "selected_ai": "Gemini + Claude",
        "auto_claude_analysis": True,
        "show_sql": True,
        "show_debug_info": False
    }
    
    for key, default_value in basic_states.items():
        if key not in st.session_state:
            st.session_state[key] = default_value
    
    # フィルタ初期化
    if "filters" not in st.session_state:
        st.session_state.filters = {
            "sheet": "メディア",
            "start_date": datetime.now().date() - timedelta(days=30),
            "end_date": datetime.now().date(),
            "media": [],
            "campaigns": []
        }
    
    # 分析結果初期化
    analysis_states = {
        "df": pd.DataFrame(),
        "sql": "",
        "user_input": "",
        "comment": "",
        "graph_cfg": {},
        "selected_recipe": "TOP10キャンペーン分析"
    }
    
    for key, default_value in analysis_states.items():
        if key not in st.session_state:
            st.session_state[key] = default_value
    
    # 履歴・統計初期化
    history_states = {
        "history": [],
        "error_history": [],
        "usage_stats": {
            "total_analyses": 0,
            "success_count": 0,
            "error_count": 0,
            "total_execution_time": 0.0,
            "last_analysis_time": None
        }
    }
    
    for key, default_value in history_states.items():
        if key not in st.session_state:
            st.session_state[key] = default_value
    
    # UI機能初期化
    ui_states = {
        "show_advanced_settings": False,
        "show_system_status": False,
        "tutorial_completed": False
    }
    
    for key, default_value in ui_states.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

# =========================================================================
# フィルタUI統合
# =========================================================================

def show_filter_ui(bq_client):
    """統合フィルタUI"""
    try:
        if IMPORT_STATUS["looker_handler"]:
            # looker_handler.pyの関数を使用
            from looker_handler import show_filter_ui as show_filter_ui_handler
            show_filter_ui_handler(bq_client)
        else:
            # フォールバック：基本フィルタUI
            show_basic_filter_ui(bq_client)
    except Exception as e:
        st.sidebar.error(f"❌ フィルタUI表示エラー: {e}")
        show_basic_filter_ui(bq_client)

def show_basic_filter_ui(bq_client):
    """基本フィルタUI（フォールバック用）"""
    st.sidebar.markdown("### 📊 フィルタ設定")
    
    # 日付範囲
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input(
            "開始日",
            value=st.session_state.filters.get("start_date", datetime.now().date() - timedelta(days=30)),
            key="filter_start_date"
        )
    with col2:
        end_date = st.date_input(
            "終了日",
            value=st.session_state.filters.get("end_date", datetime.now().date()),
            key="filter_end_date"
        )
    
    # メディア選択（簡易版）
    media_options = ["Google広告", "Facebook広告", "Yahoo!広告", "Instagram広告", "Twitter広告"]
    selected_media = st.sidebar.multiselect(
        "メディア",
        options=media_options,
        default=st.session_state.filters.get("media", []),
        key="filter_media"
    )
    
    # フィルタ更新
    st.session_state.filters.update({
        "start_date": start_date,
        "end_date": end_date,
        "media": selected_media
    })

# =========================================================================
# 表示モード別関数
# =========================================================================

def show_dashboard_mode():
    """📊 ダッシュボード表示モード"""
    st.title("📊 Looker Studio ダッシュボード")
    
    # 現在のフィルタ状況表示
    filters = st.session_state.get("filters", {})
    filter_info = []
    
    if filters.get("media"):
        filter_info.append(f"メディア: {len(filters['media'])}件")
    if filters.get("campaigns"):
        filter_info.append(f"キャンペーン: {len(filters['campaigns'])}件")
    if filters.get("start_date") and filters.get("end_date"):
        filter_info.append(f"期間: {filters['start_date']} ～ {filters['end_date']}")
    
    if filter_info:
        st.info(f"🎯 適用中フィルタ: {', '.join(filter_info)}")
    
    # フィルタ適用制御
    col1, col2 = st.columns([3, 1])
    with col1:
        current_sheet = filters.get("sheet", "メディア")
        st.subheader(f"現在表示中: {current_sheet}")
    
    with col2:
        filter_enabled = st.checkbox(
            "フィルタ適用",
            value=st.session_state.get("apply_streamlit_filters", True),
            help="Streamlitのフィルタ設定をLooker Studioに反映します",
            key="dashboard_filter_toggle"
        )
        if filter_enabled != st.session_state.get("apply_streamlit_filters"):
            st.session_state.apply_streamlit_filters = filter_enabled
            st.rerun()
    
    # Looker Studio統合表示
    try:
        if IMPORT_STATUS["looker_handler"] and st.session_state.get("bq_client"):
            show_looker_studio_integration(
                st.session_state.bq_client,
                st.session_state.get("gemini_model"),
                key_prefix="dashboard",
                sheet_analysis_queries={}
            )
        else:
            st.warning("⚠️ Looker Studio統合が利用できません。APIクライアントの設定を確認してください。")
            
            # フォールバック：基本ダッシュボード
            show_fallback_dashboard()
            
    except Exception as e:
        st.error(f"❌ ダッシュボード表示エラー: {e}")
        show_fallback_dashboard()

def show_fallback_dashboard():
    """フォールバック用の基本ダッシュボード"""
    st.info("📊 基本ダッシュボード表示中")
    
    if not st.session_state.get("df", pd.DataFrame()).empty:
        df = st.session_state.df
        
        # 基本統計
        st.subheader("📈 基本統計")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if "Cost" in df.columns:
                total_cost = df["Cost"].sum()
                st.metric("総コスト", f"¥{total_cost:,.0f}")
        
        with col2:
            if "Clicks" in df.columns:
                total_clicks = df["Clicks"].sum()
                st.metric("総クリック数", f"{total_clicks:,.0f}")
        
        with col3:
            if "Conversions" in df.columns:
                total_conversions = df["Conversions"].sum()
                st.metric("総コンバージョン数", f"{total_conversions:,.0f}")
        
        with col4:
            if "CTR" in df.columns:
                avg_ctr = df["CTR"].mean()
                st.metric("平均CTR", f"{avg_ctr:.2%}")
        
        # データ表示
        st.subheader("📋 データ")
        st.dataframe(df.head(20), use_container_width=True)
    else:
        st.info("📊 分析を実行してデータを表示してください")

def show_ai_assistant_mode():
    """🤖 AIアシスタント分析モード"""
    # 現在のフィルタ状況を表示
    filters = st.session_state.get("filters", {})
    if filters.get("media") or filters.get("campaigns"):
        with st.expander("🎯 現在適用中のフィルタ", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                if filters.get("media"):
                    st.write("**メディア:**")
                    for media in filters["media"]:
                        st.write(f"• {media}")
            with col2:
                if filters.get("campaigns"):
                    st.write("**キャンペーン:**")
                    for campaign in filters["campaigns"][:5]:  # 最大5件表示
                        st.write(f"• {campaign}")
                    if len(filters["campaigns"]) > 5:
                        st.write(f"... 他{len(filters['campaigns']) - 5}件")
    
    # メイン分析ワークベンチの表示
    try:
        if IMPORT_STATUS["ui_main"]:
            show_analysis_workbench(
                st.session_state.get("gemini_model"),
                st.session_state.get("claude_client"),
                st.session_state.get("claude_model_name"),
                {}  # sheet_analysis_queries
            )
        else:
            st.error("❌ AI分析ワークベンチが利用できません")
            show_fallback_analysis()
            
    except Exception as e:
        st.error(f"❌ AIアシスタント表示エラー: {e}")
        show_fallback_analysis()

def show_fallback_analysis():
    """フォールバック用の基本分析"""
    st.info("🤖 基本AI分析機能")
    
    user_input = st.text_area(
        "分析したい内容を入力してください",
        placeholder="例: 過去30日間のキャンペーン別パフォーマンスを分析してください",
        height=100
    )
    
    if st.button("🚀 分析実行", type="primary") and user_input:
        if st.session_state.get("bq_client"):
            try:
                # 基本的なSQL生成
                basic_sql = f"""
                SELECT
                    CampaignName,
                    SUM(CostIncludingFees) as Cost,
                    SUM(Clicks) as Clicks,
                    SUM(Conversions) as Conversions,
                    SAFE_DIVIDE(SUM(CostIncludingFees), SUM(Conversions)) as CPA,
                    SAFE_DIVIDE(SUM(Conversions), SUM(Clicks)) * 100 as CVR
                FROM `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`
                WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                GROUP BY CampaignName
                ORDER BY Cost DESC
                LIMIT 20
                """
                
                with st.spinner("🔄 データを取得中..."):
                    df = st.session_state.bq_client.query(basic_sql).to_dataframe()
                    
                if not df.empty:
                    st.session_state.df = df
                    st.session_state.sql = basic_sql
                    st.session_state.user_input = user_input
                    
                    st.success(f"✅ 分析完了！{len(df)}行のデータを取得しました")
                    st.dataframe(df, use_container_width=True)
                else:
                    st.warning("⚠️ データが見つかりませんでした")
                    
            except Exception as e:
                st.error(f"❌ 分析実行エラー: {e}")
        else:
            st.error("❌ BigQueryクライアントが設定されていません")

def show_workspace_mode():
    """📊/🤖 分析ワークスペースモード"""
    st.title("📊/🤖 分析ワークスペース")
    
    # 2カラムレイアウト
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📊 ダッシュボード")
        try:
            if IMPORT_STATUS["looker_handler"] and st.session_state.get("bq_client"):
                show_looker_studio_integration(
                    st.session_state.bq_client,
                    st.session_state.get("gemini_model"),
                    key_prefix="workspace_dash",
                    sheet_analysis_queries={}
                )
            else:
                st.info("📊 ダッシュボード機能を初期化中...")
                show_fallback_dashboard()
        except Exception as e:
            st.error(f"❌ ダッシュボード表示エラー: {e}")
            show_fallback_dashboard()
    
    with col2:
        st.subheader("🤖 AI分析")
        try:
            if IMPORT_STATUS["ui_main"]:
                show_analysis_workbench(
                    st.session_state.get("gemini_model"),
                    st.session_state.get("claude_client"),
                    st.session_state.get("claude_model_name"),
                    {}
                )
            else:
                show_fallback_analysis()
        except Exception as e:
            st.error(f"❌ AI分析表示エラー: {e}")
            show_fallback_analysis()

# =========================================================================
# システム状態・デバッグ情報
# =========================================================================

def show_system_status():
    """システム状態表示"""
    with st.expander("🔧 システム状態", expanded=False):
        st.subheader("📊 接続状況")
        
        # API接続状況
        col1, col2, col3 = st.columns(3)
        
        with col1:
            bq_status = "✅ 接続中" if st.session_state.get("bq_client") else "❌ 未接続"
            st.metric("BigQuery", bq_status)
        
        with col2:
            gemini_status = "✅ 接続中" if st.session_state.get("gemini_model") else "❌ 未接続"
            st.metric("Gemini", gemini_status)
        
        with col3:
            claude_status = "✅ 接続中" if st.session_state.get("claude_client") else "❌ 未接続"
            st.metric("Claude", claude_status)
        
        st.subheader("📁 モジュール状況")
        
        # インポート状況表示
        for module_name, status in IMPORT_STATUS.items():
            status_text = "✅ 正常" if status else "❌ エラー"
            st.write(f"**{module_name}**: {status_text}")
        
        st.subheader("📈 使用統計")
        
        usage_stats = st.session_state.get("usage_stats", {})
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("総分析数", usage_stats.get("total_analyses", 0))
        
        with col2:
            st.metric("成功数", usage_stats.get("success_count", 0))
        
        with col3:
            st.metric("エラー数", usage_stats.get("error_count", 0))
        
        # セッション状態（デバッグ用）
        if st.session_state.get("debug_mode", False):
            st.subheader("🐛 デバッグ情報")
            
            debug_info = {
                "view_mode": st.session_state.get("view_mode"),
                "filters": st.session_state.get("filters", {}),
                "current_sheet": st.session_state.get("filters", {}).get("sheet"),
                "df_shape": st.session_state.get("df", pd.DataFrame()).shape,
                "sql_length": len(st.session_state.get("sql", "")),
                "comment_length": len(st.session_state.get("comment", ""))
            }
            
            st.json(debug_info)

# =========================================================================
# サンプル分析・デモ機能
# =========================================================================

def run_sample_analysis():
    """サンプル分析の実行"""
    with st.spinner("📊 サンプル分析を実行中..."):
        try:
            if not st.session_state.get("bq_client"):
                st.error("❌ BigQueryクライアントが設定されていません")
                return
            
            # サンプルクエリ
            sample_query = """
            SELECT 
                FORMAT_DATE('%Y-%m-%d', Date) as Date,
                SUM(CostIncludingFees) as Cost,
                SUM(Clicks) as Clicks,
                SUM(Conversions) as Conversions,
                SAFE_DIVIDE(SUM(CostIncludingFees), SUM(Conversions)) as CPA,
                SAFE_DIVIDE(SUM(Conversions), SUM(Clicks)) * 100 as CVR,
                SAFE_DIVIDE(SUM(Clicks), SUM(Impressions)) * 100 as CTR
            FROM `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`
            WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
            GROUP BY Date
            ORDER BY Date DESC
            """
            
            df = st.session_state.bq_client.query(sample_query).to_dataframe()
            
            # セッション状態に保存
            st.session_state.df = df
            st.session_state.sql = sample_query
            st.session_state.user_input = "過去7日間の日別パフォーマンスサマリー"
            st.session_state.comment = "サンプル分析が完了しました。過去7日間のデータが表示されています。"
            
            st.success(f"✅ サンプル分析完了！{len(df)}行のデータを取得しました")
            
            # 使用統計更新
            log_analysis_usage("サンプル分析", "sample", 0, False)
            
            st.rerun()
            
        except Exception as e:
            st.error(f"❌ サンプル分析に失敗しました: {str(e)}")
            log_analysis_usage("サンプル分析", "sample", 0, True)

# =========================================================================
# メイン実行関数
# =========================================================================

def main():
    """メイン実行関数"""
    
    # 初期化
    initialize_session_state()
    initialize_analysis_tracking()
    
    # API クライアント設定
    if not setup_clients():
        st.warning("⚠️ 一部のAPIクライアントが利用できません。機能が制限される可能性があります。")
    
    # サイドバーレイアウト
    with st.sidebar:
        st.title("🚀 分析プラットフォーム")
        
        # ========================================
        # モード選択
        # ========================================
        current_mode = st.session_state.get("view_mode", "📊 ダッシュボード表示")
        mode_options = ["📊 ダッシュボード表示", "📊/🤖 分析ワークスペース", "🤖 AIアシスタント分析"]
        
        try:
            current_index = mode_options.index(current_mode)
        except ValueError:
            current_index = 0
            st.session_state.view_mode = mode_options[0]
        
        new_mode = st.radio(
            "表示モードを選択",
            mode_options,
            index=current_index,
            key="view_mode_selector"
        )
        
        # モード変更の検出
        if new_mode != current_mode:
            st.session_state.view_mode = new_mode
            st.rerun()
        
        st.markdown("---")
        
        # ========================================
        # フィルタパネル
        # ========================================
        show_filters = st.toggle(
            "フィルタパネル表示", 
            value=st.session_state.get("show_filters", True),
            key="filter_panel_toggle"
        )
        
        if show_filters != st.session_state.get("show_filters"):
            st.session_state.show_filters = show_filters
        
        if st.session_state.show_filters:
            st.markdown("### ⚙️ フィルタ設定")
            show_filter_ui(st.session_state.get("bq_client"))
        
        st.markdown("---")
        
        # ========================================
        # クイックアクション
        # ========================================
        st.markdown("### ⚡ クイックアクション")
        
        if st.button("📊 サンプル分析実行", use_container_width=True):
            run_sample_analysis()
        
        if st.button("🔄 セッション初期化", use_container_width=True):
            for key in ["df", "sql", "comment", "user_input"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.success("✅ セッションを初期化しました")
            st.rerun()
        
        st.markdown("---")
        
        # ========================================
        # 分析履歴
        # ========================================
        st.markdown("### 📚 分析履歴")
        
        history = st.session_state.get("history", [])
        if not history:
            st.info("分析履歴がありません")
        else:
            # 最新5件の履歴を表示
            max_display = 5
            displayed_history = history[-max_display:]
            
            for i, hist in enumerate(reversed(displayed_history)):
                hist_title = hist.get('user_input', '不明な分析')[:30]
                if len(hist_title) < len(hist.get('user_input', '')):
                    hist_title += "..."
                
                if st.button(
                    f"📊 {hist_title}",
                    use_container_width=True,
                    key=f"history_btn_{len(displayed_history) - i}",
                    help=f"実行時刻: {hist.get('timestamp', 'Unknown')}"
                ):
                    # 履歴から復元
                    for key in ["user_input", "sql", "df", "comment", "graph_cfg"]:
                        if key in hist:
                            st.session_state[key] = hist[key]
                    
                    # AIアシスタントモードに切り替え
                    st.session_state.view_mode = "🤖 AIアシスタント分析"
                    st.rerun()
            
            if len(history) > max_display:
                st.caption(f"※ 最新{max_display}件を表示中（全{len(history)}件）")
        
        st.markdown("---")
        
        # ========================================
        # システム情報・設定
        # ========================================
        with st.expander("⚙️ 詳細設定"):
            st.session_state.debug_mode = st.checkbox(
                "デバッグモード",
                value=st.session_state.get("debug_mode", False),
                help="詳細な技術情報を表示します"
            )
            
            st.session_state.auto_claude_analysis = st.checkbox(
                "Claude自動分析",
                value=st.session_state.get("auto_claude_analysis", True),
                help="SQL実行後にClaude分析を自動実行します"
            )
            
            if st.button("🧹 エラー履歴クリア", use_container_width=True):
                st.session_state.error_history = []
                st.success("✅ エラー履歴をクリアしました")
        
        # システム状態表示
        show_system_status()
    
    # ========================================
    # メインコンテンツエリア
    # ========================================
    
    current_view_mode = st.session_state.get("view_mode", "📊 ダッシュボード表示")
    
    try:
        if current_view_mode == "🤖 AIアシスタント分析":
            show_ai_assistant_mode()
        elif current_view_mode == "📊/🤖 分析ワークスペース":
            show_workspace_mode()
        else:  # ダッシュボード表示
            show_dashboard_mode()
    except Exception as e:
        st.error(f"❌ メインコンテンツ表示エラー: {e}")
        
        # エラー詳細（デバッグモード時）
        if st.session_state.get("debug_mode", False):
            with st.expander("🐛 エラー詳細"):
                st.code(traceback.format_exc())
        
        # エラー履歴に追加
        add_error_to_history(
            str(e),
            "メインコンテンツエラー",
            ["ページを再読み込みしてください", "APIクライアント設定を確認してください"]
        )

# =========================================================================
# アプリケーション実行
# =========================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ アプリケーション起動エラー: {e}")
        
        # 緊急時のフォールバック情報
        st.markdown("""
        ## 🚨 緊急時対応
        
        アプリケーションの起動に問題が発生しました。以下をご確認ください：
        
        ### 📋 チェックリスト
        - [ ] 必要なPythonパッケージがインストールされているか
        - [ ] `.streamlit/secrets.toml` が正しく設定されているか
        - [ ] 環境変数が適切に設定されているか
        - [ ] ファイル権限に問題がないか
        
        ### 🔧 解決方法
        1. **依存関係の再インストール**: `pip install -r requirements.txt`
        2. **設定ファイルの確認**: API KeyやBigQuery設定
        3. **ページの再読み込み**: Ctrl+F5 (Windows) / Cmd+R (Mac)
        4. **ブラウザキャッシュのクリア**
        
        ### 📞 サポート
        問題が解決しない場合は、エラーメッセージとともに技術チームにお問い合わせください。
        """)
        
        # エラー詳細の表示
        with st.expander("🐛 詳細エラー情報"):
            st.code(traceback.format_exc())
        
        # インポート状況の表示
        st.subheader("📦 モジュール状況")
        for module_name, status in IMPORT_STATUS.items():
            status_icon = "✅" if status else "❌"
            st.write(f"{status_icon} **{module_name}**")