# main.py - datetime エラー修正版
"""
デジタルマーケティング分析アプリケーション - メインファイル
BigQuery + AI(Gemini/Claude) による広告データ分析プラットフォーム
"""

import streamlit as st
import pandas as pd
import os
import traceback
from datetime import datetime, date  # ← 修正: datetime と date を個別にインポート
from typing import Dict, List, Optional, Any

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
# インポート状況管理
# =========================================================================

IMPORT_STATUS = {
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
# 必須ライブラリのインポート
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
# プロンプトシステム
# =========================================================================

# 基本プロンプトシステム
try:
    from prompts import (
        select_best_prompt, 
        GENERAL_SQL_TEMPLATE, 
        MODIFY_SQL_TEMPLATE, 
        CLAUDE_COMMENT_PROMPT_TEMPLATE
    )
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

# =========================================================================
# UIコンポーネント
# =========================================================================

# メインUI
try:
    from ui_main import show_analysis_workbench, initialize_main_session_state
    IMPORT_STATUS["ui_main"] = True
    print("✅ ui_main.py インポート成功")
except ImportError as e:
    print(f"❌ ui_main.py インポートエラー: {e}")
    st.error("❌ ui_main.py が見つかりません。アプリケーションを正常に動作させるには必要です。")
    
    # 緊急フォールバック
    def show_analysis_workbench(*args, **kwargs):
        st.error("❌ メインUI機能が利用できません")
        st.markdown("手動でSQLを入力してテストしてください。")
        
        manual_sql = st.text_area("SQL入力", height=200)
        if st.button("実行") and manual_sql:
            try:
                df = st.session_state.bq_client.query(manual_sql).to_dataframe()
                st.dataframe(df)
            except Exception as e:
                st.error(f"エラー: {e}")
    
    def initialize_main_session_state():
        ensure_session_state()

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
    # フォールバック関数の定義
    def initialize_analysis_tracking():
        ensure_session_state()
    
    def log_analysis_usage(user_input: str, system_type: str, execution_time: float = 0, error: bool = False):
        ensure_session_state()
        st.session_state.usage_stats["total_analyses"] += 1
        if error:
            st.session_state.usage_stats["error_count"] += 1
    
    def add_error_to_history(error_message: str, error_category: str = "一般エラー", solutions: List[str] = None):
        ensure_session_state()
        st.session_state.error_history.append({
            "timestamp": datetime.now(),  # ← 修正: datetime.now() に変更
            "message": error_message,
            "category": error_category,
            "solutions": solutions or []
        })
    
    def show_analysis_summary_panel():
        st.info("📊 分析サマリーパネルは一時的に利用できません")
    
    def show_data_quality_panel():
        st.info("🔍 データ品質パネルは一時的に利用できません")

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
    
    def show_manual_sql_input():
        st.info("手動SQL入力機能は一時的に利用できません")

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

# Looker連携
try:
    from looker_handler import *
    IMPORT_STATUS["looker_handler"] = True
    print("✅ looker_handler.py インポート成功")
except ImportError as e:
    print(f"⚠️ looker_handler.py インポートエラー: {e}")

# =========================================================================
# セッション状態管理 - 修正版
# =========================================================================

def ensure_session_state():
    """セッション状態の確実な初期化"""
    defaults = {
        "usage_stats": {
            "total_analyses": 0,
            "error_count": 0,
            "enhanced_usage": 0,
            "avg_execution_time": 0.0
        },
        "error_history": [],
        "analysis_history": [],
        "filter_settings": {
            "start_date": datetime.now().date(),  # ← 修正: datetime.now().date()
            "end_date": datetime.now().date(),    # ← 修正: datetime.now().date()
            "media": [],
            "campaigns": []
        },
        "last_analysis_result": None,
        "last_sql": "",
        "last_user_input": "",
        "debug_mode": False,
        "auto_claude_analysis": True,
        "view_mode": "📊 ダッシュボード表示",
        "accessibility_settings": {
            "high_contrast": False,
            "large_text": False,
            "reduced_motion": False
        }
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

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
        st.markdown("""
        ### 🔧 BigQuery接続の修正方法
        
        **方法1: Streamlit Secrets使用（推奨）**
        ```toml
        # .streamlit/secrets.toml
        [gcp_service_account]
        type = "service_account"
        project_id = "your-project-id"
        private_key_id = "..."
        private_key = "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n"
        client_email = "..."
        client_id = "..."
        auth_uri = "https://accounts.google.com/o/oauth2/auth"
        token_uri = "https://oauth2.googleapis.com/token"
        ```
        
        **方法2: 環境変数使用**
        ```bash
        export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
        ```
        
        **方法3: デフォルト認証**
        ```bash
        gcloud auth application-default login
        ```
        """)
        
        return None

def setup_gemini_client():
    """Gemini APIクライアントのセットアップ"""
    try:
        # API キーの取得
        api_key = None
        
        if "GEMINI_API_KEY" in st.secrets:
            api_key = st.secrets["GEMINI_API_KEY"]
        elif "GEMINI_API_KEY" in os.environ:
            api_key = os.environ["GEMINI_API_KEY"]
        
        if not api_key:
            st.error("❌ Gemini API キーが設定されていません")
            st.markdown("""
            ### 🔧 Gemini API キーの設定方法
            
            **Streamlit Secrets (.streamlit/secrets.toml)**
            ```toml
            GEMINI_API_KEY = "your-gemini-api-key"
            ```
            
            **環境変数**
            ```bash
            export GEMINI_API_KEY="your-gemini-api-key"
            ```
            """)
            return None
        
        # Gemini設定
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-pro')
        
        st.success("✅ Gemini API 接続成功")
        return model
        
    except Exception as e:
        st.error(f"❌ Gemini API接続エラー: {str(e)}")
        return None

def setup_claude_client():
    """Claude APIクライアントのセットアップ"""
    try:
        # API キーの取得
        api_key = None
        
        if "CLAUDE_API_KEY" in st.secrets:
            api_key = st.secrets["CLAUDE_API_KEY"]
        elif "ANTHROPIC_API_KEY" in st.secrets:
            api_key = st.secrets["ANTHROPIC_API_KEY"]
        elif "CLAUDE_API_KEY" in os.environ:
            api_key = os.environ["CLAUDE_API_KEY"]
        elif "ANTHROPIC_API_KEY" in os.environ:
            api_key = os.environ["ANTHROPIC_API_KEY"]
        
        if not api_key:
            st.error("❌ Claude API キーが設定されていません")
            st.markdown("""
            ### 🔧 Claude API キーの設定方法
            
            **Streamlit Secrets (.streamlit/secrets.toml)**
            ```toml
            CLAUDE_API_KEY = "your-claude-api-key"
            # または
            ANTHROPIC_API_KEY = "your-claude-api-key"
            ```
            
            **環境変数**
            ```bash
            export CLAUDE_API_KEY="your-claude-api-key"
            # または
            export ANTHROPIC_API_KEY="your-claude-api-key"
            ```
            """)
            return None, None
        
        # Claude クライアント初期化
        client = anthropic.Anthropic(api_key=api_key)
        model_name = "claude-3-sonnet-20240229"
        
        st.success("✅ Claude API 接続成功")
        return client, model_name
        
    except Exception as e:
        st.error(f"❌ Claude API接続エラー: {str(e)}")
        return None, None

# =========================================================================
# システム状態表示
# =========================================================================

def show_system_status():
    """システム状態の表示"""
    with st.expander("🔧 システム状態", expanded=False):
        st.markdown("### 📦 モジュール読み込み状況")
        
        for module_name, status in IMPORT_STATUS.items():
            status_icon = "✅" if status else "❌"
            st.markdown(f"{status_icon} **{module_name}**")
        
        st.markdown("---")
        st.markdown("### 🔑 API接続状況")
        
        # BigQuery
        bq_status = "✅ 接続済み" if hasattr(st.session_state, 'bq_client') and st.session_state.bq_client else "❌ 未接続"
        st.markdown(f"**BigQuery**: {bq_status}")
        
        # Gemini
        gemini_status = "✅ 接続済み" if hasattr(st.session_state, 'gemini_model') and st.session_state.gemini_model else "❌ 未接続"
        st.markdown(f"**Gemini**: {gemini_status}")
        
        # Claude
        claude_status = "✅ 接続済み" if hasattr(st.session_state, 'claude_client') and st.session_state.claude_client else "❌ 未接続"
        st.markdown(f"**Claude**: {claude_status}")

# =========================================================================
# メイン表示モード
# =========================================================================

def show_ai_assistant_mode():
    """AIアシスタント分析モード"""
    try:
        show_analysis_workbench(
            st.session_state.get('gemini_model'),
            st.session_state.get('claude_client'),
            st.session_state.get('claude_model_name', 'claude-3-sonnet-20240229'),
            {}  # sheet_analysis_queries
        )
    except Exception as e:
        st.error(f"❌ AIアシスタント表示エラー: {e}")
        if st.session_state.get("debug_mode", False):
            with st.expander("🐛 エラー詳細"):
                st.code(traceback.format_exc())

def show_dashboard_mode():
    """ダッシュボード表示モード"""
    st.header("📊 ダッシュボード表示")
    
    try:
        # Looker Studio連携
        if IMPORT_STATUS.get("looker_handler", False):
            st.markdown("### 🔗 Looker Studio 連携")
            # Looker Studio関連の機能を呼び出し
            st.info("Looker Studio連携機能は開発中です")
        else:
            st.warning("⚠️ Looker Studio連携機能が利用できません")
        
        # 基本統計表示
        if hasattr(st.session_state, 'last_analysis_result') and st.session_state.last_analysis_result is not None:
            st.markdown("### 📈 最新の分析結果")
            st.dataframe(st.session_state.last_analysis_result)
        else:
            st.info("まだ分析が実行されていません。AIアシスタント分析から開始してください。")
            
    except Exception as e:
        st.error(f"❌ ダッシュボード表示エラー: {e}")

def show_workspace_mode():
    """分析ワークスペースモード"""
    st.header("📊🤖 分析ワークスペース")
    
    # 2カラムレイアウト
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown("### 📊 データ表示")
        if hasattr(st.session_state, 'last_analysis_result') and st.session_state.last_analysis_result is not None:
            st.dataframe(st.session_state.last_analysis_result, use_container_width=True)
        else:
            st.info("分析データはありません")
    
    with col2:
        st.markdown("### 🤖 AI分析")
        show_ai_assistant_mode()

# =========================================================================
# メイン関数
# =========================================================================

def main():
    """メインアプリケーション"""
    
    # セッション状態の初期化
    ensure_session_state()
    initialize_analysis_tracking()
    
    # ========================================
    # ヘッダー
    # ========================================
    st.title("🚀 AIデジタルマーケティング分析プラットフォーム")
    st.markdown("**BigQuery** × **Gemini** × **Claude** による次世代広告分析")
    
    # ========================================
    # APIクライアント初期化
    # ========================================
    
    if 'initialization_complete' not in st.session_state:
        with st.spinner("🔧 システム初期化中..."):
            # BigQuery
            if not hasattr(st.session_state, 'bq_client') or st.session_state.bq_client is None:
                st.session_state.bq_client = setup_bigquery_client()
            
            # Gemini
            if not hasattr(st.session_state, 'gemini_model') or st.session_state.gemini_model is None:
                st.session_state.gemini_model = setup_gemini_client()
            
            # Claude
            if not hasattr(st.session_state, 'claude_client') or st.session_state.claude_client is None:
                claude_client, claude_model_name = setup_claude_client()
                st.session_state.claude_client = claude_client
                st.session_state.claude_model_name = claude_model_name
            
            st.session_state.initialization_complete = True
    
    # ========================================
    # サイドバー
    # ========================================
    
    with st.sidebar:
        st.header("🎛️ 制御パネル")
        
        # 表示モード選択
        view_modes = [
            "📊 ダッシュボード表示",
            "🤖 AIアシスタント分析", 
            "📊/🤖 分析ワークスペース"
        ]
        
        selected_mode = st.selectbox(
            "表示モード",
            view_modes,
            index=view_modes.index(st.session_state.get("view_mode", "📊 ダッシュボード表示"))
        )
        
        if selected_mode != st.session_state.get("view_mode"):
            st.session_state.view_mode = selected_mode
            st.rerun()
        
        st.markdown("---")
        
        # ========================================
        # 分析履歴
        # ========================================
        
        if st.session_state.get("analysis_history"):
            st.subheader("📈 分析履歴")
            
            history = st.session_state.analysis_history
            max_display = 5
            
            for i, record in enumerate(reversed(history[-max_display:])):
                timestamp = record["timestamp"].strftime("%H:%M")  # ← 修正: timestamp処理
                user_input = record["user_input"][:30] + "..." if len(record["user_input"]) > 30 else record["user_input"]
                
                if st.button(f"🕐 {timestamp}: {user_input}", key=f"history_{i}", use_container_width=True):
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