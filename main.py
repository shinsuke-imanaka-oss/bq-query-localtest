# main.py - datetime エラー修正版
"""
デジタルマーケティング分析アプリケーション - メインファイル
BigQuery + AI(Gemini/Claude) による広告データ分析プラットフォーム
"""

import streamlit as st
import pandas as pd
import os
import traceback
from datetime import datetime as dt, date
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
            "timestamp": dt.now(),
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
    from looker_handler import show_looker_studio_integration, show_filter_ui
    from dashboard_analyzer import SHEET_ANALYSIS_QUERIES
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
            "start_date": dt.now().date(),
            "end_date": dt.now().date(),
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
        if "gcp_service_account" in st.secrets:
            credentials_info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            project_id = credentials_info.get("project_id")
            client = bigquery.Client(credentials=credentials, project=project_id)
            st.success(f"✅ BigQuery接続成功 (Secrets) - プロジェクト: {project_id}")
            return client
        elif "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
            client = bigquery.Client()
            st.success(f"✅ BigQuery接続成功 (環境変数) - プロジェクト: {client.project}")
            return client
        else:
            client = bigquery.Client()
            st.success(f"✅ BigQuery接続成功 (デフォルト) - プロジェクト: {client.project}")
            return client
    except Exception as e:
        st.error(f"❌ BigQuery接続エラー: {str(e)}")
        st.markdown("""
        ### 🔧 BigQuery接続の修正方法
        **方法1: Streamlit Secrets使用（推奨）**
        ```toml
        # .streamlit/secrets.toml
        [gcp_service_account]
        type = "service_account"
        project_id = "your-project-id"
        # ...
        ```
        """)
        return None

def setup_gemini_client():
    """Gemini APIクライアントのセットアップ"""
    try:
        api_key = st.secrets.get("GEMINI_API_KEY") or os.environ.get("GEMINI_API_KEY")
        if not api_key:
            st.error("❌ Gemini API キーが設定されていません")
            return None
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.0-flash-001')
        st.success("✅ Gemini API 接続成功")
        return model
    except Exception as e:
        st.error(f"❌ Gemini API接続エラー: {str(e)}")
        return None

def setup_claude_client():
    """Claude APIクライアントのセットアップ"""
    try:
        api_key = st.secrets.get("CLAUDE_API_KEY") or st.secrets.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            st.error("❌ Claude API キーが設定されていません")
            return None, None
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
            st.markdown(f"{'✅' if status else '❌'} **{module_name}**")
        st.markdown("---")
        st.markdown("### 🔑 API接続状況")
        st.markdown(f"**BigQuery**: {'✅ 接続済み' if st.session_state.get('bq_client') else '❌ 未接続'}")
        st.markdown(f"**Gemini**: {'✅ 接続済み' if st.session_state.get('gemini_model') else '❌ 未接続'}")
        st.markdown(f"**Claude**: {'✅ 接続済み' if st.session_state.get('claude_client') else '❌ 未接続'}")

# =========================================================================
# メイン表示モード
# =========================================================================

def show_ai_assistant_mode():
    """AIアシスタント分析モード"""
    try:
        show_analysis_workbench(
            st.session_state.get('gemini_model'),
            st.session_state.get('claude_client'),
            st.session_state.get('claude_model_name'),
            {}
        )
    except Exception as e:
        st.error(f"❌ AIアシスタント表示エラー: {e}")
        if st.session_state.get("debug_mode"):
            st.code(traceback.format_exc())

def show_dashboard_mode():
    """ダッシュボード表示モード"""
    st.header("📊 ダッシュボード表示")
    try:
        if IMPORT_STATUS.get("looker_handler"):
            st.markdown("### 🔗 Looker Studio 連携")
            show_looker_studio_integration(
                bq_client=st.session_state.bq_client,
                model=st.session_state.gemini_model,
                sheet_analysis_queries=SHEET_ANALYSIS_QUERIES
            )
        else:
            st.warning("⚠️ Looker Studio連携機能が利用できません")
    except Exception as e:
        st.error(f"❌ ダッシュボード表示エラー: {e}")

def show_workspace_mode():
    """分析ワークスペースモード"""
    st.header("📊🤖 分析ワークスペース")
    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown("### 📊 データ表示")
        if st.session_state.get('last_analysis_result') is not None:
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
    ensure_session_state()
    initialize_analysis_tracking()

    st.title("🚀 AIデジタルマーケティング分析プラットフォーム")
    st.markdown("**BigQuery** × **Gemini** × **Claude** による次世代広告分析")

    if 'initialization_complete' not in st.session_state:
        with st.spinner("🔧 システム初期化中..."):
            st.session_state.bq_client = setup_bigquery_client()
            st.session_state.gemini_model = setup_gemini_client()
            st.session_state.claude_client, st.session_state.claude_model_name = setup_claude_client()
            st.session_state.initialization_complete = True

    with st.sidebar:
        st.header("🎛️ 制御パネル")
        view_modes = ["📊 ダッシュボード表示", "🤖 AIアシスタント分析", "📊/🤖 分析ワークスペース"]
        selected_mode = st.selectbox(
            "表示モード",
            view_modes,
            index=view_modes.index(st.session_state.get("view_mode", "📊 ダッシュボード表示"))
        )
        if selected_mode != st.session_state.get("view_mode"):
            st.session_state.view_mode = selected_mode
            st.rerun()

        if selected_mode == "📊 ダッシュボード表示":
            st.markdown("---")
            st.subheader("絞り込みフィルター")
            if IMPORT_STATUS.get("looker_handler") and st.session_state.get('bq_client'):
                show_filter_ui(st.session_state.bq_client)

        st.markdown("---")
        if st.session_state.get("analysis_history"):
            st.subheader("📈 分析履歴")
            history = st.session_state.analysis_history
            for i, record in enumerate(reversed(history[-5:])):
                user_input_short = record["user_input"][:30] + "..." if len(record["user_input"]) > 30 else record["user_input"]
                if st.button(f"🕐 {record['timestamp'].strftime('%H:%M')}: {user_input_short}", key=f"history_{i}", use_container_width=True):
                    st.session_state.view_mode = "🤖 AIアシスタント分析"
                    st.rerun()

        st.markdown("---")
        with st.expander("⚙️ 詳細設定"):
            st.session_state.debug_mode = st.checkbox("デバッグモード", value=st.session_state.get("debug_mode", False))
            st.session_state.auto_claude_analysis = st.checkbox("Claude自動分析", value=st.session_state.get("auto_claude_analysis", True))
            if st.button("🧹 エラー履歴クリア", use_container_width=True):
                st.session_state.error_history = []
                st.success("✅ エラー履歴をクリアしました")
        show_system_status()

    current_view_mode = st.session_state.get("view_mode", "📊 ダッシュボード表示")
    try:
        if current_view_mode == "🤖 AIアシスタント分析":
            show_ai_assistant_mode()
        elif current_view_mode == "📊/🤖 分析ワークスペース":
            show_workspace_mode()
        else:
            show_dashboard_mode()
    except Exception as e:
        st.error(f"❌ メインコンテンツ表示エラー: {e}")
        if st.session_state.get("debug_mode"):
            st.code(traceback.format_exc())
        add_error_to_history(str(e), "メインコンテンツエラー")

# =========================================================================
# アプリケーション実行
# =========================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ アプリケーション起動エラー: {e}")
        st.markdown("## 🚨 緊急時対応")
        st.code(traceback.format_exc())