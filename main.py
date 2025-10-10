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
from datetime import datetime as dt, date, timedelta
from typing import Dict, List, Optional, Any
import diagnostics
from error_handler import handle_error_with_ai
# from troubleshooter import display_troubleshooting_guide
from display_functions import display_comparative_analysis, display_action_recommendations


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
    "looker_handler": False,
    "master_analyzer": False
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

#パフォーマンス診断機能
try:
    from performance_analyzer import run_performance_diagnosis
    IMPORT_STATUS["performance_analyzer"] = True
    print("✅ performance_analyzer.py インポート成功")
except ImportError as e:
    print(f"⚠️ performance_analyzer.py インポートエラー: {e}")
    IMPORT_STATUS["performance_analyzer"] = False

#時系列診断機能
try:
    from forecast_analyzer import run_forecast_analysis
    IMPORT_STATUS["forecast_analyzer"] = True
    print("✅ forecast_analyzer.py インポート成功")
except ImportError as e:
    print(f"⚠️ forecast_analyzer.py インポートエラー: {e}")
    IMPORT_STATUS["forecast_analyzer"] = False

#インサイト分析機能
try:
    from insight_miner import run_insight_analysis
    IMPORT_STATUS["insight_miner"] = True
    print("✅ insight_miner.py インポート成功")
except ImportError as e:
    print(f"⚠️ insight_miner.py インポートエラー: {e}")
    IMPORT_STATUS["insight_miner"] = False

#戦略立案機能
try:
    from strategy_simulator import run_strategy_simulation
    IMPORT_STATUS["strategy_simulator"] = True
    print("✅ strategy_simulator.py インポート成功")
except ImportError as e:
    print(f"⚠️ strategy_simulator.py インポートエラー: {e}")
    IMPORT_STATUS["strategy_simulator"] = False

#統合分析機能
try:
    from master_analyzer import show_comprehensive_report_mode
    IMPORT_STATUS["master_analyzer"] = True
    print("✅ master_analyzer.py インポート成功")
except ImportError as e:
    print(f"⚠️ master_analyzer.py インポートエラー: {e}")
    IMPORT_STATUS["master_analyzer"] = False

# =========================================================================
# 【追加】週次・月次サマリー機能のインポート
# =========================================================================

# 既存のインポートセクションに以下を追加
try:
    from targets_manager import TargetsManager
    from achievement_analyzer import AchievementAnalyzer
    from summary_report_generator import SummaryReportGenerator
    from ui_summary import SummaryUI
    from datetime import datetime, date
    SUMMARY_AVAILABLE = True
except ImportError as e:
    SUMMARY_AVAILABLE = False
    print(f"⚠️ サマリー機能のインポート失敗: {e}")

# IMPORT_STATUS辞書に追加
IMPORT_STATUS["summary_system"] = SUMMARY_AVAILABLE

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
            model_name = "claude-sonnet-4-20250514"
        
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
# 【追加】週次・月次サマリー表示モード
# =========================================================================

def show_summary_mode():
    """週次・月次サマリー表示モード"""
    st.header("📅 週次・月次サマリーレポート")
    
    # 機能が利用可能かチェック
    if not SUMMARY_AVAILABLE:
        st.error("❌ サマリー機能が利用できません")
        st.info("以下のファイルが正しく配置されているか確認してください:")
        st.code("""
- targets_manager.py
- achievement_analyzer.py
- summary_report_generator.py
- ui_summary.py
        """)
        return
    
    # BigQueryクライアントの確認
    bq_client = st.session_state.get('bq_client')
    if not bq_client:
        st.warning("⚠️ BigQueryに接続してください")
        if st.button("🔄 BigQuery接続", type="primary"):
            try:
                with st.spinner("BigQuery接続中..."):
                    from main import setup_bigquery_client
                    bq_client = setup_bigquery_client()
                    if bq_client:
                        st.session_state.bq_client = bq_client
                        st.rerun()
            except Exception as e:
                st.error(f"接続エラー: {e}")
        return
    
    # UIインスタンス作成
    ui = SummaryUI()
    targets_manager = TargetsManager()
    
    # サイドバーでの設定
    config = ui.render_sidebar()

    # Phase 3: 最低キャンペーン数の設定を追加
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔧 Phase 3設定")
    min_campaigns = st.sidebar.number_input(
        "比較分析の最低キャンペーン数",
        min_value=1,
        max_value=10,
        value=3,
        help="高/低パフォーマンスグループそれぞれに必要な最低キャンペーン数"
    )    
    
    # 対象年月の取得
    year_month = config["period"]["start_date"].strftime("%Y-%m")
    
    # 目標設定状態の表示
    targets = targets_manager.get_targets(year_month)
    ui.display_targets_summary(targets, year_month)
    
    # メインエリア
    st.markdown("---")
    
    # 目標設定モーダル
    if config["targets"] == "open_targets_modal":
        st.subheader("⚙️ 目標・予算設定")
        
        saved = ui.render_targets_modal(targets_manager)
        
        if saved:
            st.success("✅ 目標を保存しました")
            # 3秒後に自動リロード
            import time
            time.sleep(2)
            st.rerun()
        
        st.markdown("---")
        st.info("👆 目標設定後、サイドバーの「🚀 レポート生成」ボタンを押してください")
        return
    
    # レポート生成ボタンが押された場合
    if config["generate"]:
        try:
            # レポート生成
            with st.spinner("📊 レポートを生成中..."):
                # AIクライアントの取得
                gemini_client = st.session_state.get('gemini_model')
                claude_client = st.session_state.get('claude_client')
                
                # テーブルIDの構築
                if SETTINGS_AVAILABLE and settings:
                    table_id = f"{settings.bigquery.project_id}.{settings.bigquery.dataset}.LookerStudio_report_campaign"
                else:
                    table_id = "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign"
                
                # ジェネレーター作成
                generator = SummaryReportGenerator(
                    bq_client=bq_client,
                    gemini_client=gemini_client,
                    claude_client=claude_client
                )
                
                # レポート生成
                report = generator.generate_report(
                    start_date=config["period"]["start_date"],
                    end_date=config["period"]["end_date"],
                    comparison_mode=config["comparison"]["mode"],
                    table_id=table_id,
                    min_campaigns_for_comparison=min_campaigns
                )
                
                if not report:
                    st.error("❌ レポートの生成に失敗しました")
                    return
                
                # レポート表示
                display_summary_report(report, config)
                
        except Exception as e:
            st.error(f"❌ エラーが発生しました: {e}")
            
            # デバッグ情報
            if SETTINGS_AVAILABLE and settings and settings.app.debug_mode:
                st.exception(e)
    
    else:
        # 初期表示
        st.info("👈 サイドバーで期間を選択し、「🚀 レポート生成」ボタンを押してください")
        
        # 説明
        with st.expander("📖 週次・月次サマリー機能について", expanded=True):
            st.markdown("""
            ### 機能概要
            
            定期レポート作成を自動化し、指定した期間のパフォーマンスサマリーをワンクリックで生成できます。
            
            ### 生成されるレポート
            
            1. **📝 エグゼクティブサマリー** - AI生成の要約
            2. **🎯 目標達成状況** - 予算消化率、KPI達成率
            3. **📊 主要KPI** - Cost, CPA, CVR, CTR等
            4. **📈 期間トレンド** - 日次推移グラフ
            5. **⭐ ハイライト** - 最高/最低パフォーマンス
            
            ### 使い方
            
            1. サイドバーで**期間を選択**（今週/先週/今月/先月等）
            2. 必要に応じて**目標・予算を設定**
            3. **比較設定**で前期間との比較を有効化
            4. 「🚀 レポート生成」ボタンをクリック
            
            ### 目標設定について
            
            目標を設定しなくてもレポートは生成できますが、設定すると以下の情報が追加されます：
            - 予算消化ペース（アンダー/オン/オーバー）
            - KPI達成率
            - 目標との差異分析
            """)


def display_summary_report(report: Dict[str, Any], config: Dict[str, Any]):
    """
    生成されたレポートを表示（Phase 2対応版）
    
    Args:
        report: レポートデータ
        config: UI設定
    """
    from datetime import datetime
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import pandas as pd
    
    # ヘッダー
    st.success("✅ レポートを生成しました")
    
    period_info = report["period"]
    st.markdown(f"""
    **期間**: {period_info['start_date'].strftime('%Y/%m/%d')} - {period_info['end_date'].strftime('%Y/%m/%d')}  
    **生成日時**: {datetime.now().strftime('%Y/%m/%d %H:%M')}
    """)
    
    st.markdown("---")
    
    # セクション1: エグゼクティブサマリー
    st.subheader("🤖 エグゼクティブサマリー")
    
    if report.get("section_1_executive_summary"):
        st.markdown(report["section_1_executive_summary"])
    else:
        st.info("エグゼクティブサマリーを生成できませんでした")
    
    st.markdown("---")
    
    # セクション2: 目標達成状況
    st.subheader("🎯 目標達成状況")
    
    achievement = report["section_2_achievement"]
    
    # 予算消化ペース
    st.markdown("#### 💰 予算消化ペース")
    
    budget_pacing = achievement["budget_pacing"]
    
    if budget_pacing["has_target"]:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "実コスト",
                f"¥{budget_pacing['actual_cost']:,.0f}",
                f"目標: ¥{budget_pacing['target_budget']:,.0f}"
            )
        
        with col2:
            st.metric(
                "消化率",
                f"{budget_pacing['progress_rate']:.1%}",
                f"期待: {budget_pacing['expected_progress_rate']:.1%}"
            )
        
        with col3:
            status_icon = {
                "under": "🟢",
                "on_track": "🟡",
                "over": "🔴"
            }.get(budget_pacing["pace_status"], "⚪")
            
            st.metric(
                "ペース判定",
                f"{status_icon} {budget_pacing['pace_status_text']}"
            )
        
        # 詳細情報
        with st.expander("📊 詳細情報"):
            st.write(f"日次平均コスト: ¥{budget_pacing['daily_average']:,.0f}")
            st.write(f"月末予測: ¥{budget_pacing['projected_month_end']:,.0f}")
            st.write(f"残予算: ¥{budget_pacing['remaining_budget']:,.0f}")
            st.write(f"残日数: {budget_pacing['days_remaining']}日")
    
    else:
        st.info("⚠️ 目標が設定されていません")
        st.write(f"実コスト: ¥{budget_pacing['actual_cost']:,.0f}")
        st.write(f"日次平均: ¥{budget_pacing['daily_average']:,.0f}")
        st.write(f"月末予測: ¥{budget_pacing['projected_month_end']:,.0f}")
    
    # KPI達成率
    st.markdown("#### 📊 KPI達成状況")
    
    kpi_achievement = achievement["kpi_achievement"]
    
    if kpi_achievement["has_target"]:
        kpi_cols = st.columns(len(kpi_achievement["kpis"]))
        
        for idx, (kpi_name, kpi_data) in enumerate(kpi_achievement["kpis"].items()):
            with kpi_cols[idx]:
                # メトリクス名の整形
                display_name = {
                    "conversions": "CV数",
                    "cpa": "CPA",
                    "cvr": "CVR",
                    "ctr": "CTR"
                }.get(kpi_name, kpi_name.upper())
                
                # 値のフォーマット
                if kpi_name in ["cvr", "ctr"]:
                    actual_display = f"{kpi_data['actual']:.2%}"
                    target_display = f"{kpi_data['target']:.2%}"
                elif kpi_name in ["cpa", "conversions"]:
                    actual_display = f"{kpi_data['actual']:,.0f}"
                    target_display = f"{kpi_data['target']:,.0f}"
                else:
                    actual_display = f"{kpi_data['actual']}"
                    target_display = f"{kpi_data['target']}"
                
                st.metric(
                    display_name,
                    actual_display,
                    f"目標: {target_display}"
                )
                st.write(f"達成率: {kpi_data['status_text']}")
    
    else:
        st.info("⚠️ KPI目標が設定されていません")
    
    st.markdown("---")
    
    # セクション3: 主要KPI
    st.subheader("📊 主要KPI")
    
    metrics = report["section_3_kpis"]["metrics"]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("コスト", f"¥{metrics['cost']:,.0f}")
    
    with col2:
        st.metric("CV数", f"{metrics['conversions']:,.0f}件")
    
    with col3:
        st.metric("CPA", f"¥{metrics['cpa']:,.0f}")
    
    with col4:
        st.metric("CVR", f"{metrics['cvr']:.2%}")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Imp", f"{metrics['impressions']:,.0f}")
    
    with col2:
        st.metric("Clicks", f"{metrics['clicks']:,.0f}")
    
    with col3:
        st.metric("CTR", f"{metrics['ctr']:.2%}")
    
    with col4:
        st.metric("CPC", f"¥{metrics['cpc']:,.0f}")
    
    # KPI洞察を表示（Phase 2）
    if report.get("kpi_insights"):
        with st.expander("💡 KPI分析", expanded=True):
            st.markdown(report["kpi_insights"])
    
    # 前期間比較（ある場合）
    if "comparison" in report:
        st.markdown("#### 📈 前期間比較")
        
        comparison = report["comparison"]
        
        if comparison["has_comparison"]:
            comp_data = comparison["comparisons"]
            
            comp_cols = st.columns(5)
            
            for idx, metric in enumerate(["cost", "conversions", "cpa", "cvr", "ctr"]):
                if metric in comp_data:
                    data = comp_data[metric]
                    
                    with comp_cols[idx]:
                        display_name = {
                            "cost": "コスト",
                            "conversions": "CV数",
                            "cpa": "CPA",
                            "cvr": "CVR",
                            "ctr": "CTR"
                        }.get(metric, metric)
                        
                        if data["change_rate"] is not None:
                            st.write(f"**{display_name}**")
                            st.write(data["trend_text"])
                        else:
                            st.write(f"**{display_name}**")
                            st.write("比較不可")
    
    st.markdown("---")
    
    # セクション4: 期間トレンド
    st.subheader("📈 期間トレンド")
    
    trend_data = report["section_4_trends"]["daily_data"]
    
    if trend_data:
        df_trend = pd.DataFrame(trend_data)
        df_trend['date'] = pd.to_datetime(df_trend['date'])
        
        # 2行のサブプロット
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("コスト推移", "CV数推移", "CPA推移", "CVR推移")
        )
        
        # コスト
        fig.add_trace(
            go.Scatter(x=df_trend['date'], y=df_trend['cost'], name="コスト", line=dict(color='blue')),
            row=1, col=1
        )
        
        # CV数
        fig.add_trace(
            go.Scatter(x=df_trend['date'], y=df_trend['conversions'], name="CV数", line=dict(color='green')),
            row=1, col=2
        )
        
        # CPA
        fig.add_trace(
            go.Scatter(x=df_trend['date'], y=df_trend['cpa'], name="CPA", line=dict(color='red')),
            row=2, col=1
        )
        
        # CVR
        fig.add_trace(
            go.Scatter(x=df_trend['date'], y=df_trend['cvr'], name="CVR", line=dict(color='purple')),
            row=2, col=2
        )
        
        fig.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.info("トレンドデータがありません")
    
    st.markdown("---")
    
    # セクション5: ハイライト
    st.subheader("⭐ ハイライト")
    
    highlights = report["section_5_highlights"]
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🏆 最高パフォーマンス")
        
        if highlights["best_campaign"]:
            best = highlights["best_campaign"]
            st.success(f"**{best['campaign_name']}**")
            st.write(f"CPA: ¥{best['cpa']:,.0f}")
            st.write(f"CV数: {best['conversions']:,.0f}件")
            st.write(f"コスト: ¥{best['cost']:,.0f}")
        else:
            st.info("データなし")
    
    with col2:
        st.markdown("#### 📉 要改善")
        
        if highlights["worst_campaign"]:
            worst = highlights["worst_campaign"]
            st.warning(f"**{worst['campaign_name']}**")
            st.write(f"CPA: ¥{worst['cpa']:,.0f}")
            st.write(f"CV数: {worst['conversions']:,.0f}件")
            st.write(f"コスト: ¥{worst['cost']:,.0f}")
        else:
            st.info("データなし")
    
    # Phase 3: セクション6と7
    st.header("📊 セクション6: パフォーマンス比較分析")
    if "section_6_comparative_analysis" in report:
        display_comparative_analysis(report["section_6_comparative_analysis"])

    st.header("🎯 セクション7: 推奨アクション")
    if "section_7_action_recommendations" in report:
        display_action_recommendations(report["section_7_action_recommendations"])

        # ハイライト洞察を表示（Phase 2）
        if report.get("highlights_insights"):
            st.markdown("---")
            with st.expander("💡 パフォーマンス差の分析", expanded=True):
                st.markdown(report["highlights_insights"])
    
    # エクスポート機能
    st.markdown("---")
    st.subheader("💾 エクスポート")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📄 テキストでダウンロード"):
            # テキスト形式でレポートを生成
            report_text = generate_text_report(report)
            st.download_button(
                label="💾 ダウンロード",
                data=report_text,
                file_name=f"summary_report_{period_info['year_month']}.txt",
                mime="text/plain"
            )
    
    with col2:
        if st.button("📊 CSVでダウンロード"):
            # CSV形式でメトリクスを出力
            csv_data = generate_csv_report(report)
            st.download_button(
                label="💾 ダウンロード",
                data=csv_data,
                file_name=f"summary_metrics_{period_info['year_month']}.csv",
                mime="text/csv"
            )


def generate_text_report(report: Dict[str, Any]) -> str:
    """テキスト形式のレポート生成"""
    period = report["period"]
    text = f"""
週次・月次サマリーレポート
{'='*60}

期間: {period['start_date'].strftime('%Y/%m/%d')} - {period['end_date'].strftime('%Y/%m/%d')}
生成日時: {datetime.now().strftime('%Y/%m/%d %H:%M')}

{'='*60}
エグゼクティブサマリー
{'='*60}

{report.get('section_1_executive_summary', 'サマリーなし')}

{'='*60}
主要KPI
{'='*60}

"""
    
    metrics = report["section_3_kpis"]["metrics"]
    text += f"""
コスト: ¥{metrics['cost']:,.0f}
CV数: {metrics['conversions']:,.0f}件
CPA: ¥{metrics['cpa']:,.0f}
CVR: {metrics['cvr']:.2%}
CTR: {metrics['ctr']:.2%}
"""
    
    return text


def generate_csv_report(report: Dict[str, Any]) -> str:
    """CSV形式のレポート生成"""
    import io
    import pandas as pd
    
    metrics = report["section_3_kpis"]["metrics"]
    
    data = {
        "指標": ["コスト", "CV数", "CPA", "CVR", "CTR", "Impressions", "Clicks", "CPC"],
        "値": [
            metrics['cost'],
            metrics['conversions'],
            metrics['cpa'],
            metrics['cvr'],
            metrics['ctr'],
            metrics['impressions'],
            metrics['clicks'],
            metrics['cpc']
        ]
    }
    
    df = pd.DataFrame(data)
    
    return df.to_csv(index=False, encoding='utf-8-sig')


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
        view_options = [
            "📊 統合分析レポート", 
            "📅 週次・月次サマリー",
            "💡 戦略提案 & シミュレーション", 
            "📈 パフォーマンス診断", 
            "🔮 予測分析 & 異常検知", 
            "🧠 自動インサイト分析", 
            "📊 ダッシュボード表示", 
            "🤖 AI分析", 
            "⚙️ 手動SQL実行", 
            "🩺 システム診断", 
            "📈 監視ダッシュボード", 
            "🔬 環境デバッグ"]
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
            if st.session_state.get("show_fix_review"):
                from ui_main import show_sql_fix_review_ui
                show_sql_fix_review_ui()
            elif st.session_state.view_mode == "📊 統合分析レポート":
                if IMPORT_STATUS.get("master_analyzer"):
                    show_comprehensive_report_mode() # 引数なしで呼び出す
                else:
                    st.error("❌ 統合分析モジュールがロードされていません。")
            elif st.session_state.view_mode == "📅 週次・月次サマリー":
                if IMPORT_STATUS.get("summary_system"):
                    show_summary_mode()  
                else:
                    st.error("❌ サマリー機能がロードされていません。")
            elif st.session_state.view_mode == "💡 戦略提案 & シミュレーション":
                if IMPORT_STATUS.get("strategy_simulator"):
                    run_strategy_simulation()
                else:
                    st.error("❌ 戦略提案モジュールがロードされていません。")
            elif st.session_state.view_mode == "📈 パフォーマンス診断":
                if IMPORT_STATUS.get("performance_analyzer"):
                    run_performance_diagnosis()
                else:
                    st.error("❌ パフォーマンス診断モジュールがロードされていません。")
            elif st.session_state.view_mode == "🔮 予測分析 & 異常検知": # この elif ブロックを丸ごと追加
                if IMPORT_STATUS.get("forecast_analyzer"):
                    run_forecast_analysis()
                else:
                    st.error("❌ 予測分析モジュールがロードされていません。")
            elif st.session_state.view_mode == "🧠 自動インサイト分析": # この elif ブロックを丸ごと追加
                if IMPORT_STATUS.get("insight_miner"):
                    run_insight_analysis()
                else:
                    st.error("❌ 自動インサイト分析モジュールがロードされていません。")
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

def display_comparative_analysis(analysis):
    """パフォーマンス比較分析を表示"""
    if analysis.skipped:
        st.warning(f"⚠️ {analysis.skip_reason}")
        return
    
    # サマリー
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            "高パフォーマンス", 
            f"{len(analysis.high_performers)}キャンペーン",
            f"平均CPA: ¥{analysis.analysis_summary['high_perf_avg_cpa']:,.0f}"
        )
    with col2:
        st.metric(
            "低パフォーマンス", 
            f"{len(analysis.low_performers)}キャンペーン",
            f"平均CPA: ¥{analysis.analysis_summary['low_perf_avg_cpa']:,.0f}"
        )
    with col3:
        st.metric(
            "有意な差異", 
            f"{len(analysis.significant_differences)}項目",
            "20%以上の差"
        )
    
    st.divider()
    
    # 有意な差異の詳細表示
    if analysis.significant_differences:
        st.subheader("📈 有意な差異（20%以上）")
        
        # データフレーム形式で表示
        diff_data = []
        for diff in analysis.significant_differences:
            diff_data.append({
                '指標': diff.metric_name,
                '高パフォーマンス': f"{diff.high_perf_avg:,.2f}",
                '低パフォーマンス': f"{diff.low_perf_avg:,.2f}",
                '差異': f"{diff.difference_pct:+.1f}%"
            })
        
        df = pd.DataFrame(diff_data)
        st.dataframe(df, use_container_width=True, hide_index=True)
        
        # 差異の可視化
        st.subheader("📊 差異の可視化")
        
        fig = go.Figure()
        
        metric_names = [diff.metric_name for diff in analysis.significant_differences]
        differences = [diff.difference_pct for diff in analysis.significant_differences]
        
        # 色分け（正の差異 vs 負の差異）
        colors = ['green' if d > 0 else 'red' for d in differences]
        
        fig.add_trace(go.Bar(
            x=differences,
            y=metric_names,
            orientation='h',
            marker=dict(color=colors),
            text=[f"{d:+.1f}%" for d in differences],
            textposition='outside'
        ))
        
        fig.update_layout(
            title="高パフォーマンス vs 低パフォーマンスの差異",
            xaxis_title="差異（%）",
            yaxis_title="指標",
            height=400,
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.info("20%以上の有意な差異は検出されませんでした。")
    
    # AI分析結果
    if hasattr(analysis, 'ai_insights') and analysis.ai_insights:
        st.divider()
        st.subheader("🤖 AI分析: 差異要因の解説")
        st.markdown(analysis.ai_insights)
    
    # 詳細データ（エクスパンダー内）
    with st.expander("🔍 高パフォーマンスキャンペーン詳細"):
        high_perf_data = []
        for camp in analysis.high_performers[:10]:  # 上位10件
            high_perf_data.append({
                'キャンペーン名': camp.campaign_name,
                'CPA': f"¥{camp.cpa:,.0f}",
                'ROAS': f"{camp.roas:.2f}",
                'CVR': f"{camp.conversion_rate:.2%}",
                'CTR': f"{camp.click_rate:.2%}",
                'コスト': f"¥{camp.cost:,.0f}"
            })
        
        df_high = pd.DataFrame(high_perf_data)
        st.dataframe(df_high, use_container_width=True, hide_index=True)
    
    with st.expander("🔍 低パフォーマンスキャンペーン詳細"):
        low_perf_data = []
        for camp in analysis.low_performers[:10]:  # 下位10件
            low_perf_data.append({
                'キャンペーン名': camp.campaign_name,
                'CPA': f"¥{camp.cpa:,.0f}",
                'ROAS': f"{camp.roas:.2f}",
                'CVR': f"{camp.conversion_rate:.2%}",
                'CTR': f"{camp.click_rate:.2%}",
                'コスト': f"¥{camp.cost:,.0f}"
            })
        
        df_low = pd.DataFrame(low_perf_data)
        st.dataframe(df_low, use_container_width=True, hide_index=True)

def display_action_recommendations(recommendations):
    """アクション提案を表示"""
    if not recommendations.actions:
        st.warning("⚠️ " + recommendations.summary)
        return
    
    # サマリー
    st.markdown(recommendations.summary)
    
    st.divider()
    
    # 優先度別カウント
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("高優先度", recommendations.high_priority_count, "即座に実施")
    with col2:
        st.metric("中優先度", recommendations.medium_priority_count, "2週間以内")
    with col3:
        st.metric("低優先度", recommendations.low_priority_count, "状況に応じて")
    
    st.divider()
    
    # アクション項目の表示
    priority_icons = {
        Priority.HIGH: "🔴",
        Priority.MEDIUM: "🟡",
        Priority.LOW: "🟢"
    }
    
    for i, action in enumerate(recommendations.actions, 1):
        icon = priority_icons.get(action.priority, "⚪")
        
        with st.expander(
            f"{icon} {action.title} (優先度: {action.priority.value})",
            expanded=(i <= 3)  # 上位3件は展開表示
        ):
            st.markdown(f"**カテゴリ:** {action.category}")
            st.markdown(f"**内容:**")
            st.write(action.description)
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**期待効果:**")
                st.info(action.expected_impact)
            with col2:
                st.markdown("**検証方法:**")
                st.success(action.validation_method)
    
    # AI分析結果
    if hasattr(recommendations, 'ai_insights') and recommendations.ai_insights:
        st.divider()
        st.subheader("🤖 AI分析: 実施ガイダンス")
        st.markdown(recommendations.ai_insights)
    
    # アクション実行チェックリスト（ダウンロード可能）
    st.divider()
    st.subheader("📋 アクション実行チェックリスト")
    
    checklist_md = "# アクション実行チェックリスト\n\n"
    checklist_md += f"生成日時: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}\n\n"
    
    for priority in [Priority.HIGH, Priority.MEDIUM, Priority.LOW]:
        priority_actions = [a for a in recommendations.actions if a.priority == priority]
        if priority_actions:
            checklist_md += f"\n## {priority.value}優先度\n\n"
            for action in priority_actions:
                checklist_md += f"### ☐ {action.title}\n\n"
                checklist_md += f"- **内容:** {action.description}\n"
                checklist_md += f"- **期待効果:** {action.expected_impact}\n"
                checklist_md += f"- **検証方法:** {action.validation_method}\n"
                checklist_md += f"- **カテゴリ:** {action.category}\n"
                checklist_md += f"- **実施予定日:** ____________\n"
                checklist_md += f"- **担当者:** ____________\n"
                checklist_md += f"- **完了日:** ____________\n\n"
    
    st.download_button(
        label="📥 チェックリストをダウンロード",
        data=checklist_md,
        file_name=f"action_checklist_{pd.Timestamp.now().strftime('%Y%m%d')}.md",
        mime="text/markdown"
    )

