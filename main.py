# main.py - 完全版（修正済み）
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import google.generativeai as genai
import anthropic
import json
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# =========================================================================
# ページ設定・初期化（変更なし）
# =========================================================================

st.set_page_config(
    page_title="BigQuery データ分析ツール",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# main.py の既存のインポート部分を以下のように修正してください

# =========================================================================
# 分割ファイルからのインポート（修正版 - エラーハンドリング強化）
# =========================================================================

# 基本プロンプトシステムのインポート（最優先）
try:
    from prompts import *
    print("✅ prompts.py からのインポート成功")
    PROMPTS_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ prompts.py が見つかりません: {e}")
    PROMPTS_AVAILABLE = False
    # フォールバック: 基本的なプロンプト関数を定義
    def select_best_prompt(user_input: str) -> dict:
        return {
            "description": "基本分析",
            "template": f"以下の分析を実行してください: {user_input}"
        }
    MODIFY_SQL_TEMPLATE = "SQLを修正してください: {original_sql}\n指示: {modification_instruction}"
    CLAUDE_COMMENT_PROMPT_TEMPLATE = "以下のデータを分析してください: {data_sample}"

# メイン UI インポート（ui_componentsの代替）
try:
    from ui_main import show_analysis_workbench
    print("✅ ui_main.py からのインポート成功")
except ImportError as e:
    print(f"❌ ui_main.py が見つかりません: {e}")
    st.error("❌ ui_main.py が見つかりません")
    st.stop()

# 機能パネル インポート
try:
    from ui_features import initialize_analysis_tracking, show_filter_ui
    print("✅ ui_features.py からのインポート成功")
except ImportError as e:
    print(f"⚠️ ui_features.py が見つかりません: {e}")
    # 代替関数の定義
    def initialize_analysis_tracking():
        if "usage_stats" not in st.session_state:
            st.session_state.usage_stats = {"total_analyses": 0, "error_count": 0}
    def show_filter_ui(bq_client):
        st.sidebar.info("フィルター機能は一時的に利用できません")

# 分析制御 インポート
try:
    from analysis_controller import run_analysis_flow
    print("✅ analysis_controller.py からのインポート成功")
except ImportError as e:
    print(f"⚠️ analysis_controller.py が見つかりません: {e}")
    # 代替関数の定義
    def run_analysis_flow(gemini_model, claude_client, claude_model_name, user_input, sheet_analysis_queries):
        st.error("分析制御機能が一時的に利用できません")
        st.info("基本的なSQL実行機能は利用可能です")

# エラーハンドリング インポート
try:
    from error_handler import EnhancedErrorHandler, show_enhanced_error_message
    print("✅ error_handler.py からのインポート成功")
except ImportError as e:
    print(f"⚠️ error_handler.py が見つかりません: {e}")
    EnhancedErrorHandler = None
    def show_enhanced_error_message(error, context):
        st.error(f"エラー: {str(error)}")

# Looker Studio統合 インポート
try:
    from looker_handler import show_looker_studio_integration
    # show_filter_ui は ui_features で既に定義されている場合はスキップ
    if 'show_filter_ui' not in globals():
        from looker_handler import show_filter_ui
    print("✅ looker_handler.py からのインポート成功")
except ImportError as e:
    print(f"⚠️ looker_handler.py が見つかりません: {e}")
    def show_looker_studio_integration(*args, **kwargs):
        st.error("Looker Studio統合機能が一時的に利用できません")

# ダッシュボード分析クエリ インポート
try:
    from dashboard_analyzer import SHEET_ANALYSIS_QUERIES
    print("✅ dashboard_analyzer.py からのインポート成功")
except ImportError as e:
    print(f"⚠️ dashboard_analyzer.py が見つかりません: {e}")
    SHEET_ANALYSIS_QUERIES = {
        "default": {
            "table": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign",
            "query": "SELECT * FROM `{table}` {where_clause} LIMIT 10",
            "supported_filters": ["date", "media", "campaign"]
        }
    }

# 既存の analysis_logic インポート
try:
    import analysis_logic
    print("✅ analysis_logic.py からのインポート成功")
except ImportError as e:
    print(f"⚠️ analysis_logic.py が見つかりません: {e}")
    st.error(f"❌ 必要なモジュールが見つかりません: analysis_logic.py")
    st.stop()

# インポート完了の確認
print("🎉 全モジュールのインポート処理完了")

# プロンプトシステムの状態をセッションに保存
if "prompts_available" not in st.session_state:
    st.session_state.prompts_available = PROMPTS_AVAILABLE

# =========================================================================
# セッション状態の初期化
# =========================================================================

def initialize_session_state():
    """セッション状態の初期化"""
    # 分析追跡の初期化（ui_features.pyから）
    initialize_analysis_tracking()
    
    # 既存の初期化コード
    if "history" not in st.session_state:
        st.session_state.history = []
    if "user_input" not in st.session_state:
        st.session_state.user_input = ""
    if "sql" not in st.session_state:
        st.session_state.sql = ""
    if "df" not in st.session_state:
        st.session_state.df = pd.DataFrame()
    if "comment" not in st.session_state:
        st.session_state.comment = ""
    if "selected_recipe" not in st.session_state:
        st.session_state.selected_recipe = "自由入力"
    if "graph_cfg" not in st.session_state:
        st.session_state.graph_cfg = {}
    if "bq_client" not in st.session_state:
        st.session_state.bq_client = None
    if "gemini_model" not in st.session_state:
        st.session_state.gemini_model = None
    if "claude_client" not in st.session_state:
        st.session_state.claude_client = None
    if "view_mode" not in st.session_state:
        st.session_state.view_mode = "📊/🤖 分析ワークスペース"
    if "show_filters" not in st.session_state:
        st.session_state.show_filters = True

# =========================================================================
# APIクライアントセットアップ
# =========================================================================

def setup_clients():
    """API クライアントのセットアップ"""
    try:
        # BigQuery クライアント
        credentials_info = st.secrets["gcp_service_account"]
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        st.session_state.bq_client = bigquery.Client(credentials=credentials)
        
        # Gemini クライアント
        genai.configure(api_key=st.secrets["google_api_key"])
        st.session_state.gemini_model = genai.GenerativeModel('gemini-2.0-flash-001')
        
        # Claude クライアント
        st.session_state.claude_client = anthropic.Anthropic(api_key=st.secrets["anthropic_api_key"])
        
        return True
        
    except Exception as e:
        st.error(f"❌ API クライアントの設定に失敗しました: {str(e)}")
        return False

# =========================================================================
# UI表示モード関数
# =========================================================================

def show_dashboard_mode():
    """ダッシュボードモードの表示"""
    st.header("📊 ダッシュボード表示")
    
    # Looker Studio統合機能のインポートと呼び出し
    try:
        # Looker Studioダッシュボードを表示
        show_looker_studio_integration(
            bq_client=st.session_state.bq_client,
            model=st.session_state.gemini_model,
            key_prefix="dashboard",
            sheet_analysis_queries=SHEET_ANALYSIS_QUERIES  # 修正: 実際のクエリ辞書を渡す
        )
        
    except Exception as e:
        st.error(f"❌ Looker Studio表示エラー: {e}")
        st.info("💡 設定を確認してください")
        show_dashboard_debug_info()

def show_workspace_mode():
    """分析ワークスペースモードの表示"""
    st.header("📊/🤖 分析ワークスペース")
    
    # タブでの表示切り替え
    tab1, tab2 = st.tabs(["📊 データ分析", "🤖 AI分析"])
    
    with tab1:
        # 従来のデータ分析機能
        show_traditional_analysis()
    
    with tab2:
        # AI分析機能（分割されたUI）
        show_analysis_workbench(
            gemini_model=st.session_state.gemini_model,
            claude_client=st.session_state.claude_client,
            claude_model_name="claude-3-sonnet-20240229",
            sheet_analysis_queries=SHEET_ANALYSIS_QUERIES
        )

def show_ai_assistant_mode():
    """AIアシスタント分析モードの表示"""
    show_analysis_workbench(
        gemini_model=st.session_state.gemini_model,
        claude_client=st.session_state.claude_client,
        claude_model_name="claude-3-sonnet-20240229",
        sheet_analysis_queries=SHEET_ANALYSIS_QUERIES
    )

def show_traditional_analysis():
    """従来の分析機能（既存コードを維持）"""
    st.write("📊 従来のデータ分析機能")
    
    # 基本的な分析機能のプレースホルダー
    st.info("こちらでは従来のSQLクエリ実行やデータ表示機能を提供します。")
    
    # サンプルクエリ実行機能
    with st.expander("💡 サンプルクエリ実行", expanded=False):
        sample_query = st.text_area(
            "SQLクエリを入力",
            value="SELECT * FROM `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign` LIMIT 10",
            height=100
        )
        
        if st.button("🔄 クエリ実行"):
            try:
                df = st.session_state.bq_client.query(sample_query).to_dataframe()
                st.success(f"✅ クエリ実行完了（{len(df)}行取得）")
                st.dataframe(df)
                
                # セッションに保存
                st.session_state.df = df
                st.session_state.sql = sample_query
                
            except Exception as e:
                st.error(f"❌ クエリ実行エラー: {str(e)}")

def show_data_dashboard():
    """データダッシュボードの表示"""
    df = st.session_state.df
    
    # 基本メトリクス
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("データ行数", len(df))
    with col2:
        numeric_cols = df.select_dtypes(include=['number']).columns
        st.metric("数値列数", len(numeric_cols))
    with col3:
        # コスト系の指標があれば表示
        cost_cols = [col for col in df.columns if 'cost' in col.lower()]
        if cost_cols:
            total_cost = df[cost_cols[0]].sum()
            st.metric("総コスト", f"{total_cost:,.0f}")
        else:
            st.metric("総コスト", "N/A")
    with col4:
        st.metric("実行時刻", datetime.now().strftime("%H:%M"))

    # 簡単なグラフ表示
    if len(df) > 0 and len(numeric_cols) > 0:
        st.markdown("---")
        st.subheader("📊 データ概要")
        
        # 最初の数値列でヒストグラムを作成
        first_numeric_col = numeric_cols[0]
        fig = px.histogram(df, x=first_numeric_col, title=f"{first_numeric_col}の分布")
        st.plotly_chart(fig, use_container_width=True)

def show_dashboard_debug_info():
    """ダッシュボードのデバッグ情報表示"""
    with st.expander("🔍 デバッグ情報", expanded=False):
        st.markdown("### 📋 設定確認項目")
        
        # 環境変数の確認
        import os
        looker_report_id = os.environ.get("LOOKER_REPORT_ID")
        
        if looker_report_id:
            st.success(f"✅ LOOKER_REPORT_ID: {looker_report_id[:10]}...")
        else:
            st.error("❌ 環境変数 LOOKER_REPORT_ID が設定されていません")
            st.markdown("""
            **修正方法:**
            1. `.streamlit/secrets.toml` ファイルに以下を追加:
            ```toml
            LOOKER_REPORT_ID = "あなたのレポートID"
            ```
            2. またはシステム環境変数で設定
            """)
        
        # セッション状態の確認
        st.markdown("### 🔧 セッション状態")
        st.text(f"BigQuery Client: {'✅ OK' if st.session_state.get('bq_client') else '❌ なし'}")
        st.text(f"Gemini Model: {'✅ OK' if st.session_state.get('gemini_model') else '❌ なし'}")
        st.text(f"Filters: {st.session_state.get('filters', {})}")

def sample_analysis():
    """サンプル分析の実行"""
    with st.spinner("サンプル分析を実行中..."):
        try:
            # サンプルクエリの実行
            sample_query = """
            SELECT 
                FORMAT_DATE('%Y-%m-%d', Date) as Date,
                SUM(CostIncludingFees) as Cost,
                SUM(Clicks) as Clicks,
                SUM(Conversions) as Conversions
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
            
            st.success("✅ サンプル分析が完了しました！")
            st.rerun()
            
        except Exception as e:
            st.error(f"❌ サンプル分析に失敗しました: {str(e)}")

# =========================================================================
# メイン実行部
# =========================================================================

def main():
    """メイン実行関数"""
    # 初期化
    initialize_session_state()
    
    # API クライアントの設定
    if not setup_clients():
        st.stop()
    
    # サイドバーのレイアウト
    with st.sidebar:
        st.title("統合分析プラットフォーム")
        
        # 3つの画面モード切り替え
        st.session_state.view_mode = st.radio(
            "表示モードを選択", 
            ["📊 ダッシュボード表示", "📊/🤖 分析ワークスペース", "🤖 AIアシスタント分析"],
            key="view_mode_selector"
        )
        
        # フィルタUIの表示/非表示
        st.session_state.show_filters = st.toggle("フィルタパネル表示", value=True)
        if st.session_state.show_filters:
            show_filter_ui(st.session_state.bq_client)
            
        # 分析履歴
        st.markdown("---")
        st.subheader("分析履歴")
        if not st.session_state.history:
            st.info("分析履歴がありません。")
        else:
            for i, history in enumerate(reversed(st.session_state.history)):
                if st.button(f"履歴 {len(st.session_state.history) - i}: {history['user_input'][:30]}...", use_container_width=True):
                    # 履歴からセッション状態を復元
                    st.session_state.user_input = history["user_input"]
                    st.session_state.selected_recipe = "自由入力"
                    st.session_state.sql = history["sql"]
                    st.session_state.df = history["df"]
                    st.session_state.graph_cfg = history["graph_cfg"]
                    st.session_state.comment = history["comment"]
                    st.rerun()
    
    # メインコンテンツエリア
    if st.session_state.view_mode == "🤖 AIアシスタント分析":
        show_ai_assistant_mode()
        
    elif st.session_state.view_mode == "📊/🤖 分析ワークスペース":
        show_workspace_mode()
        
    else:  # ダッシュボード表示
        show_dashboard_mode()

# =========================================================================
# アプリケーション実行
# =========================================================================

if __name__ == "__main__":
    main()