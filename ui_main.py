# ui_main.py を以下の内容で完全に置き換えてください

import streamlit as st
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import difflib

# =========================================================================
# 安全なインポート処理
# =========================================================================
try:
    from error_handler import handle_error_with_ai
except ImportError:
    def handle_error_with_ai(e, model, context): st.error(f"❌ エラーハンドラが利用できません: {e}")
try:
    from analysis_controller import run_analysis_flow, execute_sql_query
except ImportError:
    def run_analysis_flow(*args, **kwargs): st.error("❌ 分析コントローラが利用できません。"); return False
    def execute_sql_query(*args, **kwargs): st.error("❌ SQL実行機能が利用できません。"); return None
try:
    from ui_features import (
        show_analysis_summary_panel, show_data_quality_panel,
        show_error_history, show_usage_statistics, show_quick_reanalysis
    )
except ImportError:
    def show_analysis_summary_panel(): st.info("分析サマリーパネルは一時的に利用できません")
    def show_data_quality_panel(): st.info("データ品質パネルは一時的に利用できません")
    def show_error_history(): st.info("エラー履歴機能は一時的に利用できません")
    def show_usage_statistics(): st.info("使用統計機能は一時的に利用できません")
    def show_quick_reanalysis(): st.info("再分析機能は一時的に利用できません")

# =========================================================================
# 分析レシピの定義
# =========================================================================
ANALYSIS_RECIPES = {
    "自由入力": "",
    "TOP10キャンペーン分析": "コスト上位10キャンペーンのROAS、CPA、CVRを分析し、最も効率的なキャンペーンを特定してください",
    "今月のパフォーマンス": "今月のデータに絞って、メディア別の主要KPI（CTR、CPA、ROAS）の変化を分析してください",
    "コスト効率分析": "CPA（顧客獲得単価）が最も良いキャンペーンを特定し、改善の余地があるキャンペーンと比較してください",
    "時系列トレンド": "過去30日間の日別パフォーマンス推移を可視化し、トレンドと異常値を特定してください",
    "メディア比較": "Google広告、Facebook広告、Yahoo!広告の効果を比較し、各メディアの特徴を比較してください",
    "曜日別分析": "曜日別のパフォーマンス（クリック数、コンバージョン数、CTR）を比較し、配信最適化を提案してください",
    "ROI最適化": "投資対効果（ROAS）の高いキャンペーンの特徴を分析し、予算配分の最適化案を提示してください"
}

# =========================================================================
# 分析実行ロジック
# =========================================================================
def execute_main_analysis(user_input: str):
    """AI分析の実行"""
    if not user_input.strip():
        st.error("❌ 分析指示を入力してください"); return

    st.session_state.analysis_in_progress = True
    try:
        gemini_model = st.session_state.get('gemini_model')
        bq_client = st.session_state.get('bq_client')
        if not gemini_model or not bq_client:
            st.error("❌ AIモデルまたはBigQueryクライアントが初期化されていません。"); return

        success = run_analysis_flow(
            gemini_model=gemini_model, user_input=user_input,
            prompt_system=st.session_state.get("prompt_system", "enhanced"), bq_client=bq_client
        )
        if success:
            st.success("✅ 分析が正常に完了しました。")
            st.session_state.pop("show_fix_review", None)
    except Exception as e:
        # contextを作成する際に、正しい変数を使うように修正
        context = {
            "user_input": user_input, # "手動SQL実行" ではなく、引数の user_input を使う
            "sql": st.session_state.get("last_sql", ""), # 未定義の sql ではなく、セッション状態の last_sql を使う
            "operation": "AI分析実行"
        }

        # error_handlerを呼び出して、エラー表示と修正案生成を依頼
        handle_error_with_ai(e, st.session_state.get('gemini_model'), context)

        # もし error_handler が修正案を準備してくれていたら...
        if st.session_state.get("show_fix_review"):

            # ▼▼▼【重要】ご指摘のコードをこの位置に配置します ▼▼▼
            # デバッグモードが有効なら、st.rerun()の前にセッション状態をすべて表示する
            if st.session_state.get("debug_mode", False):
                st.warning("🔍 デバッグ情報: st.session_state の内容 (再描画直前)")
                st.json(st.session_state.to_dict())

            # UIを更新してレビュー画面を表示する
            st.rerun()
            
    finally:
        st.session_state.analysis_in_progress = False

def execute_manual_sql(sql: str):
    """手動SQLの実行"""
    if not sql or not sql.strip():
        st.error("❌ SQLを入力してください"); return

    st.session_state.analysis_in_progress = True
    try:
        bq_client = st.session_state.get('bq_client')
        if not bq_client:
            st.error("❌ BigQueryクライアントが初期化されていません。"); return

        with st.spinner("クエリを実行中..."):
            df = execute_sql_query(bq_client, sql)

        if df is not None:
            st.session_state.last_analysis_result = df
            st.session_state.last_sql = sql
            st.session_state.last_user_input = "手動SQL実行"
            st.session_state.pop("show_fix_review", None) # 成功時はレビューフラグを消す
            if not df.empty:
                st.success(f"✅ クエリ実行完了！ {len(df)}行のデータを取得しました。")
            else:
                 st.warning("⚠️ クエリは成功しましたが、結果は空でした。")
    except Exception as e:
        context = {"user_input": "手動SQL実行", "sql": sql, "operation": "手動SQL実行"}
        # error_handlerを呼び出して、エラー表示と修正案生成を依頼
        handle_error_with_ai(e, st.session_state.get('gemini_model'), context)

        # もし error_handler が修正案を準備してくれていたら...
        if st.session_state.get("show_fix_review"):

            # ▼▼▼【重要】ご指摘のコードをこの位置に配置します ▼▼▼
            # デバッグモードが有効なら、st.rerun()の前にセッション状態をすべて表示する
            if st.session_state.get("debug_mode", False):
                st.warning("🔍 デバッグ情報: st.session_state の内容 (再描画直前)")
                st.json(st.session_state.to_dict())

            # UIを更新してレビュー画面を表示する
            st.rerun()
    finally:
        st.session_state.analysis_in_progress = False

# =========================================================================
# ✨【新機能】✨: 修正案レビューUI
# =========================================================================
def show_sql_fix_review_ui():
    """AIによるSQL修正案を比較・承認するためのUI"""
    st.warning("🤖 AIがSQLの修正案を提案しました")

    original_sql = st.session_state.get("original_erroneous_sql", "")
    suggested_sql = st.session_state.get("sql_fix_suggestion", "")

    diff = difflib.unified_diff(
        original_sql.splitlines(keepends=True),
        suggested_sql.splitlines(keepends=True),
        fromfile='変更前 (エラーが発生したSQL)',
        tofile='変更後 (AIの提案)',
    )
    diff_text = "".join(diff)

    with st.container(border=True):
        st.markdown("##### ▼ 変更点の比較")
        st.code(diff_text, language="diff")

        st.markdown("##### ▼ アクションを選択してください")
        col1, col2, _ = st.columns([1, 1, 2])

        def accept_fix():
            """修正案を受け入れ、即座にSQLを実行して結果を表示する"""
            corrected_sql = st.session_state.get("sql_fix_suggestion", "")
            if not corrected_sql:
                st.error("修正案のSQLが見つかりません。")
                return

            try:
                # 修正されたSQLを実行
                with st.spinner("修正されたSQLを実行しています..."):
                    bq_client = st.session_state.get('bq_client')
                    df = execute_sql_query(bq_client, corrected_sql)
                
                if df is not None:
                    # 成功したら、セッション状態を更新して結果表示に進む
                    st.session_state.last_analysis_result = df
                    st.session_state.last_sql = corrected_sql
                    # エラー関連のフラグをすべてクリア
                    st.session_state.pop("show_fix_review", None)
                    st.session_state.pop("original_erroneous_sql", None)
                    st.session_state.pop("sql_fix_suggestion", None)
                    st.success("✅ 修正されたSQLの実行に成功しました。")
                    st.rerun() # 画面を再描画して結果を表示
            
            except Exception as e:
                # AIの修正案でもエラーが出た場合
                st.error(f"🤖 AIによる修正案もエラーになりました: {e}")
                st.info("手動編集モードに切り替えます。")
                # ユーザーが最終確認できるよう、失敗したSQLを手動編集画面に渡す
                st.session_state.manual_sql_input = corrected_sql
                st.session_state.view_mode = "⚙️ 手動SQL実行"
                st.session_state.pop("show_fix_review", None)
                st.rerun()
        
        def reject_fix():
            """元のSQLで手動編集を続ける"""
            st.session_state.manual_sql_input = st.session_state.get("original_erroneous_sql", "")
            st.session_state.view_mode = "⚙️ 手動SQL実行"
            st.session_state.pop("show_fix_review", None)

        col1.button("✅ この修正案を受け入れる", type="primary", on_click=accept_fix)
        col2.button("✏️ 元のSQLで編集を続ける", on_click=reject_fix)

# =========================================================================
# UIコンポーネント関数群
# =========================================================================
def show_ai_selection():
    st.markdown("### 🤖 AI選択")
    # ... (省略)
    st.selectbox("使用するAI", ["🤖🧠 Gemini + Claude（推奨）", "🤖 Gemini（SQL生成専用）", "🧠 Claude（分析専用）"])

def show_prompt_system_selection():
    st.markdown("### ⚙️ プロンプトシステム")
    # ... (省略)
    st.selectbox("プロンプト品質", ["🚀 高品質モード（推奨）", "⚡ 基本モード"])

def show_analysis_recipe_selection():
    st.markdown("### 📋 分析レシピ")
    # ... (省略)
    st.selectbox("よく使われる分析パターン", list(ANALYSIS_RECIPES.keys()))

def show_main_input_interface():
    st.markdown("### 💭 分析指示入力")
    user_input = st.text_area("どのような分析を行いますか？", value=st.session_state.get("current_user_input", ""), height=100)
    st.session_state.current_user_input = user_input
    
    col1, col2 = st.columns([3, 1])
    with col1:
        if st.button("🚀 分析実行", disabled=not user_input.strip(), type="primary"):
            execute_main_analysis(user_input)
    with col2:
        if st.button("🖋️ 手動でSQLを編集"):
            st.session_state.view_mode = "⚙️ 手動SQL実行"
            st.session_state.manual_sql_input = st.session_state.get("last_sql", "")
            st.rerun()

def show_manual_sql_interface():
    st.markdown("---")
    st.markdown("### 🖋️ 手動SQL入力")
    manual_sql = st.text_area("SQLクエリを直接入力:", value=st.session_state.get("manual_sql_input", ""), height=200)
    st.session_state.manual_sql_input = manual_sql
    if st.button("🔍 手動でSQLを実行", disabled=not manual_sql.strip(), type="primary"):
        execute_manual_sql(manual_sql)

def show_analysis_results():
    """分析結果と、それに関連する付加情報をタブで表示する"""
    if st.session_state.get("last_analysis_result") is not None:
        st.markdown("---")
        st.subheader("📊 最新の分析結果")
        
        # 分析結果のデータフレームを表示
        df = st.session_state.last_analysis_result
        st.dataframe(df, use_container_width=True)

        # タブを使って、追加情報を整理して表示する
        tab1, tab2, tab3, tab4 = st.tabs([
            "🔄 クイック操作", 
            "🔍 データ品質", 
            "📈 使用統計",
            "⚠️ エラー履歴"
        ])

        with tab1:
            # 「クイック再分析」機能をここに移動
            show_quick_reanalysis()

        with tab2:
            # 「データ品質パネル」を呼び出す
            show_data_quality_panel()

        with tab3:
            # 「使用統計」を呼び出す
            show_usage_statistics()

        with tab4:
            # 「エラー履歴」を呼び出す
            show_error_history()

# =========================================================================
# メインのワークベンチ関数
# =========================================================================
def show_analysis_workbench(gemini_model, claude_client, claude_model_name, sheet_analysis_queries):
    st.header("🤖 AIアシスタント分析")

    # --- 以下は通常のUI表示 ---
    show_analysis_summary_panel()
    with st.expander("🎛️ AI・プロンプト設定", expanded=True):
        col1, col2 = st.columns(2)
        with col1: show_ai_selection()
        with col2: show_prompt_system_selection()
    
    # 表示モードに応じてUIを切り替え
    if st.session_state.get("view_mode") == "⚙️ 手動SQL実行":
        show_manual_sql_interface()
    else:
        st.session_state.view_mode = "🤖 AI分析"
        with st.expander("📋 分析レシピ（よく使われるパターン）", expanded=False):
            show_analysis_recipe_selection()
        show_main_input_interface()

    show_analysis_results()