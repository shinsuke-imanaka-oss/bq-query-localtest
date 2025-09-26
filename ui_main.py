# ui_main.py - 完全修正版
"""
メイン分析ワークベンチUI
- 基本的な分析画面構成
- プロンプトシステム選択
- AI選択機能
- 基本的な入力UI
- 循環インポートの解決
"""

import streamlit as st
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from error_handler import handle_error_with_ai

# =========================================================================
# 安全なインポート処理
# =========================================================================

def safe_import_ui_features():
    """ui_features.pyの安全なインポート"""
    try:
        from ui_features import (
            show_analysis_summary_panel,
            show_data_quality_panel, 
            show_error_history,
            show_usage_statistics,
            show_quick_reanalysis
        )
        return {
            "show_analysis_summary_panel": show_analysis_summary_panel,
            "show_data_quality_panel": show_data_quality_panel,
            "show_error_history": show_error_history,
            "show_usage_statistics": show_usage_statistics,
            "show_quick_reanalysis": show_quick_reanalysis
        }
    except ImportError:
        # フォールバック関数
        return {
            "show_analysis_summary_panel": lambda: st.info("📊 分析サマリーパネルは一時的に利用できません"),
            "show_data_quality_panel": lambda: st.info("🔍 データ品質パネルは一時的に利用できません"),
            "show_error_history": lambda: st.info("⚠️ エラー履歴機能は一時的に利用できません"),
            "show_usage_statistics": lambda: st.info("📈 使用統計機能は一時的に利用できません"),
            "show_quick_reanalysis": lambda: st.info("🔄 再分析機能は一時的に利用できません")
        }

def safe_import_analysis_controller():
    """analysis_controller.pyの安全なインポート"""
    try:
        from analysis_controller import run_analysis_flow
        return run_analysis_flow
    except ImportError:
        def fallback_analysis_flow(*args, **kwargs):
            st.error("❌ 分析制御機能が利用できません")
            st.info("💡 手動でSQLを入力して実行してください")
            return False
        return fallback_analysis_flow

def safe_import_prompts():
    """プロンプトシステムの安全なインポート"""
    try:
        from prompts import select_best_prompt, PROMPT_DEFINITIONS
        return select_best_prompt, PROMPT_DEFINITIONS
    except ImportError:
        def fallback_prompt_selector(user_input: str):
            return {
                "description": "基本分析",
                "template": f"以下の分析を実行してください: {user_input}"
            }
        return fallback_prompt_selector, {}

def safe_import_enhanced_prompts():
    """強化プロンプトシステムの安全なインポート"""
    try:
        from enhanced_prompts import generate_enhanced_sql_prompt, generate_enhanced_claude_prompt
        return generate_enhanced_sql_prompt, generate_enhanced_claude_prompt
    except ImportError:
        def fallback_enhanced_sql(user_input: str):
            return f"以下の分析を実行してください: {user_input}"
        def fallback_enhanced_claude(user_input: str, df: pd.DataFrame, sample_data: str):
            return f"データを分析してください: {user_input}"
        return fallback_enhanced_sql, fallback_enhanced_claude

# 実際のインポート実行
ui_features = safe_import_ui_features()
run_analysis_flow = safe_import_analysis_controller()
select_best_prompt, PROMPT_DEFINITIONS = safe_import_prompts()
generate_enhanced_sql_prompt, generate_enhanced_claude_prompt = safe_import_enhanced_prompts()

# =========================================================================
# 分析レシピの定義
# =========================================================================

ANALYSIS_RECIPES = {
    "TOP10キャンペーン分析": "コスト上位10キャンペーンのROAS、CPA、CVRを分析し、最も効率的なキャンペーンを特定してください",
    "今月のパフォーマンス": "今月のデータに絞って、メディア別の主要KPI（CTR、CPA、ROAS）の変化を分析してください",
    "コスト効率分析": "CPA（顧客獲得単価）が最も良いキャンペーンを特定し、改善の余地があるキャンペーンと比較してください",
    "時系列トレンド": "過去30日間の日別パフォーマンス推移を可視化し、トレンドと異常値を特定してください",
    "メディア比較": "Google広告、Facebook広告、Yahoo!広告の効果を比較し、各メディアの特徴を分析してください",
    "曜日別分析": "曜日別のパフォーマンス（クリック数、コンバージョン数、CTR）を比較し、配信最適化を提案してください",
    "ROI最適化": "投資対効果（ROAS）の高いキャンペーンの特徴を分析し、予算配分の最適化案を提示してください",
    "自由入力": ""
}

# =========================================================================
# セッション状態初期化
# =========================================================================

def initialize_main_session_state():
    """メインUIのセッション状態初期化"""
    defaults = {
        "current_user_input": "",
        "selected_recipe": "自由入力",
        "prompt_system": "enhanced",  # enhanced または basic
        "selected_ai": "gemini+claude",  # gemini, claude, gemini+claude
        "analysis_in_progress": False,
        "show_advanced_options": False,
        "filter_start_date": datetime.now().date(),
        "filter_end_date": datetime.now().date(),
        "selected_media": [],
        "selected_campaigns": [],
        "last_analysis_timestamp": None
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

# =========================================================================
# AI選択UI
# =========================================================================

def show_ai_selection():
    """AI選択インターフェース"""
    st.markdown("### 🤖 AI選択")
    
    ai_options = {
        "🤖 Gemini（SQL生成専用）": "gemini",
        "🧠 Claude（分析専用）": "claude", 
        "🤖🧠 Gemini + Claude（推奨）": "gemini+claude"
    }
    
    selected_ai_label = st.selectbox(
        "使用するAI",
        list(ai_options.keys()),
        index=2,  # デフォルトはGemini + Claude
        help="Geminiは自然言語からSQLを生成し、Claudeはデータからビジネスインサイトを提供します"
    )
    
    st.session_state.selected_ai = ai_options[selected_ai_label]
    
    # AI選択に応じた説明表示
    if st.session_state.selected_ai == "gemini":
        st.info("🤖 **Geminiモード**: 自然言語からSQLを自動生成します")
    elif st.session_state.selected_ai == "claude":
        st.info("🧠 **Claudeモード**: データから戦略的な分析コメントを生成します（手動SQLが必要）")
    else:
        st.info("🤖🧠 **統合モード**: GeminiでSQL生成後、Claudeで詳細分析を実行します（推奨）")

# =========================================================================
# プロンプトシステム選択UI
# =========================================================================

def show_prompt_system_selection():
    """プロンプトシステム選択UI"""
    st.markdown("### ⚙️ プロンプトシステム")
    
    prompt_options = {
        "🚀 高品質モード（推奨）": "enhanced",
        "⚡ 基本モード": "basic"
    }
    
    selected_prompt_label = st.selectbox(
        "プロンプト品質",
        list(prompt_options.keys()),
        index=0,  # デフォルトは高品質モード
        help="高品質モードは業界ベンチマークと戦略的分析を含みます"
    )
    
    st.session_state.prompt_system = prompt_options[selected_prompt_label]
    
    # プロンプトシステムに応じた説明
    if st.session_state.prompt_system == "enhanced":
        st.success("🚀 **高品質モード**: 業界ベンチマーク比較・戦略提案・詳細分析を含みます")
    else:
        st.info("⚡ **基本モード**: シンプルで高速なSQL生成・基本的な分析を提供します")

# =========================================================================
# 分析レシピ選択UI  
# =========================================================================

def show_analysis_recipe_selection():
    """分析レシピ選択UI"""
    st.markdown("### 📋 分析レシピ")
    
    recipe_names = list(ANALYSIS_RECIPES.keys())
    selected_recipe = st.selectbox(
        "よく使われる分析パターン",
        recipe_names,
        index=recipe_names.index(st.session_state.get("selected_recipe", "自由入力"))
    )
    
    st.session_state.selected_recipe = selected_recipe
    
    # レシピに応じた説明表示
    if selected_recipe != "自由入力":
        recipe_description = ANALYSIS_RECIPES[selected_recipe]
        st.info(f"📝 **{selected_recipe}**: {recipe_description}")
        
        # レシピをテキストエリアに自動挿入
        if st.button(f"📋 「{selected_recipe}」を使用", width='stretch'):
            st.session_state.current_user_input = recipe_description
            st.rerun()

# =========================================================================
# 高度フィルター機能
# =========================================================================

def show_advanced_filters():
    """高度なフィルター機能"""
    if st.checkbox("🔍 詳細フィルター", value=st.session_state.get("show_advanced_options", False)):
        st.session_state.show_advanced_options = True
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📅 期間設定")
            st.session_state.filter_start_date = st.date_input(
                "開始日",
                value=st.session_state.get("filter_start_date", datetime.now().date())
            )
            st.session_state.filter_end_date = st.date_input(
                "終了日", 
                value=st.session_state.get("filter_end_date", datetime.now().date())
            )
        
        with col2:
            st.markdown("#### 📊 メディア・キャンペーン")
            
            # メディア選択（サンプル）
            media_options = ["Google広告", "Facebook広告", "Yahoo!広告", "LINE広告"]
            st.session_state.selected_media = st.multiselect(
                "メディア",
                media_options,
                default=st.session_state.get("selected_media", [])
            )
            
            # キャンペーン選択（サンプル）
            campaign_options = ["キャンペーン1", "キャンペーン2", "キャンペーン3"]
            st.session_state.selected_campaigns = st.multiselect(
                "キャンペーン",
                campaign_options,
                default=st.session_state.get("selected_campaigns", [])
            )
    else:
        st.session_state.show_advanced_options = False

# =========================================================================
# メイン入力UI
# =========================================================================

def show_main_input_interface():
    """メイン入力インターフェース"""
    st.markdown("### 💭 分析指示入力")
    
    # テキストエリアでの指示入力
    user_input = st.text_area(
        "どのような分析を行いますか？",
        value=st.session_state.get("current_user_input", ""),
        height=120,
        placeholder="例: 今月の広告効果が良いキャンペーンTOP5を、CPAとROASで比較分析してください",
        help="自然言語で分析したい内容を入力してください。AIが自動でSQLを生成します。"
    )
    
    st.session_state.current_user_input = user_input
    
    # 分析実行ボタン
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        analysis_disabled = not user_input.strip() or st.session_state.get("analysis_in_progress", False)
        
        if st.button(
            "🚀 分析実行" if not st.session_state.get("analysis_in_progress", False) else "⏳ 分析中...",
            disabled=analysis_disabled,
            width='stretch',
            type="primary"
        ):
            execute_main_analysis(user_input)
    
    with col2:
        if st.button("🧹 クリア", width='stretch'):
            st.session_state.current_user_input = ""
            st.rerun()
    
    with col3:
        if st.button("📝 SQL手動入力", width='stretch'):
            show_manual_sql_interface()

# =========================================================================
# 手動SQL入力インターフェース
# =========================================================================

def show_manual_sql_interface():
    """手動SQL入力インターフェース"""
    st.markdown("---")
    st.markdown("### 🖋️ 手動SQL入力")
    
    manual_sql = st.text_area(
        "SQLクエリを直接入力:",
        height=200,
        placeholder="""SELECT 
    CampaignName,
    SUM(CostIncludingFees) as Total_Cost,
    SUM(Conversions) as Total_Conversions,
    SAFE_DIVIDE(SUM(CostIncludingFees), SUM(Conversions)) as CPA
FROM `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`
GROUP BY CampaignName
ORDER BY CPA ASC
LIMIT 10""",
        help="BigQuery SQLを直接入力して実行できます"
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔍 手動SQL実行", disabled=not manual_sql.strip(), width='stretch'):
            execute_manual_sql(manual_sql)
    
    with col2:
        if st.button("🔙 戻る", width='stretch'):
            st.rerun()

# =========================================================================
# 分析実行処理
# =========================================================================

def execute_main_analysis(user_input: str):
    """メイン分析の実行"""
    if not user_input.strip():
        st.error("❌ 分析指示を入力してください")
        return
    
    # 分析開始の状態設定
    st.session_state.analysis_in_progress = True
    st.session_state.last_analysis_timestamp = datetime.now()
    
    try:
        # 必要なクライアントの確認
        gemini_model = st.session_state.get('gemini_model')
        claude_client = st.session_state.get('claude_client')
        claude_model_name = st.session_state.get('claude_model_name', 'claude-3-sonnet-20240229')
        bq_client = st.session_state.get('bq_client')
        
        # クライアントの状態確認
        if not bq_client:
            st.error("❌ BigQueryクライアントが初期化されていません")
            return
        
        if st.session_state.selected_ai in ["gemini", "gemini+claude"] and not gemini_model:
            st.error("❌ Geminiモデルが初期化されていません")
            return
        
        if st.session_state.selected_ai in ["claude", "gemini+claude"] and not claude_client:
            st.warning("⚠️ Claudeクライアントが利用できません。SQL生成のみ実行します。")
        
        # 分析履歴への追加
        if "analysis_history" not in st.session_state:
            st.session_state.analysis_history = []
        
        st.session_state.analysis_history.append({
            "timestamp": datetime.now(),
            "user_input": user_input,
            "prompt_system": st.session_state.prompt_system,
            "selected_ai": st.session_state.selected_ai
        })
        
        # 分析実行
        sheet_analysis_queries = {}  # Looker連携用（将来の拡張）
        
        success = run_analysis_flow(
            gemini_model=gemini_model,
            user_input=user_input,
            prompt_system=st.session_state.prompt_system,
            selected_ai=st.session_state.selected_ai,
            bq_client=bq_client
        )
        
        if success:
            st.success("✅ 分析が完了しました")
        
    except Exception as e:

        # AIに渡すためのコンテキスト情報を準備
        error_context = {
            "user_input": user_input,
            "generated_sql": st.session_state.get("last_sql", "SQL生成前にエラーが発生"),
            "ai_selection": st.session_state.selected_ai,
            "prompt_system": st.session_state.prompt_system
        }
        
        # AIエラーハンドラを呼び出し
        handle_error_with_ai(
            e=e, 
            model=st.session_state.get('gemini_model'), 
            context=error_context
        )
    
    finally:
        # 分析完了状態にリセット
        st.session_state.analysis_in_progress = False

def execute_manual_sql(sql: str):
    """手動SQL実行"""
    if not sql.strip():
        st.error("❌ SQLを入力してください")
        return
    
    try:
        bq_client = st.session_state.get('bq_client')
        if not bq_client:
            st.error("❌ BigQueryクライアントが初期化されていません")
            return
        
        with st.spinner("🔍 SQLを実行中..."):
            # analysis_controllerの関数を使用（利用可能な場合）
            try:
                from analysis_controller import execute_sql_with_enhanced_handling
                df = execute_sql_with_enhanced_handling(bq_client, sql)
            except ImportError:
                # フォールバック
                df = bq_client.query(sql).to_dataframe()
        
        if df is not None and not df.empty:
            st.success(f"✅ SQL実行完了 ({len(df):,}行)")
            
            # 結果の保存
            st.session_state.last_analysis_result = df
            st.session_state.last_sql = sql
            st.session_state.last_user_input = "手動SQL実行"
            
            # データの表示
            st.subheader("📊 実行結果")
            st.dataframe(df, width='stretch')
            
            # 基本統計
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("データ行数", f"{len(df):,}")
            with col2:
                st.metric("列数", len(df.columns))
            with col3:
                numeric_cols = df.select_dtypes(include=['number']).columns
                st.metric("数値列", len(numeric_cols))
        else:
            st.warning("⚠️ クエリ結果が空です")
            
    except Exception as e:

        # AIに渡すためのコンテキスト情報を準備
        error_context = {
            "user_input": "手動SQL実行",
            "generated_sql": sql
        }
        
        # AIエラーハンドラを呼び出し
        handle_error_with_ai(
            e=e,
            model=st.session_state.get('gemini_model'),
            context=error_context
        )

# =========================================================================
# 結果表示UI
# =========================================================================

def show_analysis_results():
    """分析結果の表示"""
    if not hasattr(st.session_state, 'last_analysis_result') or st.session_state.last_analysis_result is None:
        return
    
    df = st.session_state.last_analysis_result
    
    st.markdown("---")
    st.subheader("📊 最新の分析結果")
    
    # データ表示
    st.dataframe(df, width='stretch')
    
    # 基本統計情報
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("データ行数", f"{len(df):,}")
    
    with col2:
        st.metric("列数", len(df.columns))
    
    with col3:
        numeric_cols = df.select_dtypes(include=['number']).columns
        st.metric("数値列", len(numeric_cols))
    
    with col4:
        if st.session_state.get("last_analysis_timestamp"):
            elapsed = datetime.now() - st.session_state.last_analysis_timestamp
            st.metric("実行時間", f"{elapsed.total_seconds():.1f}秒")
    
    # エクスポート機能
    show_export_options(df)

# =========================================================================
# エクスポート機能
# =========================================================================

def show_export_options(df: pd.DataFrame):
    """エクスポートオプションの表示"""
    if df is None or df.empty:
        return
    
    st.markdown("### 📤 データエクスポート")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📊 CSV ダウンロード", width='stretch'):
            csv_data = df.to_csv(index=False)
            st.download_button(
                label="💾 CSVをダウンロード",
                data=csv_data,
                file_name=f"analysis_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                width='stretch'
            )
    
    with col2:
        if st.button("📋 JSON ダウンロード", width='stretch'):
            json_data = df.to_json(orient='records', ensure_ascii=False, indent=2)
            st.download_button(
                label="💾 JSONをダウンロード",
                data=json_data,
                file_name=f"analysis_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                width='stretch'
            )
    
    with col3:
        if st.button("📈 分析レポート", width='stretch'):
            report = generate_analysis_report(df)
            st.download_button(
                label="💾 レポートをダウンロード",
                data=report,
                file_name=f"analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md",
                mime="text/markdown",
                width='stretch'
            )

def generate_analysis_report(df: pd.DataFrame) -> str:
    """分析レポートの生成"""
    report = f"""# 分析レポート

## 📊 データ概要
- **生成日時**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}
- **データ行数**: {len(df):,}行
- **列数**: {len(df.columns)}列

## 📋 データ構造
"""
    
    # 列情報の追加
    for col in df.columns:
        dtype = str(df[col].dtype)
        non_null = df[col].count()
        report += f"- **{col}**: {dtype} ({non_null:,}/{len(df):,} 非NULL)\n"
    
    # 数値列の統計情報
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        report += f"\n## 📈 数値列統計\n"
        for col in numeric_cols:
            stats = df[col].describe()
            report += f"\n### {col}\n"
            report += f"- **平均**: {stats['mean']:.2f}\n"
            report += f"- **中央値**: {stats['50%']:.2f}\n"
            report += f"- **最小値**: {stats['min']:.2f}\n"
            report += f"- **最大値**: {stats['max']:.2f}\n"
    
    # サンプルデータ
    report += f"\n## 📋 サンプルデータ（上位5行）\n\n"
    report += df.head().to_markdown(index=False)
    
    return report

# =========================================================================
# メイン分析ワークベンチ
# =========================================================================

def show_analysis_workbench(gemini_model, claude_client, claude_model_name, sheet_analysis_queries):
    """AIアシスタント分析ワークベンチのメイン画面"""
    st.header("🤖 AIアシスタント分析")
    
    # セッション状態の初期化
    initialize_main_session_state()

    # =====================================================
    # 分析サマリー・機能パネル（ui_features.pyから）
    # =====================================================
    try:
        ui_features["show_analysis_summary_panel"]()
        
        # データ品質チェック
        if st.session_state.get("last_analysis_result") is not None:
            with st.expander("🔍 データ品質チェック", expanded=False):
                ui_features["show_data_quality_panel"]()
        
        # エラー履歴
        ui_features["show_error_history"]()
        
        # 使用統計
        ui_features["show_usage_statistics"]()
        
    except Exception as e:
        st.warning(f"⚠️ 一部の拡張機能が利用できません: {e}")

    # =====================================================  
    # AIシステム選択
    # =====================================================
    
    with st.expander("🎛️ AI・プロンプト設定", expanded=True):
        col1, col2 = st.columns(2)
        
        with col1:
            show_ai_selection()
        
        with col2:
            show_prompt_system_selection()

    # =====================================================
    # 分析レシピ選択
    # =====================================================
    
    with st.expander("📋 分析レシピ（よく使われるパターン）", expanded=False):
        show_analysis_recipe_selection()

    # =====================================================
    # 高度フィルター
    # =====================================================
    
    show_advanced_filters()

    # =====================================================
    # メイン入力インターフェース
    # =====================================================
    
    show_main_input_interface()

    # =====================================================
    # 分析結果表示
    # =====================================================
    
    show_analysis_results()

    # =====================================================
    # 再分析・履歴機能
    # =====================================================
    
    if st.session_state.get("last_analysis_result") is not None:
        st.markdown("---")
        st.subheader("🔄 再分析・履歴")
        
        try:
            ui_features["show_quick_reanalysis"]()
        except Exception as e:
            st.info("🔄 再分析機能は一時的に利用できません")
        
        # 分析履歴の表示
        if st.session_state.get("analysis_history"):
            with st.expander("📈 分析履歴", expanded=False):
                history = st.session_state.analysis_history[-10:]  # 最新10件
                
                for i, record in enumerate(reversed(history)):
                    timestamp = record["timestamp"].strftime("%m/%d %H:%M")
                    user_input = record["user_input"][:50] + "..." if len(record["user_input"]) > 50 else record["user_input"]
                    
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.markdown(f"**{timestamp}**: {user_input}")
                        st.caption(f"AI: {record['selected_ai']} | プロンプト: {record['prompt_system']}")
                    
                    with col2:
                        if st.button("🔄 再実行", key=f"rerun_{i}"):
                            st.session_state.current_user_input = record["user_input"]
                            st.rerun()

# =========================================================================
# エラー処理・ログ機能
# =========================================================================

def log_ui_interaction(action: str, details: Dict = None):
    """UI操作のログ記録"""
    try:
        if "ui_interaction_log" not in st.session_state:
            st.session_state.ui_interaction_log = []
        
        log_entry = {
            "timestamp": datetime.now(),
            "action": action,
            "details": details or {},
            "session_id": id(st.session_state)  # セッション識別用
        }
        
        st.session_state.ui_interaction_log.append(log_entry)
        
        # ログサイズの制限
        if len(st.session_state.ui_interaction_log) > 100:
            st.session_state.ui_interaction_log = st.session_state.ui_interaction_log[-100:]
            
    except Exception:
        pass  # ログ記録エラーはアプリケーションを停止させない

def handle_ui_error(error: Exception, context: str = "UI操作"):
    """UI固有のエラーハンドリング"""
    error_message = f"{context}中にエラーが発生しました: {str(error)}"
    
    # ユーザー向け表示
    st.error(f"❌ {error_message}")
    
    # デバッグ情報（デバッグモード時のみ）
    if st.session_state.get("debug_mode", False):
        with st.expander("🐛 詳細エラー情報"):
            st.code(f"エラー: {error}")
            st.code(f"コンテキスト: {context}")
    
    # ログ記録
    log_ui_interaction("error", {
        "error_message": str(error),
        "context": context,
        "error_type": type(error).__name__
    })

# =========================================================================
# アクセシビリティ機能
# =========================================================================

def apply_accessibility_settings():
    """アクセシビリティ設定の適用"""
    accessibility_settings = st.session_state.get("accessibility_settings", {})
    
    if accessibility_settings.get("high_contrast", False):
        st.markdown("""
        <style>
        .stApp { 
            background-color: #000000;
            color: #FFFFFF;
        }
        </style>
        """, unsafe_allow_html=True)
    
    if accessibility_settings.get("large_text", False):
        st.markdown("""
        <style>
        .stMarkdown { font-size: 18px; }
        .stButton button { font-size: 16px; }
        </style>
        """, unsafe_allow_html=True)

# =========================================================================
# パフォーマンス最適化
# =========================================================================

@st.cache_data(ttl=300)  # 5分間キャッシュ
def get_cached_recipe_suggestions(user_input: str) -> List[str]:
    """レシピ提案のキャッシュ版"""
    # 入力に基づく関連レシピの提案
    suggestions = []
    
    keywords = user_input.lower()
    
    if "キャンペーン" in keywords or "top" in keywords:
        suggestions.append("TOP10キャンペーン分析")
    
    if "コスト" in keywords or "cpa" in keywords:
        suggestions.append("コスト効率分析")
    
    if "時系列" in keywords or "トレンド" in keywords:
        suggestions.append("時系列トレンド")
    
    if "メディア" in keywords or "比較" in keywords:
        suggestions.append("メディア比較")
    
    return suggestions