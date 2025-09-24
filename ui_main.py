# ui_main.py
"""
メイン分析ワークベンチUI
- 基本的な分析画面構成
- プロンプトシステム選択
- AI選択機能
- 基本的な入力UI
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any

# 他の分割ファイルからのインポート
try:
    from ui_features import (
        show_analysis_summary_panel,
        show_data_quality_panel, 
        show_error_history,
        show_usage_statistics,
        show_quick_reanalysis
    )
except ImportError:
    st.warning("ui_features.py が見つかりません")

try:
    from analysis_controller import run_analysis_flow
except ImportError:
    st.warning("analysis_controller.py が見つかりません")

try:
    from prompts import *  # 基本プロンプトシステム
except ImportError:
    st.error("prompts.py が見つかりません")

try:
    from enhanced_prompts import *  # 強化プロンプトシステム
except ImportError:
    st.warning("enhanced_prompts.py が見つかりません - 基本プロンプトのみ使用")

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
        show_analysis_summary_panel()
        
        # データ品質チェック
        if not st.session_state.get("df", pd.DataFrame()).empty:
            show_data_quality_panel(st.session_state.df)
        
        # エラー履歴・使用統計
        show_error_history()
        show_usage_statistics()
        
    except NameError:
        # ui_features.pyが利用できない場合の基本表示
        show_basic_summary()

    # =====================================================
    # メイン入力UI
    # =====================================================
    with st.expander("📝 データ出力指示", expanded=True):
        st.subheader("① データ出力指示")
        st.markdown("---")

        # プロンプトシステム選択
        show_prompt_system_selector()
        
        # 分析指示入力
        show_analysis_input_section()
        
        # クイック分析（ui_features.pyから）
        try:
            if not st.session_state.get("df", pd.DataFrame()).empty:
                st.markdown("---")
                quick_instruction = show_quick_reanalysis()
                if quick_instruction:
                    st.session_state.user_input = quick_instruction
        except NameError:
            pass

        # 分析実行ボタン
        show_analysis_execution_buttons(gemini_model, claude_client, claude_model_name, sheet_analysis_queries)

    # =====================================================
    # 結果表示エリア
    # =====================================================
    show_results_sections(claude_client, claude_model_name)

def show_prompt_system_selector():
    """プロンプトシステム選択UI"""
    system_col, ai_col = st.columns(2)
    
    with system_col:
        prompt_system = st.selectbox(
            "📊 分析レベル",
            options=["高品質分析 (Enhanced)", "コスト重視 (Basic)"],
            index=0,  # デフォルトは Enhanced
            key="prompt_system_selector",
            help="高品質: 詳細な業界知識と戦略的洞察 / コスト重視: シンプルで高速な基本分析"
        )
        st.session_state.use_enhanced_prompts = (prompt_system == "高品質分析 (Enhanced)")
    
    with ai_col:
        st.session_state.selected_ai = st.selectbox(
            "🤖 使用するAI",
            ["Gemini (SQL生成)", "Claude (解説・洞察)"],
            key="ai_selector",
            help="Gemini: 高精度なSQL生成 / Claude: 戦略的な分析洞察"
        )

    # プロンプトシステムの説明表示
    if st.session_state.use_enhanced_prompts:
        st.success("✅ **高品質分析モード**: 業界ベンチマーク・戦略的洞察・詳細な改善提案を含む分析")
        st.caption("💰 APIコスト: 高 | 📊 分析品質: 最高 | ⏱️ 実行時間: やや長")
    else:
        st.info("⚡ **コスト重視モード**: シンプルで高速な基本分析")
        st.caption("💰 APIコスト: 低 | 📊 分析品質: 標準 | ⏱️ 実行時間: 高速")

def show_analysis_input_section():
    """分析指示入力セクション"""
    st.markdown("---")
    
    # 分析レシピ選択
    selected_recipe = st.selectbox(
        "分析レシピを選択",
        list(ANALYSIS_RECIPES.keys()),
        key="selected_recipe",
        help="事前定義された分析パターンから選択できます"
    )
    
    # 自由入力エリア
    user_input = st.text_area(
        "AIへの指示を自由に入力してください",
        value=ANALYSIS_RECIPES[selected_recipe],
        height=150,
        key="user_input",
        help="具体的で詳細な指示ほど、より良い分析結果が得られます"
    )
    
    return user_input

def show_analysis_execution_buttons(gemini_model, claude_client, claude_model_name, sheet_analysis_queries):
    """分析実行ボタンセクション"""
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if st.button("▶️ 新しい分析を開始", type="primary", use_container_width=True):
            user_input = st.session_state.get("user_input", "").strip()
            if not user_input:
                st.warning("指示を入力してください。")
            else:
                # analysis_controller.pyの分析フロー実行
                try:
                    run_analysis_flow(
                        gemini_model=gemini_model,
                        claude_client=claude_client,
                        claude_model_name=claude_model_name,
                        user_input=user_input,
                        sheet_analysis_queries=sheet_analysis_queries
                    )
                except NameError:
                    st.error("analysis_controller.py が見つかりません")
    
    with col2:
        if st.button("🔄 データ再読込", use_container_width=True):
            if st.session_state.get("sql"):
                st.session_state.rerun_sql = True
                st.rerun()

def show_results_sections(claude_client, claude_model_name):
    """結果表示セクション"""
    # SQL表示・編集セクション
    if st.session_state.get("sql"):
        show_sql_section()
    
    # データ表示・可視化セクション
    if st.session_state.get("df") is not None and not st.session_state.df.empty:
        show_data_visualization_section()
    
    # AI分析・コメントセクション
    if st.session_state.get("df") is not None and not st.session_state.df.empty:
        show_ai_comments_section(claude_client, claude_model_name)

def show_sql_section():
    """SQL表示・編集セクション"""
    with st.expander("② SQL表示・編集", expanded=True):
        st.subheader("② SQL表示・編集")
        
        # SQL実行結果の表示
        if st.session_state.get("df") is not None and not st.session_state.df.empty:
            st.success(f"✅ SQLが正常に実行されました。{len(st.session_state.df)}行のデータを取得。")
        elif st.session_state.get("sql_error"):
            st.error(f"❌ SQLエラー: {st.session_state.sql_error}")
        
        # SQLの表示・編集
        editable_sql = st.text_area(
            "生成されたSQL（編集可能）",
            value=st.session_state.sql,
            height=300,
            key="editable_sql"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 SQLを再実行", use_container_width=True):
                st.session_state.sql = editable_sql
                st.session_state.rerun_sql = True
                st.rerun()
        
        with col2:
            if st.button("📋 SQLをコピー", use_container_width=True):
                st.code(editable_sql, language="sql")
                st.success("SQLが表示されました。手動でコピーしてください。")

def show_data_visualization_section():
    """データ可視化セクション"""
    with st.expander("③ データ表示・可視化", expanded=True):
        st.subheader("③ データ表示・可視化")
        
        # データテーブル表示
        st.dataframe(st.session_state.df, use_container_width=True, height=400)
        
        # 基本的なグラフ機能
        show_basic_chart_options()

def show_basic_chart_options():
    """基本的なグラフオプション"""
    if len(st.session_state.df) > 0:
        st.markdown("---")
        st.markdown("**📊 グラフ設定**")
        
        numeric_columns = st.session_state.df.select_dtypes(include=['number']).columns.tolist()
        all_columns = st.session_state.df.columns.tolist()
        
        if numeric_columns and all_columns:
            col1, col2, col3 = st.columns(3)
            with col1:
                chart_type = st.selectbox("グラフの種類", ["棒グラフ", "線グラフ", "散布図"])
            with col2:
                x_axis = st.selectbox("X軸", all_columns)
            with col3:
                y_axis = st.selectbox("Y軸", numeric_columns)
            
            # 簡単なグラフ生成
            try:
                import plotly.express as px
                
                if chart_type == "棒グラフ":
                    fig = px.bar(st.session_state.df, x=x_axis, y=y_axis, title=f"{y_axis} by {x_axis}")
                elif chart_type == "線グラフ":
                    fig = px.line(st.session_state.df, x=x_axis, y=y_axis, title=f"{y_axis} over {x_axis}")
                elif chart_type == "散布図":
                    fig = px.scatter(st.session_state.df, x=x_axis, y=y_axis, title=f"{y_axis} vs {x_axis}")
                
                st.plotly_chart(fig, use_container_width=True)
                
            except Exception as e:
                st.warning(f"グラフの生成に失敗しました: {str(e)}")

def show_ai_comments_section(claude_client, claude_model_name):
    """AI分析コメントセクション"""
    with st.expander("④ AI分析・コメント", expanded=True):
        st.subheader("④ AI分析・コメント")
        
        if st.session_state.get("comment"):
            # 既存のコメントを表示
            st.markdown("**🎯 分析結果・提案**")
            st.markdown(st.session_state.comment)
            
            # 再分析ボタン
            if st.button("🔄 分析を更新", key="rerun_claude"):
                run_claude_analysis(claude_client, claude_model_name)
        else:
            # 初回分析
            if st.button("🎯 Claude分析を開始", type="primary"):
                run_claude_analysis(claude_client, claude_model_name)

def run_claude_analysis(claude_client, claude_model_name):
    """Claude分析の実行"""
    if st.session_state.get("df") is not None and not st.session_state.df.empty:
        with st.spinner("Claudeで分析中..."):
            try:
                data_sample = st.session_state.df.head(20).to_string()
                
                # プロンプトシステムに応じたプロンプト生成
                if st.session_state.get("use_enhanced_prompts", True):
                    try:
                        prompt = generate_enhanced_claude_prompt(
                            data_sample,
                            str(st.session_state.get("graph_cfg", {}))
                        )
                    except NameError:
                        prompt = f"以下のデータを詳細に分析してください:\n\n{data_sample}"
                else:
                    prompt = f"以下のデータを分析してください:\n\n{data_sample}"
                
                response = claude_client.messages.create(
                    model=claude_model_name,
                    max_tokens=3000,
                    messages=[{"role": "user", "content": prompt}]
                )
                st.session_state.comment = response.content[0].text
                st.success("✅ Claude分析が完了しました！")
                st.rerun()
                
            except Exception as e:
                st.error(f"Claude分析に失敗しました: {str(e)}")
    else:
        st.warning("分析対象のデータがありません。")

def show_basic_summary():
    """基本的なサマリー表示（ui_features.pyが利用できない場合）"""
    if not st.session_state.get("df", pd.DataFrame()).empty:
        df = st.session_state.df
        
        with st.expander("📈 分析結果サマリー", expanded=True):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("データ行数", len(df))
            
            with col2:
                numeric_cols = df.select_dtypes(include=['number']).columns
                st.metric("数値列数", len(numeric_cols))
            
            with col3:
                st.metric("実行時刻", datetime.now().strftime("%H:%M"))

def initialize_main_session_state():
    """メイン画面用のセッション状態初期化"""
    if "use_enhanced_prompts" not in st.session_state:
        st.session_state.use_enhanced_prompts = True  # デフォルトは高品質分析
    
    if "selected_ai" not in st.session_state:
        st.session_state.selected_ai = "Gemini (SQL生成)"

# =========================================================================
# 他のファイルから呼び出し用のメイン関数
# =========================================================================

def show_main_analysis_interface(gemini_model, claude_client, claude_model_name, sheet_analysis_queries):
    """メイン分析インターフェースの表示（他ファイルから呼び出し用）"""
    show_analysis_workbench(gemini_model, claude_client, claude_model_name, sheet_analysis_queries)