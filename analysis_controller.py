# analysis_controller.py - 完全修正版
"""
分析フロー制御 - SQL生成エラー修正版
- 分析実行の統合管理
- プロンプトシステムの選択・実行
- エラーハンドリングの統合
- 実行時間の管理
- 履歴・ログの記録
- SQL生成・実行の完全なエラーハンドリング
"""

import streamlit as st
import pandas as pd
import re
from datetime import datetime, date
from typing import Dict, List, Optional, Any

# =========================================================================
# SQL生成とクリーニング機能
# =========================================================================

def validate_basic_sql_syntax(sql: str) -> bool:
    """基本的なSQL構文チェック"""
    if not sql or not sql.strip():
        return False
    
    sql_upper = sql.strip().upper()
    
    # 基本的な構文チェック
    valid_starts = ['SELECT', 'WITH', 'CREATE', 'INSERT', 'UPDATE', 'DELETE']
    if not any(sql_upper.startswith(start) for start in valid_starts):
        return False
    
    # 括弧の対応チェック
    open_count = sql.count('(')
    close_count = sql.count(')')
    if open_count != close_count:
        return False
    
    # 基本的なキーワードの存在チェック（SELECT文の場合）
    if sql_upper.startswith('SELECT'):
        if 'FROM' not in sql_upper:
            return False
    
    return True

def clean_generated_sql(raw_sql: str) -> str:
    """
    Geminiで生成されたSQLからクリーンなSQLを抽出
    """
    if not raw_sql or not raw_sql.strip():
        raise ValueError("生成されたSQLが空です")
    
    # マークダウンのコードブロックを除去
    sql = re.sub(r'```sql\s*\n?', '', raw_sql)
    sql = re.sub(r'```\s*$', '', sql)
    
    # コメント行の除去（#で始まる行）
    lines = sql.split('\n')
    cleaned_lines = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith('#') and not line.startswith('--'):
            cleaned_lines.append(line)
    
    sql = '\n'.join(cleaned_lines).strip()
    
    # 複数のSQLが含まれている場合は最初のものを取得
    sql_statements = sql.split(';')
    main_sql = sql_statements[0].strip()
    
    if not main_sql:
        raise ValueError("有効なSQLが見つかりません")
    
    return main_sql

# =========================================================================
# プロンプトシステム統合
# =========================================================================

def get_prompt_system():
    """使用するプロンプトシステムを取得"""
    try:
        # 強化プロンプトシステムの優先使用
        from enhanced_prompts import generate_enhanced_sql_prompt
        return "enhanced", generate_enhanced_sql_prompt
    except ImportError:
        try:
            # 基本プロンプトシステムにフォールバック
            from prompts import select_best_prompt
            return "basic", select_best_prompt
        except ImportError:
            # 最終フォールバック
            return "fallback", None

def create_sql_prompt(user_input: str, system_type: str = "enhanced") -> str:
    """統一されたSQL生成プロンプト作成"""
    
    system_type, prompt_func = get_prompt_system()
    
    if system_type == "enhanced" and prompt_func:
        try:
            return prompt_func(user_input)
        except Exception as e:
            st.warning(f"強化プロンプト生成エラー: {e}")
            # 基本プロンプトにフォールバック
            pass
    
    if system_type == "basic" and prompt_func:
        try:
            prompt_info = prompt_func(user_input)
            return prompt_info.get("template", "").format(user_input=user_input)
        except Exception as e:
            st.warning(f"基本プロンプト生成エラー: {e}")
            # フォールバックプロンプトに移行
            pass
    
    # 最終フォールバック
    return f"""
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}
# 分析対象: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`
# 出力: 実行可能なBigQuery SQLのみを返してください。説明やマークダウン記法は不要です。

# 重要な出力要件:
1. SQLコードのみを返してください
2. ```sql``` などのマークダウン記法は使用しないでください  
3. コメント行(#で始まる行)は含めないでください
4. 説明文は不要です
5. 実行可能なSQLのみを出力してください
"""

# =========================================================================
# Gemini SQL生成機能
# =========================================================================

def execute_gemini_sql_generation(gemini_model, user_input: str, max_attempts: int = 3) -> Optional[str]:
    """
    Gemini APIを使用してSQLを生成する統合機能
    """
    if not gemini_model:
        st.error("❌ Gemini モデルが初期化されていません")
        return None
    
    for attempt in range(max_attempts):
        try:
            # プロンプトの生成
            prompt = create_sql_prompt(user_input)
            
            # Gemini APIの呼び出し
            with st.spinner(f"🧠 AIがSQLを生成中... (試行 {attempt + 1}/{max_attempts})"):
                response = gemini_model.generate_content(prompt)
                
                if not response or not response.text:
                    st.warning(f"⚠️ Geminiからの応答が空です (試行 {attempt + 1})")
                    continue
            
            # SQLのクリーニング
            try:
                cleaned_sql = clean_generated_sql(response.text)
            except ValueError as e:
                st.warning(f"⚠️ SQL抽出エラー: {e} (試行 {attempt + 1})")
                continue
            
            # SQL構文の基本的な検証
            if validate_basic_sql_syntax(cleaned_sql):
                st.success("✅ 有効なSQLが生成されました")
                return cleaned_sql
            else:
                st.warning(f"⚠️ SQL構文に問題があります。再試行します... (試行 {attempt + 1})")
                continue
                
        except Exception as e:
            st.warning(f"⚠️ Gemini API エラー: {e} (試行 {attempt + 1})")
            if attempt == max_attempts - 1:
                st.error("❌ SQL生成に失敗しました。手動でSQLを入力してください。")
                return None
            continue
    
    return None

# =========================================================================
# SQL実行機能（改良版）
# =========================================================================

def execute_sql_with_enhanced_handling(bq_client, sql: str) -> Optional[pd.DataFrame]:
    """
    BigQueryでのSQL実行（エラーハンドリング強化版）
    """
    if not sql or not sql.strip():
        st.error("❌ 実行するSQLが空です")
        return None
    
    if not bq_client:
        st.error("❌ BigQueryクライアントが初期化されていません")
        return None
    
    try:
        # SQL実行前の検証
        if not validate_basic_sql_syntax(sql):
            st.error("❌ SQL構文に問題があります")
            return None
        
        # BigQueryでの実行
        with st.spinner("🔍 SQLを実行中..."):
            query_job = bq_client.query(sql)
            df = query_job.to_dataframe()
        
        if df.empty:
            st.warning("⚠️ クエリ結果が空です")
            return df
        
        st.success(f"✅ データ取得完了 ({len(df):,}行)")
        return df
        
    except Exception as e:
        # 詳細なエラー情報の表示
        error_str = str(e)
        st.error(f"❌ SQL実行エラー: {error_str}")
        
        # エラーの種類別対応
        if "Syntax error" in error_str:
            st.error("🔍 **SQL構文エラー**")
            st.info("💡 生成されたSQLに構文エラーがあります。以下を確認してください:")
            st.markdown("""
            - SELECT文で始まっているか
            - カンマやクォートが正しく記述されているか  
            - テーブル名が正確に記述されているか
            """)
            
        elif "Table" in error_str and "not found" in error_str:
            st.error("🔍 **テーブル未発見エラー**")
            st.info("💡 指定されたテーブルが存在しません")
            
        elif "Column" in error_str and "not found" in error_str:
            st.error("🔍 **列未発見エラー**") 
            st.info("💡 存在しない列名が指定されています")
            
        elif "Access Denied" in error_str or "permission" in error_str.lower():
            st.error("🔍 **アクセス権限エラー**")
            st.info("💡 BigQueryへのアクセス権限を確認してください")
            
        else:
            st.error("🔍 **その他のエラー**")
            st.info("💡 詳細なエラーメッセージを確認し、システム管理者にお問い合わせください")
        
        # エラー履歴への追加
        try:
            add_error_to_history(error_str, "SQL実行エラー", ["SQL構文を確認", "権限を確認", "テーブル名を確認"])
        except:
            pass  # エラーハンドラーが利用できない場合はスキップ
            
        return None

# =========================================================================
# Claude分析機能
# =========================================================================

def execute_claude_analysis(claude_client, claude_model_name: str, df: pd.DataFrame, user_input: str) -> Optional[str]:
    """
    Claude APIを使用したデータ分析
    """
    if not claude_client or df is None or df.empty:
        return None
    
    try:
        # データサンプルの準備
        sample_data = df.head(10).to_string() if len(df) > 10 else df.to_string()
        
        # 分析プロンプトの生成
        try:
            from enhanced_prompts import generate_enhanced_claude_prompt
            analysis_prompt = generate_enhanced_claude_prompt(user_input, df, sample_data)
        except ImportError:
            analysis_prompt = f"""
以下のデータ分析結果について、マーケティング戦略の観点から分析・コメントを提供してください：

【分析指示】
{user_input}

【データサンプル】
{sample_data}

【求める分析内容】
1. 主要な傾向・パターンの特定
2. ビジネス上の示唆・洞察
3. 具体的な改善提案
4. 次のアクション
"""
        
        # Claude APIの呼び出し
        with st.spinner("🧠 AIが分析中..."):
            message = claude_client.messages.create(
                model=claude_model_name,
                max_tokens=2000,
                messages=[{
                    "role": "user",
                    "content": analysis_prompt
                }]
            )
        
        return message.content[0].text if message.content else None
        
    except Exception as e:
        st.error(f"❌ Claude分析エラー: {e}")
        return None

# =========================================================================
# UI補助機能
# =========================================================================

def show_manual_sql_input():
    """手動SQL入力UI"""
    st.markdown("---")
    st.subheader("🖋️ 手動SQL入力")
    
    manual_sql = st.text_area(
        "SQLを直接入力してください:",
        height=200,
        placeholder="SELECT * FROM `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign` LIMIT 10"
    )
    
    if st.button("🔍 手動SQLを実行", use_container_width=True):
        if manual_sql.strip():
            df = execute_sql_with_enhanced_handling(st.session_state.bq_client, manual_sql)
            if df is not None:
                st.session_state.last_analysis_result = df
                st.session_state.last_sql = manual_sql
                st.dataframe(df)

def show_error_recovery_options(user_input: str):
    """エラー回復オプション表示"""
    st.markdown("---")
    st.subheader("🔧 エラー回復オプション")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 SQL再生成", use_container_width=True):
            st.rerun()
    
    with col2:
        if st.button("🖋️ 手動入力", use_container_width=True):
            show_manual_sql_input()

# =========================================================================
# ユーティリティ関数
# =========================================================================

def log_analysis_usage(user_input: str, system_type: str, execution_time: float = 0, error: bool = False):
    """分析使用状況のログ記録"""
    try:
        from ui_features import log_analysis_usage as ui_log
        ui_log(user_input, system_type, execution_time, error)
    except ImportError:
        # フォールバック
        if "usage_stats" not in st.session_state:
            st.session_state.usage_stats = {"total_analyses": 0, "error_count": 0}
        st.session_state.usage_stats["total_analyses"] += 1
        if error:
            st.session_state.usage_stats["error_count"] += 1

def add_error_to_history(error_message: str, error_category: str = "一般エラー", solutions: List[str] = None):
    """エラー履歴への追加"""
    try:
        from ui_features import add_error_to_history as ui_add_error
        ui_add_error(error_message, error_category, solutions)
    except ImportError:
        # フォールバック
        if "error_history" not in st.session_state:
            st.session_state.error_history = []
        st.session_state.error_history.append({
            "timestamp": datetime.now(),
            "message": error_message,
            "category": error_category,
            "solutions": solutions or []
        })

# =========================================================================
# メイン分析フロー制御（修正版）
# =========================================================================

def run_analysis_flow(gemini_model, claude_client, claude_model_name, user_input, sheet_analysis_queries=None):
    """
    修正された分析フロー実行関数
    """
    if not user_input or not user_input.strip():
        st.error("❌ 分析指示を入力してください")
        return
    
    start_time = datetime.now()
    
    try:
        # SQL生成段階
        with st.spinner("🧠 AIがSQLを生成中..."):
            generated_sql = execute_gemini_sql_generation(gemini_model, user_input)
            
            if not generated_sql:
                st.error("❌ SQL生成に失敗しました")
                show_manual_sql_input()
                return
        
        # 生成されたSQLの表示
        with st.expander("📝 生成されたSQL", expanded=False):
            st.code(generated_sql, language="sql")
        
        # SQL実行段階
        with st.spinner("🔍 データを取得中..."):
            df = execute_sql_with_enhanced_handling(st.session_state.bq_client, generated_sql)
            
            if df is None or df.empty:
                st.error("❌ データの取得に失敗しました")
                show_error_recovery_options(user_input)
                return
        
        # 結果の保存
        st.session_state.last_analysis_result = df
        st.session_state.last_sql = generated_sql
        st.session_state.last_user_input = user_input
        
        # データの表示
        st.subheader("📊 分析結果")
        st.dataframe(df, use_container_width=True)
        
        # 基本統計情報
        st.subheader("📈 データ概要")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("データ行数", f"{len(df):,}")
        with col2:
            st.metric("列数", len(df.columns))
        with col3:
            numeric_cols = df.select_dtypes(include=['number']).columns
            st.metric("数値列", len(numeric_cols))
        
        # Claude分析（オプション）
        if st.session_state.get("auto_claude_analysis", True) and claude_client:
            with st.spinner("🧠 AIが詳細分析中..."):
                analysis_comment = execute_claude_analysis(claude_client, claude_model_name, df, user_input)
                
                if analysis_comment:
                    st.subheader("🎯 AI分析レポート")
                    st.markdown(analysis_comment)
        
        # 実行時間の記録
        execution_time = (datetime.now() - start_time).total_seconds()
        log_analysis_usage(user_input, "complete_flow", execution_time, False)
        
        st.success(f"✅ 分析完了 (実行時間: {execution_time:.1f}秒)")
        
    except Exception as e:
        # 全体的なエラーハンドリング
        execution_time = (datetime.now() - start_time).total_seconds()
        log_analysis_usage(user_input, "error", execution_time, True)
        add_error_to_history(str(e), "分析フロー全体エラー")
        
        st.error(f"❌ 分析フローエラー: {e}")
        show_error_recovery_options(user_input)