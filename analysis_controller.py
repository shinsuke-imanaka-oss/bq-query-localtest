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
from datetime import datetime
from typing import Dict, List, Optional, Any

# 必要なモジュールのインポート
try:
    from prompts import select_best_prompt, GENERAL_SQL_TEMPLATE
except ImportError:
    st.error("prompts.py が見つかりません")
    def select_best_prompt(user_input: str) -> dict:
        return {"description": "基本分析", "template": "基本的なSQL分析"}

try:
    from enhanced_prompts import generate_enhanced_sql_prompt, generate_enhanced_claude_prompt
except ImportError:
    st.warning("enhanced_prompts.py が見つかりません - 基本プロンプトのみ使用")
    generate_enhanced_sql_prompt = None
    generate_enhanced_claude_prompt = None

try:
    from error_handler import EnhancedErrorHandler, show_enhanced_error_message
except ImportError:
    st.warning("error_handler.py が見つかりません - 基本エラーハンドリングのみ")
    EnhancedErrorHandler = None
    show_enhanced_error_message = None

try:
    from ui_features import log_analysis_usage, add_error_to_history
except ImportError:
    # ui_features.pyが利用できない場合の代替関数
    def log_analysis_usage(user_input: str, system_type: str, execution_time: float = 0, error: bool = False):
        if "usage_stats" not in st.session_state:
            st.session_state.usage_stats = {"total_analyses": 0, "error_count": 0}
        st.session_state.usage_stats["total_analyses"] += 1
        if error:
            st.session_state.usage_stats["error_count"] += 1
    
    def add_error_to_history(error_message: str, error_category: str = "一般エラー", solutions: List[str] = None):
        if "error_history" not in st.session_state:
            st.session_state.error_history = []
        st.session_state.error_history.append({
            "timestamp": datetime.now(),
            "message": error_message,
            "category": error_category,
            "solutions": solutions or []
        })

# =========================================================================
# SQL生成とクリーニング機能（新規追加）
# =========================================================================

def clean_generated_sql(raw_sql: str) -> str:
    """
    Geminiで生成されたSQLからクリーンなSQLを抽出
    """
    if not raw_sql or not raw_sql.strip():
        raise ValueError("生成されたSQLが空です")
    
    # マークダウンのコードブロックを除去
    sql = re.sub(r'```sql\s*\n?', '', raw_sql, flags=re.IGNORECASE)
    sql = re.sub(r'```\s*$', '', sql)
    sql = re.sub(r'^```\s*', '', sql)
    
    # コメント行の除去（#で始まる行、--で始まる行）
    lines = sql.split('\n')
    sql_lines = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith('#') and not line.startswith('--'):
            sql_lines.append(line)
    
    sql = ' '.join(sql_lines)
    
    # 余分な空白を除去
    sql = re.sub(r'\s+', ' ', sql).strip()
    
    # SQLの基本的な構文チェック
    if not sql.upper().startswith('SELECT'):
        # SELECTで始まらない場合、最初のSELECT文を探す
        select_match = re.search(r'\bSELECT\b', sql, re.IGNORECASE)
        if select_match:
            sql = sql[select_match.start():]
        else:
            raise ValueError("有効なSELECT文が見つかりません")
    
    # 末尾のセミコロンを除去（BigQueryでは不要）
    sql = sql.rstrip(';')
    
    # 空のSQLチェック
    if len(sql.strip()) < 10:  # SELECT程度の最小長
        raise ValueError("生成されたSQLが短すぎます")
    
    return sql

def validate_basic_sql_syntax(sql: str) -> bool:
    """
    基本的なSQL構文の検証
    """
    if not sql or len(sql.strip()) < 6:
        return False
    
    sql_upper = sql.upper().strip()
    
    # SELECT文で始まることを確認
    if not sql_upper.startswith('SELECT'):
        return False
    
    # 基本的なキーワードの存在確認
    if 'FROM' not in sql_upper:
        return False
    
    # 危険なSQL文の除外
    dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
    for keyword in dangerous_keywords:
        if keyword in sql_upper:
            return False
    
    # テーブル名の確認
    if 'vorn-digi-mktg-poc-635a' not in sql:
        return False
    
    return True

def execute_gemini_sql_generation(gemini_model, user_input: str, max_attempts: int = 3) -> Optional[str]:
    """
    Gemini でのSQL生成（リトライ機能付き）
    """
    for attempt in range(max_attempts):
        try:
            st.info(f"🧠 Gemini でSQL生成中... (試行 {attempt + 1}/{max_attempts})")
            
            # プロンプトの作成
            sql_prompt = create_enhanced_sql_prompt(user_input)
            
            # Gemini API呼び出し
            response = gemini_model.generate_content(sql_prompt)
            
            if not response or not response.text:
                st.warning("Geminiからの応答が空でした。再試行します...")
                continue
            
            # デバッグ: 生成されたレスポンスを表示
            with st.expander(f"🔍 デバッグ: Geminiレスポンス (試行{attempt+1})", expanded=False):
                st.text(response.text[:500] + "..." if len(response.text) > 500 else response.text)
            
            # SQLのクリーニング
            cleaned_sql = clean_generated_sql(response.text)
            
            # SQL構文の基本的な検証
            if validate_basic_sql_syntax(cleaned_sql):
                st.success("✅ 有効なSQLが生成されました")
                return cleaned_sql
            else:
                st.warning(f"⚠️ SQL構文に問題があります。再試行します... (試行 {attempt + 1})")
                continue
                
        except ValueError as e:
            st.warning(f"⚠️ SQL抽出エラー: {e}")
            if attempt == max_attempts - 1:
                st.error("❌ SQL生成に失敗しました。手動でSQLを入力してください。")
                return None
            continue
            
        except Exception as e:
            st.error(f"❌ Gemini API エラー: {e}")
            if attempt == max_attempts - 1:
                return None
            continue
    
    return None

def create_enhanced_sql_prompt(user_input: str) -> str:
    """
    強化されたSQL生成プロンプト
    """
    try:
        prompt_info = select_best_prompt(user_input)
        base_template = prompt_info["template"]
    except (ImportError, NameError):
        # フォールバック用のシンプルなテンプレート
        base_template = """
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}
# 分析対象: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`
# 出力: 実行可能なBigQuery SQLのみを返してください。説明やマークダウン記法は不要です。
"""
    
    # プロンプトの強化
    enhanced_prompt = f"""
{base_template.format(user_input=user_input)}

## 重要な出力要件:
1. SQLコードのみを返してください
2. ```sql``` などのマークダウン記法は使用しないでください
3. コメント行(#で始まる行)は含めないでください
4. 説明文は不要です
5. SELECTで始まる実行可能なクエリのみ返してください

## SQL制約:
- SELECT文のみ許可（INSERT、UPDATE、DELETE等は禁止）
- テーブル名は必ず `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign` を使用
- SAFE_DIVIDE()を使用してゼロ除算を回避
- 結果は通常20行以下に制限（LIMIT句使用）

## 利用可能な主要列:
- Date: 日付
- CampaignName: キャンペーン名
- ServiceNameJA_Media: メディア名
- Impressions: インプレッション数
- Clicks: クリック数
- Conversions: コンバージョン数
- CostIncludingFees: コスト（手数料込み）

ユーザーの分析要求: {user_input}
"""
    
    return enhanced_prompt

# =========================================================================
# SQL実行機能（修正版）
# =========================================================================

def execute_sql_with_enhanced_handling(client, sql: str) -> Optional[pd.DataFrame]:
    """
    SQLを実行し、詳細なエラーハンドリングを行う（修正版）
    """
    try:
        # 入力検証
        if not sql or not sql.strip():
            st.error("❌ 実行するSQLが空です")
            return None
            
        # SQLの基本的な安全性チェック
        sql = sql.strip()
        
        # デバッグ: 実際に実行されるSQLを表示
        with st.expander("🔍 実行予定のSQL", expanded=False):
            st.code(sql, language="sql")
        
        if not sql:
            st.error("❌ SQLが空です")
            return None
            
        # SQLの基本構文チェック
        sql_upper = sql.upper()
        if not sql_upper.startswith('SELECT'):
            st.error("❌ SELECT文のみ実行可能です")
            return None
            
        # 危険なSQL文の除外
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                st.error(f"❌ 危険なSQL操作 '{keyword}' は実行できません")
                return None
        
        # BigQueryクライアントの確認
        if not client:
            st.error("❌ BigQueryクライアントが初期化されていません")
            return None
            
        # SQL実行
        st.info("⏳ BigQueryでクエリを実行中...")
        query_job = client.query(sql)
        
        # 結果の取得
        df = query_job.to_dataframe()
        
        # 結果の検証
        if df is None:
            st.warning("⚠️ クエリ結果がNullです")
            return pd.DataFrame()
            
        if df.empty:
            st.warning("⚠️ クエリは正常に実行されましたが、結果が空でした")
            st.info("💡 フィルター条件を緩くするか、データの存在する日付範囲を指定してください")
            return df
        
        # データサイズの警告
        row_count = len(df)
        if row_count > 10000:
            st.warning(f"⚠️ 大量のデータ ({row_count:,}行) が取得されました。表示に時間がかかる場合があります")
        elif row_count > 1000:
            st.info(f"ℹ️ {row_count:,}行のデータを取得しました")
        else:
            st.success(f"✅ {row_count}行のデータを取得しました")
            
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
        
        # エラー履歴への追加（可能であれば）
        try:
            if show_enhanced_error_message:
                show_enhanced_error_message(e, {"sql": sql, "operation": "SQL実行"})
        except:
            pass  # エラーハンドラーが利用できない場合はスキップ
            
        return None

# =========================================================================
# メイン分析フロー制御（修正版）
# =========================================================================

def run_analysis_flow(gemini_model, claude_client, claude_model_name, user_input, sheet_analysis_queries):
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
        st.session_state.sql = generated_sql
        st.session_state.df = df
        st.session_state.user_input = user_input
        
        st.success(f"✅ 分析完了！ {len(df)}行のデータを取得しました")
        
        # Claude分析の実行（オプション）
        if st.session_state.get("auto_claude_analysis", True):
            run_claude_analysis(claude_client, claude_model_name, df)
        
        # 実行時間の記録
        execution_time = (datetime.now() - start_time).total_seconds()
        log_analysis_usage(user_input, "enhanced", execution_time, False)
        
        # 分析後処理
        post_process_analysis_results()
        
        return True
        
    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()
        log_analysis_usage(user_input, "enhanced", execution_time, True)
        
        st.error(f"❌ 分析実行中にエラーが発生しました: {str(e)}")
        show_error_recovery_options(user_input)
        return False

def show_manual_sql_input():
    """
    手動SQL入力のUI
    """
    st.warning("⚠️ 自動SQL生成に失敗しました。手動でSQLを入力してください。")
    
    with st.expander("✏️ 手動SQL入力", expanded=True):
        manual_sql = st.text_area(
            "SQLを入力してください",
            value="SELECT CampaignName, SUM(CostIncludingFees) as Cost, SUM(Clicks) as Clicks FROM `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign` GROUP BY CampaignName ORDER BY Cost DESC LIMIT 10",
            height=200
        )
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🚀 手動SQLを実行"):
                df = execute_sql_with_enhanced_handling(st.session_state.bq_client, manual_sql)
                if df is not None:
                    st.session_state.sql = manual_sql
                    st.session_state.df = df
        
        with col2:
            if st.button("📋 サンプルSQLを使用"):
                sample_sqls = {
                    "キャンペーン別コスト上位10": "SELECT CampaignName, SUM(CostIncludingFees) as Cost FROM `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign` GROUP BY CampaignName ORDER BY Cost DESC LIMIT 10",
                    "日別クリック数": "SELECT Date, SUM(Clicks) as Clicks FROM `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign` GROUP BY Date ORDER BY Date DESC LIMIT 30",
                    "メディア別パフォーマンス": "SELECT ServiceNameJA_Media, SUM(CostIncludingFees) as Cost, SUM(Clicks) as Clicks, SUM(Conversions) as Conversions FROM `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign` GROUP BY ServiceNameJA_Media ORDER BY Cost DESC"
                }
                
                selected_sample = st.selectbox("サンプルSQLを選択", list(sample_sqls.keys()))
                st.code(sample_sqls[selected_sample], language="sql")

def show_error_recovery_options(user_input: str):
    """
    エラー回復オプションの表示
    """
    st.markdown("### 🔧 解決方法")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 再試行", type="primary"):
            st.rerun()
    
    with col2:
        if st.button("✏️ 手動入力"):
            show_manual_sql_input()
    
    with col3:
        if st.button("💡 指示を変更"):
            st.info("より具体的な指示を入力してみてください")
    
    # よくある問題と解決策
    with st.expander("💡 よくある問題と解決策"):
        st.markdown("""
        **一般的な原因:**
        - AIが理解できない曖昧な指示
        - 存在しない列名や機能の指定
        - 複雑すぎる分析要求
        
        **改善方法:**
        - より具体的で明確な指示に変更
        - 「キャンペーン別の」「日別の」など具体的な軸を指定
        - 一度に複数の分析を求めず、段階的に実行
        
        **指示の例:**
        - ✅ "コスト上位10キャンペーンを表示"
        - ✅ "過去30日間の日別クリック数"
        - ❌ "全体的な傾向を分析して"
        - ❌ "いろんな角度から詳しく"
        """)

def run_claude_analysis(claude_client, claude_model_name: str, df):
    """
    Claude分析の実行
    """
    try:
        if df is None or df.empty:
            return
        
        with st.spinner("🎯 Claudeが分析中..."):
            # データサンプルの作成
            data_sample = df.head(10).to_string()
            
            # Claudeプロンプト
            claude_prompt = f"""
あなたは経験豊富なデジタルマーケティング分析の専門家です。
以下のデータを分析して、重要な洞察と改善提案を3つ以内で簡潔に述べてください。

データ:
{data_sample}

分析結果を箇条書きで述べてください:
"""
            
            response = claude_client.messages.create(
                model=claude_model_name,
                max_tokens=1000,
                messages=[{"role": "user", "content": claude_prompt}]
            )
            
            if response and response.content:
                st.session_state.comment = response.content[0].text
                st.success("✅ Claude分析が完了しました！")
            
    except Exception as e:
        st.warning(f"⚠️ Claude分析でエラーが発生しました: {str(e)}")
        st.info("💡 SQL実行結果は正常に取得されています。")

# =========================================================================
# 既存の関数群（互換性のため維持）
# =========================================================================

def execute_gemini_sql_analysis(gemini_model, user_input: str, system_type: str) -> bool:
    """Gemini用SQL分析の実行（互換性のため）"""
    try:
        generated_sql = execute_gemini_sql_generation(gemini_model, user_input)
        if generated_sql:
            df = execute_sql_with_enhanced_handling(st.session_state.bq_client, generated_sql)
            if df is not None and not df.empty:
                st.session_state.sql = generated_sql
                st.session_state.df = df
                st.session_state.user_input = user_input
                return True
        return False
    except Exception as e:
        st.error(f"❌ Gemini SQL生成エラー: {str(e)}")
        add_error_to_history(str(e), "Gemini SQL生成エラー", ["プロンプトを見直してください", "API接続を確認してください"])
        return False

def execute_claude_analysis(claude_client, claude_model_name: str, user_input: str, system_type: str) -> bool:
    """Claude分析の実行（互換性のため）"""
    try:
        # 既存データがある場合のみClaude分析を実行
        if st.session_state.get("df") is not None and not st.session_state.df.empty:
            st.info("🎯 Claudeで詳細分析を実行中...")
            
            data_sample = st.session_state.df.head(20).to_string()
            claude_prompt = generate_claude_prompt(data_sample, system_type)
            
            # Claude分析の実行
            response = claude_client.messages.create(
                model=claude_model_name,
                max_tokens=3000,
                messages=[{"role": "user", "content": claude_prompt}]
            )
            
            st.session_state.comment = response.content[0].text
            st.success("✅ Claude分析が完了しました！")
            return True
            
        else:
            st.warning("分析対象のデータがありません。まずGeminiでSQLを生成してください。")
            return False
            
    except Exception as e:
        st.error(f"❌ Claude分析エラー: {str(e)}")
        add_error_to_history(str(e), "Claude分析エラー", ["API接続を確認してください", "データ形式を確認してください"])
        return False

# =========================================================================
# プロンプト生成（既存関数の互換性維持）
# =========================================================================

def generate_sql_prompt(user_input: str, system_type: str) -> str:
    """SQLプロンプトの生成"""
    if system_type == "enhanced" and generate_enhanced_sql_prompt:
        try:
            return generate_enhanced_sql_prompt(user_input)
        except Exception as e:
            st.warning(f"強化プロンプトの生成に失敗しました: {str(e)}。基本プロンプトを使用します。")
            return create_basic_sql_prompt(user_input)
    else:
        return create_basic_sql_prompt(user_input)

def generate_claude_prompt(data_sample: str, system_type: str) -> str:
    """Claudeプロンプトの生成"""
    if system_type == "enhanced" and generate_enhanced_claude_prompt:
        try:
            return generate_enhanced_claude_prompt(
                data_sample, 
                str(st.session_state.get("graph_cfg", {}))
            )
        except Exception as e:
            st.warning(f"強化プロンプトの生成に失敗しました: {str(e)}。基本プロンプトを使用します。")
            return f"以下のデータを詳細に分析してください:\n\n{data_sample}"
    else:
        return f"以下のデータを分析してください:\n\n{data_sample}"

def create_basic_sql_prompt(user_input: str) -> str:
    """基本SQLプロンプトの作成"""
    return create_enhanced_sql_prompt(user_input)  # 新しい関数を使用

# =========================================================================
# その他の既存関数群（そのまま維持）
# =========================================================================

def execute_sql_and_store_results(sql: str, user_input: str) -> bool:
    """SQLの実行と結果の保存"""
    try:
        df = execute_sql_with_enhanced_handling(st.session_state.bq_client, sql)
        
        if df is not None and not df.empty:
            # 結果の保存
            st.session_state.sql = sql
            st.session_state.df = df
            st.session_state.user_input = user_input
            
            # 基本的な結果情報の表示
            st.success(f"✅ SQL実行完了！{len(df)}行のデータを取得しました。")
            
            # データ品質の簡易チェック
            perform_basic_data_validation(df)
            
            return True
        else:
            st.error("❌ SQLの実行に失敗したか、結果が空でした。")
            return False
            
    except Exception as e:
        handle_sql_execution_error(e, sql)
        return False

def perform_basic_data_validation(df: pd.DataFrame):
    """基本的なデータ検証"""
    issues = []
    
    # NULL値の多い列をチェック
    null_rates = (df.isnull().sum() / len(df)) * 100
    high_null_cols = null_rates[null_rates > 30].index.tolist()
    if high_null_cols:
        issues.append(f"NULL値の多い列: {', '.join(high_null_cols)}")
    
    # 重複行のチェック
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        issues.append(f"重複行: {duplicate_count}行")
    
    # 警告の表示
    if issues:
        st.warning("⚠️ データ品質に関する注意点:")
        for issue in issues:
            st.caption(f"• {issue}")

def post_process_analysis_results():
    """分析結果の後処理"""
    if st.session_state.get("df") is not None and not st.session_state.df.empty:
        df = st.session_state.df
        
        # 基本統計の計算と保存
        basic_stats = calculate_basic_statistics(df)
        st.session_state.analysis_stats = basic_stats
        
        # グラフ設定の推奨
        recommend_visualization_settings(df)

def calculate_basic_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """基本統計の計算"""
    stats = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "numeric_columns": len(df.select_dtypes(include=['number']).columns),
        "categorical_columns": len(df.select_dtypes(include=['object', 'category']).columns),
        "null_percentage": (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
        "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024
    }
    
    return stats

def recommend_visualization_settings(df: pd.DataFrame):
    """可視化設定の推奨"""
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    
    recommendations = []
    
    # 時系列データの検出
    date_cols = [col for col in df.columns if 'date' in col.lower() or '日付' in col]
    if date_cols and numeric_cols:
        recommendations.append({
            "type": "時系列グラフ",
            "x_axis": date_cols[0],
            "y_axis": numeric_cols[0],
            "description": "時間の変化に伴うトレンドを確認"
        })
    
    # カテゴリ別比較の推奨
    if categorical_cols and numeric_cols:
        recommendations.append({
            "type": "棒グラフ",
            "x_axis": categorical_cols[0],
            "y_axis": numeric_cols[0],
            "description": "カテゴリ別のパフォーマンス比較"
        })
    
    # 相関分析の推奨
    if len(numeric_cols) >= 2:
        recommendations.append({
            "type": "散布図",
            "x_axis": numeric_cols[0],
            "y_axis": numeric_cols[1],
            "description": "2つの指標の相関関係を確認"
        })
    
    # 推奨設定の保存
    st.session_state.visualization_recommendations = recommendations

# =========================================================================
# エラーハンドリング
# =========================================================================

def handle_analysis_error(error: Exception, user_input: str, system_type: str):
    """分析エラーの統合ハンドリング"""
    error_context = {
        "user_input": user_input,
        "system_type": system_type,
        "sql": st.session_state.get("sql", "")
    }
    
    if show_enhanced_error_message:
        # 強化エラーハンドリングを使用
        show_enhanced_error_message(error, error_context)
    else:
        # 基本エラー表示
        st.error(f"❌ 分析エラー: {str(error)}")
        add_error_to_history(str(error), "分析実行エラー", ["入力内容を確認して再試行してください"])

def handle_sql_execution_error(error: Exception, sql: str):
    """SQL実行エラーの処理"""
    error_context = {
        "sql": sql,
        "error_type": "SQL実行エラー"
    }
    
    if show_enhanced_error_message:
        show_enhanced_error_message(error, error_context)
    else:
        st.error(f"❌ SQL実行エラー: {str(error)}")
        add_error_to_history(str(error), "SQL実行エラー", ["SQLの構文を確認してください", "テーブル名・列名を確認してください"])

# =========================================================================
# セッション管理
# =========================================================================

def initialize_analysis_session():
    """分析セッションの初期化"""
    if "analysis_session" not in st.session_state:
        st.session_state.analysis_session = {
            "session_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "start_time": datetime.now(),
            "analyses_count": 0,
            "total_execution_time": 0,
            "errors_count": 0,
            "last_analysis": None
        }

def update_analysis_session(execution_time: float, error: bool = False):
    """分析セッションの更新"""
    if "analysis_session" not in st.session_state:
        initialize_analysis_session()
    
    session = st.session_state.analysis_session
    session["analyses_count"] += 1
    session["total_execution_time"] += execution_time
    session["last_analysis"] = datetime.now()
    
    if error:
        session["errors_count"] += 1

def get_session_summary() -> Dict[str, Any]:
    """セッションサマリーの取得"""
    if "analysis_session" not in st.session_state:
        initialize_analysis_session()
    
    session = st.session_state.analysis_session
    
    return {
        "session_duration": (datetime.now() - session["start_time"]).total_seconds() / 60,  # 分
        "analyses_count": session["analyses_count"],
        "avg_execution_time": session["total_execution_time"] / max(session["analyses_count"], 1),
        "error_rate": session["errors_count"] / max(session["analyses_count"], 1) * 100,
        "success_rate": (session["analyses_count"] - session["errors_count"]) / max(session["analyses_count"], 1) * 100
    }

# =========================================================================
# 分析履歴管理
# =========================================================================

def save_analysis_to_history(user_input: str, sql: str, df: pd.DataFrame, system_type: str):
    """分析を履歴に保存"""
    if "analysis_history" not in st.session_state:
        st.session_state.analysis_history = []
    
    history_entry = {
        "timestamp": datetime.now(),
        "user_input": user_input,
        "sql": sql,
        "row_count": len(df) if df is not None else 0,
        "system_type": system_type,
        "columns": list(df.columns) if df is not None else [],
        "success": True
    }
    
    st.session_state.analysis_history.append(history_entry)
    
    # 履歴の上限管理
    if len(st.session_state.analysis_history) > 50:
        st.session_state.analysis_history = st.session_state.analysis_history[-50:]

def load_analysis_from_history(history_index: int):
    """履歴から分析を復元"""
    if "analysis_history" not in st.session_state or history_index >= len(st.session_state.analysis_history):
        st.error("指定された履歴が見つかりません。")
        return False
    
    history_entry = st.session_state.analysis_history[history_index]
    
    try:
        # 履歴からセッション状態を復元
        st.session_state.user_input = history_entry["user_input"]
        st.session_state.sql = history_entry["sql"]
        st.session_state.use_enhanced_prompts = (history_entry["system_type"] == "enhanced")
        
        # SQLを再実行してデータを取得
        if history_entry["sql"]:
            execute_sql_and_store_results(history_entry["sql"], history_entry["user_input"])
        
        st.success(f"✅ 履歴から分析を復元しました: {history_entry['user_input'][:50]}...")
        return True
        
    except Exception as e:
        st.error(f"履歴の復元に失敗しました: {str(e)}")
        return False

# =========================================================================
# パフォーマンス分析
# =========================================================================

def analyze_performance_metrics():
    """パフォーマンス指標の分析"""
    session_summary = get_session_summary()
    
    performance_insights = []
    
    # 実行時間の評価
    avg_time = session_summary["avg_execution_time"]
    if avg_time > 30:
        performance_insights.append({
            "type": "warning",
            "message": f"平均実行時間が長めです（{avg_time:.1f}秒）",
            "suggestion": "データ量を減らすかLIMIT句の使用を検討してください"
        })
    elif avg_time < 5:
        performance_insights.append({
            "type": "success",
            "message": f"高速な実行時間です（{avg_time:.1f}秒）",
            "suggestion": "現在のクエリ設計が効率的です"
        })
    
    # エラー率の評価
    error_rate = session_summary["error_rate"]
    if error_rate > 30:
        performance_insights.append({
            "type": "critical",
            "message": f"エラー率が高いです（{error_rate:.1f}%）",
            "suggestion": "入力内容やSQL構文の確認をお勧めします"
        })
    elif error_rate == 0:
        performance_insights.append({
            "type": "success",
            "message": "エラーなく実行できています",
            "suggestion": "安定した分析フローです"
        })
    
    return performance_insights

def show_performance_insights():
    """パフォーマンス洞察の表示"""
    insights = analyze_performance_metrics()
    
    if insights:
        with st.expander("⚡ パフォーマンス分析", expanded=False):
            for insight in insights:
                if insight["type"] == "critical":
                    st.error(f"🔴 {insight['message']}")
                    st.info(f"💡 {insight['suggestion']}")
                elif insight["type"] == "warning":
                    st.warning(f"🟡 {insight['message']}")
                    st.info(f"💡 {insight['suggestion']}")
                elif insight["type"] == "success":
                    st.success(f"🟢 {insight['message']}")
                    st.info(f"💡 {insight['suggestion']}")

# =========================================================================
# 自動最適化機能
# =========================================================================

def suggest_query_optimizations(sql: str) -> List[Dict[str, str]]:
    """クエリ最適化の提案"""
    suggestions = []
    sql_upper = sql.upper()
    
    # SELECT *の使用チェック
    if "SELECT *" in sql_upper:
        suggestions.append({
            "type": "performance",
            "description": "SELECT * の使用を避け、必要な列のみを選択",
            "example": "SELECT column1, column2 FROM table",
            "priority": "medium"
        })
    
    # LIMIT句の使用チェック
    if "LIMIT" not in sql_upper and "COUNT" not in sql_upper:
        suggestions.append({
            "type": "performance",
            "description": "大量データの場合はLIMIT句の使用を推奨",
            "example": "SELECT ... FROM table LIMIT 1000",
            "priority": "low"
        })
    
    # WHERE句の日付フィルタチェック
    if "WHERE" not in sql_upper and ("Date" in sql or "date" in sql):
        suggestions.append({
            "type": "performance",
            "description": "日付範囲でのフィルタリングを推奨",
            "example": "WHERE Date >= '2024-01-01'",
            "priority": "medium"
        })
    
    # SAFE関数の使用チェック
    if "/" in sql and "SAFE_DIVIDE" not in sql_upper:
        suggestions.append({
            "type": "safety",
            "description": "除算にはSAFE_DIVIDE()の使用を推奨",
            "example": "SAFE_DIVIDE(numerator, denominator)",
            "priority": "high"
        })
    
    return suggestions

# =========================================================================
# 分析品質評価
# =========================================================================

def evaluate_analysis_quality(user_input: str, sql: str, df: pd.DataFrame) -> Dict[str, Any]:
    """分析品質の評価"""
    quality_score = 0
    quality_factors = []
    
    # 入力の具体性評価
    if len(user_input.split()) >= 10:
        quality_score += 20
        quality_factors.append("詳細な分析指示")
    else:
        quality_factors.append("簡潔な分析指示")
    
    # SQLの複雑度評価
    sql_upper = sql.upper()
    complexity_score = 0
    
    if "JOIN" in sql_upper:
        complexity_score += 10
        quality_factors.append("複数テーブル結合")
    
    if "GROUP BY" in sql_upper:
        complexity_score += 10
        quality_factors.append("データ集約")
    
    if "ORDER BY" in sql_upper:
        complexity_score += 5
        quality_factors.append("適切なソート")
    
    if "LIMIT" in sql_upper:
        complexity_score += 5
        quality_factors.append("結果制限")
    
    quality_score += min(complexity_score, 30)
    
    # データ品質評価
    if df is not None and not df.empty:
        data_score = 0
        
        # データ量の評価
        if 10 <= len(df) <= 10000:
            data_score += 20
            quality_factors.append("適切なデータ量")
        elif len(df) > 10000:
            data_score += 10
            quality_factors.append("大量データ")
        else:
            data_score += 5
            quality_factors.append("少量データ")
        
        # NULL値の評価
        null_rate = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
        if null_rate < 10:
            data_score += 15
            quality_factors.append("低NULL値率")
        elif null_rate < 30:
            data_score += 10
            quality_factors.append("中程度NULL値率")
        else:
            data_score += 0
            quality_factors.append("高NULL値率")
        
        quality_score += min(data_score, 35)
    
    # 総合評価
    quality_level = "低"
    if quality_score >= 70:
        quality_level = "高"
    elif quality_score >= 50:
        quality_level = "中"
    
    return {
        "score": quality_score,
        "level": quality_level,
        "factors": quality_factors,
        "recommendations": generate_quality_recommendations(quality_score, quality_factors)
    }

def generate_quality_recommendations(score: int, factors: List[str]) -> List[str]:
    """品質改善の推奨事項"""
    recommendations = []
    
    if score < 50:
        recommendations.append("より具体的で詳細な分析指示を入力してください")
        recommendations.append("複数の指標を組み合わせた分析を検討してください")
    
    if "高NULL値率" in factors:
        recommendations.append("NULL値の多いデータは品質チェックを実行してください")
    
    if "少量データ" in factors:
        recommendations.append("分析期間を延長してより多くのデータを取得してください")
    
    if "大量データ" in factors:
        recommendations.append("必要に応じてLIMIT句でデータ量を制限してください")
    
    if score >= 70:
        recommendations.append("高品質な分析が実行されています！")
    
    return recommendations

# =========================================================================
# 外部呼び出し用の統合関数
# =========================================================================

def run_comprehensive_analysis(gemini_model, claude_client, claude_model_name, user_input, sheet_analysis_queries):
    """包括的な分析実行（すべての機能を統合）"""
    # セッション初期化
    initialize_analysis_session()
    
    # メイン分析フロー実行
    success = run_analysis_flow(gemini_model, claude_client, claude_model_name, user_input, sheet_analysis_queries)
    
    if success:
        # 後処理
        post_process_analysis_results()
        
        # 履歴保存
        if st.session_state.get("df") is not None:
            save_analysis_to_history(
                user_input, 
                st.session_state.get("sql", ""), 
                st.session_state.df,
                "enhanced" if st.session_state.get("use_enhanced_prompts", True) else "basic"
            )
        
        # 品質評価
        if st.session_state.get("df") is not None:
            quality_report = evaluate_analysis_quality(
                user_input, 
                st.session_state.get("sql", ""), 
                st.session_state.df
            )
            st.session_state.analysis_quality = quality_report
        
        # パフォーマンス洞察表示
        show_performance_insights()
    
    return success

# =========================================================================
# バックアップ・復元機能
# =========================================================================

def export_analysis_session():
    """分析セッションのエクスポート"""
    import json
    
    export_data = {
        "session_info": st.session_state.get("analysis_session", {}),
        "current_analysis": {
            "user_input": st.session_state.get("user_input", ""),
            "sql": st.session_state.get("sql", ""),
            "system_type": "enhanced" if st.session_state.get("use_enhanced_prompts", True) else "basic"
        },
        "history": st.session_state.get("analysis_history", []),
        "stats": st.session_state.get("usage_stats", {}),
        "export_timestamp": datetime.now().isoformat()
    }
    
    return json.dumps(export_data, ensure_ascii=False, indent=2, default=str)

def import_analysis_session(import_data: str):
    """分析セッションのインポート"""
    try:
        import json
        data = json.loads(import_data)
        
        # セッション情報の復元
        if "session_info" in data:
            st.session_state.analysis_session = data["session_info"]
        
        # 現在の分析の復元
        if "current_analysis" in data:
            current = data["current_analysis"]
            st.session_state.user_input = current.get("user_input", "")
            st.session_state.sql = current.get("sql", "")
            st.session_state.use_enhanced_prompts = (current.get("system_type") == "enhanced")
        
        # 履歴の復元
        if "history" in data:
            st.session_state.analysis_history = data["history"]
        
        # 統計の復元
        if "stats" in data:
            st.session_state.usage_stats = data["stats"]
        
        st.success("✅ 分析セッションが正常にインポートされました。")
        return True
        
    except Exception as e:
        st.error(f"❌ インポートに失敗しました: {str(e)}")
        return False