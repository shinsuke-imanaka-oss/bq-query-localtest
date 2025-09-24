# analysis_logic.py - 型ヒント修正版
"""
分析ロジック - 型ヒント修正版
- BigQuery SQL実行機能
- エラーハンドリング
- パフォーマンス最適化
"""

import streamlit as st
import pandas as pd
import json
import traceback
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple  # ✅ Tuple を追加

# 既存のプロンプトとの互換性維持のため、基本プロンプトもインポート
try:
    from prompts import select_best_prompt, MODIFY_SQL_TEMPLATE, CLAUDE_COMMENT_PROMPT_TEMPLATE
except ImportError:
    st.warning("prompts.py が見つかりません")

# 強化されたプロンプトシステム 
try:
    from enhanced_prompts import (
        generate_enhanced_sql_prompt, 
        generate_enhanced_claude_prompt,
        ENHANCED_MODIFY_SQL_TEMPLATE,
        PromptContextEnhancer
    )
except ImportError:
    st.warning("enhanced_prompts.py が見つかりません - 基本プロンプトのみ使用")

# 強化されたエラーハンドリング
try:
    from error_handler import handle_analysis_error, show_enhanced_error_message
except ImportError:
    st.warning("error_handler.py が見つかりません - 基本エラーハンドリング使用")
    def handle_analysis_error(error, sql="", operation=""):
        st.error(f"エラー: {str(error)}")
        return False
    def show_enhanced_error_message(error, context):
        st.error(f"エラー: {str(error)}")

MAX_ATTEMPTS = 3

def json_converter(o):
    """JSON変換用のコンバーター"""
    import datetime, decimal
    if isinstance(o, (datetime.date, datetime.datetime)): 
        return o.isoformat()
    if isinstance(o, decimal.Decimal): 
        return float(o)
    return str(o)

def add_modification_to_current_session(modification_type: str, instruction: str, new_sql: Optional[str] = None):
    """現在のセッションに修正履歴を追加（成功時のみ）"""
    if not st.session_state.get('current_session_id'):
        return
    
    for session in st.session_state.get('analysis_sessions', []):
        if session["session_id"] == st.session_state.current_session_id:
            modification = {
                "timestamp": datetime.now(),
                "type": modification_type,  # "ai_modify", "manual_edit", "initial"
                "instruction": instruction,
                "sql": new_sql
            }
            session["modifications"].append(modification)
            if new_sql:
                session["current_sql"] = new_sql
            break

def execute_sql_with_error_handling(client, sql: str) -> Optional[pd.DataFrame]:
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
        st.write(f"🔍 **デバッグ**: 実行SQL: `{sql[:100]}...`")
        
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
        
        # エラー発生時のSQL表示
        with st.expander("🔧 実行しようとしたSQL"):
            st.code(sql, language="sql")
            
        # エラー履歴への追加（可能であれば）
        try:
            show_enhanced_error_message(e, {"sql": sql, "operation": "SQL実行"})
        except:
            pass  # エラーハンドラーが利用できない場合はスキップ
            
        return None

def validate_sql_before_execution(sql: str) -> tuple[bool, str]:
    """
    SQL実行前の検証
    Returns: (is_valid, error_message)
    """
    if not sql or not sql.strip():
        return False, "SQLが空です"
    
    sql = sql.strip()
    sql_upper = sql.upper()
    
    # SELECT文チェック
    if not sql_upper.startswith('SELECT'):
        return False, "SELECT文で始まる必要があります"
    
    # FROM句チェック
    if 'FROM' not in sql_upper:
        return False, "FROM句が必要です"
    
    # テーブル名チェック
    if 'vorn-digi-mktg-poc-635a' not in sql:
        return False, "正しいプロジェクトIDを含むテーブル名を指定してください"
    
    # 危険なSQL文チェック
    dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
    for keyword in dangerous_keywords:
        if keyword in sql_upper:
            return False, f"危険なSQL操作 '{keyword}' は実行できません"
    
    # 基本的な構文チェック
    if sql.count('(') != sql.count(')'):
        return False, "括弧の数が一致していません"
    
    return True, ""

def validate_sql_safety(sql: str) -> Tuple[bool, List[str]]:
    """SQLの安全性をチェック"""
    warnings = []
    
    # 基本的な安全性チェック
    sql_upper = sql.upper()
    
    # 危険な操作のチェック
    dangerous_operations = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'TRUNCATE']
    for op in dangerous_operations:
        if op in sql_upper:
            warnings.append(f"危険な操作 '{op}' が検出されました")
    
    # 大量データを返す可能性のチェック
    if 'LIMIT' not in sql_upper and 'COUNT' not in sql_upper:
        warnings.append("LIMIT句がないため、大量のデータが返される可能性があります")
    
    # WHERE句なしのチェック
    if 'WHERE' not in sql_upper and 'LIMIT' not in sql_upper:
        warnings.append("WHERE句またはLIMIT句の使用を推奨します")
    
    return len(warnings) == 0, warnings

def generate_ai_comment(gemini_model, claude_client, claude_model_name: str, selected_ai: str, df: pd.DataFrame, graph_cfg: Dict) -> str:
    """AIコメントを生成する（強化版）"""
    try:
        sample = df.head(10).to_dict(orient="records")
        chart_type = graph_cfg.get('main_chart_type', '未選択')
        analysis_focus = f"「{chart_type}」で可視化しています。"
        
        if legend_col := graph_cfg.get('legend_col'):
            if legend_col != "なし":
                analysis_focus += f" 「{legend_col}」でグループ化しています。"
        
        if selected_ai == "Gemini (SQL生成)":
            # Geminiは簡潔なプロンプトで効果的
            prompt = f"""
            あなたはデジタルマーケティング分析の専門家です。
            以下のデータサンプルと可視化設定を基に、簡潔で実用的な分析コメントを日本語で提供してください。

            データサンプル: {json.dumps(sample, ensure_ascii=False, default=json_converter)[:2000]}
            可視化設定: {analysis_focus}
            
            以下の観点で分析してください：
            1. 主要な数値やトレンド
            2. 注目すべきポイント
            3. 改善のための具体的提案
            
            200文字以内で回答してください。
            """
            
            response = gemini_model.generate_content(prompt)
            return response.text if response.text else "Geminiからの応答を取得できませんでした。"
            
        else:  # Claude
            # 強化プロンプトが利用可能かチェック
            try:
                prompt = generate_enhanced_claude_prompt(
                    json.dumps(sample, ensure_ascii=False, default=json_converter)[:2000],
                    analysis_focus
                )
            except (NameError, TypeError):
                # フォールバック用の基本プロンプト
                prompt = f"""
                以下のマーケティングデータを分析し、戦略的な洞察を提供してください：
                
                データ: {json.dumps(sample, ensure_ascii=False, default=json_converter)[:2000]}
                可視化: {analysis_focus}
                
                分析の観点：
                1. パフォーマンスの評価
                2. 問題点と機会の特定
                3. 具体的な改善アクション
                
                300文字程度で実用的な提案をしてください。
                """
            
            response = claude_client.messages.create(
                model=claude_model_name,
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text if response.content else "Claudeからの応答を取得できませんでした。"
            
    except Exception as e:
        error_msg = f"AIコメント生成中にエラーが発生しました: {str(e)}"
        handle_analysis_error(e, "", "AIコメント生成")
        return error_msg

def run_analysis_flow(gemini_model, claude_client, claude_model_name: str, selected_ai: str, 
                     user_input: str, sheet_analysis_queries: Dict):
    """分析フロー実行の統合関数"""
    try:
        start_time = time.time()
        
        # プロンプトシステムの選択
        use_enhanced = st.session_state.get("use_enhanced_prompts", False)
        
        if selected_ai == "Gemini (SQL生成)":
            st.info("🧠 Geminiで分析用SQLを生成中...")
            
            # SQL生成
            if use_enhanced:
                try:
                    sql_prompt = generate_enhanced_sql_prompt(user_input)
                except (NameError, TypeError):
                    st.warning("強化プロンプトが利用できません。基本プロンプトを使用します。")
                    sql_prompt = create_basic_sql_prompt(user_input)
            else:
                sql_prompt = create_basic_sql_prompt(user_input)
            
            # SQL生成の実行
            sql_response = gemini_model.generate_content(sql_prompt)
            generated_sql = sql_response.text.strip()
            
            # SQL実行
            df = execute_sql_with_error_handling(st.session_state.bq_client, generated_sql)
            
            if df is not None and not df.empty:
                st.session_state.sql = generated_sql
                st.session_state.df = df
                st.session_state.user_input = user_input
                
                execution_time = time.time() - start_time
                st.success(f"✅ 分析完了！{len(df)}行のデータを取得しました。（実行時間: {execution_time:.1f}秒）")
                
                # 分析履歴に追加
                add_to_history(user_input, generated_sql, df)
                
            else:
                st.error("❌ SQLの実行に失敗しました。")
                
        else:  # Claude分析
            if st.session_state.get("df") is not None and not st.session_state.df.empty:
                st.info("🎯 Claudeで詳細分析を実行中...")
                
                # Claude分析の実行
                df = st.session_state.df
                graph_cfg = st.session_state.get("graph_cfg", {})
                
                comment = generate_ai_comment(
                    gemini_model, claude_client, claude_model_name, 
                    selected_ai, df, graph_cfg
                )
                
                st.session_state.comment = comment
                execution_time = time.time() - start_time
                st.success(f"✅ Claude分析が完了しました！（実行時間: {execution_time:.1f}秒）")
                
            else:
                st.warning("分析対象のデータがありません。まずGeminiでSQLを生成してください。")
                
    except Exception as e:
        handle_analysis_error(e, st.session_state.get("sql", ""), "分析フロー実行")

def create_basic_sql_prompt(user_input: str) -> str:
    """基本SQLプロンプトの作成"""
    try:
        prompt_info = select_best_prompt(user_input)
        return f"""
# あなたは広告分析の専門家です。
# ユーザー指示: {user_input}
# 分析対象: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`

{prompt_info.get("template", "")}

# 出力: 実行可能な BigQuery SQL だけ返す（説明なし）
"""
    except (NameError, TypeError):
        # プロンプト関数が利用できない場合の緊急用
        return f"""
# BigQuery SQL生成依頼

以下の分析要求に基づいてSQLを生成してください：
{user_input}

テーブル: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`

利用可能な主要列:
- Date, Impressions, Clicks, CostIncludingFees, Conversions
- ServiceNameJA_Media, CampaignName, AccountName
- ConversionValue, VideoViews

実行可能なBigQuery SQLのみを返してください。
"""

def add_to_history(user_input: str, sql: str, df: pd.DataFrame):
    """分析履歴への追加"""
    if "history" not in st.session_state:
        st.session_state.history = []
    
    history_entry = {
        "timestamp": datetime.now(),
        "user_input": user_input,
        "sql": sql,
        "df": df.copy(),  # DataFrameのコピーを保存
        "row_count": len(df),
        "columns": list(df.columns)
    }
    
    st.session_state.history.append(history_entry)
    
    # 履歴の上限管理（メモリ節約）
    if len(st.session_state.history) > 20:
        st.session_state.history = st.session_state.history[-20:]

def rerun_sql_flow(client, sql: str):
    """SQL再実行フロー"""
    try:
        st.info("🔄 SQLを再実行中...")
        df = execute_sql_with_error_handling(client, sql)
        
        if df is not None:
            st.session_state.df = df
            st.success(f"✅ 再実行完了！{len(df)}行のデータを取得しました。")
        else:
            st.error("❌ SQL再実行に失敗しました。")
            
    except Exception as e:
        handle_analysis_error(e, sql, "SQL再実行")

def modify_and_rerun_sql_flow(gemini_model, client, original_sql: str, modification_instruction: str):
    """SQL修正・再実行フロー"""
    try:
        st.info("🔧 SQLを修正中...")
        
        # 修正プロンプトの作成
        modify_prompt = f"""
以下のSQLを修正してください：

元のSQL:
```sql
{original_sql}
```

修正指示: {modification_instruction}

修正されたBigQuery SQLのみを返してください。説明は不要です。
"""
        
        # SQL修正の実行
        response = gemini_model.generate_content(modify_prompt)
        modified_sql = response.text.strip()
        
        # 修正されたSQLの実行
        df = execute_sql_with_error_handling(client, modified_sql)
        
        if df is not None and not df.empty:
            st.session_state.sql = modified_sql
            st.session_state.df = df
            st.success(f"✅ SQL修正・実行完了！{len(df)}行のデータを取得しました。")
            
            # 修正履歴に追加
            add_modification_to_current_session("ai_modify", modification_instruction, modified_sql)
        else:
            st.error("❌ 修正されたSQLの実行に失敗しました。")
            
    except Exception as e:
        handle_analysis_error(e, original_sql, "SQL修正")

def analyze_query_performance(sql: str, execution_time: float, row_count: int) -> Dict[str, Any]:
    """クエリパフォーマンスの分析"""
    performance = {
        "execution_time": execution_time,
        "row_count": row_count,
        "complexity_score": 0,
        "recommendations": []
    }
    
    sql_upper = sql.upper()
    
    # 複雑度スコアの計算
    complexity_factors = {
        "JOIN": sql_upper.count("JOIN") * 2,
        "SUBQUERY": (sql_upper.count("SELECT") - 1) * 3,
        "GROUP BY": sql_upper.count("GROUP BY") * 1,
        "ORDER BY": sql_upper.count("ORDER BY") * 1,
        "WINDOW": sql_upper.count("OVER(") * 2
    }
    
    performance["complexity_score"] = sum(complexity_factors.values())
    
    # 推奨事項の生成
    if execution_time > 10:
        performance["recommendations"].append("実行時間が長いため、LIMITやWHERE句での絞り込みを検討してください")
    
    if row_count > 5000:
        performance["recommendations"].append("大量のデータが返されています。必要に応じて集約を検討してください")
    
    if "SELECT *" in sql_upper:
        performance["recommendations"].append("SELECT * の代わりに必要な列のみを選択することを推奨します")
    
    return performance

def optimize_sql_query(sql: str) -> str:
    """SQLクエリの最適化提案"""
    optimizations = []
    
    sql_upper = sql.upper()
    
    # SELECT * の最適化
    if "SELECT *" in sql_upper:
        optimizations.append("SELECT * を具体的な列名に変更")
    
    # LIMIT句の追加提案
    if "LIMIT" not in sql_upper and "COUNT" not in sql_upper:
        optimizations.append("結果を制限するためのLIMIT句の追加")
    
    # WHERE句の追加提案
    if "WHERE" not in sql_upper:
        optimizations.append("データを絞り込むためのWHERE句の追加")
    
    # SAFE_DIVIDE の使用提案
    if "/" in sql and "SAFE_DIVIDE" not in sql_upper:
        optimizations.append("ゼロ除算エラーを防ぐためのSAFE_DIVIDE()の使用")
    
    return "; ".join(optimizations) if optimizations else "最適化の提案はありません"

def get_query_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """クエリ結果の統計情報"""
    if df is None or df.empty:
        return {"error": "データがありません"}
    
    stats = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "numeric_columns": len(df.select_dtypes(include=['number']).columns),
        "text_columns": len(df.select_dtypes(include=['object']).columns),
        "null_values": df.isnull().sum().sum(),
        "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024
    }
    
    # データ型の分布
    dtype_counts = df.dtypes.value_counts().to_dict()
    stats["dtype_distribution"] = {str(k): v for k, v in dtype_counts.items()}
    
    return stats

# =========================================================================
# エクスポート・ユーティリティ関数
# =========================================================================

def export_analysis_results(df: pd.DataFrame, sql: str, comment: str) -> Dict[str, Any]:
    """分析結果のエクスポート"""
    export_data = {
        "export_timestamp": datetime.now().isoformat(),
        "sql_query": sql,
        "row_count": len(df) if df is not None else 0,
        "columns": list(df.columns) if df is not None else [],
        "ai_comment": comment,
        "data_sample": df.head(5).to_dict(orient="records") if df is not None else [],
        "statistics": get_query_statistics(df)
    }
    
    return export_data

def import_analysis_session(session_data: Dict[str, Any]) -> bool:
    """分析セッションのインポート"""
    try:
        # セッションデータの検証
        required_keys = ["sql_query", "user_input"]
        if not all(key in session_data for key in required_keys):
            st.error("❌ 不正なセッションデータです")
            return False
        
        # セッション状態への復元
        st.session_state.sql = session_data["sql_query"]
        st.session_state.user_input = session_data["user_input"]
        
        if "ai_comment" in session_data:
            st.session_state.comment = session_data["ai_comment"]
        
        st.success("✅ 分析セッションをインポートしました")
        return True
        
    except Exception as e:
        st.error(f"❌ インポートに失敗しました: {str(e)}")
        return False
    
def build_where_clause(filters: dict, apply_date: bool = True, apply_media: bool = True, 
                      apply_campaign: bool = True, prefix: str = "WHERE") -> str:
    """
    フィルター条件からWHERE句を構築する関数
    
    Args:
        filters: フィルター条件の辞書
        apply_date: 日付フィルターを適用するか
        apply_media: メディアフィルターを適用するか
        apply_campaign: キャンペーンフィルターを適用するか
        prefix: WHERE句のプレフィックス ("WHERE" or "AND")
    
    Returns:
        構築されたWHERE句の文字列
    """
    where_conditions = []
    
    # 日付フィルターの処理
    if apply_date and filters.get("start_date") and filters.get("end_date"):
        start_date = filters["start_date"]
        end_date = filters["end_date"]
        
        # 日付が文字列の場合の処理
        if isinstance(start_date, str):
            start_date_str = start_date
        else:
            start_date_str = start_date.strftime('%Y-%m-%d')
            
        if isinstance(end_date, str):
            end_date_str = end_date
        else:
            end_date_str = end_date.strftime('%Y-%m-%d')
        
        where_conditions.append(f"Date >= '{start_date_str}'")
        where_conditions.append(f"Date <= '{end_date_str}'")
    
    # メディアフィルターの処理 - 修正版
    if apply_media and filters.get("media") and len(filters["media"]) > 0:
        media_list = filters["media"]
        if media_list:
            # SQLインジェクション対策のため、シングルクォートをエスケープ
            escaped_media = []
            for media in media_list:
                escaped_value = media.replace("'", "''")  # SQLエスケープ
                escaped_media.append(f"'{escaped_value}'")
            
            media_condition = f"ServiceNameJA_Media IN ({', '.join(escaped_media)})"
            where_conditions.append(media_condition)
    
    # キャンペーンフィルターの処理 - 修正版
    if apply_campaign and filters.get("campaigns") and len(filters["campaigns"]) > 0:
        campaign_list = filters["campaigns"]
        if campaign_list:
            # SQLインジェクション対策のため、シングルクォートをエスケープ
            escaped_campaigns = []
            for campaign in campaign_list:
                escaped_value = campaign.replace("'", "''")  # SQLエスケープ
                escaped_campaigns.append(f"'{escaped_value}'")
            
            campaign_condition = f"CampaignName IN ({', '.join(escaped_campaigns)})"
            where_conditions.append(campaign_condition)
    
    # WHERE句の構築
    if where_conditions:
        if prefix == "WHERE":
            return f"WHERE {' AND '.join(where_conditions)}"
        elif prefix == "AND":
            return f"AND {' AND '.join(where_conditions)}"
        else:
            return ' AND '.join(where_conditions)
    else:
        return ""

def build_safe_where_clause(filters: dict, table_schema: dict = None) -> str:
    """
    より安全なWHERE句構築（推奨版）
    
    Args:
        filters: フィルター条件
        table_schema: テーブルスキーマ情報（オプション）
    
    Returns:
        構築されたWHERE句
    """
    conditions = []
    
    # 日付範囲フィルター
    if filters.get("start_date") and filters.get("end_date"):
        start_date = filters["start_date"]
        end_date = filters["end_date"]
        
        # 日付の正規化
        if hasattr(start_date, 'strftime'):
            start_str = start_date.strftime('%Y-%m-%d')
        else:
            start_str = str(start_date)
            
        if hasattr(end_date, 'strftime'):
            end_str = end_date.strftime('%Y-%m-%d')
        else:
            end_str = str(end_date)
        
        conditions.append(f"Date BETWEEN '{start_str}' AND '{end_str}'")
    
    # メディアフィルター（配列が空でない場合のみ）- 修正版
    if filters.get("media") and len(filters["media"]) > 0:
        media_values = []
        for media in filters["media"]:
            # 基本的な文字列検証とエスケープ
            if isinstance(media, str) and media.strip():
                escaped = media.replace("'", "''")  # SQLエスケープ（修正版）
                media_values.append(f"'{escaped}'")
        
        if media_values:
            conditions.append(f"ServiceNameJA_Media IN ({', '.join(media_values)})")
    
    # キャンペーンフィルター（配列が空でない場合のみ）- 修正版
    if filters.get("campaigns") and len(filters["campaigns"]) > 0:
        campaign_values = []
        for campaign in filters["campaigns"]:
            # 基本的な文字列検証とエスケープ
            if isinstance(campaign, str) and campaign.strip():
                escaped = campaign.replace("'", "''")  # SQLエスケープ（修正版）
                campaign_values.append(f"'{escaped}'")
        
        if campaign_values:
            conditions.append(f"CampaignName IN ({', '.join(campaign_values)})")
    
    # 条件の結合
    if conditions:
        return f"WHERE {' AND '.join(conditions)}"
    else:
        return ""

# より高度な検索条件構築（オプション機能）
def build_advanced_where_clause(filters: dict, advanced_options: dict = None) -> str:
    """
    高度なWHERE句構築（将来的な機能拡張用）
    """
    base_where = build_safe_where_clause(filters)
    
    if not advanced_options:
        return base_where
    
    additional_conditions = []
    
    # コストフィルター
    if advanced_options.get("min_cost"):
        additional_conditions.append(f"CostIncludingFees >= {advanced_options['min_cost']}")
    
    if advanced_options.get("max_cost"):
        additional_conditions.append(f"CostIncludingFees <= {advanced_options['max_cost']}")
    
    # クリック数フィルター
    if advanced_options.get("min_clicks"):
        additional_conditions.append(f"Clicks >= {advanced_options['min_clicks']}")
    
    # 追加条件がある場合の統合
    if additional_conditions:
        if base_where:
            return f"{base_where} AND {' AND '.join(additional_conditions)}"
        else:
            return f"WHERE {' AND '.join(additional_conditions)}"
    
    return base_where