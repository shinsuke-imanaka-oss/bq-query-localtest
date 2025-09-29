# analysis_controller.py - エラー修正版
"""
分析制御システム
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime
from typing import Optional
import json

try:
    from bq_tool_config import settings
    SETTINGS_AVAILABLE = settings is not None
except ImportError:
    SETTINGS_AVAILABLE = False
    settings = None

def build_sql_from_plan(plan: dict) -> str:
    """【新・改4】AIが生成したシンプルな設計書(plan)から、安全なSQL文を組み立てる"""
    
    # AIが選択したテーブル名を取得
    table_name = plan.get("table_to_use")
    if not table_name or not isinstance(table_name, str):
        st.warning("AIが使用するテーブルを特定できませんでした。デフォルトのキャンペーンテーブルを使用します。")
        # デフォルトのテーブル名をフルで指定
        table_name = "LookerStudio_report_campaign"

    # BigQueryクライアントがプロジェクトとデータセットを補完することを前提とする
    from_clause = f"FROM\n  `{settings.bigquery.dataset}.{table_name}`"

    # SELECT句とGROUP BY句を同時に組み立てる
    dimensions = plan.get("dimensions", [])
    metrics = plan.get("metrics", [])
    
    select_parts = []
    group_by_cols = []

    # dimensions（分析軸）の処理
    for col in dimensions:
        if isinstance(col, str) and col:
            select_parts.append(f"`{col}`")
            group_by_cols.append(f"`{col}`")
        else:
            st.warning(f"⚠️ SQL設計書の'dimensions'に不正な値が含まれているため無視します: {col}")

    # metrics（指標）の処理
    for metric in metrics:
        if isinstance(metric, dict) and metric.get("expression") and metric.get("alias"):
            select_parts.append(f"{metric['expression']} AS `{metric['alias']}`")
        else:
            st.warning(f"⚠️ SQL設計書の'metrics'に不正な値が含まれているため無視します: {metric}")
            
    select_clause = "SELECT\n  " + (",\n  ".join(select_parts) if select_parts else "*")
    group_by_clause = "GROUP BY\n  " + ", ".join(group_by_cols) if group_by_cols else ""

    # WHERE句
    where_clause = ""
    if plan.get("filters"):
        conditions = []
        for f in plan.get("filters", []):
            if isinstance(f, dict) and all(k in f for k in ["column", "operator", "value"]):
                column = f['column']
                operator = f['operator']
                value = f['value']

                # valueが文字列型ならシングルクォートで囲み、数値ならそのまま使う
                if isinstance(value, str):
                    # 簡単なSQLインジェクション対策として、値の中のシングルクォートをエスケープ
                    escaped_value = value.replace("'", "''")
                    value_str = f"'{escaped_value}'"
                else:
                    value_str = str(value)

                conditions.append(f"`{column}` {operator} {value_str}")
                
        if conditions:
            where_clause = "WHERE\n  " + "\n  AND ".join(conditions)
        
    # ORDER BY句
    order_by_clause = ""
    if plan.get("order_by") and isinstance(plan.get("order_by"), dict) and plan["order_by"].get("column"):
        ob = plan["order_by"]
        direction = ob.get("direction", "DESC")
        # ORDER BY句ではエイリアスを使うため、バッククォートで囲むのが安全
        order_by_clause = f"ORDER BY\n  `{ob['column']}` {direction}"
        
    # LIMIT句
    limit_clause = ""
    if plan.get("limit"):
        limit_clause = f"LIMIT {int(plan['limit'])}"
        
    final_sql = "\n".join(filter(None, [select_clause, from_clause, where_clause, group_by_clause, order_by_clause, limit_clause]))
    return final_sql + ";"

def run_analysis_flow(gemini_model, user_input: str, prompt_system: str = "basic", selected_ai: str = "gemini", bq_client=None) -> bool:
    """【新】分析フローの実行（設計書ベースに全面改修）"""
    try:
        st.info("🔄 分析を開始しています...")
        
        if bq_client is None:
            bq_client = st.session_state.get("bq_client")

        # プロンプトの準備
        if prompt_system == "enhanced":
            from enhanced_prompts import generate_sql_plan_prompt
            prompt = generate_sql_plan_prompt(user_input)
            st.info("🚀 高品質プロンプト（設計書モード）を使用")
        else:
            from prompts import get_optimized_bigquery_template
            prompt = get_optimized_bigquery_template(user_input)
            st.info("⚡ 基本プロンプトを使用")

        # Gemini で処理
        st.info("🤖 Gemini が分析プランを設計中...")
        response = gemini_model.generate_content(prompt)

        # 高品質モードの場合（設計書からSQLを組み立てる）
        if prompt_system == "enhanced":
            plan_json_str = response.text.strip()
            if "```json" in plan_json_str:
                plan_json_str = plan_json_str.split("```json")[1].split("```")[0]
            
            plan = json.loads(plan_json_str)
            
            with st.expander("📄 AIが生成した分析設計書 (JSON)"):
                st.json(plan)
            
            final_sql = build_sql_from_plan(plan)
        
        # 基本モードの場合（従来通りSQLを直接生成）
        else:
            sql = response.text.strip()
            if "```sql" in sql:
                sql = sql.split("```sql")[1].split("```")[0].strip()
            final_sql = sql

        if not final_sql.strip():
            st.error("❌ SQLが生成されませんでした")
            return False
        
        with st.expander("📄 実行されるSQL (最終版)", expanded=False):
            st.code(final_sql, language="sql")
        
        st.session_state.last_sql = final_sql
        st.session_state.last_user_input = user_input
        
        # SQL実行
        st.info("📊 BigQuery でSQL実行中...")
        df = execute_sql_query(bq_client, final_sql)
        
        if df is not None and not df.empty:
            st.session_state.last_analysis_result = df
            st.success(f"✅ 分析完了！ {len(df)}行のデータを取得しました。")
            st.dataframe(df, use_container_width=True)
            
            if selected_ai in ["claude", "both"] and st.session_state.get("claude_client"):
                st.info("🧠 Claude が分析中...")
                try:
                    claude_analysis = generate_claude_analysis(df, user_input)
                    if claude_analysis:
                        st.markdown("### 🧠 Claude による分析")
                        st.info(claude_analysis)
                except Exception as e:
                    st.warning(f"⚠️ Claude分析エラー: {str(e)}")
            
            update_usage_stats(user_input, True, prompt_system)
            return True
        else:
            st.warning("⚠️ データが取得できませんでした")
            st.info("💡 以下を確認してください：")
            st.write("- 日付範囲が適切か")
            st.write("- テーブル名が正しいか")
            st.write("- データが存在する期間か")
            update_usage_stats(user_input, False, prompt_system)
            return False
            
    except Exception as e:
        st.error(f"分析フローで予期せぬエラーが発生しました: {e}")
        import traceback
        st.code(traceback.format_exc())
        update_usage_stats(user_input, False, prompt_system)
        return False

def execute_sql_query(client, sql: str) -> Optional[pd.DataFrame]:
    """SQL実行"""
    try:
        if not sql or not sql.strip():
            st.error("❌ SQLが空です")
            return None
            
        # 危険なSQL文の除外
        sql_upper = sql.upper()
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                st.error(f"❌ 危険なSQL操作 '{keyword}' は実行できません")
                return None
        
        # SELECT文のチェック
        if not sql_upper.strip().startswith('SELECT'):
            st.error("❌ SELECT文のみ実行可能です")
            return None
        
        query_job = client.query(sql)
        df = query_job.to_dataframe()
        
        if df.empty:
            st.warning("⚠️ クエリは成功しましたが、結果が空でした")
            return df
            
        return df
        
    except Exception as e:
        st.error(f"❌ SQL実行エラー: {str(e)}")
        raise e

def generate_claude_analysis(df: pd.DataFrame, user_input: str) -> str:
    """Claude による分析コメント生成"""
    try:
        claude_client = st.session_state.get("claude_client")
        claude_model = st.session_state.get("claude_model_name", "claude-3-sonnet-20240229")
        
        if not claude_client:
            return ""
        
        # データサマリーの作成
        data_summary = f"""
データ概要:
- 行数: {len(df)}
- 列数: {len(df.columns)}
- 列名: {', '.join(df.columns)}

数値データのサマリー:
{df.describe().to_string() if len(df.select_dtypes(include=['number']).columns) > 0 else '数値列なし'}

最初の3行のサンプル:
{df.head(3).to_string()}
"""
        
        # Claude用プロンプト
        prompt = f"""
マーケティング分析専門家として、以下のデータを分析してください。

分析要求: {user_input}

{data_summary}

以下の形式で回答してください：
1. **データサマリー**: 重要な数値とトレンド
2. **主要な洞察**: 発見されたパターンや特徴
3. **改善提案**: 具体的なアクションプラン

簡潔で実用的な分析をお願いします。
"""
        
        message = claude_client.messages.create(
            model=claude_model,
            max_tokens=1000,
            temperature=0.3,
            messages=[{
                "role": "user",
                "content": prompt
            }]
        )
        
        return message.content[0].text
        
    except Exception as e:
        st.warning(f"Claude分析エラー: {str(e)}")
        return ""

def update_usage_stats(user_input: str, success: bool, system: str):
    """使用統計の更新"""
    if "usage_stats" not in st.session_state:
        st.session_state.usage_stats = {
            "total_analyses": 0,
            "error_count": 0,
            "enhanced_usage": 0,
            "avg_execution_time": 0.0
        }
    
    st.session_state.usage_stats["total_analyses"] += 1
    if not success:
        st.session_state.usage_stats["error_count"] += 1
    if system == "enhanced":
        st.session_state.usage_stats["enhanced_usage"] += 1
    
    # 分析ログの記録
    if "analysis_logs" not in st.session_state:
        st.session_state.analysis_logs = []
    
    st.session_state.analysis_logs.append({
        "timestamp": datetime.now(),
        "user_input": user_input,
        "success": success,
        "system": system,
        "execution_time": 0.0  # 実際の実行時間は別途計測
    })
    
    # ログの上限管理
    if len(st.session_state.analysis_logs) > 50:
        st.session_state.analysis_logs = st.session_state.analysis_logs[-50:]