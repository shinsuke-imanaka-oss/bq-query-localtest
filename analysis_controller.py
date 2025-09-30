# analysis_controller.py

import streamlit as st
import pandas as pd
import time
from datetime import datetime
from typing import Optional
import json

# --- 安全なインポート ---
try:
    from bq_tool_config import settings
    SETTINGS_AVAILABLE = settings is not None
except ImportError:
    SETTINGS_AVAILABLE = False
    settings = None

try:
    from error_handler import handle_error_with_ai
except ImportError:
    def handle_error_with_ai(e, model, context):
        st.error(f"❌ エラーハンドラが利用できません: {e}")


def build_sql_from_plan(plan: dict) -> str:
    """AIが生成した設計書(plan)から、安全なSQL文を組み立てる"""
    table_name = plan.get("table_to_use")
    if not table_name or not isinstance(table_name, str):
        st.warning("AIが使用するテーブルを特定できませんでした。デフォルトのキャンペーンテーブルを使用します。")
        table_name = "LookerStudio_report_campaign"

    full_table_path = f"`{settings.bigquery.project_id}.{settings.bigquery.dataset}.{table_name}`"
    from_clause = f"FROM\n  {full_table_path}"

    dimensions = plan.get("dimensions", [])
    metrics = plan.get("metrics", [])
    select_parts = []
    group_by_cols = []

    for col in dimensions:
        if isinstance(col, str) and col:
            select_parts.append(f"`{col}`")
            group_by_cols.append(f"`{col}`")

    for metric in metrics:
        if isinstance(metric, dict) and metric.get("expression") and metric.get("alias"):
            select_parts.append(f"{metric['expression']} AS `{metric['alias']}`")

    select_clause = "SELECT\n  " + (",\n  ".join(select_parts) if select_parts else "*")
    group_by_clause = "GROUP BY\n  " + ", ".join(group_by_cols) if group_by_cols else ""

    where_clause = ""
    if plan.get("filters"):
        conditions = []
        for f in plan.get("filters", []):
            if isinstance(f, dict) and all(k in f for k in ["column", "operator", "value"]):
                column, operator, value = f['column'], f['operator'], f['value']
                if isinstance(value, str):
                    escaped_value = value.replace("'", "''")
                    value_str = f"'{escaped_value}'"
                else:
                    value_str = str(value)
                conditions.append(f"`{column}` {operator} {value_str}")
        if conditions:
            where_clause = "WHERE\n  " + "\n  AND ".join(conditions)

    order_by_clause = ""
    if plan.get("order_by") and isinstance(plan.get("order_by"), dict) and plan["order_by"].get("column"):
        ob = plan["order_by"]
        direction = ob.get("direction", "DESC")
        order_by_clause = f"ORDER BY\n  `{ob['column']}` {direction}"

    limit_clause = f"LIMIT {int(plan['limit'])}" if plan.get("limit") else ""
    
    final_sql = "\n".join(filter(None, [select_clause, from_clause, where_clause, group_by_clause, order_by_clause, limit_clause]))
    return final_sql + ";"


def run_analysis_flow(gemini_model, user_input: str, prompt_system: str = "basic", bq_client=None) -> bool:
    """分析フローの実行（エラーハンドリング強化版）"""
    st.info("🔄 分析を開始しています...")
    final_sql = "" # final_sqlをtryブロックの外で初期化

    try:
        if bq_client is None:
            bq_client = st.session_state.get("bq_client")

        if prompt_system == "enhanced":
            from enhanced_prompts import generate_sql_plan_prompt
            prompt = generate_sql_plan_prompt(user_input)
            st.info("🚀 高品質プロンプト（設計書モード）を使用")
        else:
            from prompts import get_optimized_bigquery_template
            prompt = get_optimized_bigquery_template(user_input)
            st.info("⚡ 基本プロンプトを使用")

        st.info("🤖 Gemini が分析プランを設計中...")
        response = gemini_model.generate_content(prompt)

        if prompt_system == "enhanced":
            if "```json" in response.text:
                plan_json_str = response.text.strip().split("```json")[1].split("```")[0]
            else:
                plan_json_str = response.text.strip()
            plan = json.loads(plan_json_str)
            with st.expander("📄 AIが生成した分析設計書 (JSON)"):
                st.json(plan)
            final_sql = build_sql_from_plan(plan)
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

        st.info("📊 BigQuery でSQL実行中...")
        df = execute_sql_query(bq_client, final_sql)

        if df is not None:
            if not df.empty:
                st.session_state.last_analysis_result = df
                st.success(f"✅ 分析完了！ {len(df)}行のデータを取得しました。")
                update_usage_stats(user_input, True, prompt_system)
                st.session_state.pop("show_fix_review", None)
                return True
            else:
                st.warning("⚠️ データが取得できませんでした。期間や条件を変えてお試しください。")
                update_usage_stats(user_input, False, prompt_system)
                return False
        else:
            update_usage_stats(user_input, False, prompt_system)
            return False

    except Exception as e:
        st.error(f"分析フローでエラーが発生しました: {type(e).__name__}")
        context = { "user_input": user_input, "sql": final_sql, "generated_sql": final_sql, "operation": "AI分析実行" }
        handle_error_with_ai(e, st.session_state.get('gemini_model'), context)
        update_usage_stats(user_input, False, prompt_system)
        return False


def execute_sql_query(client, sql: str) -> Optional[pd.DataFrame]:
    """SQL実行。エラーは呼び出し元にraiseして集中的に処理させる"""
    if not sql or not sql.strip():
        st.error("❌ SQLが空です")
        return None

    sql_upper = sql.upper().strip()
    dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
    if any(keyword in sql_upper for keyword in dangerous_keywords):
        st.error(f"❌ 危険なSQL操作は実行できません")
        return None

    if not sql_upper.startswith('SELECT'):
        st.error("❌ SELECT文のみ実行可能です")
        return None

    query_job = client.query(sql)
    df = query_job.to_dataframe()
    return df


def update_usage_stats(user_input: str, success: bool, system: str):
    """使用統計の更新"""
    if "usage_stats" not in st.session_state:
        st.session_state.usage_stats = {"total_analyses": 0, "error_count": 0, "enhanced_usage": 0}
    
    st.session_state.usage_stats["total_analyses"] += 1
    if not success:
        st.session_state.usage_stats["error_count"] += 1
    if system == "enhanced":
        st.session_state.usage_stats["enhanced_usage"] += 1