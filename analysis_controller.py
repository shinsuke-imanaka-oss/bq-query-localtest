# analysis_controller.py - エラー修正版
"""
分析制御システム
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime
from typing import Optional

import json # <-- jsonライブラリのインポートも確認してください

def build_sql_from_plan(plan: dict, table_name: str) -> str:
    """【新】AIが生成した設計書(plan)から、安全なSQL文を組み立てる"""
    
    # SELECT句の組み立て
    select_parts = []
    if plan.get("select_columns"):
        # 空の文字列やNoneを除外
        select_parts.extend([col for col in plan["select_columns"] if col])
    if plan.get("aggregations"):
        for agg in plan["aggregations"]:
            # expressionとaliasが存在することを確認
            if agg.get("expression") and agg.get("alias"):
                select_parts.append(f"{agg['expression']} AS {agg['alias']}")
    
    if not select_parts:
        # AIがプランを正しく生成できなかった場合、基本的なクエリにフォールバック
        select_clause = "SELECT *"
    else:
        select_clause = "SELECT\n  " + ",\n  ".join(select_parts)
    
    # FROM句
    from_clause = f"FROM\n  `{table_name}`"
    
    # WHERE句
    where_clause = ""
    if plan.get("filters"):
        conditions = []
        for f in plan["filters"]:
            # 必要なキーが存在することを確認
            if f.get("column") and f.get("operator") and f.get("value") is not None:
                conditions.append(f"{f['column']} {f['operator']} {f['value']}")
        if conditions:
            where_clause = "WHERE\n  " + "\n  AND ".join(conditions)
        
    # GROUP BY句
    group_by_clause = ""
    if plan.get("group_by"):
        # 空の文字列やNoneを除外
        group_by_cols = [col for col in plan["group_by"] if col]
        if group_by_cols:
            group_by_clause = "GROUP BY\n  " + ", ".join(group_by_cols)
        
    # ORDER BY句
    order_by_clause = ""
    if plan.get("order_by") and plan["order_by"].get("column"):
        ob = plan["order_by"]
        direction = ob.get("direction", "DESC") # directionがなければDESCをデフォルトに
        order_by_clause = f"ORDER BY\n  {ob['column']} {direction}"
        
    # LIMIT句
    limit_clause = ""
    if plan.get("limit"):
        limit_clause = f"LIMIT {int(plan['limit'])}"
        
    # 全ての句を結合（Noneや空文字列の句は除外）
    final_sql = "\n".join(filter(None, [select_clause, from_clause, where_clause, group_by_clause, order_by_clause, limit_clause]))
    return final_sql + ";"

try:
    from bq_tool_config import settings
    SETTINGS_AVAILABLE = settings is not None
except ImportError:
    SETTINGS_AVAILABLE = False
    settings = None

def run_analysis_flow(gemini_model, user_input: str, prompt_system: str = "basic", selected_ai: str = "gemini", bq_client=None) -> bool:
    """【新】分析フローの実行（設計書ベースに全面改修）"""
    try:
        st.info("🔄 分析を開始しています...")
        
        if bq_client is None:
            bq_client = st.session_state.get("bq_client")

        # プロンプトの準備
        if prompt_system == "enhanced":
            # ▼▼▼【修正点】古い関数ではなく、新しい関数をインポートする ▼▼▼
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
            
            # ▼▼▼【修正点】jsonライブラリをインポートし、新しいbuild_sql_from_plan関数を呼び出す ▼▼▼
            import json 
            plan = json.loads(plan_json_str)
            
            with st.expander("📄 AIが生成した分析設計書 (JSON)"):
                st.json(plan)
            
            correct_table_name = settings.bigquery.get_full_table_name("campaign")
            final_sql = build_sql_from_plan(plan, correct_table_name)
        
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