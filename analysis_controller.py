# analysis_controller.py - エラー修正版
"""
分析制御システム
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime
from typing import Optional

def run_analysis_flow(gemini_model, user_input: str, prompt_system: str = "basic", selected_ai: str = "gemini", bq_client=None) -> bool:
    """分析フローの実行 - 修正版"""
    try:
        st.info("🔄 分析を開始しています...")
        
        # BigQueryクライアントの取得（改善版）
        if bq_client is None:
            bq_client = st.session_state.get("bq_client")
        
        # デバッグ情報を表示
        if st.session_state.get("debug_mode", False):
            st.write(f"🔍 デバッグ: bq_client = {bq_client is not None}")
            st.write(f"🔍 セッション状態のキー: {list(st.session_state.keys())}")
        
        #if not bq_client:
        #    st.error("❌ BigQuery接続が見つかりません")
        #    st.info("💡 解決方法:")
        #    st.write("1. サイドバーで「🔄 BigQuery接続」をクリック")
        #    st.write("2. 接続成功メッセージを確認")
        #    st.write("3. 再度分析を実行")
        #    
        #    # 自動再接続を試行
        #    st.info("🔄 自動再接続を試行中...")
        #    from main import setup_bigquery_client
        #    bq_client = setup_bigquery_client()
        #    
        #    if not bq_client:
        #        return False
        
        # プロンプトの準備
        try:
            if prompt_system == "enhanced":
                from enhanced_prompts import generate_enhanced_sql_prompt
                prompt = generate_enhanced_sql_prompt(user_input)
                st.info("🚀 高品質プロンプトを使用")
            else:
                from prompts import get_optimized_bigquery_template
                prompt = get_optimized_bigquery_template(user_input)
                st.info("⚡ 基本プロンプトを使用")
        except ImportError as e:
            st.warning(f"⚠️ プロンプトシステムエラー: {e}")
            # フォールバックプロンプト
            prompt = f"""
以下の要求に基づいて、BigQueryで実行可能なSQLクエリを生成してください：

{user_input}

テーブル: `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`

重要な列:
- Date: 日付
- CampaignName: キャンペーン名  
- ServiceNameJA_Media: メディア名
- Clicks: クリック数
- Impressions: インプレッション数
- CostIncludingFees: コスト（手数料込み）
- Conversions: コンバージョン数

SQLのみを出力してください。説明は不要です。
"""
        
        # Gemini でSQL生成
        if not gemini_model:
            st.error("❌ Gemini接続が必要です。サイドバーで「🔄 Gemini接続」をクリックしてください。")
            return False
        
        st.info("🤖 Gemini がSQLを生成中...")
        
        try:
            response = gemini_model.generate_content(prompt)
            sql = response.text.strip()
        except Exception as e:
            st.error(f"❌ Gemini API エラー: {str(e)}")
            return False
        
        # SQLクリーンアップ
        if "```sql" in sql:
            sql = sql.split("```sql")[1].split("```")[0].strip()
        elif "```" in sql:
            sql = sql.split("```")[1].strip()
        
        if not sql:
            st.error("❌ SQLが生成されませんでした")
            return False
        
        # 生成されたSQLを表示
        with st.expander("📄 生成されたSQL", expanded=False):
            st.code(sql, language="sql")
        
        st.session_state.last_sql = sql
        st.session_state.last_user_input = user_input
        
        # SQL実行
        st.info("📊 BigQuery でSQL実行中...")
        df = execute_sql_query(bq_client, sql)
        
        if df is not None and not df.empty:
            st.session_state.last_analysis_result = df
            st.success(f"✅ 分析完了！ {len(df)}行のデータを取得しました。")
            
            # データを表示
            st.dataframe(df, use_container_width=True)
            
            # Claude分析（選択されている場合）
            if selected_ai in ["claude", "both"] and st.session_state.get("claude_client"):
                st.info("🧠 Claude が分析中...")
                try:
                    claude_analysis = generate_claude_analysis(df, user_input)
                    if claude_analysis:
                        st.markdown("### 🧠 Claude による分析")
                        st.info(claude_analysis)
                except Exception as e:
                    st.warning(f"⚠️ Claude分析エラー: {str(e)}")
            
            # 使用統計更新
            update_usage_stats(user_input, True, prompt_system)
            return True
        else:
            st.warning("⚠️ データが取得できませんでした")
            st.info("💡 以下を確認してください：")
            st.write("- 日付範囲が適切か")
            st.write("- テーブル名が正しいか") 
            st.write("- データが存在する期間か")
            return False
            
    except Exception as e:
        raise e
        
        # エラーハンドリング
        try:
            from error_handler import handle_error_with_ai
            suggestion = handle_error_with_ai(str(e))
            st.markdown("### 💡 解決策の提案")
            st.info(suggestion)
        except ImportError:
            st.info("💡 SQLの構文やテーブル名を確認してください")
        
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