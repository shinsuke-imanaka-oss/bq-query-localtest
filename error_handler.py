# error_handler.py

import streamlit as st
import traceback
from datetime import datetime
from typing import Dict, Any, Optional
import re

def _record_error(e: Exception, context: Dict[str, Any]):
    """エラーをセッション履歴に記録する"""
    if "error_history" not in st.session_state:
        st.session_state.error_history = []
    simplified_context = {k: v for k, v in context.items() if not hasattr(v, 'to_dataframe')}
    st.session_state.error_history.append({
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "error_type": type(e).__name__, "error_message": str(e), "context": simplified_context
    })
    if len(st.session_state.error_history) > 10:
        st.session_state.error_history = st.session_state.error_history[-10:]

def _suggest_sql_fix(e: Exception, model, context: Dict[str, Any]) -> Optional[str]:
    """AIにSQLの自動修正を試みさせる"""
    error_message = str(e).lower()
    original_sql = context.get("generated_sql") or context.get("sql")

    if original_sql and ("syntax" in error_message or "not found" in error_message or "unrecognized name" in error_message):
        try:
            with st.spinner("🤖 AIがSQLの自動修正を試みています..."):
                # ▼▼▼【重要】AIへの指示プロンプトをより強力で明確なものに修正 ▼▼▼
                prompt = f"""
                # 指示
                あなたは優秀なSQLデバッガーです。以下のエラーが発生したBigQuery SQLを修正し、修正後のSQLのみをMarkdownのSQLコードブロックで出力してください。
                **あなたの応答には、解説、挨拶、言い訳など、SQLコード以外のテキストを絶対に含めてはいけません。**

                # エラーメッセージ
                {str(e)}

                # 修正対象のSQL
                ```sql
                {original_sql}
                ```

                # 出力形式（この形式を厳守してください）
                ```sql
                (ここに修正後のSQLを記述)
                ```
                """
                response = model.generate_content(prompt)
                response_text = response.text.strip()
                
                # --- ▼▼▼ ここからデバッグ機能 ▼▼▼ ---
                if st.session_state.get("debug_mode", False):
                    with st.expander("🔍 デバッグ: AIからの修正案応答"):
                        st.text(response_text)
                # --- ▲▲▲ デバッグ機能ここまで ▲▲▲ ---

                match = re.search(r"```(?:sql)?\n(.*?)\n```", response_text, re.DOTALL)
                if match:
                    return match.group(1).strip()
                elif response_text.upper().lstrip().startswith("SELECT"):
                    return response_text
        except Exception as ai_e:
            st.warning(f"AIによるSQL修正中にエラーが発生: {ai_e}")
    return None

def handle_error_with_ai(e: Exception, model, context: Dict[str, Any]):
    """エラーを処理し、AIによる修正案があればレビュー用の情報をセッション状態に格納する"""
    st.error(f"分析中にエラーが発生しました: {type(e).__name__}")
    with st.expander("エラー詳細"):
        st.code(str(e))
    _record_error(e, context)
    
    if model:
        fixed_sql = _suggest_sql_fix(e, model, context)

        # --- ▼▼▼ ここからデバッグ機能 ▼▼▼ ---
        if st.session_state.get("debug_mode", False):
            st.info(f"🔍 デバッグ: 抽出された修正SQL: {'見つかりました' if fixed_sql else '見つかりませんでした'}")
            if fixed_sql:
                st.code(fixed_sql, language="sql")
        # --- ▲▲▲ デバッグ機能ここまで ▲▲▲ ---

        if fixed_sql:
            st.session_state.show_fix_review = True
            st.session_state.original_erroneous_sql = context.get("sql") or context.get("generated_sql")
            st.session_state.sql_fix_suggestion = fixed_sql
        else:
            with st.spinner("🤖 AIがエラー原因を分析しています..."):
                try:
                    prompt = f"""
                    あなたはデータ分析アプリのデバッグアシスタントです。以下のエラー情報を分析し、原因と解決策を初心者にも分かりやすく解説してください。
                    # エラー情報:
                    - 種類: {type(e).__name__}
                    - メッセージ: {str(e)}
                    - 実行しようとしたSQL: {context.get("sql")}
                    """
                    response = model.generate_content(prompt)
                    st.subheader("🤖 AIによるエラー解説")
                    st.warning(response.text)
                except Exception:
                    pass