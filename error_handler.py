# error_handler.py を以下の内容で完全に置き換えてください

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
        "error_type": type(e).__name__,
        "error_message": str(e),
        "context": simplified_context
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
                prompt = f"""
                以下のBigQuery SQLはエラーになりました。エラーメッセージを参考にSQLを修正してください。
                修正後のSQLコードブロックのみを返してください。説明は不要です。

                # エラーメッセージ: {str(e)}
                # 修正対象のSQL:
                ```sql
                {original_sql}
                ```
                """
                response = model.generate_content(prompt)
                response_text = response.text.strip()
                match = re.search(r"```(?:sql)?\n(.*?)\n```", response_text, re.DOTALL)
                if match:
                    return match.group(1).strip()
                elif response_text.upper().lstrip().startswith("SELECT"):
                    return response_text
        except Exception as ai_e:
            st.warning(f"AIによるSQL修正中にエラーが発生: {ai_e}")
    return None

def handle_error_with_ai(e: Exception, model, context: Dict[str, Any]):
    """
    ✨最終確定版✨
    エラーを表示し、修正案があればレビュー用の情報をセッション状態に格納する
    """
    # ✨修正点✨: 必ず最初にエラー内容を表示する
    st.error(f"分析中にエラーが発生しました: {type(e).__name__}")
    with st.expander("エラー詳細"):
        st.code(str(e))
    
    _record_error(e, context)
    
    if model:
        fixed_sql = _suggest_sql_fix(e, model, context)
        if fixed_sql:
            # 修正案が見つかった場合、レビュー画面表示のフラグを立てる
            st.session_state.show_fix_review = True
            st.session_state.original_erroneous_sql = context.get("sql") or context.get("generated_sql")
            st.session_state.sql_fix_suggestion = fixed_sql
        else:
            # 修正案が見つからなかった場合、AIに原因を解説させる
            with st.spinner("🤖 AIがエラー原因を分析しています..."):
                try:
                    prompt = f"""
                    あなたはデータ分析アプリのデバッグアシスタントです。以下のエラー情報を分析し、原因と解決策を初心者にも分かりやすく解説してください。
                    # エラー情報:
                    - 種類: {type(e).__name__}
                    - メッセージ: {str(e)}
                    """
                    response = model.generate_content(prompt)
                    st.subheader("🤖 AIによるエラー解説")
                    st.warning(response.text)
                except Exception:
                    pass