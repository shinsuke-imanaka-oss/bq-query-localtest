# error_handler.py (ハイブリッド版)
import streamlit as st
import traceback
from datetime import datetime
from typing import Dict, Any

def _handle_error_statically(error_message: str) -> str:
    """
    (応用) キーワードベースのシンプルなエラー分析（AIが利用できない場合のフォールバック）
    """
    error_lower = error_message.lower()
    
    if "not found" in error_lower or "does not exist" in error_lower:
        suggestion = """
        **原因の可能性:** テーブル名または列名が存在しない可能性があります。
        **解決策:**
        - `config.py`で設定したテーブル名や列名のスペルが正しいか確認してください。
        - BigQuery上でテーブルが実際に存在するか確認してください。
        """
    elif "syntax" in error_lower:
        suggestion = """
        **原因の可能性:** SQLの構文に誤りがあります。
        **解決策:**
        - カンマ、括弧、引用符（`'` or `"`）の対応が取れているか確認してください。
        - AIが生成したSQLの場合は、プロンプトを少し変更して再試行してみてください。
        """
    elif "permission" in error_lower or "access" in error_lower:
        suggestion = """
        **原因の可能性:** BigQueryへのアクセス権限が不足しています。
        **解決策:**
        - GCPのサービスアカウントに「BigQuery ユーザー」のIAMロールが付与されているか確認してください。
        """
    else:
        suggestion = """
        **原因の可能性:** 一般的なエラーが発生しました。
        **解決策:**
        - 「システム診断」パネルでAPI接続などを確認してください。
        - 入力内容や選択したオプションを見直して、再度実行してみてください。
        """
    return suggestion

def _record_error(e: Exception, suggestion: str, context: Dict[str, Any]):
    """
    (応用) エラーと提案内容をセッション履歴に記録する
    """
    if "error_history" not in st.session_state:
        st.session_state.error_history = []
    
    st.session_state.error_history.append({
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "error_type": type(e).__name__,
        "error_message": str(e),
        "suggestion": suggestion,
        "context": context
    })
    
    # 履歴の上限管理
    if len(st.session_state.error_history) > 10:
        st.session_state.error_history = st.session_state.error_history[-10:]

def handle_error_with_ai(e: Exception, model, context: Dict[str, Any]):
    """
    エラーをAIで分析し、失敗した場合は静的分析にフォールバックする
    """
    st.error(f"エラーが発生しました: {str(e)}")

    suggestion = ""
    
    # AIモデルが利用可能なら、AIによる分析を試みる
    if model:
        with st.spinner("🤖 AIがエラー原因を分析し、解決策を検討しています..."):
            try:
                error_details = f"""
                ## エラー情報
                - **種類:** {type(e).__name__}
                - **メッセージ:** {str(e)}
                - **コンテキスト:** {context}
                - **トレースバック:**
                {traceback.format_exc()}
                """
                prompt = f"""
                あなたはデータ分析アプリのデバッグアシスタントです。以下のエラー情報を分析し、原因・具体的な修正案・再発防止策を初心者にも分かりやすくマークダウンで回答してください。
                ---
                {error_details}
                """
                response = model.generate_content(prompt)
                suggestion = response.text
                
                st.subheader("🤖 AIによるエラー分析と解決策")
                st.markdown(suggestion)

            except Exception as ai_e:
                st.error(f"🤖 AIによるエラー分析中に、別のエラーが発生しました: {ai_e}")
                st.info("基本的なエラー分析に切り替えます。")
                # AI分析が失敗したら、静的分析にフォールバック
                suggestion = _handle_error_statically(str(e))
                st.subheader("💡 トラブルシューティングのヒント")
                st.markdown(suggestion)
    else:
        # AIモデルがない場合は、最初から静的分析を行う
        st.info("基本的なエラー分析を行います。")
        suggestion = _handle_error_statically(str(e))
        st.subheader("💡 トラブルシューティングのヒント")
        st.markdown(suggestion)
    
    # 最終的な提案内容をエラー履歴に記録
    _record_error(e, suggestion, context)