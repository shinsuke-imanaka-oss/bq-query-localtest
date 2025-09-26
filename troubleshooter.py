# troubleshooter.py
import streamlit as st

# google-cloud-bigquery と google-api-core がインストールされている必要があります
try:
    from google.api_core.exceptions import PermissionDenied, NotFound, BadRequest
except ImportError:
    # ライブラリがない場合は、ダミーのクラスを定義してエラーを防ぐ
    class PermissionDenied(Exception): pass
    class NotFound(Exception): pass
    class BadRequest(Exception): pass

def display_troubleshooting_guide(e: Exception):
    """
    発生した例外オブジェクトを元に、原因と解決策のヒントを表示する
    """
    st.warning("💡 トラブルシューティングのヒント")

    if isinstance(e, PermissionDenied):
        st.markdown("""
        **原因の可能性:** Google Cloudの権限が不足しています。
        **解決策:**
        - サービスアカウントを使用している場合、そのアカウントに「BigQuery ユーザー」や「BigQuery データ閲覧者」のIAMロールが付与されているか確認してください。
        - デフォルト認証の場合、`gcloud auth application-default login` コマンドで認証を再試行してみてください。
        """)
    elif isinstance(e, NotFound):
        st.markdown("""
        **原因の可能性:** 指定されたプロジェクトID、データセット、またはテーブルが見つかりません。
        **解決策:**
        - `config.py` や設定パネルで指定しているBigQueryのプロジェクトID、データセット名が正しいか確認してください。
        - 大文字・小文字やハイフン (`-`)、アンダースコア (`_`) の間違いがないか確認してください。
        """)
    elif isinstance(e, BadRequest):
         st.markdown("""
        **原因の可能性:** BigQueryに送信したSQLクエリに文法エラーがあるか、リクエストが無効です。
        **解決策:**
        - 「手動SQL実行」モードで実行したクエリの場合、SQLの構文を見直してください。
        - AIが生成したSQLに問題がある可能性も考えられます。
        """)
    elif isinstance(e, AttributeError):
        st.markdown("""
        **原因の可能性:** プログラムのコードレベルでのエラー（オブジェクトの属性名の間違いなど）。
        **解決策:**
        - これは開発者向けのバグの可能性が高いです。エラーログの詳細を確認し、コードを修正する必要があります。
        - 直近でコードを編集した場合、その箇所に間違いがないか確認してください。
        """)
    elif "api_key" in str(e).lower():
         st.markdown("""
        **原因の可能性:** GeminiまたはClaudeのAPIキーが無効、または利用制限に達している可能性があります。
        **解決策:**
        - `secrets.toml` や環境変数に設定したAPIキーが正しいか確認してください。
        - 各AIサービスのプラットフォームで、APIキーが有効か、請求先アカウントが設定されているかを確認してください。
        """)
    else:
        st.markdown("""
        **一般的な確認項目:**
        - インターネット接続は安定していますか？
        - 各種APIキーや設定は正しく入力されていますか？
        - 「システム診断」パネルで、すべての項目が正常（✅）になっているか確認してみてください。
        """)