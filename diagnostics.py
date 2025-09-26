# diagnostics.py
import streamlit as st
import os
from google.cloud import bigquery
import google.generativeai as genai
import anthropic

def check_api_keys():
    """APIキーが設定されているかチェックする"""
    results = {}
    
    # Gemini API Key
    gemini_key = st.secrets.get("GEMINI_API_KEY") or st.secrets.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    results["Gemini API Key"] = "✅ 設定済み" if gemini_key else "❌ 未設定"
    
    # Claude API Key
    claude_key = st.secrets.get("CLAUDE_API_KEY") or st.secrets.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
    results["Claude API Key"] = "✅ 設定済み" if claude_key else "❌ 未設定"
    
    # GCP Service Account (BigQuery)
    gcp_secret = "gcp_service_account" in st.secrets
    gcp_env = "GOOGLE_APPLICATION_CREDENTIALS" in os.environ
    if gcp_secret or gcp_env:
        source = "(Secrets)" if gcp_secret else "(環境変数)"
        results["GCP認証情報"] = f"✅ 設定済み {source}"
    else:
        results["GCP認証情報"] = "⚠️ 未設定（デフォルト認証を試みます）"
        
    return results

def check_api_connectivity(bq_client, gemini_model, claude_client):
    """各APIへの接続状況をチェックする"""
    results = {}
    results["BigQuery"] = "✅ 接続済み" if bq_client else "❌ 未接続"
    results["Gemini"] = "✅ 接続済み" if gemini_model else "❌ 未接続"
    results["Claude"] = "✅ 接続済み" if claude_client else "❌ 未接続"
    return results

def run_all_checks(settings, bq_client, gemini_model, claude_client):
    """全ての診断を実行して結果を返す"""
    st.header("🩺 システム診断パネル")
    
    st.subheader("🔑 APIキー設定")
    key_results = check_api_keys()
    for item, status in key_results.items():
        st.markdown(f"- **{item}**: {status}")
        
    st.subheader("🔌 API接続状況")
    conn_results = check_api_connectivity(bq_client, gemini_model, claude_client)
    for item, status in conn_results.items():
        st.markdown(f"- **{item}**: {status}")

    st.subheader("⚙️ アプリケーション設定")
    if settings:
        validation = settings.get_validation_status()
        if validation["valid"]:
            st.success("✅ 設定ファイルは正常です。")
        else:
            st.error("❌ 設定ファイルにエラーがあります。")
            for error in validation["errors"]:
                st.error(f"- {error}")
        if validation["warnings"]:
            st.warning("⚠️ 設定ファイルに警告があります。")
            for warning in validation["warnings"]:
                st.warning(f"- {warning}")
    else:
        st.warning("⚠️ 設定管理システムは利用できません。")