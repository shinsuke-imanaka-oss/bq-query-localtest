# ▼▼▼【このコードで全文を置き換え】▼▼▼
# master_analyzer.py

import streamlit as st
import pandas as pd
from typing import Dict, Any, Optional
from datetime import date

# --- 依存モジュールの安全なインポート ---
try:
    from performance_analyzer import get_performance_data, calculate_kpis, evaluate_performance
    # 【修正点1】インポートする関数名を get_forecast_data に変更
    from forecast_analyzer import get_daily_kpi_data, get_forecast_data
    from insight_miner import find_key_drivers_safe
    # from strategy_simulator import generate_action_plan # 現状未使用のためコメントアウト
    MODULES_AVAILABLE = True
except ImportError as e:
    st.error(f"分析モジュールのインポートに失敗しました: {e}")
    MODULES_AVAILABLE = False

# --- 分析のオーケストレーション ---
def gather_all_analyses(bq_client, start_date: date, end_date: date) -> Dict[str, Any]:
    """全ての分析モジュールを実行し、結果を一つの辞書にまとめる"""
    if not MODULES_AVAILABLE:
        return {"error": "分析モジュールがロードできませんでした。"}

    results = {}

    with st.spinner("ステップ1/3: パフォーマンス診断を実行中..."):
        perf_data = get_performance_data(bq_client) # TODO: 期間指定
        results["performance"] = evaluate_performance(calculate_kpis(perf_data)) if perf_data is not None else "データなし"

    with st.spinner("ステップ2/3: 将来予測を計算中..."):
        daily_data = get_daily_kpi_data(bq_client, start_date=start_date, end_date=end_date)
        if daily_data is not None:
            # 【修正点2】新しい予測関数を呼び出す
            results["forecast"] = get_forecast_data(daily_data, periods=30)
        else:
            results["forecast"] = "データなし"

    with st.spinner("ステップ3/3: 主要因を分析中..."):
        results["drivers"] = find_key_drivers_safe(bq_client, target_kpi_en='cvr') # TODO: 期間指定

    return results

# --- (以降の関数は変更なし) ---
def generate_executive_summary(analysis_results: Dict, model_choice: str, gemini_model, claude_client, claude_model_name) -> str:
    # ...
    if model_choice == "Gemini" and not gemini_model: return "Geminiモデルが利用できません。"
    if model_choice == "Claude" and not claude_client: return "Claudeモデルが利用できません。"
    perf_summary = pd.DataFrame(analysis_results.get("performance")).to_string() if isinstance(analysis_results.get("performance"), pd.DataFrame) else "データなし"
    drivers_summary = pd.DataFrame(analysis_results.get("drivers")).head().to_string() if isinstance(analysis_results.get("drivers"), pd.DataFrame) else "データなし"
    prompt = f"""
    あなたはCEO向けの経営コンサルタントです。以下の断片的な分析レポートを統合し、
    一つのストーリーとして一貫性のある「エグゼクティブサマリー」を作成してください。

    # 分析レポート
    ## 1. 現状のパフォーマンス診断
    {perf_summary}

    ## 2. パフォーマンスに影響を与えている主要因
    {drivers_summary}

    # あなたのタスク
    以下の構成で、簡潔かつ示唆に富んだレポートを生成してください。
    - **現状の要約:**
    - **将来の見通し:** (予測は今回省略)
    - **成功と課題の要因:**
    - **推奨されるネクストアクション:** (最も重要なものを1つだけ)
    """
    try:
        with st.spinner(f"ステップ4/4: {model_choice}が最終レポートを執筆中..."):
            if model_choice == "Gemini":
                response = gemini_model.generate_content(prompt)
                return response.text
            elif model_choice == "Claude":
                response = claude_client.messages.create(
                    model=claude_model_name,
                    max_tokens=2000,
                    messages=[{"role": "user", "content": prompt}]
                )
                return response.content[0].text
    except Exception as e:
        return f"AIサマリー生成中にエラーが発生: {e}"

def run_comprehensive_analysis(bq_client, gemini_model, claude_client, claude_model_name, model_choice: str, start_date: date, end_date: date):
    # ...
    analysis_results = gather_all_analyses(bq_client, start_date, end_date)
    if "error" in analysis_results:
        st.error(analysis_results["error"])
        return
    summary = generate_executive_summary(analysis_results, model_choice, gemini_model, claude_client, claude_model_name)
    st.session_state.comprehensive_report = {
        "summary": summary,
        "details": analysis_results,
        "model_used": model_choice
    }
    