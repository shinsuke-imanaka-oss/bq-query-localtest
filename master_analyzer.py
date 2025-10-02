# master_analyzer.py

import streamlit as st
import pandas as pd
from typing import Dict, Any
from datetime import date, timedelta

# --- 依存モジュールの安全なインポート ---
try:
    from performance_analyzer import get_performance_data, calculate_kpis, evaluate_performance
    from forecast_analyzer import get_daily_kpi_data, get_forecast_data
    from insight_miner import find_key_drivers_safe
    MODULES_AVAILABLE = True
except ImportError as e:
    st.error(f"分析モジュールのインポートに失敗しました: {e}")
    MODULES_AVAILABLE = False

# --- 分析のオーケストレーション（ロジック） ---
def gather_all_analyses(bq_client, start_date: date, end_date: date) -> Dict[str, Any]:
    """全ての分析モジュールを実行し、結果を一つの辞書にまとめる"""
    # ... (この関数の中身は変更なし) ...
    if not MODULES_AVAILABLE:
        return {"error": "分析モジュールがロードできませんでした。"}

    results = {}

    with st.spinner("ステップ1/3: パフォーマンス診断を実行中..."):
        perf_data = get_performance_data(bq_client)
        if perf_data is not None:
            results["performance"] = evaluate_performance(calculate_kpis(perf_data))
        else:
            results["performance"] = "パフォーマンスデータがありません"

    with st.spinner("ステップ2/3: 将来予測を計算中..."):
        daily_data = get_daily_kpi_data(bq_client, start_date=start_date, end_date=end_date)
        if daily_data is not None:
            results["forecast"] = get_forecast_data(daily_data, periods=30)
        else:
            results["forecast"] = "予測データがありません"

    with st.spinner("ステップ3/3: 主要因を分析中..."):
        drivers_df = find_key_drivers_safe(bq_client, target_kpi_en='cvr')
        if drivers_df is not None:
             results["drivers"] = drivers_df
        else:
            results["drivers"] = "要因分析データがありません"

    return results


# --- AIによるサマリー生成（ロジック） ---
def generate_executive_summary(analysis_results: Dict, model_choice: str, gemini_model, claude_client, claude_model_name) -> str:
    """分析結果を統合し、経営層向けのサマリーを生成する"""
    # ... (この関数の中身は変更なし) ...
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

    ## 2. パフォーマンスに影響を与えている主要因 (CVR貢献度順)
    {drivers_summary}

    # あなたのタスク
    以下の構成で、簡潔かつ示唆に富んだレポートを生成してください。
    - **現状の要約:** (アカウント全体の健全性について)
    - **将来の見通し:** (予測分析の結果を踏まえて)
    - **成功と課題の要因:** (パフォーマンスと要因分析の結果を結びつけて)
    - **推奨されるネクストアクション:** (最もインパクトの大きい施策を1つ提案)
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


# --- ▼▼▼【ここからが重要】UI表示とロジック呼び出しをこの関数に集約 ▼▼▼ ---
def show_comprehensive_report_mode():
    """統合分析レポートモードのUIを表示し、分析フローを制御する"""
    st.header("📊 統合分析レポート")
    st.markdown("複数のAI分析を連携させ、アカウント全体の状況を一つのレポートに統合します。")

    # セッション状態から必要なクライアントを取得
    bq_client = st.session_state.get("bq_client")
    gemini_model = st.session_state.get("gemini_model")
    claude_client = st.session_state.get("claude_client")
    claude_model_name = st.session_state.get("claude_model_name")

    # 必須クライアントの存在チェック
    if not bq_client or (not gemini_model and not claude_client):
         st.error("この機能を利用するには、BigQueryとAIモデルの両方に接続してください。")
         return

    # --- 1. コントロールパネルUI ---
    with st.expander("分析設定", expanded=True):
        model_options = []
        if gemini_model: model_options.append("Gemini")
        if claude_client: model_options.append("Claude")
        
        model_choice = st.selectbox("レポート生成AIを選択", options=model_options)
        
        start_date = st.date_input("分析開始日", value=date.today() - timedelta(days=90))
        end_date = st.date_input("分析終了日", value=date.today() - timedelta(days=1))

    # --- 2. レポート生成ボタンとロジック呼び出し ---
    if st.button("🚀 最新データで統合分析レポートを生成", type="primary"):
        # 分析オーケストレーションを実行
        analysis_results = gather_all_analyses(bq_client, start_date, end_date)
        if "error" in analysis_results:
            st.error(analysis_results["error"])
            return
        
        # AIサマリー生成を実行
        summary = generate_executive_summary(analysis_results, model_choice, gemini_model, claude_client, claude_model_name)
        
        # 結果をセッション状態に保存
        st.session_state.comprehensive_report = {
            "summary": summary,
            "details": analysis_results,
            "model_used": model_choice
        }
        st.rerun()

    # --- 3. レポート表示エリア ---
    if "comprehensive_report" in st.session_state:
        report = st.session_state.comprehensive_report
        st.markdown("---")
        st.subheader(f"🤖 エグゼクティブサマリー (by {report['model_used']})")
        st.info(report["summary"])

        st.subheader("詳細データ")
        tab1, tab2, tab3 = st.tabs(["📈 パフォーマンス診断", "🔮 予測分析", "🧠 要因分析"])

        with tab1:
            st.dataframe(report["details"]["performance"], use_container_width=True)
        with tab2:
            st.dataframe(report["details"]["forecast"], use_container_width=True)
        with tab3:
            st.dataframe(report["details"]["drivers"], use_container_width=True)