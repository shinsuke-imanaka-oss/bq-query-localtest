# strategy_simulator.py

import streamlit as st
import pandas as pd
from typing import Optional, Dict, List

# --- 依存モジュールのインポート ---
try:
    from performance_analyzer import get_performance_data, calculate_kpis, evaluate_performance
    from insight_miner import find_key_drivers_safe
    PERFORMANCE_MODULE_AVAILABLE = True
except ImportError:
    st.error("分析モジュール(performance_analyzer, insight_miner)が見つかりません。")
    PERFORMANCE_MODULE_AVAILABLE = False


# --- 総合分析 & コンテキスト生成 ---

def gather_analysis_context(bq_client) -> Optional[Dict]:
    """
    過去の分析(B-1, B-3)を実行し、AIへのインプットとなるコンテキストを生成する
    """
    if not PERFORMANCE_MODULE_AVAILABLE:
        return None

    with st.spinner("戦略立案のため、現状のパフォーマンスと主要因を再分析しています..."):
        # B-1: パフォーマンス診断の実行
        perf_data = get_performance_data(bq_client)
        if perf_data is None: return {"error": "パフォーマンスデータを取得できませんでした。"}
        kpi_df = calculate_kpis(perf_data)
        evaluated_df = evaluate_performance(kpi_df)

        # B-3: 要因分析の実行 (CVRをターゲットとする)
        drivers_df = find_key_drivers_safe(bq_client, target_kpi_en='cvr')
        if drivers_df is None: return {"error": "要因分析データを取得できませんでした。"}

        # 最も影響の大きい要因を抽出
        top_positive_driver = drivers_df.iloc[0]
        top_negative_driver = drivers_df.iloc[-1]

        return {
            "performance_summary": evaluated_df.to_dict(orient='records'),
            "top_positive_driver": top_positive_driver.to_dict(),
            "top_negative_driver": top_negative_driver.to_dict(),
            "base_cvr": kpi_df['cvr'].mean(), # シミュレーションの基準値
            "total_clicks": kpi_df['clicks'].sum() # シミュレーションの基準値
        }

# --- AIによる戦略提案 ---

def generate_action_plan(context: Dict, gemini_model) -> str:
    """分析コンテキストに基づき、AIに具体的なアクションプランを複数提案させる"""
    if not gemini_model: return "AIモデルが利用できません。"

    prompt = f"""
    あなたは、データに基づいた戦略を立案するプロのマーケティング戦略家です。
    以下の現状分析レポートを読み解き、具体的なアクションプランを3つ提案してください。
    各プランは「**【プラン名】:**」で始め、その後に「**施策内容:**」「**期待される効果:**」を記述してください。

    # 現状分析レポート
    ## 1. 全体パフォーマンス
    {pd.DataFrame(context['performance_summary']).to_string()}

    ## 2. CVRへの影響度が最も大きい要因
    - ポジティブ要因: 「{context['top_positive_driver']['dimension']}」の「{context['top_positive_driver']['factor']}」
    - ネガティブ要因: 「{context['top_negative_driver']['dimension']}」の「{context['top_negative_driver']['factor']}」

    # 出力形式 (この形式を厳守)
    【プランA: 強みの強化】:
    施策内容: ...
    期待される効果: ...

    【プランB: 弱点の改善】:
    施策内容: ...
    期待される効果: ...

    【プランC: 新規テスト】:
    施策内容: ...
    期待される効果: ...
    """
    try:
        with st.spinner("AIが分析結果に基づき、戦略プランを立案中..."):
            response = gemini_model.generate_content(prompt)
            return response.text
    except Exception as e:
        return f"AIによる戦略提案の生成中にエラーが発生: {e}"


# --- What-if シミュレーション ---

def run_what_if_simulation(context: Dict):
    """
    シンプルな仮説に基づき、提案された施策の効果をシミュレーションする
    """
    st.subheader("📈 What-if シミュレーション")
    st.markdown("もし提案された施策を実行した場合、主要KPIがどのように変化するかを簡易的に予測します。")

    base_cvr = context['base_cvr']
    total_clicks = context['total_clicks']
    base_conversions = total_clicks * base_cvr

    # --- シミュレーションのロジック ---
    # ここでは「CVRがX%改善した場合」というシンプルなモデルを使用
    cvr_improvement_rate = st.slider(
        "仮説: もしCVRがこのくらい改善したら？ (%)",
        min_value=-20, max_value=50, value=10, step=5
    ) / 100

    simulated_cvr = base_cvr * (1 + cvr_improvement_rate)
    simulated_conversions = total_clicks * simulated_cvr

    # --- 結果表示 ---
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            label="現在の平均CVR",
            value=f"{base_cvr:.2%}"
        )
        st.metric(
            label="現在の総コンバージョン数",
            value=f"{base_conversions:,.0f} 件"
        )
    with col2:
        st.metric(
            label="シミュレーション後のCVR",
            value=f"{simulated_cvr:.2%}",
            delta=f"{(simulated_cvr - base_cvr):.2%}"
        )
        st.metric(
            label="シミュレーション後の総CV数",
            value=f"{simulated_conversions:,.0f} 件",
            delta=f"{(simulated_conversions - base_conversions):,.0f} 件"
        )
    with col3:
        st.info(f"**シミュレーションの仮説:**\n\n提案されたアクションプランを実行した結果、全体のCVRが **{cvr_improvement_rate:.0%}** 変化するという仮説に基づいています。")


# --- メイン実行関数 ---

def run_strategy_simulation():
    """戦略提案とシミュレーションのメインフローを実行する"""
    st.header("💡 戦略提案 & シミュレーション")
    st.markdown("これまでの分析結果を統合し、AIが具体的な次のアクションを提案します。")

    bq_client = st.session_state.get("bq_client")
    gemini_model = st.session_state.get("gemini_model")
    if not bq_client or not gemini_model:
        st.error("この機能を利用するには、BigQueryとGeminiの両方に接続してください。")
        return

    if st.button("🚀 最新データで戦略提案を生成する", type="primary"):
        # 1. 総合分析を実行し、コンテキストを生成
        context = gather_analysis_context(bq_client)
        if context is None or "error" in context:
            st.error(f"戦略提案の生成に必要な分析を実行できませんでした。 Error: {context.get('error', 'Unknown')}")
            return

        # 2. AIにアクションプランを提案させる
        action_plan_text = generate_action_plan(context, gemini_model)

        # 結果をセッション状態に保存して再利用
        st.session_state.strategy_context = context
        st.session_state.action_plan = action_plan_text

    # --- 保存された結果があれば表示 ---
    if "action_plan" in st.session_state:
        st.subheader("🤖 AIによるアクションプラン提案")
        st.markdown(st.session_state.action_plan)
        st.markdown("---")
        run_what_if_simulation(st.session_state.strategy_context)