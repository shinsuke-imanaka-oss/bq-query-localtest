# comprehensive_analyzer.py

import streamlit as st
import pandas as pd
from typing import Optional, Dict

# --- 依存モジュールのインポート ---
try:
    from performance_analyzer import get_performance_data, calculate_kpis
    from insight_miner import find_key_drivers_safe
    from forecast_analyzer import get_daily_kpi_data
    MODULES_AVAILABLE = True
except ImportError:
    st.error("分析モジュール(performance_analyzer, insight_miner, forecast_analyzer)が見つかりません。")
    MODULES_AVAILABLE = False

# --- AIによるレポート生成 ---

def generate_comprehensive_report(context: Dict, gemini_model) -> str:
    """
    収集した分析コンテキストに基づき、AIに総合的なレポートを生成させる
    """
    if not gemini_model:
        return "AIモデルが利用できません。"

    # AIへのプロンプトを作成
    prompt = f"""
    あなたは、経営層やマーケティング責任者へ報告を行う、非常に優秀なデータアナリストです。
    以下の断片的な分析データを統合し、プロフェッショナルな視点から総合的な「マーケティング分析レポート」を作成してください。

    # 分析データ
    ## 1. 主要KPIサマリー
    {pd.DataFrame(context.get('performance_summary', [])).to_string()}

    ## 2. CVRに最も影響を与える要因
    - ポジティブ要因: {context.get('top_positive_driver', {})}
    - ネガティブ要因: {context.get('top_negative_driver', {})}

    ## 3. 直近30日間のコスト推移
    {context.get('recent_trend_data', pd.DataFrame()).to_string()}

    # 出力形式（この形式を厳守）
    ##  EXECUTIVE SUMMARY
    （経営層向けの総括を3行以内で記述）

    ## 📈 パフォーマンス概観
    （主要KPIサマリーから読み取れる、アカウント全体の健全性や特筆すべき点を記述）

    ## 🔍 主要因の分析
    （CVRへの影響要因を基に、何がうまくいっており、何が課題となっているのかを具体的に分析）

    ## 📊 最近のトレンド
    （直近のコスト推移データから、傾向や注意すべき点などを分析）

    ## 💡 推奨アクションプラン
    （上記全ての分析結果を統合し、次に取り組むべき具体的なアクションを3つ提案）
    """
    try:
        with st.spinner("AIが収集したデータを統合し、総合レポートを作成中..."):
            response = gemini_model.generate_content(prompt)
            return response.text
    except Exception as e:
        return f"AIによるレポート生成中にエラーが発生: {e}"

# --- メイン実行関数 ---

def run_comprehensive_analysis():
    """
    総合分析レポートのメインフローを実行する
    """
    st.header("📑 総合分析レポート")
    st.markdown("アカウント全体のパフォーマンス、主要因、トレンドをAIが統合的に分析し、レポートを自動生成します。")

    bq_client = st.session_state.get("bq_client")
    gemini_model = st.session_state.get("gemini_model")

    if not all([MODULES_AVAILABLE, bq_client, gemini_model]):
        st.error("この機能を利用するには、必要な分析モジュールがすべて存在し、BigQueryとGeminiの両方に接続している必要があります。")
        return

    # レポート生成ボタン
    if st.button("🚀 最新データで総合分析レポートを生成する", type="primary"):
        analysis_context = {}
        with st.spinner("各分析モジュールからデータを収集中... (1/3)"):
            perf_data = get_performance_data(bq_client)
            if perf_data is not None:
                kpi_df = calculate_kpis(perf_data)
                analysis_context['performance_summary'] = kpi_df.to_dict('records')

        with st.spinner("各分析モジュールからデータを収集中... (2/3)"):
            drivers_df = find_key_drivers_safe(bq_client, target_kpi_en='cvr')
            if drivers_df is not None:
                analysis_context['top_positive_driver'] = drivers_df.iloc[0].to_dict()
                analysis_context['top_negative_driver'] = drivers_df.iloc[-1].to_dict()

        with st.spinner("各分析モジュールからデータを収集中... (3/3)"):
            from datetime import date, timedelta
            today = date.today()
            start_date = today - timedelta(days=30)
            end_date = today - timedelta(days=1)
            trend_data = get_daily_kpi_data(bq_client, target_kpi='CostIncludingFees', start_date=start_date, end_date=end_date)
            if trend_data is not None:
                analysis_context['recent_trend_data'] = trend_data

        # AIにレポートを生成させる
        report_text = generate_comprehensive_report(analysis_context, gemini_model)

        # 結果をセッション状態に保存
        st.session_state.comprehensive_report = report_text

    # 保存されたレポートがあれば表示
    if "comprehensive_report" in st.session_state:
        st.markdown("---")
        st.subheader("🤖 AIによる総合分析レポート")
        st.markdown(st.session_state.comprehensive_report)