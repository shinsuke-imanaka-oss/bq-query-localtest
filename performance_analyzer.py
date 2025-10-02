# performance_analyzer.py - 日付範囲対応版

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Any, Optional
from datetime import date

# --- 依存モジュールの安全なインポート ---
try:
    from enhanced_prompts import get_industry_benchmarks
except ImportError:
    st.error("enhanced_prompts.py が見つかりません。")
    def get_industry_benchmarks(): return {}

# --- データ取得・加工（日付範囲対応版） ---

def get_performance_data(bq_client, start_date: date = None, end_date: date = None) -> Optional[pd.DataFrame]:
    """
    メディア別のパフォーマンスデータをBigQueryから取得し、KPIも計算する（日付範囲対応版）
    
    Args:
        bq_client: BigQueryクライアント
        start_date: 開始日（Noneの場合は全期間）
        end_date: 終了日（Noneの場合は全期間）
    
    Returns:
        パフォーマンスデータのDataFrame
    """
    if not bq_client:
        st.warning("BigQueryクライアントが初期化されていません。")
        return None

    # WHERE句の構築
    where_clauses = ["ServiceNameJA_Media IS NOT NULL"]
    
    if start_date:
        where_clauses.append(f"Date >= '{start_date}'")
    if end_date:
        where_clauses.append(f"Date <= '{end_date}'")
    
    where_clause = "WHERE " + " AND ".join(where_clauses)

    # CTE(WITH句)を使い、集計を2段階に分けることでエラーを回避する
    query = f"""
    WITH MediaSummary AS (
        -- ステップ1: まずはメディア別に必要な指標を単純に合計する
        SELECT
            ServiceNameJA_Media AS media,
            SUM(CostIncludingFees) AS cost,
            SUM(Impressions) AS impressions,
            SUM(Clicks) AS clicks,
            SUM(Conversions) AS conversions
        FROM `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`
        {where_clause}
        GROUP BY ServiceNameJA_Media
    )
    -- ステップ2: ステップ1で集計した値を使って、最終的なKPIを計算する
    SELECT
        media,
        cost,
        impressions,
        clicks,
        conversions,
        SAFE_DIVIDE(cost, clicks) AS cpc,
        SAFE_DIVIDE(clicks, impressions) AS ctr,
        SAFE_DIVIDE(conversions, clicks) AS cvr,
        SAFE_DIVIDE(cost, conversions) AS cpa
    FROM MediaSummary
    WHERE impressions > 0 AND clicks > 0 AND conversions > 0
    ORDER BY cost DESC
    """
    
    try:
        with st.spinner("診断データをBigQueryから取得中..."):
            df = bq_client.query(query).to_dataframe()
            if df.empty:
                st.warning("分析対象のデータが見つかりませんでした。")
                return None
            return df
    except Exception as e:
        st.error(f"データ取得エラー: {e}")
        return None


def calculate_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """SQLでKPI計算済みのため、この関数はデータをそのまま返すだけで良い"""
    # SQLクエリ内で計算が完了しているため、ここでは何もしない
    return df


# --- 診断と評価 ---

def evaluate_performance(df: pd.DataFrame) -> pd.DataFrame:
    """KPIを業界ベンチマークと比較し、評価付けを行う"""
    benchmarks = get_industry_benchmarks()
    if not benchmarks:
        st.warning("ベンチマーク情報がありません。評価はスキップされます。")
        return df

    evaluations = []
    for _, row in df.iterrows():
        # メディア名に応じて適切なベンチマークを選択 (ここでは簡易的に「検索広告」で代用)
        # TODO: メディア名とベンチマークのマッピングを実装する
        benchmark = benchmarks.get("検索広告", {})

        # CPAは低い方が良いので、評価ロジックを反転させる
        cpa_ratio = row['cpa'] / benchmark.get("平均CPA", float('inf'))
        if cpa_ratio < 0.8:
            cpa_eval = "◎ 非常に良い"
        elif cpa_ratio < 1.1:
            cpa_eval = "○ 良い"
        else:
            cpa_eval = "△ 要改善"

        # CVRは高い方が良い
        cvr_ratio = row['cvr'] / benchmark.get("平均CVR", 0)
        if cvr_ratio > 1.2:
            cvr_eval = "◎ 非常に良い"
        elif cvr_ratio > 0.9:
            cvr_eval = "○ 良い"
        else:
            cvr_eval = "△ 要改善"

        evaluations.append({"media": row["media"], "CPA評価": cpa_eval, "CVR評価": cvr_eval})

    eval_df = pd.DataFrame(evaluations)
    return pd.merge(df, eval_df, on="media")


# --- AIによるコメント生成 ---

def generate_ai_summary(df: pd.DataFrame, gemini_model) -> str:
    """評価結果を元に、AIに総評コメントを生成させる"""
    if not gemini_model:
        return "AIモデルが利用できないため、総評は生成できません。"

    prompt = f"""
    あなたは優秀なデジタルマーケティングコンサルタントです。
    以下のメディア別パフォーマンス診断結果を分析し、経営層にも分かるように、
    現在の状況、最も注目すべき点、そして最初に取り組むべき改善アクションを箇条書きで簡潔にまとめてください。

    # 診断結果データ
    {df.to_string()}
    """
    try:
        with st.spinner("AIが診断結果を分析・要約中..."):
            response = gemini_model.generate_content(prompt)
            return response.text
    except Exception as e:
        return f"AIコメント生成中にエラーが発生: {e}"


# --- 可視化 ---

def create_comparison_chart(df: pd.DataFrame):
    """CPAとCVRを比較する複合グラフを作成する"""
    fig = go.Figure()

    # CPA (棒グラフ) - 低い方が良い
    fig.add_trace(go.Bar(
        x=df['media'],
        y=df['cpa'],
        name='CPA (円)',
        marker_color='skyblue'
    ))

    # CVR (折れ線グラフ) - 高い方が良い
    fig.add_trace(go.Scatter(
        x=df['media'],
        y=df['cvr'],
        name='CVR (%)',
        mode='lines+markers',
        yaxis='y2',
        line=dict(color='orange', width=3),
        marker=dict(size=8)
    ))

    fig.update_layout(
        title='メディア別 CPA vs CVR パフォーマンス',
        xaxis_title='メディア',
        yaxis_title='CPA (円)',
        yaxis2=dict(
            title='CVR (%)',
            overlaying='y',
            side='right',
            tickformat='.2%' # パーセント表示
        ),
        legend=dict(x=0.1, y=1.1, orientation="h")
    )
    st.plotly_chart(fig, use_container_width=True)


# --- メイン実行関数 ---

def run_performance_diagnosis():
    """パフォーマンス診断のメインフローを実行する"""
    st.header("📈 パフォーマンス診断")
    st.markdown("主要KPIを業界標準と比較し、アカウントの健全性を評価します。")

    bq_client = st.session_state.get("bq_client")
    gemini_model = st.session_state.get("gemini_model")

    # 1. データ取得（日付範囲なし = 全期間）
    data = get_performance_data(bq_client)
    if data is None:
        return

    # 2. KPI計算
    kpi_df = calculate_kpis(data)

    # 3. パフォーマンス評価
    evaluated_df = evaluate_performance(kpi_df)

    # 4. AIによる総評
    summary_text = generate_ai_summary(evaluated_df, gemini_model)
    st.subheader("🤖 AIによる診断サマリー")
    st.info(summary_text)

    # 5. 可視化
    st.subheader("📊 パフォーマンス詳細")
    create_comparison_chart(evaluated_df)

    with st.expander("📝 診断データ詳細"):
        # 不要な列を非表示にし、数値をフォーマットして表示
        display_df = evaluated_df.copy()
        format_dict = {
            'cost': '{:,.0f} 円',
            'impressions': '{:,.0f} 回',
            'clicks': '{:,.0f} 回',
            'conversions': '{:,.0f} 件',
            'cpc': '{:,.1f} 円',
            'ctr': '{:.2%}',
            'cvr': '{:.2%}',
            'cpa': '{:,.0f} 円'
        }
        st.dataframe(display_df.style.format(format_dict), use_container_width=True)