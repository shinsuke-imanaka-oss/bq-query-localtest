# forecast_analyzer.py

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from prophet import Prophet
from typing import Optional
from datetime import date, timedelta

# --- データ取得 ---

# ▼▼▼【修正点1】期間指定に対応 ▼▼▼
def get_daily_kpi_data(bq_client, target_kpi: str = 'CostIncludingFees', start_date: date = None, end_date: date = None) -> Optional[pd.DataFrame]:
    """日別の主要KPIデータをBigQueryから取得する"""
    if not bq_client:
        st.warning("BigQueryクライアントが初期化されていません。")
        return None

    # 期間指定のWHERE句を動的に生成
    where_clause = f"WHERE Date BETWEEN '{start_date}' AND '{end_date}'"

    query = f"""
    SELECT
        Date AS ds,
        SUM({target_kpi}) AS y
    FROM `vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign`
    {where_clause}
    GROUP BY Date
    ORDER BY Date ASC
    """
    try:
        with st.spinner(f"日別の {target_kpi} データを取得中..."):
            df = bq_client.query(query).to_dataframe()
            if df.empty or len(df) < 14: # 安定した予測のため最低14日分のデータを要求
                st.warning("予測するには少なくとも14日分のデータが必要です。期間を広げてください。")
                return None
            df['ds'] = pd.to_datetime(df['ds']).dt.tz_localize(None)
            return df
    except Exception as e:
        st.error(f"データ取得エラー: {e}")
        return None

# --- 可視化 ---

# ▼▼▼【修正点2】Plotlyを使ったインタラクティブなグラフ描画に全面変更 ▼▼▼
def create_forecast_chart(forecast: pd.DataFrame, target_kpi_ja: str):
    """Plotlyを使い、予測結果と実測値をインタラクティブなグラフにプロットする"""
    st.subheader(f"📊 {target_kpi_ja} の将来予測と異常検知")

    fig = go.Figure()

    # 1. 予測の信頼区間をエリアとしてプロット (yhat_lower, yhat_upper)
    fig.add_trace(go.Scatter(
        x=forecast['ds'],
        y=forecast['yhat_upper'],
        mode='lines',
        line=dict(width=0),
        fillcolor='rgba(68, 68, 68, 0.2)',
        showlegend=False,
        name='信頼区間'
    ))
    fig.add_trace(go.Scatter(
        x=forecast['ds'],
        y=forecast['yhat_lower'],
        mode='lines',
        line=dict(width=0),
        fill='tonexty', # yhat_upper との間を塗りつぶす
        fillcolor='rgba(68, 68, 68, 0.2)',
        showlegend=False,
        name='信頼区間'
    ))

    # 2. 実績値をプロット (y)
    fig.add_trace(go.Scatter(
        x=forecast['ds'],
        y=forecast['y'],
        mode='markers',
        marker=dict(color='black', size=5),
        name='実績値',
        # ホバー時に表示するテキストをカスタマイズ
        hovertemplate='日付: %{x|%Y-%m-%d}<br>実績: %{y:,.0f}<extra></extra>'
    ))

    # 3. 予測の中心線をプロット (yhat)
    fig.add_trace(go.Scatter(
        x=forecast['ds'],
        y=forecast['yhat'],
        mode='lines',
        line=dict(color='royalblue', dash='dash'),
        name='予測値',
        hovertemplate='日付: %{x|%Y-%m-%d}<br>予測: %{y:,.0f}<extra></extra>'
    ))

    # 4. 異常検知点をプロット
    anomalies = forecast[(forecast['y'] < forecast['yhat_lower']) | (forecast['y'] > forecast['yhat_upper'])]
    if not anomalies.empty:
        fig.add_trace(go.Scatter(
            x=anomalies['ds'],
            y=anomalies['y'],
            mode='markers',
            marker=dict(color='red', size=10, symbol='x'),
            name='異常検知',
            hovertemplate='日付: %{x|%Y-%m-%d}<br><b>異常値</b>: %{y:,.0f}<extra></extra>'
        ))

    # グラフ全体のレイアウト設定（ここで日本語の軸名を設定）
    fig.update_layout(
        title=f'{target_kpi_ja} の時系列予測と異常検知',
        xaxis_title='日付',
        yaxis_title=target_kpi_ja,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    st.plotly_chart(fig, use_container_width=True)

    # 異常検知された日のリスト
    if not anomalies.empty:
        with st.expander("🚨 異常検知された日付の詳細"):
            st.info(f"{len(anomalies)}件の異常（予測範囲からの乖離）が検知されました。")
            st.dataframe(anomalies[['ds', 'y', 'yhat', 'yhat_lower', 'yhat_upper']].rename(columns={
                'ds': '日付', 'y': '実績値', 'yhat': '予測中心値', 'yhat_lower': '予測下限', 'yhat_upper': '予測上限'
            }), use_container_width=True)
    else:
        st.success("✅ 期間中に統計的な異常は検知されませんでした。")


# --- メイン実行関数 ---

def run_forecast_analysis():
    """予測分析のメインフローを実行する"""
    st.header("🔮 予測分析 & 異常検知")
    st.markdown("過去のデータから将来のKPIを予測し、統計的に異常な数値を自動で検知します。")

    bq_client = st.session_state.get("bq_client")
    if not bq_client:
        st.error("この機能を利用するには、まずBigQueryに接続してください。")
        return

    # ▼▼▼【修正点3】UIコントロールに期間指定フィルターを追加 ▼▼▼
    st.markdown("##### 1. 予測に使用するデータ期間を選択")
    today = date.today()
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("開始日", value=today - timedelta(days=90))
    with col2:
        end_date = st.date_input("終了日", value=today - timedelta(days=1))

    if start_date >= end_date:
        st.error("エラー: 開始日は終了日より前の日付を選択してください。")
        return

    st.markdown("---")
    st.markdown("##### 2. 予測内容を選択して実行")
    col3, col4 = st.columns(2)
    with col3:
        kpi_options_map = {
            'コスト': 'CostIncludingFees', 'クリック数': 'Clicks',
            'コンバージョン数': 'Conversions', '表示回数': 'Impressions'
        }
        target_kpi_ja = st.selectbox("予測対象のKPIを選択", options=list(kpi_options_map.keys()))
        target_kpi_en = kpi_options_map[target_kpi_ja]

    with col4:
        forecast_days = st.slider("何日先まで予測しますか？", min_value=7, max_value=90, value=30)

    if st.button("🚀 予測を実行する", type="primary"):
        # 1. データ取得 (期間指定の引数を追加)
        daily_data = get_daily_kpi_data(bq_client, target_kpi=target_kpi_en, start_date=start_date, end_date=end_date)
        if daily_data is None:
            return

        # 2. 予測モデルの作成と実行
        try:
            with st.spinner("予測モデルを学習・実行中..."):
                prophet_model = Prophet(yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=False)
                prophet_model.add_country_holidays(country_name='JP')
                prophet_model.fit(daily_data)
                future_df = prophet_model.make_future_dataframe(periods=forecast_days)
                forecast_result = prophet_model.predict(future_df)

                # 【重要】実績値と予測結果をここで結合しておく
                results_with_history = pd.merge(forecast_result, daily_data, on='ds', how='left')

            # 3. 可視化
            create_forecast_chart(results_with_history, target_kpi_ja)

        except Exception as e:
            st.error(f"予測処理中に予期せぬエラーが発生しました: {e}")