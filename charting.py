# charting.py
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

def render_plotly_chart(df: pd.DataFrame, cfg: dict):
    """
    Plotlyグラフを描画する。
    - 組合せグラフ: 左右のY軸を持つ棒グラフ＋折れ線グラフ
    - その他グラフ: 凡例ごとにグループ化されたグラフを生成
    """
    try:
        # 入力検証
        if df is None or df.empty:
            st.warning("表示するデータがありません。")
            return go.Figure()
            
        chart_type = cfg.get("main_chart_type")
        x_axis = cfg.get("x_axis")
        y_axis_left = cfg.get("y_axis_left")
        y_axis_right = cfg.get("y_axis_right")
        legend_col_raw = cfg.get("legend_col")
        legend_col = legend_col_raw if legend_col_raw != "なし" else None

        # 必須パラメータの検証
        if not all([chart_type, x_axis, y_axis_left]):
            st.warning("グラフを描画できません。グラフの種類、X軸、Y軸を正しく設定してください。")
            return go.Figure()
            
        # 列の存在確認
        required_cols = [x_axis, y_axis_left]
        if y_axis_right and y_axis_right != "なし":
            required_cols.append(y_axis_right)
        if legend_col:
            required_cols.append(legend_col)
            
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            st.error(f"以下の列がデータに存在しません: {', '.join(missing_cols)}")
            return go.Figure()

        # ========================================
        # 組合せグラフの処理
        # ========================================
        if chart_type == "組合せグラフ" and y_axis_right and y_axis_right != "なし":
            fig = make_subplots(specs=[[{"secondary_y": True}]])

            # 左軸の棒グラフを追加
            fig.add_trace(
                go.Bar(
                    x=df[x_axis],
                    y=df[y_axis_left],
                    name=y_axis_left,
                    showlegend=True,
                    yaxis='y'
                ),
                secondary_y=False,
            )
            
            # 右軸の折れ線グラフを追加
            fig.add_trace(
                go.Scatter(
                    x=df[x_axis],
                    y=df[y_axis_right],
                    name=y_axis_right,
                    mode='lines+markers',
                    showlegend=True,
                    yaxis='y2'
                ),
                secondary_y=True,
            )

            # 軸ラベルを設定
            fig.update_yaxes(title_text=y_axis_left, secondary_y=False)
            fig.update_yaxes(title_text=y_axis_right, secondary_y=True)
            fig.update_xaxes(title_text=x_axis)

        # ========================================
        # 通常グラフの処理（凡例対応）
        # ========================================
        else:
            # 円グラフの特別処理
            if chart_type == "円グラフ":
                # 凡例が指定されている場合は、凡例ごとにy軸の値を集計
                if legend_col:
                    df_grouped = df.groupby(legend_col)[y_axis_left].sum().reset_index()
                    fig = px.pie(df_grouped, names=legend_col, values=y_axis_left, hole=.3)
                else:
                    fig = px.pie(df, names=x_axis, values=y_axis_left, hole=.3)
            
            # その他のグラフタイプ
            else:
                px_func_map = {
                    "棒グラフ": px.bar,
                    "折れ線グラフ": px.line,
                    "面グラフ": px.area,
                    "散布図": px.scatter
                }
                px_func = px_func_map.get(chart_type)

                if px_func:
                    kwargs = {
                        'x': x_axis,
                        'y': y_axis_left,
                        'color': legend_col  # 凡例でグループ化
                    }
                    
                    # 折れ線グラフ、散布図、面グラフにマーカーを追加
                    if chart_type in ["折れ線グラフ", "散布図", "面グラフ"]:
                        kwargs['markers'] = True
                    
                    fig = px_func(df, **kwargs)
                else:
                    st.error(f"サポートされていないグラフタイプです: {chart_type}")
                    return go.Figure()

        # 共通レイアウト設定
        fig.update_layout(
            title=None,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            xaxis_title=x_axis if chart_type != "円グラフ" else None,
            yaxis_title=y_axis_left if chart_type not in ["円グラフ", "組合せグラフ"] else None
        )

        return fig

    except Exception as e:
        st.error(f"グラフ描画中にエラーが発生しました: {e}")
        import traceback
        st.error(f"詳細エラー: {traceback.format_exc()}")
        return go.Figure()