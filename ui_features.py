# ui_features.py - 最小版
"""
UI機能拡張モジュール
分析サマリーパネル・データ品質パネル・統計表示など
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# =========================================================================
# 分析サマリーパネル
# =========================================================================

def show_analysis_summary_panel():
    """分析サマリーパネルの表示"""
    st.markdown("### 📊 分析サマリー")
    
    # 最後の分析結果の表示
    if st.session_state.get("last_analysis_result") is not None:
        df = st.session_state.last_analysis_result
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("データ行数", f"{len(df):,}")
        with col2:
            st.metric("データ列数", len(df.columns))
        with col3:
            # 数値列の数をカウント
            numeric_cols = df.select_dtypes(include=['number']).columns
            st.metric("数値列数", len(numeric_cols))
        with col4:
            # 最終更新時刻
            current_time = datetime.now().strftime("%H:%M")
            st.metric("更新時刻", current_time)
        
        # 最後のSQL表示
        if st.session_state.get("last_sql"):
            with st.expander("📄 実行されたSQL", expanded=False):
                st.code(st.session_state.last_sql, language="sql")
        
        # 最後のユーザー入力表示
        if st.session_state.get("last_user_input"):
            st.info(f"💭 分析内容: {st.session_state.last_user_input}")
    
    else:
        st.info("📈 分析を実行すると、ここにサマリーが表示されます")

# =========================================================================
# データ品質パネル
# =========================================================================

def show_data_quality_panel():
    """データ品質パネルの表示"""
    if st.session_state.get("last_analysis_result") is not None:
        df = st.session_state.last_analysis_result
        
        st.markdown("### 🔍 データ品質チェック")
        
        # データ品質チェック実行
        try:
            from data_quality_checker import check_data_quality, show_data_quality_summary
            quality_report = check_data_quality(df)
            show_data_quality_summary(quality_report)
        except ImportError:
            # フォールバック: 基本的な品質情報
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**基本統計:**")
                st.write(f"- 総行数: {len(df):,}")
                st.write(f"- 総列数: {len(df.columns)}")
                
                # NULL値チェック
                null_info = []
                for col in df.columns:
                    null_count = df[col].isnull().sum()
                    if null_count > 0:
                        null_percentage = (null_count / len(df)) * 100
                        null_info.append(f"- {col}: {null_count} ({null_percentage:.1f}%)")
                
                if null_info:
                    st.markdown("**NULL値:**")
                    for info in null_info[:5]:  # 最大5列まで表示
                        st.write(info)
                else:
                    st.write("- NULL値: なし ✅")
            
            with col2:
                st.markdown("**データ型:**")
                for col in df.columns[:8]:  # 最大8列まで表示
                    st.write(f"- {col}: {df[col].dtype}")
    else:
        st.info("🔍 分析データがある時に品質チェックが表示されます")

# =========================================================================
# エラー履歴表示
# =========================================================================

def show_error_history():
    """エラー履歴の表示"""
    if st.session_state.get("error_history"):
        with st.expander("⚠️ エラー履歴", expanded=False):
            st.markdown("### 最近のエラー")
            
            for i, error in enumerate(st.session_state.error_history[-5:], 1):
                st.markdown(f"**{i}. {error['timestamp'].strftime('%H:%M:%S')}**")
                st.write(f"エラー: {error['error_message']}")
                if error.get('suggestion'):
                    st.info(error['suggestion'])
                st.markdown("---")
    else:
        st.success("✅ エラーはありません")

# =========================================================================
# 使用統計表示
# =========================================================================

def show_usage_statistics():
    """使用統計の表示"""
    st.markdown("### 📈 使用統計")
    
    stats = st.session_state.get("usage_stats", {
        "total_analyses": 0,
        "error_count": 0,
        "enhanced_usage": 0,
        "avg_execution_time": 0.0
    })
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("総分析数", stats.get("total_analyses", 0))
    
    with col2:
        error_count = stats.get("error_count", 0)
        st.metric("エラー数", error_count)
    
    with col3:
        enhanced_count = stats.get("enhanced_usage", 0)
        st.metric("高品質モード", enhanced_count)
    
    with col4:
        success_rate = 0
        if stats.get("total_analyses", 0) > 0:
            success_rate = ((stats["total_analyses"] - error_count) / stats["total_analyses"]) * 100
        st.metric("成功率", f"{success_rate:.1f}%")
    
    # 使用履歴のグラフ（データがある場合）
    if st.session_state.get("analysis_logs"):
        show_usage_chart()

def show_usage_chart():
    """使用統計のグラフ表示"""
    logs = st.session_state.get("analysis_logs", [])
    if len(logs) >= 3:  # 最低3つのデータがある場合のみグラフ表示
        
        # 時間別の使用状況
        hourly_usage = {}
        for log in logs:
            hour = log["timestamp"].hour
            hourly_usage[hour] = hourly_usage.get(hour, 0) + 1
        
        if hourly_usage:
            fig = px.bar(
                x=list(hourly_usage.keys()),
                y=list(hourly_usage.values()),
                title="時間別使用状況",
                labels={"x": "時間", "y": "分析回数"}
            )
            fig.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig, use_container_width=True)

# =========================================================================
# クイック再分析機能
# =========================================================================

def show_quick_reanalysis():
    """クイック再分析機能"""
    if st.session_state.get("last_user_input") and st.session_state.get("last_sql"):
        st.markdown("### 🔄 クイック再分析")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔄 同じ分析を再実行", use_container_width=True):
                st.info("同じ分析を再実行します...")
                # 再分析の実行（実際の実装では analysis_controller を呼び出し）
                st.rerun()
        
        with col2:
            if st.button("📝 分析内容を編集", use_container_width=True):
                # 編集モードにする（セッション状態を更新）
                st.session_state.edit_mode = True
                st.info("分析内容を編集できます")

# =========================================================================
# 高度な可視化オプション
# =========================================================================

def show_advanced_visualization_options():
    """高度な可視化オプション"""
    if st.session_state.get("last_analysis_result") is not None:
        df = st.session_state.last_analysis_result
        
        st.markdown("### 📊 高度な可視化")
        
        # 数値列を取得
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        if len(numeric_cols) >= 2:
            viz_type = st.selectbox(
                "可視化タイプ選択",
                ["散布図", "棒グラフ", "線グラフ", "ヒートマップ"]
            )
            
            col1, col2 = st.columns(2)
            
            with col1:
                x_axis = st.selectbox("X軸", df.columns)
            with col2:
                y_axis = st.selectbox("Y軸", numeric_cols)
            
            if st.button("📊 グラフ生成"):
                try:
                    if viz_type == "散布図":
                        fig = px.scatter(df, x=x_axis, y=y_axis, title=f"{y_axis} vs {x_axis}")
                    elif viz_type == "棒グラフ":
                        # 上位10件のデータのみ表示
                        top_data = df.nlargest(10, y_axis)
                        fig = px.bar(top_data, x=x_axis, y=y_axis, title=f"{y_axis} by {x_axis} (Top 10)")
                    elif viz_type == "線グラフ":
                        fig = px.line(df, x=x_axis, y=y_axis, title=f"{y_axis} Trend")
                    else:  # ヒートマップ
                        # 数値列のみでヒートマップ作成
                        numeric_df = df[numeric_cols].corr()
                        fig = px.imshow(numeric_df, title="Correlation Heatmap")
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                except Exception as e:
                    st.error(f"グラフ生成エラー: {str(e)}")
        else:
            st.info("可視化には2つ以上の数値列が必要です")

# =========================================================================
# データエクスポート機能
# =========================================================================

def show_export_options():
    """データエクスポートオプション"""
    if st.session_state.get("last_analysis_result") is not None:
        df = st.session_state.last_analysis_result
        
        st.markdown("### 💾 データエクスポート")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # CSV エクスポート
            csv_data = df.to_csv(index=False, encoding='utf-8')
            st.download_button(
                label="📥 CSV ダウンロード",
                data=csv_data,
                file_name=f"analysis_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col2:
            # JSON エクスポート
            json_data = df.to_json(orient='records', ensure_ascii=False, indent=2)
            st.download_button(
                label="📥 JSON ダウンロード",
                data=json_data,
                file_name=f"analysis_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                use_container_width=True
            )

# =========================================================================
# ヘルプ・ガイド機能
# =========================================================================

def show_help_panel():
    """ヘルプパネルの表示"""
    with st.expander("❓ ヘルプ・使い方", expanded=False):
        st.markdown("""
        ### 🚀 基本的な使い方
        
        1. **分析内容を入力**: 自然言語で分析したい内容を入力
        2. **AI・プロンプト選択**: 用途に応じて選択
        3. **実行**: 「🚀 分析実行」ボタンをクリック
        4. **結果確認**: データとグラフを確認
        
        ### 📝 入力例
        - "過去30日のキャンペーン別コストを分析して"
        - "CTRが高いキャンペーンTOP10を表示"
        - "デバイス別のコンバージョン率を比較"
        
        ### 💡 Tips
        - 具体的な期間を指定すると精度が上がります
        - 分析したい指標を明確に指定してください
        - エラーが出た場合は、エラー履歴を確認してください
        """)

# =========================================================================
# パフォーマンス監視
# =========================================================================

def show_performance_monitoring():
    """パフォーマンス監視パネル"""
    with st.expander("⚡ パフォーマンス", expanded=False):
        if st.session_state.get("analysis_logs"):
            logs = st.session_state.analysis_logs
            
            # 実行時間の統計
            execution_times = [log.get("execution_time", 0) for log in logs if log.get("execution_time")]
            
            if execution_times:
                avg_time = sum(execution_times) / len(execution_times)
                max_time = max(execution_times)
                min_time = min(execution_times)
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("平均実行時間", f"{avg_time:.1f}秒")
                with col2:
                    st.metric("最長実行時間", f"{max_time:.1f}秒")
                with col3:
                    st.metric("最短実行時間", f"{min_time:.1f}秒")
        
        # システム情報
        st.markdown("**システム情報:**")
        st.write(f"- Streamlit バージョン: {st.__version__}")
        st.write(f"- 現在時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# =========================================================================
# メイン機能統合関数
# =========================================================================

def show_main_features():
    """メイン機能の統合表示"""
    # タブで機能を整理
    tab1, tab2, tab3, tab4 = st.tabs(["📊 サマリー", "🔍 品質", "📈 統計", "💾 エクスポート"])
    
    with tab1:
        show_analysis_summary_panel()
        show_quick_reanalysis()
    
    with tab2:
        show_data_quality_panel()
        show_error_history()
    
    with tab3:
        show_usage_statistics()
        show_performance_monitoring()
    
    with tab4:
        show_export_options()
        show_advanced_visualization_options()

# =========================================================================
# 外部から呼び出される関数（main.pyとの互換性）
# =========================================================================

def get_current_analysis_stats():
    """現在の分析統計を取得"""
    return st.session_state.get("usage_stats", {
        "total_analyses": 0,
        "error_count": 0,
        "enhanced_usage": 0,
        "avg_execution_time": 0.0
    })