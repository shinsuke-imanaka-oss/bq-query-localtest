# ui_features.py
"""
分析機能パネル UI
- データ品質チェックパネル
- エラー履歴表示
- 使用統計パネル
- 分析提案機能
- ワンクリック分析
- パフォーマンス監視
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import warnings
warnings.filterwarnings('ignore')

# データ品質チェック機能のインポート
try:
    from data_quality_checker import DataQualityChecker
except ImportError:
    st.warning("data_quality_checker.py が見つかりません - データ品質チェック機能は無効")
    DataQualityChecker = None

# =========================================================================
# 分析追跡・履歴管理機能
# =========================================================================

def initialize_analysis_tracking():
    """分析追跡の初期化"""
    if "analysis_logs" not in st.session_state:
        st.session_state.analysis_logs = []
    if "error_history" not in st.session_state:
        st.session_state.error_history = []
    if "usage_stats" not in st.session_state:
        st.session_state.usage_stats = {
            "total_analyses": 0,
            "enhanced_usage": 0,
            "basic_usage": 0,
            "error_count": 0,
            "avg_execution_time": 0
        }

def log_analysis_usage(user_input: str, system_type: str, execution_time: float = 0, error: bool = False):
    """分析使用ログの記録"""
    log_entry = {
        "timestamp": datetime.now(),
        "user_input": user_input[:50] + "..." if len(user_input) > 50 else user_input,
        "system": system_type,  # "enhanced" or "basic"
        "execution_time": execution_time,
        "error": error
    }
    
    st.session_state.analysis_logs.append(log_entry)
    
    # 統計の更新
    st.session_state.usage_stats["total_analyses"] += 1
    if system_type == "enhanced":
        st.session_state.usage_stats["enhanced_usage"] += 1
    else:
        st.session_state.usage_stats["basic_usage"] += 1
    
    if error:
        st.session_state.usage_stats["error_count"] += 1
    
    # 平均実行時間の更新
    if execution_time > 0:
        current_avg = st.session_state.usage_stats["avg_execution_time"]
        total = st.session_state.usage_stats["total_analyses"]
        st.session_state.usage_stats["avg_execution_time"] = (current_avg * (total - 1) + execution_time) / total
    
    # ログ数の制限（メモリ節約）
    if len(st.session_state.analysis_logs) > 100:
        st.session_state.analysis_logs = st.session_state.analysis_logs[-100:]

# =========================================================================
# 分析サマリーパネル
# =========================================================================

def show_analysis_summary_panel():
    """分析サマリーパネルの表示"""
    if not st.session_state.get("df", pd.DataFrame()).empty:
        df = st.session_state.df
        
        with st.expander("📈 分析結果サマリー", expanded=True):
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("データ行数", len(df))
            
            with col2:
                numeric_cols = df.select_dtypes(include=['number']).columns
                st.metric("数値列数", len(numeric_cols))
            
            with col3:
                # コスト系の列があれば合計表示
                cost_cols = [col for col in df.columns if 'cost' in col.lower() or 'コスト' in col]
                if cost_cols:
                    total_cost = df[cost_cols[0]].sum()
                    st.metric("総コスト", f"¥{total_cost:,.0f}")
                else:
                    st.metric("分析期間", "データ依存")
            
            with col4:
                # 最新データの日付
                date_cols = [col for col in df.columns if 'date' in col.lower() or '日付' in col]
                if date_cols:
                    latest_date = df[date_cols[0]].max()
                    st.metric("最新データ", latest_date)
                else:
                    st.metric("実行時刻", datetime.now().strftime("%H:%M"))

# =========================================================================
# データ品質チェック機能
# =========================================================================

def show_data_quality_panel(df: pd.DataFrame):
    """データ品質チェックパネルの表示"""
    if DataQualityChecker is None:
        return
    
    with st.expander("🔍 データ品質チェック", expanded=False):
        if df.empty:
            st.info("分析データがありません。")
            return
        
        quality_checker = DataQualityChecker()
        quality_report = quality_checker.comprehensive_quality_check(df)
        
        # 品質スコア表示
        col1, col2, col3 = st.columns(3)
        with col1:
            score = quality_report.get("overall_score", 0)
            st.metric("総合品質スコア", f"{score}/100")
        
        with col2:
            status = quality_report.get("status", "unknown")
            status_colors = {"excellent": "🟢", "good": "🟡", "warning": "🟠", "critical": "🔴"}
            st.write(f"**ステータス**: {status_colors.get(status, '❓')} {status}")
        
        with col3:
            st.metric("データ行数", len(df))
        
        # 問題点の表示
        if quality_report.get("issues"):
            st.markdown("**検出された問題:**")
            for issue in quality_report["issues"]:
                if issue["severity"] == "critical":
                    st.error(f"🔴 {issue['message']}")
                elif issue["severity"] == "warning":
                    st.warning(f"🟡 {issue['message']}")
                else:
                    st.info(f"ℹ️ {issue['message']}")
        
        # 改善提案
        if quality_report.get("suggestions"):
            st.markdown("**改善提案:**")
            for suggestion in quality_report["suggestions"]:
                st.info(f"💡 {suggestion}")

# =========================================================================
# エラー履歴表示機能
# =========================================================================

def show_error_history():
    """エラー履歴の表示"""
    if not st.session_state.get("error_history"):
        return
    
    with st.expander("⚠️ エラー履歴", expanded=False):
        st.markdown("**最近のエラーと解決策:**")
        
        # 最新5件のエラーを表示
        recent_errors = st.session_state.error_history[-5:]
        for error in reversed(recent_errors):
            timestamp = error["timestamp"].strftime("%H:%M:%S")
            st.markdown(f"**{timestamp}** - {error.get('category', 'エラー')}")
            st.caption(f"エラー: {error.get('original_message', error.get('error_message', ''))}")
            
            # 解決策の表示
            solutions = error.get("solutions", [])
            if solutions:
                st.info(f"💡 解決策: {solutions[0]}")
            
            st.markdown("---")

# =========================================================================
# 使用統計表示機能
# =========================================================================

def show_usage_statistics():
    """使用統計の表示"""
    # 分析追跡の初期化を確実に行う
    initialize_analysis_tracking()
    
    stats = st.session_state.usage_stats
    
    with st.expander("📊 使用統計", expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("総分析回数", stats["total_analyses"])
        
        with col2:
            enhanced_pct = (stats["enhanced_usage"] / max(stats["total_analyses"], 1)) * 100
            st.metric("高品質分析率", f"{enhanced_pct:.1f}%")
        
        with col3:
            error_rate = (stats["error_count"] / max(stats["total_analyses"], 1)) * 100
            st.metric("エラー率", f"{error_rate:.1f}%")
        
        with col4:
            st.metric("平均実行時間", f"{stats['avg_execution_time']:.1f}秒")

# =========================================================================
# ワンクリック再分析機能
# =========================================================================

def show_quick_reanalysis():
    """ワンクリック再分析機能"""
    st.markdown("**⚡ ワンクリック分析**")
    
    quick_options = {
        "🔝 TOP10分析": "コスト上位10位のキャンペーンの詳細分析",
        "📅 今月データ": "今月のデータのみを使った分析", 
        "💰 コスト順": "コスト効率の良い順での並び替え分析",
        "📈 トレンド": "時系列でのパフォーマンス変化分析",
        "📊 メディア比較": "メディア別の効果比較分析",
        "📋 全体俯瞰": "全データの包括的なサマリー分析"
    }
    
    cols = st.columns(3)
    selected_quick = None
    
    for i, (key, description) in enumerate(quick_options.items()):
        col_idx = i % 3
        with cols[col_idx]:
            if st.button(key, help=description, use_container_width=True):
                # 簡単な分析レシピマッピング
                recipe_mapping = {
                    "🔝 TOP10分析": "コスト上位10キャンペーンのROAS、CPA、CVRを分析し、最も効率的なキャンペーンを特定してください",
                    "📅 今月データ": "今月のデータに絞って、メディア別の主要KPI（CTR、CPA、ROAS）の変化を分析してください",
                    "💰 コスト順": "CPA（顧客獲得単価）が最も良いキャンペーンを特定し、改善の余地があるキャンペーンと比較してください",
                    "📈 トレンド": "過去30日間の日別パフォーマンス推移を可視化し、トレンドと異常値を特定してください",
                    "📊 メディア比較": "Google広告、Facebook広告、Yahoo!広告の効果を比較し、各メディアの特徴を分析してください",
                    "📋 全体俯瞰": "全データの包括的なサマリー分析を実行してください"
                }
                selected_quick = recipe_mapping.get(key, description)
    
    return selected_quick

# =========================================================================
# 分析提案機能
# =========================================================================

def show_analysis_suggestions():
    """分析提案機能"""
    if st.session_state.get("df") is not None and not st.session_state.df.empty:
        df = st.session_state.df
        
        with st.expander("💡 分析提案", expanded=False):
            st.markdown("**データの特徴に基づく分析提案:**")
            
            suggestions = []
            
            # データ特徴の分析
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
            
            # 時系列データの判定
            date_cols = [col for col in df.columns if 'date' in col.lower() or '日付' in col]
            if date_cols:
                suggestions.append("📈 **時系列トレンド分析**: 日別・週別・月別の変化パターンを分析")
            
            # カテゴリ別分析の提案
            if categorical_cols and numeric_cols:
                suggestions.append(f"📊 **カテゴリ別比較**: {categorical_cols[0]}別の{numeric_cols[0]}を比較分析")
            
            # 相関分析の提案
            if len(numeric_cols) >= 2:
                suggestions.append(f"🔗 **相関分析**: {numeric_cols[0]}と{numeric_cols[1]}の関係性を分析")
            
            # 外れ値分析の提案
            for col in numeric_cols[:2]:  # 最初の2つの数値列をチェック
                q75 = df[col].quantile(0.75)
                q25 = df[col].quantile(0.25)
                iqr = q75 - q25
                outliers = df[(df[col] < (q25 - 1.5 * iqr)) | (df[col] > (q75 + 1.5 * iqr))]
                if len(outliers) > 0:
                    suggestions.append(f"⚠️ **外れ値分析**: {col}に{len(outliers)}個の外れ値を検出")
            
            # TOP/BOTTOMランキング分析
            if numeric_cols:
                suggestions.append(f"🏆 **ランキング分析**: {numeric_cols[0]}の上位・下位ランキング")
            
            # 提案の表示
            for suggestion in suggestions:
                st.markdown(f"• {suggestion}")
            
            if not suggestions:
                st.info("現在のデータ構造では、特別な分析提案はありません。")

# =========================================================================
# パフォーマンス監視機能
# =========================================================================

def show_performance_monitoring():
    """パフォーマンス監視の表示"""
    with st.expander("⚡ パフォーマンス監視", expanded=False):
        st.markdown("**システムパフォーマンス:**")
        
        # メモリ使用量（概算）
        if st.session_state.get("df") is not None:
            memory_usage = st.session_state.df.memory_usage(deep=True).sum() / 1024 / 1024  # MB
            st.metric("データメモリ使用量", f"{memory_usage:.1f} MB")
        
        # レスポンス時間の表示
        if st.session_state.usage_stats["avg_execution_time"] > 0:
            avg_time = st.session_state.usage_stats["avg_execution_time"]
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("平均実行時間", f"{avg_time:.1f}秒")
            with col2:
                if avg_time < 5:
                    st.success("🟢 高速")
                elif avg_time < 15:
                    st.warning("🟡 標準")
                else:
                    st.error("🔴 低速")
        
        # API使用状況
        enhanced_usage = st.session_state.usage_stats["enhanced_usage"]
        basic_usage = st.session_state.usage_stats["basic_usage"]
        
        if enhanced_usage + basic_usage > 0:
            st.markdown("**今日のAPI使用分布:**")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("高品質分析", f"{enhanced_usage}回")
            with col2:
                st.metric("基本分析", f"{basic_usage}回")

# =========================================================================
# 使用履歴の詳細表示
# =========================================================================

def show_usage_history():
    """使用履歴の詳細表示"""
    if st.session_state.get("analysis_logs"):
        with st.expander("📋 詳細使用履歴", expanded=False):
            st.markdown("**最近の分析履歴:**")
            
            # 履歴をDataFrameに変換して表示
            history_data = []
            for log in st.session_state.analysis_logs[-10:]:  # 最新10件
                history_data.append({
                    "時刻": log["timestamp"].strftime("%H:%M:%S"),
                    "システム": "🚀 高品質" if log["system"] == "enhanced" else "⚡ 基本",
                    "指示内容": log["user_input"],
                    "実行時間": f"{log['execution_time']:.1f}秒",
                    "ステータス": "❌ エラー" if log["error"] else "✅ 成功"
                })
            
            if history_data:
                history_df = pd.DataFrame(history_data)
                st.dataframe(history_df, use_container_width=True)
        
        # 使用ログの消去機能
        if st.button("🗑️ 履歴をクリア"):
            st.session_state.analysis_logs = []
            st.session_state.error_history = []
            st.session_state.usage_stats = {
                "total_analyses": 0,
                "enhanced_usage": 0,
                "basic_usage": 0,
                "error_count": 0,
                "avg_execution_time": 0
            }
            st.success("使用履歴がクリアされました。")
            st.rerun()

# =========================================================================
# フィルタUI機能
# =========================================================================

def show_filter_ui(bq_client):
    """フィルタUI（サイドバー用）"""
    st.markdown("### 📊 データフィルタ")
    
    # 日付範囲フィルタ
    date_range = st.date_input(
        "分析期間",
        value=(datetime.now() - timedelta(days=30), datetime.now()),
        key="date_filter"
    )
    
    # メディアフィルタ
    media_options = st.multiselect(
        "メディア選択",
        ["Google広告", "Facebook広告", "Yahoo!広告", "LINE広告"],
        default=["Google広告", "Facebook広告"],
        key="media_filter"
    )
    
    # キャンペーンタイプフィルタ
    campaign_types = st.multiselect(
        "キャンペーンタイプ",
        ["検索", "ディスプレイ", "ショッピング", "動画", "アプリ"],
        key="campaign_type_filter"
    )
    
    # 最小コストフィルタ
    min_cost = st.number_input(
        "最小コスト（円）",
        min_value=0,
        value=1000,
        step=1000,
        key="min_cost_filter"
    )
    
    # フィルタ適用ボタン
    if st.button("🔍 フィルタ適用", use_container_width=True):
        st.success("フィルタが適用されました！")
        # フィルタ条件をセッション状態に保存
        st.session_state.active_filters = {
            "date_range": date_range,
            "media": media_options,
            "campaign_types": campaign_types,
            "min_cost": min_cost
        }

# =========================================================================
# データ可視化支援機能
# =========================================================================

def show_advanced_visualization_options():
    """高度な可視化オプション"""
    if st.session_state.get("df") is not None and not st.session_state.df.empty:
        df = st.session_state.df
        
        with st.expander("📊 高度な可視化", expanded=False):
            viz_type = st.selectbox(
                "可視化タイプ",
                ["ヒートマップ", "箱ひげ図", "相関行列", "分布図", "時系列グラフ"]
            )
            
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            
            if viz_type == "ヒートマップ" and len(numeric_cols) >= 2:
                show_heatmap(df, numeric_cols)
            elif viz_type == "箱ひげ図" and numeric_cols:
                show_boxplot(df, numeric_cols)
            elif viz_type == "相関行列" and len(numeric_cols) >= 2:
                show_correlation_matrix(df, numeric_cols)
            elif viz_type == "分布図" and numeric_cols:
                show_distribution_plot(df, numeric_cols)
            elif viz_type == "時系列グラフ":
                show_timeseries_plot(df)

def show_heatmap(df, numeric_cols):
    """ヒートマップ表示"""
    try:
        # 数値列のみを選択して相関行列を計算
        corr_matrix = df[numeric_cols].corr()
        
        fig = px.imshow(
            corr_matrix,
            title="相関ヒートマップ",
            color_continuous_scale="RdBu",
            aspect="auto"
        )
        st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.warning(f"ヒートマップの生成に失敗しました: {str(e)}")

def show_boxplot(df, numeric_cols):
    """箱ひげ図表示"""
    try:
        selected_col = st.selectbox("対象列を選択", numeric_cols)
        
        fig = px.box(df, y=selected_col, title=f"{selected_col} の分布")
        st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.warning(f"箱ひげ図の生成に失敗しました: {str(e)}")

def show_correlation_matrix(df, numeric_cols):
    """相関行列表示"""
    try:
        corr_matrix = df[numeric_cols].corr()
        st.dataframe(corr_matrix.style.background_gradient(cmap='coolwarm', axis=None))
        
    except Exception as e:
        st.warning(f"相関行列の生成に失敗しました: {str(e)}")

def show_distribution_plot(df, numeric_cols):
    """分布図表示"""
    try:
        selected_col = st.selectbox("対象列を選択", numeric_cols, key="dist_col")
        
        fig = px.histogram(df, x=selected_col, title=f"{selected_col} の分布")
        st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.warning(f"分布図の生成に失敗しました: {str(e)}")

def show_timeseries_plot(df):
    """時系列グラフ表示"""
    try:
        # 日付列を検索
        date_cols = [col for col in df.columns if 'date' in col.lower() or '日付' in col]
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        if date_cols and numeric_cols:
            date_col = st.selectbox("日付列", date_cols)
            value_col = st.selectbox("値列", numeric_cols, key="ts_col")
            
            fig = px.line(df, x=date_col, y=value_col, title=f"{value_col} の時系列変化")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("時系列グラフには日付列と数値列が必要です。")
            
    except Exception as e:
        st.warning(f"時系列グラフの生成に失敗しました: {str(e)}")

# =========================================================================
# 機能統合表示関数
# =========================================================================

def show_all_feature_panels(df=None):
    """すべての機能パネルを表示"""
    # 初期化
    initialize_analysis_tracking()
    
    # データがある場合の機能パネル
    if df is not None and not df.empty:
        show_analysis_summary_panel()
        show_data_quality_panel(df)
        show_analysis_suggestions()
        show_advanced_visualization_options()
    
    # データに依存しない機能パネル
    show_error_history()
    show_usage_statistics()
    show_performance_monitoring()
    show_usage_history()

# =========================================================================
# 外部呼び出し用の便利関数
# =========================================================================

def get_current_analysis_stats():
    """現在の分析統計を取得"""
    initialize_analysis_tracking()
    return st.session_state.usage_stats

def add_error_to_history(error_message: str, error_category: str = "一般エラー", solutions: List[str] = None):
    """エラーを履歴に追加"""
    if "error_history" not in st.session_state:
        st.session_state.error_history = []
    
    error_entry = {
        "timestamp": datetime.now(),
        "original_message": error_message,
        "category": error_category,
        "solutions": solutions or ["エラーメッセージを確認し、適切な修正を行ってください"]
    }
    
    st.session_state.error_history.append(error_entry)
    
    # 履歴の上限制御
    if len(st.session_state.error_history) > 50:
        st.session_state.error_history = st.session_state.error_history[-50:]

# =========================================================================
# 分析レポート生成機能
# =========================================================================

def generate_analysis_report():
    """分析レポートの生成"""
    if not st.session_state.get("df", pd.DataFrame()).empty:
        df = st.session_state.df
        
        report = {
            "generated_at": datetime.now().isoformat(),
            "data_summary": {
                "rows": len(df),
                "columns": len(df.columns),
                "numeric_columns": len(df.select_dtypes(include=['number']).columns),
                "categorical_columns": len(df.select_dtypes(include=['object']).columns)
            },
            "usage_statistics": get_current_analysis_stats(),
            "user_input": st.session_state.get("user_input", ""),
            "sql": st.session_state.get("sql", "")
        }
        
        return report
    
    return None

def show_export_options():
    """エクスポートオプションの表示"""
    with st.expander("📤 データエクスポート", expanded=False):
        if st.session_state.get("df") is not None and not st.session_state.df.empty:
            df = st.session_state.df
            
            col1, col2 = st.columns(2)
            
            with col1:
                # CSV エクスポート
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📄 CSVダウンロード",
                    data=csv,
                    file_name=f"analysis_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            with col2:
                # 分析レポートのエクスポート
                report = generate_analysis_report()
                if report:
                    import json
                    report_json = json.dumps(report, ensure_ascii=False, indent=2, default=str)
                    st.download_button(
                        label="📋 分析レポート",
                        data=report_json,
                        file_name=f"analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json",
                        use_container_width=True
                    )
        else:
            st.info("エクスポートするデータがありません。")

# =========================================================================
# ユーザーフィードバック機能
# =========================================================================

def show_feedback_panel():
    """ユーザーフィードバックパネル"""
    with st.expander("💬 フィードバック", expanded=False):
        st.markdown("**分析の使いやすさを教えてください:**")
        
        rating = st.select_slider(
            "満足度",
            options=["😞 不満", "😐 普通", "😊 満足", "😍 非常に満足"],
            value="😊 満足"
        )
        
        feedback_text = st.text_area(
            "改善提案やコメント（任意）",
            placeholder="より良い分析環境にするためのご意見をお聞かせください..."
        )
        
        if st.button("📝 フィードバック送信"):
            # フィードバックを履歴に保存（実際のアプリでは外部サービスに送信）
            if "user_feedback" not in st.session_state:
                st.session_state.user_feedback = []
            
            st.session_state.user_feedback.append({
                "timestamp": datetime.now(),
                "rating": rating,
                "feedback": feedback_text
            })
            
            st.success("✅ フィードバックありがとうございました！")

# =========================================================================
# メイン表示統合関数
# =========================================================================

def show_comprehensive_feature_dashboard():
    """包括的な機能ダッシュボードの表示"""
    # 分析追跡初期化
    initialize_analysis_tracking()
    
    # タブ形式での機能表示
    tab1, tab2, tab3, tab4 = st.tabs(["📊 サマリー", "🔍 品質", "⚡ パフォーマンス", "📋 履歴"])
    
    with tab1:
        show_analysis_summary_panel()
        if st.session_state.get("df") is not None and not st.session_state.df.empty:
            show_analysis_suggestions()
    
    with tab2:
        if st.session_state.get("df") is not None and not st.session_state.df.empty:
            show_data_quality_panel(st.session_state.df)
            show_advanced_visualization_options()
    
    with tab3:
        show_performance_monitoring()
        show_usage_statistics()
    
    with tab4:
        show_usage_history()
        show_error_history()

def show_sidebar_features(bq_client=None):
    """サイドバー用の機能表示"""
    if bq_client:
        show_filter_ui(bq_client)
    
    st.markdown("---")
    show_export_options()
    
    st.markdown("---")
    show_feedback_panel()

# =========================================================================
# 分析ワークフロー支援機能
# =========================================================================

def show_analysis_workflow_guide():
    """分析ワークフロー案内の表示"""
    with st.expander("🗺️ 分析ワークフロー案内", expanded=False):
        st.markdown("""
        ### 📋 推奨分析フロー
        
        #### Step 1: データ確認
        1. **データ品質チェック** でデータの状態を確認
        2. **基本統計** でデータの概要を把握
        3. 必要に応じて **データクリーニング** を実行
        
        #### Step 2: 探索的分析
        1. **ワンクリック分析** で全体像を把握
        2. **分析提案** から興味深い観点を選択
        3. **高度な可視化** でパターンを発見
        
        #### Step 3: 詳細分析
        1. **カスタム分析** で具体的な疑問を調査
        2. **Claude分析** で戦略的洞察を取得
        3. **結果の解釈** と次のアクション検討
        
        #### Step 4: 成果活用
        1. **分析レポート** の生成・ダウンロード
        2. **SQL保存** で再利用可能な形で保管
        3. **分析履歴** で過去の分析との比較
        """)

def show_tips_and_tricks():
    """使い方のコツとトリックの表示"""
    with st.expander("💡 使い方のコツ", expanded=False):
        st.markdown("""
        ### 🚀 効率的な分析のコツ
        
        #### プロンプト作成のベストプラクティス
        - **具体的な指示**: 「売上を見たい」→「過去30日のメディア別売上推移を比較したい」
        - **期間の明確化**: 「今月」「過去3ヶ月」など期間を明記
        - **比較軸の指定**: 「メディア別」「キャンペーン別」など軸を明記
        
        #### データ品質向上のコツ
        - **定期的チェック**: 分析前に必ずデータ品質チェックを実行
        - **外れ値処理**: 明らかな外れ値は除外してから分析
        - **NULL値対応**: NULL値が多い列は事前に除外を検討
        
        #### パフォーマンス最適化
        - **期間限定**: 大量データの場合は期間を限定
        - **列の選択**: 必要な列のみを選択してクエリを軽量化
        - **段階的分析**: 概要→詳細の順で段階的に分析を深める
        """)

# =========================================================================
# ヘルプ・サポート機能
# =========================================================================

def show_help_panel():
    """ヘルプパネルの表示"""
    with st.expander("❓ ヘルプ・サポート", expanded=False):
        st.markdown("""
        ### 🆘 困ったときは
        
        #### よくある問題と解決策
        
        **Q1: SQLエラーが発生する**
        - ✅ **解決策**: エラー履歴を確認して提案された解決策を試す
        - ✅ **予防策**: データ品質チェックで事前に問題を発見
        
        **Q2: 分析結果が期待と異なる**
        - ✅ **確認点**: データの期間・フィルタ条件を確認
        - ✅ **対処法**: より具体的な指示でプロンプトを再作成
        
        **Q3: 処理が遅い**
        - ✅ **対処法**: データ期間を短縮、LIMIT句を追加
        - ✅ **確認点**: パフォーマンス監視で実行時間をチェック
        
        **Q4: 結果をどう解釈すべきかわからない**
        - ✅ **活用法**: Claude分析で戦略的洞察を取得
        - ✅ **参考**: 分析提案から類似の分析例を参照
        
        ### 📞 サポート機能の活用
        - **エラー履歴**: 過去の問題と解決策を確認
        - **分析提案**: データに適した分析方法を提案
        - **ワンクリック分析**: 迷ったときの出発点として活用
        - **フィードバック**: 改善要望や不明点を報告
        """)

def show_keyboard_shortcuts():
    """キーボードショートカットの表示"""
    with st.expander("⌨️ ショートカット", expanded=False):
        st.markdown("""
        ### ⚡ キーボードショートカット
        
        #### 一般操作
        - `Ctrl + Enter`: 分析実行
        - `Ctrl + R`: ページリロード
        - `Tab`: 次の入力フィールドに移動
        
        #### 分析操作
        - `Ctrl + 1`: 高品質分析モードに切替
        - `Ctrl + 2`: コスト重視モードに切替
        - `F1`: ヘルプパネルを表示
        
        #### 便利機能
        - `Ctrl + D`: データ品質チェック実行
        - `Ctrl + H`: 分析履歴を表示
        - `Ctrl + S`: 現在の分析を保存
        """)

# =========================================================================
# アクセシビリティ機能
# =========================================================================

def show_accessibility_options():
    """アクセシビリティオプションの表示"""
    with st.expander("♿ アクセシビリティ", expanded=False):
        st.markdown("### 🔧 表示設定")
        
        # フォントサイズ調整
        font_size = st.select_slider(
            "フォントサイズ",
            options=["小", "標準", "大", "特大"],
            value="標準"
        )
        
        # 色覚サポート
        color_scheme = st.selectbox(
            "カラースキーム",
            ["標準", "ハイコントラスト", "色覚サポート"]
        )
        
        # 読み上げサポート
        enable_screen_reader = st.checkbox("スクリーンリーダー対応")
        
        # アニメーション設定
        reduce_animations = st.checkbox("アニメーションを減らす")
        
        if st.button("設定を適用"):
            st.session_state.accessibility_settings = {
                "font_size": font_size,
                "color_scheme": color_scheme,
                "screen_reader": enable_screen_reader,
                "reduce_animations": reduce_animations
            }
            st.success("✅ アクセシビリティ設定が適用されました")

# =========================================================================
# デバッグ・開発者向け機能
# =========================================================================

def show_debug_panel():
    """デバッグパネルの表示（開発者向け）"""
    if st.checkbox("🔧 開発者モード"):
        with st.expander("🐛 デバッグ情報", expanded=False):
            st.markdown("### システム情報")
            
            # セッション状態の表示
            st.markdown("#### セッション状態")
            debug_session = {
                "データ行数": len(st.session_state.get("df", pd.DataFrame())),
                "使用プロンプト": "Enhanced" if st.session_state.get("use_enhanced_prompts", True) else "Basic",
                "選択AI": st.session_state.get("selected_ai", "未設定"),
                "総分析回数": st.session_state.usage_stats.get("total_analyses", 0),
                "エラー数": st.session_state.usage_stats.get("error_count", 0)
            }
            
            for key, value in debug_session.items():
                st.text(f"{key}: {value}")
            
            # 詳細セッション情報
            if st.button("詳細セッション情報を表示"):
                st.json({
                    k: str(v) for k, v in st.session_state.items() 
                    if not k.startswith('_') and k != 'df'
                })
            
            # ログ出力
            st.markdown("#### システムログ")
            if st.session_state.get("analysis_logs"):
                recent_logs = st.session_state.analysis_logs[-3:]
                for log in recent_logs:
                    st.text(f"{log['timestamp']}: {log['user_input']} ({log['system']})")

# =========================================================================
# 統合メニュー表示
# =========================================================================

def show_feature_menu():
    """機能メニューの統合表示"""
    menu_option = st.selectbox(
        "機能を選択",
        [
            "📊 基本機能",
            "🔍 品質・可視化",
            "📋 履歴・統計", 
            "💡 ヘルプ・サポート",
            "🔧 設定・デバッグ"
        ]
    )
    
    if menu_option == "📊 基本機能":
        show_analysis_summary_panel()
        if st.session_state.get("df") is not None and not st.session_state.df.empty:
            show_analysis_suggestions()
    
    elif menu_option == "🔍 品質・可視化":
        if st.session_state.get("df") is not None and not st.session_state.df.empty:
            show_data_quality_panel(st.session_state.df)
            show_advanced_visualization_options()
    
    elif menu_option == "📋 履歴・統計":
        show_usage_statistics()
        show_usage_history()
        show_performance_monitoring()
        show_error_history()
    
    elif menu_option == "💡 ヘルプ・サポート":
        show_analysis_workflow_guide()
        show_tips_and_tricks()
        show_help_panel()
        show_keyboard_shortcuts()
    
    elif menu_option == "🔧 設定・デバッグ":
        show_accessibility_options()
        show_debug_panel()

# =========================================================================
# クイックアクション機能
# =========================================================================

def show_quick_actions():
    """クイックアクション機能"""
    st.markdown("### ⚡ クイックアクション")
    
    action_cols = st.columns(4)
    
    with action_cols[0]:
        if st.button("🔍 品質チェック", use_container_width=True):
            if st.session_state.get("df") is not None and not st.session_state.df.empty:
                show_data_quality_panel(st.session_state.df)
            else:
                st.warning("分析データがありません")
    
    with action_cols[1]:
        if st.button("📊 統計表示", use_container_width=True):
            show_usage_statistics()
    
    with action_cols[2]:
        if st.button("📋 履歴表示", use_container_width=True):
            show_usage_history()
    
    with action_cols[3]:
        if st.button("🗑️ 履歴クリア", use_container_width=True):
            if st.confirm("履歴をすべてクリアしますか？"):
                st.session_state.analysis_logs = []
                st.session_state.error_history = []
                st.session_state.usage_stats = {
                    "total_analyses": 0,
                    "enhanced_usage": 0,
                    "basic_usage": 0,
                    "error_count": 0,
                    "avg_execution_time": 0
                }
                st.success("✅ 履歴をクリアしました")
                st.rerun()

# =========================================================================
# 外部API統合用の拡張機能
# =========================================================================

def register_external_feature(feature_name: str, feature_function, icon: str = "🔧"):
    """外部機能の登録"""
    if "external_features" not in st.session_state:
        st.session_state.external_features = {}
    
    st.session_state.external_features[feature_name] = {
        "function": feature_function,
        "icon": icon
    }

def show_external_features():
    """登録された外部機能の表示"""
    if st.session_state.get("external_features"):
        with st.expander("🔌 拡張機能", expanded=False):
            for feature_name, feature_info in st.session_state.external_features.items():
                if st.button(f"{feature_info['icon']} {feature_name}", use_container_width=True):
                    try:
                        feature_info["function"]()
                    except Exception as e:
                        st.error(f"拡張機能エラー: {str(e)}")

# =========================================================================
# パフォーマンス最適化関数
# =========================================================================

@st.cache_data(ttl=300)  # 5分間キャッシュ
def get_cached_analysis_stats():
    """キャッシュされた分析統計の取得"""
    return get_current_analysis_stats()

def optimize_dataframe_display(df: pd.DataFrame, max_rows: int = 1000):
    """データフレーム表示の最適化"""
    if len(df) > max_rows:
        st.warning(f"⚠️ データが大きすぎます ({len(df):,}行)。最初の{max_rows:,}行のみ表示します。")
        return df.head(max_rows)
    return df

# =========================================================================
# 最終統合・エクスポート関数
# =========================================================================

def export_all_features_config():
    """全機能の設定をエクスポート"""
    config = {
        "version": "1.0",
        "timestamp": datetime.now().isoformat(),
        "features": {
            "analysis_tracking": True,
            "data_quality": DataQualityChecker is not None,
            "error_handling": True,
            "performance_monitoring": True,
            "advanced_visualization": True,
            "accessibility": True
        },
        "settings": st.session_state.get("accessibility_settings", {}),
        "stats": get_current_analysis_stats()
    }
    
    import json
    return json.dumps(config, ensure_ascii=False, indent=2, default=str)

def import_features_config(config_json: str):
    """機能設定のインポート"""
    try:
        config = json.loads(config_json)
        
        # 設定の復元
        if "settings" in config:
            st.session_state.accessibility_settings = config["settings"]
        
        # 統計の復元
        if "stats" in config:
            st.session_state.usage_stats.update(config["stats"])
        
        st.success("✅ 設定がインポートされました")
        return True
        
    except Exception as e:
        st.error(f"❌ インポートに失敗しました: {str(e)}")
        return False