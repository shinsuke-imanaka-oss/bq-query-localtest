# data_quality_ui.py
"""
データ品質チェック UI表示機能
- Streamlit用の品質レポート表示
- 可視化・グラフ表示
- インタラクティブなクリーニング機能
- ユーザーフレンドリーな結果表示
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Optional, Any
import warnings
warnings.filterwarnings('ignore')

# コア機能のインポート
try:
    from data_quality_checker import (
        DataQualityChecker, 
        DataProfiler, 
        DataCleaner, 
        StatisticalAnalyzer,
        quick_quality_check,
        generate_quality_report,
        auto_clean_data
    )
except ImportError:
    st.error("data_quality_checker.py が見つかりません")

# =========================================================================
# メイン品質レポート表示
# =========================================================================

def show_data_quality_report(df: pd.DataFrame, analysis_context: str = ""):
    """Streamlit用データ品質レポート表示"""
    if df.empty:
        st.warning("データが空のため、品質チェックを実行できません。")
        return
    
    checker = DataQualityChecker()
    report = checker.comprehensive_quality_check(df, analysis_context)
    
    # メインメトリクス表示
    show_quality_metrics(report)
    
    # 問題詳細とサマリー
    show_quality_summary(report)
    
    # 詳細分析（展開可能）
    show_detailed_quality_analysis(df, report)

def show_quality_metrics(report: Dict[str, Any]):
    """品質メトリクスの表示"""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        score = report['overall_score']
        delta_color = "normal"
        if score >= 85:
            delta_color = "normal"
        elif score >= 70:
            delta_color = "normal"
        else:
            delta_color = "inverse"
        
        st.metric("品質スコア", f"{score}/100", delta=None)
    
    with col2:
        status_icons = {
            'excellent': '🟢',
            'good': '🟡', 
            'warning': '🟠',
            'critical': '🔴'
        }
        status = report['status']
        st.metric("ステータス", f"{status_icons[status]} {status}")
    
    with col3:
        st.metric("問題数", len(report['issues']))

def show_quality_summary(report: Dict[str, Any]):
    """品質サマリーの表示"""
    # サマリー表示
    if report['status'] == 'excellent':
        st.success(f"✅ {report['summary']}")
    elif report['status'] == 'good':
        st.success(f"✅ {report['summary']}")
    elif report['status'] == 'warning':
        st.warning(f"⚠️ {report['summary']}")
    else:
        st.error(f"🔴 {report['summary']}")
    
    # 問題詳細表示
    if report['issues']:
        st.markdown("### 検出された問題")
        for issue in report['issues']:
            if issue['severity'] == 'critical':
                st.error(f"🔴 **重要**: {issue['message']}")
            elif issue['severity'] == 'warning':
                st.warning(f"🟡 **警告**: {issue['message']}")
            else:
                st.info(f"ℹ️ **情報**: {issue['message']}")
    
    # 改善提案表示
    if report['suggestions']:
        st.markdown("### 💡 改善提案")
        for suggestion in report['suggestions']:
            st.markdown(f"• {suggestion}")

def show_detailed_quality_analysis(df: pd.DataFrame, report: Dict[str, Any]):
    """詳細品質分析の表示"""
    with st.expander("📊 詳細品質分析", expanded=False):
        tab1, tab2, tab3, tab4 = st.tabs(["📋 基本統計", "📈 可視化", "🔧 クリーニング", "📄 プロファイル"])
        
        with tab1:
            show_basic_statistics(df)
        
        with tab2:
            show_quality_visualizations(df)
        
        with tab3:
            show_cleaning_interface(df, report)
        
        with tab4:
            show_data_profile(df)

# =========================================================================
# 基本統計表示
# =========================================================================

def show_basic_statistics(df: pd.DataFrame):
    """基本統計の表示"""
    st.markdown("#### NULL値統計")
    null_stats = []
    for col in df.columns:
        null_count = df[col].isnull().sum()
        null_rate = (null_count / len(df)) * 100
        null_stats.append({
            '列名': col,
            'NULL数': null_count,
            'NULL率': f"{null_rate:.1f}%"
        })
    
    null_df = pd.DataFrame(null_stats)
    st.dataframe(null_df, use_container_width=True)
    
    # 数値列の基本統計
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        st.markdown("#### 数値列の基本統計")
        st.dataframe(df[numeric_cols].describe(), use_container_width=True)
    
    # カテゴリ列の統計
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns
    if len(categorical_cols) > 0:
        st.markdown("#### カテゴリ列の統計")
        for col in categorical_cols[:3]:  # 最初の3列のみ表示
            st.markdown(f"**{col}** の上位値:")
            top_values = df[col].value_counts().head(5)
            for value, count in top_values.items():
                st.caption(f"  {value}: {count}回 ({count/len(df)*100:.1f}%)")

# =========================================================================
# 品質可視化機能
# =========================================================================

def show_quality_visualizations(df: pd.DataFrame):
    """品質可視化の表示"""
    st.markdown("#### データ品質の可視化")
    
    viz_option = st.selectbox(
        "表示する可視化を選択",
        ["NULL値分布", "外れ値分析", "データ型分布", "相関ヒートマップ", "分布比較"]
    )
    
    if viz_option == "NULL値分布":
        show_null_distribution(df)
    elif viz_option == "外れ値分析":
        show_outlier_analysis(df)
    elif viz_option == "データ型分布":
        show_data_type_distribution(df)
    elif viz_option == "相関ヒートマップ":
        show_correlation_heatmap(df)
    elif viz_option == "分布比較":
        show_distribution_comparison(df)

def show_null_distribution(df: pd.DataFrame):
    """NULL値分布の可視化"""
    null_rates = (df.isnull().sum() / len(df)) * 100
    null_rates = null_rates[null_rates > 0].sort_values(ascending=True)
    
    if len(null_rates) > 0:
        fig = px.bar(
            x=null_rates.values,
            y=null_rates.index,
            orientation='h',
            title="列別NULL値率 (%)",
            labels={'x': 'NULL値率 (%)', 'y': '列名'}
        )
        
        # 警告線の追加
        fig.add_vline(x=10, line_dash="dash", line_color="orange", 
                      annotation_text="警告閾値 (10%)")
        fig.add_vline(x=30, line_dash="dash", line_color="red", 
                      annotation_text="重要警告閾値 (30%)")
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.success("✅ NULL値は検出されませんでした！")

def show_outlier_analysis(df: pd.DataFrame):
    """外れ値分析の可視化"""
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    if len(numeric_cols) > 0:
        selected_col = st.selectbox("外れ値チェック対象列", numeric_cols)
        
        if selected_col:
            col1, col2 = st.columns(2)
            
            with col1:
                # 箱ひげ図
                fig_box = px.box(df, y=selected_col, title=f"{selected_col} の分布と外れ値")
                st.plotly_chart(fig_box, use_container_width=True)
            
            with col2:
                # ヒストグラム
                fig_hist = px.histogram(df, x=selected_col, title=f"{selected_col} のヒストグラム")
                st.plotly_chart(fig_hist, use_container_width=True)
            
            # 外れ値統計
            Q1 = df[selected_col].quantile(0.25)
            Q3 = df[selected_col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = df[(df[selected_col] < lower_bound) | (df[selected_col] > upper_bound)]
            
            st.info(f"📊 外れ値統計: {len(outliers)}行 ({len(outliers)/len(df)*100:.1f}%)")
    else:
        st.info("数値列がないため、外れ値分析を実行できません。")

def show_data_type_distribution(df: pd.DataFrame):
    """データ型分布の可視化"""
    type_counts = df.dtypes.value_counts()
    
    # データ型の日本語名マッピング
    type_mapping = {
        'object': 'テキスト',
        'int64': '整数',
        'float64': '小数',
        'datetime64[ns]': '日時',
        'bool': '真偽値',
        'category': 'カテゴリ'
    }
    
    # データ型名を日本語に変換
    type_counts.index = [type_mapping.get(str(dtype), str(dtype)) for dtype in type_counts.index]
    
    fig = px.pie(
        values=type_counts.values,
        names=type_counts.index,
        title="データ型別列数の分布"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # 詳細テーブル
    st.markdown("#### データ型詳細")
    type_detail = []
    for dtype in df.dtypes.unique():
        cols = df.select_dtypes(include=[dtype]).columns.tolist()
        type_detail.append({
            'データ型': type_mapping.get(str(dtype), str(dtype)),
            '列数': len(cols),
            '列名例': ', '.join(cols[:3]) + ('...' if len(cols) > 3 else '')
        })
    
    st.dataframe(pd.DataFrame(type_detail), use_container_width=True)

def show_correlation_heatmap(df: pd.DataFrame):
    """相関ヒートマップの表示"""
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    if len(numeric_cols) >= 2:
        corr_matrix = df[numeric_cols].corr()
        
        fig = px.imshow(
            corr_matrix,
            title="数値列間の相関ヒートマップ",
            color_continuous_scale="RdBu",
            aspect="auto",
            text_auto=True
        )
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
        
        # 高い相関ペアの抽出
        high_corr_pairs = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr_value = corr_matrix.iloc[i, j]
                if abs(corr_value) > 0.7:
                    high_corr_pairs.append({
                        '列1': corr_matrix.columns[i],
                        '列2': corr_matrix.columns[j],
                        '相関係数': f"{corr_value:.3f}"
                    })
        
        if high_corr_pairs:
            st.markdown("#### 🔗 高い相関を持つ列ペア (|r| > 0.7)")
            st.dataframe(pd.DataFrame(high_corr_pairs), use_container_width=True)
        else:
            st.info("高い相関（|r| > 0.7）を持つ列ペアは見つかりませんでした。")
    else:
        st.info("相関分析には数値列が2列以上必要です。")

def show_distribution_comparison(df: pd.DataFrame):
    """分布比較の表示"""
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    if len(numeric_cols) >= 2:
        col1, col2 = st.columns(2)
        
        with col1:
            x_col = st.selectbox("X軸（列1）", numeric_cols)
        with col2:
            y_col = st.selectbox("Y軸（列2）", numeric_cols, index=1)
        
        if x_col != y_col:
            # 散布図
            fig_scatter = px.scatter(
                df, x=x_col, y=y_col,
                title=f"{x_col} vs {y_col}",
                trendline="ols"
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
            
            # 統計情報
            correlation = df[x_col].corr(df[y_col])
            st.info(f"📊 相関係数: {correlation:.3f}")
    else:
        st.info("分布比較には数値列が2列以上必要です。")

# =========================================================================
# クリーニングインターフェース
# =========================================================================

def show_cleaning_interface(df: pd.DataFrame, quality_report: Dict[str, Any]):
    """データクリーニングインターフェースの表示"""
    st.markdown("#### 🧹 データクリーニング")
    
    if not quality_report.get('issues'):
        st.success("✅ データクリーニングの必要はありません。")
        return
    
    # クリーニングオプションの選択
    cleaning_options = st.multiselect(
        "適用するクリーニング処理を選択:",
        [
            "NULL値の多い列を除去 (30%以上)",
            "完全重複行を除去",
            "数値列の外れ値を除去 (IQR基準)",
            "定数列を除去"
        ]
    )
    
    # プレビュー機能
    if cleaning_options:
        show_cleaning_preview(df, cleaning_options)
    
    # クリーニング実行
    if st.button("🚀 選択したクリーニングを実行", disabled=len(cleaning_options) == 0):
        execute_data_cleaning(df, cleaning_options)

def show_cleaning_preview(df: pd.DataFrame, cleaning_options: List[str]):
    """クリーニングプレビューの表示"""
    st.markdown("#### 👁️ クリーニング前後のプレビュー")
    
    # 仮想クリーニングの実行（元データは変更しない）
    try:
        cleaner = DataCleaner()
        preview_df = cleaner.clean_data(df.copy(), cleaning_options)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("行数", f"{len(df):,}", delta=f"{len(preview_df) - len(df):,}")
        
        with col2:
            st.metric("列数", len(df.columns), delta=len(preview_df.columns) - len(df.columns))
        
        with col3:
            memory_before = df.memory_usage(deep=True).sum() / 1024 / 1024
            memory_after = preview_df.memory_usage(deep=True).sum() / 1024 / 1024
            st.metric("メモリ使用量(MB)", f"{memory_before:.1f}", delta=f"{memory_after - memory_before:.1f}")
        
        # クリーニング効果の詳細
        show_cleaning_effects(df, preview_df)
        
    except Exception as e:
        st.error(f"プレビューの生成に失敗しました: {str(e)}")

def show_cleaning_effects(original_df: pd.DataFrame, cleaned_df: pd.DataFrame):
    """クリーニング効果の詳細表示"""
    st.markdown("#### 📈 クリーニング効果")
    
    effects = []
    
    # 行数の変化
    row_change = len(cleaned_df) - len(original_df)
    if row_change != 0:
        effects.append(f"行数: {row_change:+,}行 ({row_change/len(original_df)*100:+.1f}%)")
    
    # 列数の変化
    col_change = len(cleaned_df.columns) - len(original_df.columns)
    if col_change != 0:
        effects.append(f"列数: {col_change:+}列")
    
    # NULL値の変化
    null_before = original_df.isnull().sum().sum()
    null_after = cleaned_df.isnull().sum().sum()
    null_change = null_after - null_before
    if null_change != 0:
        effects.append(f"NULL値: {null_change:+,}個")
    
    # 重複行の変化
    dup_before = original_df.duplicated().sum()
    dup_after = cleaned_df.duplicated().sum()
    dup_change = dup_after - dup_before
    if dup_change != 0:
        effects.append(f"重複行: {dup_change:+,}行")
    
    # 効果の表示
    for effect in effects:
        st.info(f"• {effect}")
    
    if not effects:
        st.info("変更はありません。")

def execute_data_cleaning(df: pd.DataFrame, cleaning_options: List[str]):
    """データクリーニングの実行"""
    try:
        cleaner = DataCleaner()
        cleaned_df = cleaner.clean_data(df, cleaning_options)
        
        # クリーニング後のデータをセッションに保存
        st.session_state.cleaned_df = cleaned_df
        
        # 結果の表示
        st.success(f"🎉 クリーニング完了！ {len(df):,}行 → {len(cleaned_df):,}行")
        
        # クリーニング後の品質チェック
        show_post_cleaning_quality_check(cleaned_df)
        
        # ダウンロードボタン
        csv = cleaned_df.to_csv(index=False)
        st.download_button(
            label="📥 クリーニング後データをダウンロード",
            data=csv,
            file_name=f"cleaned_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
        
    except Exception as e:
        st.error(f"クリーニングに失敗しました: {str(e)}")

def show_post_cleaning_quality_check(cleaned_df: pd.DataFrame):
    """クリーニング後の品質チェック"""
    with st.expander("📊 クリーニング後の品質チェック", expanded=False):
        post_report = quick_quality_check(cleaned_df)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("品質スコア", f"{post_report['overall_score']}/100")
        with col2:
            status_icons = {"excellent": "🟢", "good": "🟡", "warning": "🟠", "critical": "🔴"}
            status = post_report['status']
            st.write(f"**ステータス**: {status_icons[status]} {status}")
        with col3:
            st.metric("残存問題", len(post_report['issues']))
        
        if post_report['issues']:
            st.markdown("**残存する問題:**")
            for issue in post_report['issues']:
                st.caption(f"• {issue['message']}")

# =========================================================================
# データプロファイル表示
# =========================================================================

def show_data_profile(df: pd.DataFrame):
    """データプロファイルの表示"""
    st.markdown("#### 📋 データプロファイル")
    
    profiler = DataProfiler()
    profile = profiler.generate_profile_report(df)
    
    # 基本情報
    basic = profile['basic_info']
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("総行数", f"{basic['row_count']:,}")
    with col2:
        st.metric("総列数", basic['column_count'])
    with col3:
        st.metric("メモリ使用量", f"{basic['memory_usage_mb']:.1f} MB")
    with col4:
        st.metric("数値列数", basic['numeric_columns'])
    
    # 列の詳細プロファイル
    show_column_profiles(profile['column_profiles'])
    
    # パターン検出結果
    show_data_patterns(profile['data_patterns'])

def show_column_profiles(column_profiles: Dict[str, Any]):
    """列プロファイルの表示"""
    with st.expander("📊 列別詳細プロファイル", expanded=False):
        profile_data = []
        
        for col_name, col_profile in column_profiles.items():
            profile_data.append({
                '列名': col_name,
                'データ型': col_profile['dtype'],
                'NULL率': f"{col_profile['null_rate']:.1f}%",
                'ユニーク数': col_profile['unique_count'],
                'ユニーク率': f"{col_profile['unique_rate']:.1f}%"
            })
        
        profile_df = pd.DataFrame(profile_data)
        st.dataframe(profile_df, use_container_width=True)
        
        # 詳細表示用の列選択
        selected_column = st.selectbox("詳細表示する列", list(column_profiles.keys()))
        
        if selected_column:
            show_individual_column_profile(selected_column, column_profiles[selected_column])

def show_individual_column_profile(col_name: str, col_profile: Dict[str, Any]):
    """個別列プロファイルの表示"""
    st.markdown(f"#### {col_name} の詳細プロファイル")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("NULL率", f"{col_profile['null_rate']:.1f}%")
    with col2:
        st.metric("ユニーク数", col_profile['unique_count'])
    with col3:
        st.metric("ユニーク率", f"{col_profile['unique_rate']:.1f}%")
    
    # 数値列の場合の統計情報
    if 'mean' in col_profile:
        st.markdown("**統計情報**")
        stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
        with stat_col1:
            st.metric("最小値", f"{col_profile['min']:,.2f}")
        with stat_col2:
            st.metric("最大値", f"{col_profile['max']:,.2f}")
        with stat_col3:
            st.metric("平均値", f"{col_profile['mean']:,.2f}")
        with stat_col4:
            st.metric("標準偏差", f"{col_profile['std']:,.2f}")
    
    # カテゴリ列の場合の上位値
    if 'top_values' in col_profile:
        st.markdown("**上位値**")
        for value, count in list(col_profile['top_values'].items())[:5]:
            percentage = (count / col_profile.get('total_count', count)) * 100
            st.caption(f"{value}: {count}回 ({percentage:.1f}%)")

def show_data_patterns(patterns: Dict[str, Any]):
    """データパターンの表示"""
    with st.expander("🔍 データパターン検出", expanded=False):
        # 定数列
        if patterns['constant_columns']:
            st.warning(f"⚠️ **定数列を検出**: {', '.join(patterns['constant_columns'])}")
            st.caption("これらの列は常に同じ値を持っているため、分析には有用でない可能性があります。")
        
        # 高カーディナリティ列
        if patterns['high_cardinality_columns']:
            st.warning(f"⚠️ **高カーディナリティ列を検出**: {', '.join(patterns['high_cardinality_columns'])}")
            st.caption("これらの列はユニーク値が多すぎるため、カテゴリ変数として扱うのに適していない可能性があります。")
        
        # 疑わしいパターン
        if patterns['suspicious_columns']:
            st.warning("⚠️ **疑わしいパターンを検出**:")
            for suspicious in patterns['suspicious_columns']:
                st.caption(f"• {suspicious['column']}: {suspicious['pattern']}")
        
        # パターンが見つからない場合
        if not any([patterns['constant_columns'], patterns['high_cardinality_columns'], patterns['suspicious_columns']]):
            st.success("✅ 特別な注意が必要なパターンは検出されませんでした。")

# =========================================================================
# 品質改善提案表示
# =========================================================================

def show_quality_improvement_suggestions(df: pd.DataFrame, quality_report: Dict[str, Any]):
    """品質改善提案の表示"""
    st.markdown("#### 💡 品質改善提案")
    
    suggestions = quality_report.get('suggestions', [])
    
    if not suggestions:
        st.success("✅ 現在のデータ品質に特別な改善提案はありません。")
        return
    
    # 提案の分類
    immediate_actions = []
    optional_improvements = []
    
    for suggestion in suggestions:
        if any(keyword in suggestion for keyword in ['除外', 'NULL値', '重複', '確認']):
            immediate_actions.append(suggestion)
        else:
            optional_improvements.append(suggestion)
    
    # 緊急度の高い提案
    if immediate_actions:
        st.markdown("**🚨 優先度高：即座に対応を推奨**")
        for action in immediate_actions:
            st.error(f"🔴 {action}")
    
    # オプション的な改善提案
    if optional_improvements:
        st.markdown("**💡 改善提案：余裕があるときに検討**")
        for improvement in optional_improvements:
            st.info(f"ℹ️ {improvement}")

# =========================================================================
# 比較表示機能
# =========================================================================

def show_data_comparison(df1: pd.DataFrame, df2: pd.DataFrame, label1: str = "データ1", label2: str = "データ2"):
    """2つのデータフレームの比較表示"""
    st.markdown(f"#### 🔄 データ比較: {label1} vs {label2}")
    
    # 基本統計の比較
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"**{label1}**")
        report1 = quick_quality_check(df1)
        st.metric("品質スコア", f"{report1['overall_score']}/100")
        st.metric("行数", len(df1))
        st.metric("列数", len(df1.columns))
        st.metric("問題数", len(report1['issues']))
    
    with col2:
        st.markdown(f"**{label2}**")
        report2 = quick_quality_check(df2)
        st.metric("品質スコア", f"{report2['overall_score']}/100", 
                 delta=report2['overall_score'] - report1['overall_score'])
        st.metric("行数", len(df2), delta=len(df2) - len(df1))
        st.metric("列数", len(df2.columns), delta=len(df2.columns) - len(df1.columns))
        st.metric("問題数", len(report2['issues']), delta=len(report2['issues']) - len(report1['issues']))
    
    # 改善効果の可視化
    if report2['overall_score'] != report1['overall_score']:
        show_improvement_visualization(report1, report2, label1, label2)

def show_improvement_visualization(report1: Dict, report2: Dict, label1: str, label2: str):
    """改善効果の可視化"""
    scores = [report1['overall_score'], report2['overall_score']]
    labels = [label1, label2]
    
    fig = px.bar(
        x=labels,
        y=scores,
        title="データ品質スコア比較",
        color=scores,
        color_continuous_scale="RdYlGn"
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

# =========================================================================
# 外部呼び出し用の統合関数
# =========================================================================

def show_comprehensive_quality_analysis(df: pd.DataFrame, context: str = ""):
    """包括的な品質分析の表示（外部から呼び出し用）"""
    if df.empty:
        st.warning("データが空のため、品質分析を実行できません。")
        return
    
    # メイン品質レポート
    show_data_quality_report(df, context)
    
    # 品質改善提案
    quality_report = quick_quality_check(df)
    show_quality_improvement_suggestions(df, quality_report)

def show_quality_dashboard(df: pd.DataFrame):
    """品質ダッシュボードの表示"""
    st.markdown("## 📊 データ品質ダッシュボード")
    
    if df.empty:
        st.error("データが空です。")
        return
    
    # サマリーメトリクス
    quality_report = quick_quality_check(df)
    
    # 4列レイアウトでメトリクス表示
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("品質スコア", f"{quality_report['overall_score']}/100")
    
    with col2:
        st.metric("データ行数", f"{len(df):,}")
    
    with col3:
        null_rate = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
        st.metric("NULL値率", f"{null_rate:.1f}%")
    
    with col4:
        duplicate_rate = (df.duplicated().sum() / len(df)) * 100
        st.metric("重複率", f"{duplicate_rate:.1f}%")
    
    # 詳細分析
    show_detailed_quality_analysis(df, quality_report)