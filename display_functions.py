"""
display_functions.py
Streamlit表示関数の拡張（Phase 3対応）
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Any  # ← この行を追加
from comparative_analyzer import ComparativeAnalysis
from action_recommender import ActionRecommendations, Priority


def display_summary_report(report: Dict[str, Any]):
    """サマリーレポートを表示（Phase 3対応）"""
    
    # 既存のセクション1-5の表示コード...
    # display_section_1(report['section_1_overview'])
    # display_section_2(report['section_2_budget'])
    # display_section_3(report['section_3_kpi'])
    # display_section_4(report['section_4_highlights'])
    # display_section_5(report['section_5_summary'])
    
    # セクション6: パフォーマンス比較分析
    st.header("📊 セクション6: パフォーマンス比較分析")
    display_comparative_analysis(report['section_6_comparative_analysis'])
    
    # セクション7: アクション提案
    st.header("🎯 セクション7: 推奨アクション")
    display_action_recommendations(report['section_7_action_recommendations'])


def display_comparative_analysis(analysis: ComparativeAnalysis):
    """パフォーマンス比較分析を表示"""
    if analysis.skipped:
        st.warning(f"⚠️ {analysis.skip_reason}")
        return
    
    # サマリー
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            "高パフォーマンス", 
            f"{len(analysis.high_performers)}キャンペーン",
            f"平均CPA: ¥{analysis.analysis_summary['high_perf_avg_cpa']:,.0f}"
        )
    with col2:
        st.metric(
            "低パフォーマンス", 
            f"{len(analysis.low_performers)}キャンペーン",
            f"平均CPA: ¥{analysis.analysis_summary['low_perf_avg_cpa']:,.0f}"
        )
    with col3:
        st.metric(
            "有意な差異", 
            f"{len(analysis.significant_differences)}項目",
            "20%以上の差"
        )
    
    st.divider()
    
    # 有意な差異の詳細表示
    if analysis.significant_differences:
        st.subheader("📈 有意な差異（20%以上）")
        
        # データフレーム形式で表示
        diff_data = []
        for diff in analysis.significant_differences:
            diff_data.append({
                '指標': diff.metric_name,
                '高パフォーマンス': f"{diff.high_perf_avg:,.2f}",
                '低パフォーマンス': f"{diff.low_perf_avg:,.2f}",
                '差異': f"{diff.difference_pct:+.1f}%"
            })
        
        df = pd.DataFrame(diff_data)
        st.dataframe(df, use_container_width=True, hide_index=True)
        
        # 差異の可視化
        st.subheader("📊 差異の可視化")
        
        fig = go.Figure()
        
        metric_names = [diff.metric_name for diff in analysis.significant_differences]
        differences = [diff.difference_pct for diff in analysis.significant_differences]
        
        # 色分け（正の差異 vs 負の差異）
        colors = ['green' if d > 0 else 'red' for d in differences]
        
        fig.add_trace(go.Bar(
            x=differences,
            y=metric_names,
            orientation='h',
            marker=dict(color=colors),
            text=[f"{d:+.1f}%" for d in differences],
            textposition='outside'
        ))
        
        fig.update_layout(
            title="高パフォーマンス vs 低パフォーマンスの差異",
            xaxis_title="差異（%）",
            yaxis_title="指標",
            height=400,
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.info("20%以上の有意な差異は検出されませんでした。")
    
    # AI分析結果
    if hasattr(analysis, 'ai_insights') and analysis.ai_insights:
        st.divider()
        st.subheader("🤖 AI分析: 差異要因の解説")
        st.markdown(analysis.ai_insights)
    
    # 詳細データ（エクスパンダー内）
    with st.expander("🔍 高パフォーマンスキャンペーン詳細"):
        high_perf_data = []
        for camp in analysis.high_performers[:10]:  # 上位10件
            high_perf_data.append({
                'キャンペーン名': camp.campaign_name,
                'CPA': f"¥{camp.cpa:,.0f}",
                'ROAS': f"{camp.roas:.2f}",
                'CVR': f"{camp.conversion_rate:.2%}",
                'CTR': f"{camp.click_rate:.2%}",
                'コスト': f"¥{camp.cost:,.0f}"
            })
        
        df_high = pd.DataFrame(high_perf_data)
        st.dataframe(df_high, use_container_width=True, hide_index=True)
    
    with st.expander("🔍 低パフォーマンスキャンペーン詳細"):
        low_perf_data = []
        for camp in analysis.low_performers[:10]:  # 下位10件
            low_perf_data.append({
                'キャンペーン名': camp.campaign_name,
                'CPA': f"¥{camp.cpa:,.0f}",
                'ROAS': f"{camp.roas:.2f}",
                'CVR': f"{camp.conversion_rate:.2%}",
                'CTR': f"{camp.click_rate:.2%}",
                'コスト': f"¥{camp.cost:,.0f}"
            })
        
        df_low = pd.DataFrame(low_perf_data)
        st.dataframe(df_low, use_container_width=True, hide_index=True)


def display_action_recommendations(recommendations: ActionRecommendations):
    """アクション提案を表示"""
    if not recommendations.actions:
        st.warning("⚠️ " + recommendations.summary)
        return
    
    # サマリー
    st.markdown(recommendations.summary)
    
    st.divider()
    
    # 優先度別カウント
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("高優先度", recommendations.high_priority_count, "即座に実施")
    with col2:
        st.metric("中優先度", recommendations.medium_priority_count, "2週間以内")
    with col3:
        st.metric("低優先度", recommendations.low_priority_count, "状況に応じて")
    
    st.divider()
    
    # アクション項目の表示
    priority_icons = {
        Priority.HIGH: "🔴",
        Priority.MEDIUM: "🟡",
        Priority.LOW: "🟢"
    }
    
    for i, action in enumerate(recommendations.actions, 1):
        icon = priority_icons.get(action.priority, "⚪")
        
        with st.expander(
            f"{icon} {action.title} (優先度: {action.priority.value})",
            expanded=(i <= 3)  # 上位3件は展開表示
        ):
            st.markdown(f"**カテゴリ:** {action.category}")
            st.markdown(f"**内容:**")
            st.write(action.description)
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**期待効果:**")
                st.info(action.expected_impact)
            with col2:
                st.markdown("**検証方法:**")
                st.success(action.validation_method)
    
    # AI分析結果
    if hasattr(recommendations, 'ai_insights') and recommendations.ai_insights:
        st.divider()
        st.subheader("🤖 AI分析: 実施ガイダンス")
        st.markdown(recommendations.ai_insights)
    
    # アクション実行チェックリスト（ダウンロード可能）
    st.divider()
    st.subheader("📋 アクション実行チェックリスト")
    
    checklist_md = "# アクション実行チェックリスト\n\n"
    checklist_md += f"生成日時: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}\n\n"
    
    for priority in [Priority.HIGH, Priority.MEDIUM, Priority.LOW]:
        priority_actions = [a for a in recommendations.actions if a.priority == priority]
        if priority_actions:
            checklist_md += f"\n## {priority.value}優先度\n\n"
            for action in priority_actions:
                checklist_md += f"### ☐ {action.title}\n\n"
                checklist_md += f"- **内容:** {action.description}\n"
                checklist_md += f"- **期待効果:** {action.expected_impact}\n"
                checklist_md += f"- **検証方法:** {action.validation_method}\n"
                checklist_md += f"- **カテゴリ:** {action.category}\n"
                checklist_md += f"- **実施予定日:** ____________\n"
                checklist_md += f"- **担当者:** ____________\n"
                checklist_md += f"- **完了日:** ____________\n\n"
    
    st.download_button(
        label="📥 チェックリストをダウンロード",
        data=checklist_md,
        file_name=f"action_checklist_{pd.Timestamp.now().strftime('%Y%m%d')}.md",
        mime="text/markdown"
    )