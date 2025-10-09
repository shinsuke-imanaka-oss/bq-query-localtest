"""
ui_summary.py
週次・月次サマリーレポート用UIモジュール

機能:
- 期間選択UI（プリセット + カスタム）
- 目標設定UI
- レポート生成ボタン
- サイドバー統合
"""

import streamlit as st
from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict, Any
import calendar


class SummaryUI:
    """サマリーレポートUIクラス"""
    
    def __init__(self):
        """初期化"""
        self.period_presets = {
            "今週": "current_week",
            "先週": "last_week",
            "今月": "current_month",
            "先月": "last_month",
            "今四半期": "current_quarter",
            "先四半期": "last_quarter",
            "カスタム": "custom"
        }
        
        self.comparison_modes = {
            "比較なし": "none",
            "WoW（週次比較）": "wow",
            "MoM（月次比較）": "mom",
            "YoY（年次比較）": "yoy",
            "カスタム比較": "custom"
        }
    
    def render_sidebar(self) -> Dict[str, Any]:
        """
        サイドバーUIをレンダリング
        
        Returns:
            ユーザー選択内容
        """
        st.sidebar.markdown("---")
        st.sidebar.header("📊 週次・月次サマリー生成")
        
        # 期間選択
        period_config = self._render_period_selection()
        
        # 比較設定
        comparison_config = self._render_comparison_settings()
        
        # 比較分析の軸
        analysis_axis = self._render_analysis_axis()
        
        # 目標設定セクション
        targets_config = self._render_targets_section()
        
        # レポート生成ボタン
        generate_button = st.sidebar.button(
            "🚀 レポート生成",
            type="primary",
            use_container_width=True
        )
        
        return {
            "period": period_config,
            "comparison": comparison_config,
            "analysis_axis": analysis_axis,
            "targets": targets_config,
            "generate": generate_button
        }
    
    def _render_period_selection(self) -> Dict[str, Any]:
        """期間選択UIをレンダリング"""
        st.sidebar.subheader("📅 期間選択")
        
        # 期間選択モードの切替
        selection_mode = st.sidebar.radio(
            "期間の指定方法",
            ["プリセット期間", "カスタム期間"],
            key="period_selection_mode",
            horizontal=True
        )
        
        selected_preset = None
        start_date = None
        end_date = None
        
        if selection_mode == "プリセット期間":
            # プリセット選択
            preset_option = st.sidebar.selectbox(
                "期間を選択",
                ["今週", "先週", "今月", "先月", "今四半期", "先四半期"],
                key="preset_selector"
            )
            
            # プリセットをコードにマッピング
            preset_mapping = {
                "今週": "current_week",
                "先週": "last_week",
                "今月": "current_month",
                "先月": "last_month",
                "今四半期": "current_quarter",
                "先四半期": "last_quarter"
            }
            
            selected_preset = preset_mapping[preset_option]
            start_date, end_date = self._get_preset_dates(selected_preset)
            
            # 選択内容を視覚的に表示
            st.sidebar.success(f"✅ **{preset_option}** を選択中")
            st.sidebar.info(f"📆 {start_date.strftime('%Y/%m/%d')} - {end_date.strftime('%Y/%m/%d')}")
        
        else:
            # カスタム期間
            col1, col2 = st.sidebar.columns(2)
            
            with col1:
                start_date = st.date_input(
                    "開始日",
                    value=datetime.now().replace(day=1).date(),
                    key="summary_start_date"
                )
            
            with col2:
                end_date = st.date_input(
                    "終了日",
                    value=datetime.now().date(),
                    key="summary_end_date"
                )
            
            # 選択内容を視覚的に表示
            if start_date and end_date:
                days_diff = (end_date - start_date).days + 1
                st.sidebar.success(f"✅ カスタム期間を設定中")
                st.sidebar.info(f"📆 {start_date.strftime('%Y/%m/%d')} - {end_date.strftime('%Y/%m/%d')}\n({days_diff}日間)")
        
        return {
            "preset": selected_preset,
            "start_date": start_date,
            "end_date": end_date
        }
    
    def _render_comparison_settings(self) -> Dict[str, Any]:
        """比較設定UIをレンダリング"""
        st.sidebar.subheader("📊 比較設定")
        
        enable_comparison = st.sidebar.checkbox(
            "前期間と比較する",
            value=True,
            key="enable_comparison"
        )
        
        comparison_mode = None
        if enable_comparison:
            comparison_mode = st.sidebar.selectbox(
                "比較モード",
                options=list(self.comparison_modes.keys()),
                key="comparison_mode"
            )
        
        return {
            "enabled": enable_comparison,
            "mode": self.comparison_modes.get(comparison_mode, "none") if comparison_mode else "none"
        }
    
    def _render_analysis_axis(self) -> str:
        """比較分析の軸選択UIをレンダリング"""
        st.sidebar.subheader("🔍 比較分析の軸")
        
        axis = st.sidebar.radio(
            "比較方法を選択",
            options=[
                "全キャンペーン（媒体横断）",
                "媒体別",
                "両方"
            ],
            index=0,
            key="analysis_axis"
        )
        
        axis_mapping = {
            "全キャンペーン（媒体横断）": "all",
            "媒体別": "by_media",
            "両方": "both"
        }
        
        return axis_mapping[axis]
    
    def _render_targets_section(self) -> Optional[str]:
        """目標設定セクションをレンダリング"""
        st.sidebar.subheader("💰 目標・予算設定")
        
        # 現在の設定状態を表示（仮）
        current_month = datetime.now().strftime("%Y年%m月")
        st.sidebar.info(f"📅 対象期間: {current_month}")
        
        # 設定編集ボタン
        if st.sidebar.button("⚙️ 目標を設定・編集", use_container_width=True):
            return "open_targets_modal"
        
        return None
    
    def render_targets_modal(self, targets_manager) -> bool:
        """
        目標設定モーダルをレンダリング
        
        Args:
            targets_manager: TargetsManagerインスタンス
        
        Returns:
            保存が実行された場合True
        """
        st.subheader("⚙️ 目標・予算設定")
        
        # 対象年月の選択
        col1, col2 = st.columns(2)
        with col1:
            year = st.selectbox(
                "年",
                options=range(2024, 2027),
                index=1,  # 2025
                key="target_year"
            )
        with col2:
            month = st.selectbox(
                "月",
                options=range(1, 13),
                index=datetime.now().month - 1,
                key="target_month"
            )
        
        year_month = f"{year}-{month:02d}"
        
        # 既存の目標を取得
        existing_targets = targets_manager.get_targets(year_month)
        
        st.markdown("---")
        
        # 入力フォーム
        col1, col2 = st.columns(2)
        
        with col1:
            budget = st.number_input(
                "月間予算（円）",
                min_value=0,
                value=int(existing_targets.get("budget", 0)) if existing_targets and existing_targets.get("budget") else 0,
                step=10000,
                key="target_budget"
            )
            
            target_cpa = st.number_input(
                "目標CPA（円）",
                min_value=0,
                value=int(existing_targets.get("target_cpa", 0)) if existing_targets and existing_targets.get("target_cpa") else 0,
                step=100,
                key="target_cpa"
            )
            
            target_cvr = st.number_input(
                "目標CVR（%）",
                min_value=0.0,
                max_value=100.0,
                value=float(existing_targets.get("target_cvr", 0) * 100) if existing_targets and existing_targets.get("target_cvr") else 0.0,
                step=0.1,
                format="%.2f",
                key="target_cvr"
            )
        
        with col2:
            target_conversions = st.number_input(
                "目標CV数（件）",
                min_value=0,
                value=int(existing_targets.get("target_conversions", 0)) if existing_targets and existing_targets.get("target_conversions") else 0,
                step=10,
                key="target_conversions"
            )
            
            target_ctr = st.number_input(
                "目標CTR（%）",
                min_value=0.0,
                max_value=100.0,
                value=float(existing_targets.get("target_ctr", 0) * 100) if existing_targets and existing_targets.get("target_ctr") else 0.0,
                step=0.1,
                format="%.2f",
                key="target_ctr"
            )
            
            notes = st.text_area(
                "メモ",
                value=existing_targets.get("notes", "") if existing_targets else "",
                key="target_notes"
            )
        
        st.markdown("---")
        
        # 保存ボタン
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col2:
            if st.button("💾 保存", type="primary", use_container_width=True):
                # 目標を保存
                success = targets_manager.set_targets(
                    year_month=year_month,
                    budget=budget if budget > 0 else None,
                    target_conversions=target_conversions if target_conversions > 0 else None,
                    target_cpa=target_cpa if target_cpa > 0 else None,
                    target_cvr=target_cvr / 100 if target_cvr > 0 else None,
                    target_ctr=target_ctr / 100 if target_ctr > 0 else None,
                    notes=notes
                )
                
                if success:
                    st.success(f"✅ {year_month}の目標を保存しました")
                    return True
                else:
                    st.error("❌ 保存に失敗しました")
        
        return False
    
    def _get_preset_dates(self, preset: str) -> Tuple[datetime, datetime]:
        """
        プリセットから日付範囲を取得
        
        Args:
            preset: プリセット種別
        
        Returns:
            (開始日, 終了日)のタプル
        """
        today = datetime.now()
        
        if preset == "current_week":
            # 今週（月曜〜日曜）
            start = today - timedelta(days=today.weekday())
            end = start + timedelta(days=6)
        
        elif preset == "last_week":
            # 先週
            start = today - timedelta(days=today.weekday() + 7)
            end = start + timedelta(days=6)
        
        elif preset == "current_month":
            # 今月
            start = today.replace(day=1)
            end = today
        
        elif preset == "last_month":
            # 先月
            first_day_this_month = today.replace(day=1)
            end = first_day_this_month - timedelta(days=1)
            start = end.replace(day=1)
        
        elif preset == "current_quarter":
            # 今四半期
            quarter_month = ((today.month - 1) // 3) * 3 + 1
            start = today.replace(month=quarter_month, day=1)
            end = today
        
        elif preset == "last_quarter":
            # 先四半期
            current_quarter_month = ((today.month - 1) // 3) * 3 + 1
            if current_quarter_month == 1:
                start = today.replace(year=today.year - 1, month=10, day=1)
                end = today.replace(year=today.year - 1, month=12, day=31)
            else:
                start = today.replace(month=current_quarter_month - 3, day=1)
                last_month = current_quarter_month - 1
                last_day = calendar.monthrange(today.year, last_month)[1]
                end = today.replace(month=last_month, day=last_day)
        
        else:
            # デフォルト: 今月
            start = today.replace(day=1)
            end = today
        
        return start.date(), end.date()
    
    def display_targets_summary(self, targets: Optional[Dict[str, Any]], year_month: str):
        """
        目標設定サマリーを表示
        
        Args:
            targets: 目標データ
            year_month: 対象年月
        """
        if not targets or not any([
            targets.get("budget"),
            targets.get("target_conversions"),
            targets.get("target_cpa"),
            targets.get("target_cvr"),
            targets.get("target_ctr")
        ]):
            st.sidebar.warning(f"⚠️ {year_month}の目標が未設定です")
            return
        
        st.sidebar.success(f"✅ {year_month}の目標設定済み")
        
        with st.sidebar.expander("📊 設定内容を表示"):
            if targets.get("budget"):
                st.write(f"予算: ¥{targets['budget']:,.0f}")
            if targets.get("target_conversions"):
                st.write(f"目標CV数: {targets['target_conversions']:,}件")
            if targets.get("target_cpa"):
                st.write(f"目標CPA: ¥{targets['target_cpa']:,.0f}")
            if targets.get("target_cvr"):
                st.write(f"目標CVR: {targets['target_cvr']:.2%}")
            if targets.get("target_ctr"):
                st.write(f"目標CTR: {targets['target_ctr']:.2%}")


# 使用例・テスト用コード（Streamlitアプリとして実行）
if __name__ == "__main__":
    st.set_page_config(page_title="サマリーUI テスト", layout="wide")
    
    st.title("📊 週次・月次サマリーUI テスト")
    
    # UIインスタンス作成
    ui = SummaryUI()
    
    # サイドバーレンダリング
    config = ui.render_sidebar()
    
    # メインエリアに選択内容を表示
    st.subheader("選択内容")
    st.json(config)
    
    # 目標設定モーダル（テスト用）
    if config["targets"] == "open_targets_modal":
        from targets_manager import TargetsManager
        manager = TargetsManager()
        ui.render_targets_modal(manager)