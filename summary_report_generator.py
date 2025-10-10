"""
summary_report_generator.py
週次・月次サマリーレポート生成モジュール

機能:
- セクション1〜5の基本レポート生成
- BigQueryからのデータ取得
- AIによるエグゼクティブサマリー生成
- 目標未設定時の適切な処理
- Phase 2: 事実ベース分析の洞察生成
"""

import streamlit as st
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, List
import pandas as pd
import numpy as np
from google.cloud import bigquery

from targets_manager import TargetsManager
from achievement_analyzer import AchievementAnalyzer
from comparative_analyzer import ComparativeAnalyzer
from action_recommender import ActionRecommender


class SummaryReportGenerator:
    """サマリーレポート生成クラス"""
    
    def __init__(self, bq_client: bigquery.Client, gemini_client=None, claude_client=None):
        """
        初期化
        
        Args:
            bq_client: BigQueryクライアント
            gemini_client: Gemini APIクライアント（オプション）
            claude_client: Claude APIクライアント（オプション）
        """
        self.bq_client = bq_client
        self.gemini_client = gemini_client
        self.claude_client = claude_client
        self.targets_manager = TargetsManager()
        self.analyzer = AchievementAnalyzer()
        # ===== 【追加】Phase 3用の分析器 =====
        self.comparative_analyzer = None  # generate_report内で初期化
        self.action_recommender = ActionRecommender()

    def generate_report(
        self,
        start_date,  # datetime または date
        end_date,    # datetime または date
        comparison_mode: str = "none",
        table_id: str = "your-project.your-dataset.your-table",
        min_campaigns_for_comparison: int = 3
    ) -> Dict[str, Any]:
        """
        レポートを生成
        
        Args:
            start_date: 開始日
            end_date: 終了日
            comparison_mode: 比較モード（"none", "wow", "mom", "yoy"）
            table_id: BigQueryテーブルID
        
        Returns:
            レポートデータ
        """
        # 1. データ取得
        st.info("📊 データを取得中...")
        data = self._fetch_data(start_date, end_date, table_id)
        
        if data.empty:
            st.error("❌ データが見つかりませんでした")
            return None
        
        # 2. 集計
        actuals = self._aggregate_metrics(data)
        
        # 3. 目標取得
        year_month = start_date.strftime("%Y-%m")
        targets = self.targets_manager.get_targets(year_month)
        
        # 4. 各セクション生成
        report = {
            "period": {
                "start_date": start_date,
                "end_date": end_date,
                "year_month": year_month
            },
            "section_1_executive_summary": None,  # 後で生成
            "section_2_achievement": self._generate_achievement_section(
                targets, actuals, start_date, end_date
            ),
            "section_3_kpis": self._generate_kpi_section(actuals),
            "section_4_trends": self._generate_trend_section(data),
            "section_5_highlights": self._generate_highlights_section(data, actuals)
        }
        
        # 5. 前期間比較（有効な場合）
        if comparison_mode != "none":
            prev_data = self._fetch_comparison_data(start_date, end_date, comparison_mode, table_id)
            if not prev_data.empty:
                prev_actuals = self._aggregate_metrics(prev_data)
                report["comparison"] = self.analyzer.compare_with_previous_period(
                    actuals, {"actual_" + k: v for k, v in prev_actuals.items()}
                )
        
        # 6. エグゼクティブサマリー生成（AI）
        st.info("🤖 AI分析中...")
        report["section_1_executive_summary"] = self._generate_executive_summary(report)
        
        # 7. KPI洞察生成（AI）
        report["kpi_insights"] = self._generate_kpi_insights(report)
        
        # 8. ハイライト洞察生成（AI）
        report["highlights_insights"] = self._generate_highlights_insights(report)
        
        # 9. Phase 3: キャンペーンデータの準備
        st.info("📊 キャンペーンデータを準備中...")
        campaigns_data = self._prepare_campaign_data(data)
        
        # 10. セクション6: パフォーマンス比較分析
        st.info("📊 パフォーマンス比較分析中...")
        report["section_6_comparative_analysis"] = self._generate_comparative_analysis(
            campaigns_data, 
            min_campaigns_for_comparison
        )
        
        # 11. セクション7: アクション提案
        st.info("🎯 アクション提案を生成中...")
        
        # 全体指標を準備
        overall_metrics = {
            'overall_roas': actuals.get('roas', 0),
            'target_roas': targets.get('target_roas', 0) if targets else 0,
            'budget_usage_pct': (
                (actuals.get('cost', 0) / targets.get('budget', 1) * 100) 
                if targets and targets.get('budget', 0) > 0 
                else 0
            ),
            'overall_cpa': actuals.get('cpa', 0),
            'target_cpa': targets.get('target_cpa', 0) if targets else 0,
        }
        
        report["section_7_action_recommendations"] = self._generate_action_recommendations(
            report["section_6_comparative_analysis"],
            overall_metrics
        )
        
        # ========== Phase 3: ここまで追加 ==========
        
        st.success("✅ レポート生成完了")
        
        return report
    
    def _fetch_data(
        self,
        start_date,  # datetime, date, またはその両方を受け付ける
        end_date,    # datetime, date, またはその両方を受け付ける
        table_id: str
    ) -> pd.DataFrame:
        """
        BigQueryからデータを取得
        
        Args:
            start_date: 開始日
            end_date: 終了日
            table_id: テーブルID
        
        Returns:
            データフレーム
        """
        query = f"""
        SELECT
            Date as date,
            CampaignName as campaign_name,
            ServiceNameJA_Media as media,
            CostIncludingFees as cost,
            Impressions as impressions,
            Clicks as clicks,
            AllConversions as conversions
        FROM `{table_id}`
        WHERE Date BETWEEN @start_date AND @end_date
        ORDER BY Date
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "start_date", 
                    "DATE", 
                    start_date.date() if isinstance(start_date, datetime) else start_date
                ),
                bigquery.ScalarQueryParameter(
                    "end_date", 
                    "DATE", 
                    end_date.date() if isinstance(end_date, datetime) else end_date
                )
            ]
        )
        
        try:
            # デバッグ情報を表示
            st.info(f"🔍 データ取得中...")
            st.code(f"テーブル: {table_id}\n期間: {start_date} - {end_date}", language="text")
            
            with st.expander("📄 実行されるSQLクエリ", expanded=False):
                st.code(query, language="sql")
            
            df = self.bq_client.query(query, job_config=job_config).to_dataframe()
            
            st.success(f"✅ データ取得成功: {len(df)}行")
            
            return df
            
        except Exception as e:
            st.error(f"❌ データ取得エラー: {e}")
            
            # エラー発生時の詳細情報
            with st.expander("🔍 エラー詳細情報", expanded=True):
                st.markdown("**テーブル情報:**")
                st.code(table_id)
                
                st.markdown("**実行したクエリ:**")
                st.code(query, language="sql")
                
                st.markdown("**パラメータ:**")
                st.json({
                    "start_date": str(start_date),
                    "end_date": str(end_date)
                })
                
                st.markdown("**エラーメッセージ:**")
                st.code(str(e))
                
                st.warning("""
                💡 **カラム名が一致しない可能性があります**
                
                BigQueryで実際のカラム名を確認してください:
                ```sql
                SELECT column_name 
                FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
                ```
                
                エラーメッセージに表示されている推奨カラム名（例: CampaignName）を使用してください。
                """)
            
            return pd.DataFrame()
    
    def _fetch_comparison_data(
        self,
        start_date,  # datetime または date
        end_date,    # datetime または date
        mode: str,
        table_id: str
    ) -> pd.DataFrame:
        """
        比較用データを取得
        
        Args:
            start_date: 基準期間の開始日
            end_date: 基準期間の終了日
            mode: 比較モード
            table_id: テーブルID
        
        Returns:
            比較期間のデータフレーム
        """
        # datetime.date型をdatetimeに変換（日付計算のため）
        if isinstance(start_date, date) and not isinstance(start_date, datetime):
            start_dt = datetime.combine(start_date, datetime.min.time())
        else:
            start_dt = start_date
            
        if isinstance(end_date, date) and not isinstance(end_date, datetime):
            end_dt = datetime.combine(end_date, datetime.min.time())
        else:
            end_dt = end_date
        
        period_days = (end_dt - start_dt).days + 1
        
        if mode == "wow":
            # 1週間前
            comp_end = start_dt - timedelta(days=1)
            comp_start = comp_end - timedelta(days=period_days - 1)
        elif mode == "mom":
            # 1ヶ月前
            comp_end = start_dt - timedelta(days=1)
            comp_start = comp_end - timedelta(days=period_days - 1)
        elif mode == "yoy":
            # 1年前
            comp_start = start_dt.replace(year=start_dt.year - 1)
            comp_end = end_dt.replace(year=end_dt.year - 1)
        else:
            return pd.DataFrame()
        
        # 比較期間のデータ取得（date型で渡す）
        return self._fetch_data(comp_start.date(), comp_end.date(), table_id)
    
    def _aggregate_metrics(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        メトリクスを集計
        
        Args:
            df: データフレーム
        
        Returns:
            集計結果
        """
        try:
            # データ型を確認・変換
            numeric_columns = ['cost', 'impressions', 'clicks', 'conversions']
            
            for col in numeric_columns:
                if col in df.columns:
                    # 数値型に変換（エラーは0に置換）
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            total_cost = float(df['cost'].sum())
            total_impressions = float(df['impressions'].sum())
            total_clicks = float(df['clicks'].sum())
            total_conversions = float(df['conversions'].sum())
            
            return {
                "cost": total_cost,
                "impressions": total_impressions,
                "clicks": total_clicks,
                "conversions": total_conversions,
                "cpa": total_cost / total_conversions if total_conversions > 0 else 0,
                "cvr": total_conversions / total_clicks if total_clicks > 0 else 0,
                "ctr": total_clicks / total_impressions if total_impressions > 0 else 0,
                "cpc": total_cost / total_clicks if total_clicks > 0 else 0
            }
        except Exception as e:
            st.error(f"❌ メトリクス集計エラー: {e}")
            st.write("データフレームの情報:")
            st.write(df.dtypes)
            st.write(df.head())
            raise
    
    def _generate_achievement_section(
        self,
        targets: Optional[Dict[str, Any]],
        actuals: Dict[str, float],
        start_date,
        end_date
    ) -> Dict[str, Any]:
        """
        セクション2: 目標達成状況を生成
        
        Args:
            targets: 目標データ
            actuals: 実績データ
            start_date: 開始日
            end_date: 終了日
        
        Returns:
            達成状況セクション
        """
        # date型をdatetimeに変換
        if isinstance(end_date, date) and not isinstance(end_date, datetime):
            end_date = datetime.combine(end_date, datetime.min.time())
        
        current_day = end_date.day
        total_days = self.analyzer.get_days_in_month(end_date.year, end_date.month)
        
        # 予算消化ペース
        budget_pacing = self.analyzer.analyze_budget_pacing(
            target_budget=targets.get("budget") if targets else None,
            actual_cost=actuals["cost"],
            current_day=current_day,
            total_days=total_days
        )
        
        # KPI達成率
        kpi_achievement = self.analyzer.calculate_kpi_achievement(targets, actuals)
        
        return {
            "budget_pacing": budget_pacing,
            "kpi_achievement": kpi_achievement
        }
    
    def _generate_kpi_section(self, actuals: Dict[str, float]) -> Dict[str, Any]:
        """
        セクション3: 主要KPIを生成
        
        Args:
            actuals: 実績データ
        
        Returns:
            KPIセクション
        """
        return {
            "metrics": actuals
        }
    
    def _generate_trend_section(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        セクション4: 期間トレンドを生成
        
        Args:
            df: データフレーム
        
        Returns:
            トレンドセクション
        """
        try:
            # dateカラムを先に文字列に変換（db_dtypes対策）
            df['date'] = df['date'].astype(str)
            
            # 数値型に変換
            numeric_columns = ['cost', 'conversions', 'clicks', 'impressions']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # 日次集計
            daily = df.groupby('date').agg({
                'cost': 'sum',
                'conversions': 'sum',
                'clicks': 'sum',
                'impressions': 'sum'
            }).reset_index()
            
            # CPAとCVRを計算（数値カラムのみ）
            daily['cpa'] = daily.apply(
                lambda row: float(row['cost'] / row['conversions']) if row['conversions'] > 0 else 0.0, 
                axis=1
            )
            daily['cvr'] = daily.apply(
                lambda row: float(row['conversions'] / row['clicks']) if row['clicks'] > 0 else 0.0, 
                axis=1
            )
            
            # 数値カラムのみNaN/Infを0に置換
            numeric_cols = ['cost', 'conversions', 'clicks', 'impressions', 'cpa', 'cvr']
            for col in numeric_cols:
                daily[col] = daily[col].fillna(0)
                daily[col] = daily[col].replace([float('inf'), -float('inf')], 0)
            
            # すべてのカラムをPythonネイティブ型に変換
            records = []
            for _, row in daily.iterrows():
                record = {
                    'date': str(row['date']),
                    'cost': float(row['cost']),
                    'conversions': float(row['conversions']),
                    'clicks': float(row['clicks']),
                    'impressions': float(row['impressions']),
                    'cpa': float(row['cpa']),
                    'cvr': float(row['cvr'])
                }
                records.append(record)
            
            return {
                "daily_data": records
            }
            
        except Exception as e:
            st.error(f"❌ トレンド生成エラー: {e}")
            st.write("データフレームの情報:")
            st.write(df.dtypes)
            st.write(df.head())
            
            import traceback
            st.code(traceback.format_exc())
            raise
    
    def _generate_highlights_section(
        self,
        df: pd.DataFrame,
        actuals: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        セクション5: ハイライトを生成
        
        Args:
            df: データフレーム
            actuals: 実績データ
        
        Returns:
            ハイライトセクション
        """
        try:
            # dateカラムを先に文字列に変換（db_dtypes対策）
            if 'date' in df.columns:
                df['date'] = df['date'].astype(str)
            
            # 数値型に変換
            numeric_columns = ['cost', 'conversions', 'clicks']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # キャンペーン別集計
            campaign_agg = df.groupby('campaign_name').agg({
                'cost': 'sum',
                'conversions': 'sum',
                'clicks': 'sum'
            }).reset_index()
            
            # CPAを計算
            campaign_agg['cpa'] = campaign_agg.apply(
                lambda row: float(row['cost'] / row['conversions']) if row['conversions'] > 0 else float('inf'),
                axis=1
            )
            
            # CV0を除外
            campaign_agg = campaign_agg[campaign_agg['conversions'] > 0]
            
            if campaign_agg.empty:
                return {
                    "best_campaign": None,
                    "worst_campaign": None,
                    "best_day": None,
                    "worst_day": None
                }
            
            # 最高・最低パフォーマンスキャンペーン（CPAで判定）
            best_row = campaign_agg.nsmallest(1, 'cpa').iloc[0]
            worst_row = campaign_agg.nlargest(1, 'cpa').iloc[0]
            
            best_campaign = {
                'campaign_name': str(best_row['campaign_name']),
                'cost': float(best_row['cost']),
                'conversions': float(best_row['conversions']),
                'clicks': float(best_row['clicks']),
                'cpa': float(best_row['cpa'])
            }
            
            worst_campaign = {
                'campaign_name': str(worst_row['campaign_name']),
                'cost': float(worst_row['cost']),
                'conversions': float(worst_row['conversions']),
                'clicks': float(worst_row['clicks']),
                'cpa': float(worst_row['cpa'])
            }
            
            # 日別集計
            daily_agg = df.groupby('date').agg({
                'conversions': 'sum'
            }).reset_index()
            
            if not daily_agg.empty:
                best_day_row = daily_agg.nlargest(1, 'conversions').iloc[0]
                worst_day_row = daily_agg.nsmallest(1, 'conversions').iloc[0]
                
                best_day = {
                    'date': str(best_day_row['date']),
                    'conversions': float(best_day_row['conversions'])
                }
                
                worst_day = {
                    'date': str(worst_day_row['date']),
                    'conversions': float(worst_day_row['conversions'])
                }
            else:
                best_day = None
                worst_day = None
            
            return {
                "best_campaign": best_campaign,
                "worst_campaign": worst_campaign,
                "best_day": best_day,
                "worst_day": worst_day
            }
            
        except Exception as e:
            st.error(f"❌ ハイライト生成エラー: {e}")
            st.write("データフレームの情報:")
            st.write(df.dtypes)
            st.write(df.head())
            
            import traceback
            st.code(traceback.format_exc())
            
            return {
                "best_campaign": None,
                "worst_campaign": None,
                "best_day": None,
                "worst_day": None
            }
    
    def _generate_executive_summary(self, report: Dict[str, Any]) -> str:
        """
        セクション1: エグゼクティブサマリーをAI生成
        
        Args:
            report: レポートデータ
        
        Returns:
            サマリーテキスト
        """
        # AIクライアントが利用できない場合は簡易版
        if not self.claude_client and not self.gemini_client:
            return self._generate_simple_summary(report)
        
        # プロンプト構築
        prompt = self._build_summary_prompt(report)
        
        try:
            # Claudeを優先的に使用
            if self.claude_client:
                response = self.claude_client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=1000,
                    messages=[{"role": "user", "content": prompt}]
                )
                return response.content[0].text
            
            # Geminiをフォールバック
            elif self.gemini_client:
                response = self.gemini_client.generate_content(prompt)
                return response.text
        
        except Exception as e:
            st.warning(f"⚠️ AI生成に失敗しました。簡易版を表示します。エラー: {e}")
            return self._generate_simple_summary(report)
    
    def _build_summary_prompt(self, report: Dict[str, Any]) -> str:
        """
        サマリー生成用プロンプトを構築
        
        Args:
            report: レポートデータ
        
        Returns:
            プロンプト文字列
        """
        achievement = report["section_2_achievement"]
        kpis = report["section_3_kpis"]
        highlights = report["section_5_highlights"]
        
        # 前期間比較データ（ある場合）
        comparison_text = ""
        if "comparison" in report and report["comparison"]["has_comparison"]:
            comp = report["comparison"]["comparisons"]
            comparison_text = f"""
        ## 前期間比較
        - コスト: {comp['cost']['trend_text'] if 'cost' in comp else 'N/A'}
        - CV数: {comp['conversions']['trend_text'] if 'conversions' in comp else 'N/A'}
        - CPA: {comp['cpa']['trend_text'] if 'cpa' in comp else 'N/A'}
        - CVR: {comp['cvr']['trend_text'] if 'cvr' in comp else 'N/A'}
        """
        
        # ハイライトデータの安全な取得
        best_campaign_name = 'N/A'
        best_campaign_cpa = 'N/A'
        worst_campaign_name = 'N/A'
        worst_campaign_cpa = 'N/A'
        
        if highlights.get('best_campaign'):
            best_campaign_name = highlights['best_campaign'].get('campaign_name', 'N/A')
            best_campaign_cpa = f"¥{highlights['best_campaign'].get('cpa', 0):,.0f}"
        
        if highlights.get('worst_campaign'):
            worst_campaign_name = highlights['worst_campaign'].get('campaign_name', 'N/A')
            worst_campaign_cpa = f"¥{highlights['worst_campaign'].get('cpa', 0):,.0f}"
        
        prompt = f"""
        あなたはデータドリブンなデジタルマーケティングアナリストです。以下のデータを分析し、事実ベースのエグゼクティブサマリーを生成してください。

        # 厳守事項:
        1. **事実**: データから直接読み取れることのみ記述
        2. **解釈**: 複数の事実から論理的に導かれることを記述
        3. **仮説**: 「可能性がある」「考えられる」と明記し、検証方法をセット
        4. **禁止**: データ外の憶測、検証不可能な推測、主観的評価

        # 仮説提示レベル:
        ✅ Aレベル（控えめ）: 「〜に何らかの課題がある可能性」
        ✅ Bレベル（具体的）: 「A、B、Cのいずれかに課題がある可能性」
        ❌ Cレベル（詳細すぎ）: 「過去ケースではAが原因」← 使用禁止

        ---

        # データ
        期間: {report['period']['start_date'].strftime('%Y/%m/%d')} - {report['period']['end_date'].strftime('%Y/%m/%d')}

        ## 実績
        - コスト: ¥{kpis['metrics']['cost']:,.0f}
        - CV数: {kpis['metrics']['conversions']:,.0f}件
        - CPA: ¥{kpis['metrics']['cpa']:,.0f}
        - CVR: {kpis['metrics']['cvr']:.2%}
        - CTR: {kpis['metrics']['ctr']:.2%}

        ## 予算消化状況
        {achievement['budget_pacing']['pace_status_text']}
        {"（目標: ¥" + f"{achievement['budget_pacing']['target_budget']:,.0f}）" if achievement['budget_pacing']['has_target'] else ""}

        {comparison_text}

        ## ハイライト
        - 最高パフォーマンスキャンペーン: {best_campaign_name}
        CPA: {best_campaign_cpa}
        - 最低パフォーマンスキャンペーン: {worst_campaign_name}
        CPA: {worst_campaign_cpa}

        ---

        # 出力形式（簡潔に3-4文）:

        **📊 事実（データから読み取れること）**
        (数値データから直接読み取れる変化・状況)

        **💡 解釈（論理的に導かれること）**
        (複数の事実を組み合わせた状況説明)

        **🔍 仮説（可能性のある要因 - A-Bレベル）**
        (考えられる要因を1-2つ、「可能性がある」と明記)

        **✅ 推奨アクション**
        (最も優先度が高い施策を1つ、具体的に提案)
        (検証方法や期待効果も簡潔に記載)

        # 注意事項:
        - 数値は必ず記載されているものを使用
        - 「〜と思われる」「おそらく」などの曖昧な表現は使用しない
        - 仮説には必ず「可能性」「考えられる」を付ける
        - データで確認できないことは書かない
        """
        return prompt
    
    def _generate_simple_summary(self, report: Dict[str, Any]) -> str:
        """
        簡易版サマリーを生成（AI不使用）
        
        Args:
            report: レポートデータ
        
        Returns:
            サマリーテキスト
        """
        kpis = report["section_3_kpis"]
        achievement = report["section_2_achievement"]
        
        summary = f"""
        **📊 現状の要約**
        期間中のコストは¥{kpis['metrics']['cost']:,.0f}、CV数は{kpis['metrics']['conversions']:,.0f}件、CPAは¥{kpis['metrics']['cpa']:,.0f}でした。
        予算消化ペースは{achievement['budget_pacing']['pace_status_text']}です。

        **💡 成功と課題の要因**
        全体的なパフォーマンスは安定しており、主要KPIは目標水準を維持しています。

        **🎯 推奨アクション**
        引き続き現在の施策を継続しつつ、低パフォーマンスキャンペーンの改善を検討してください。
        """
        return summary.strip()
            
    def _generate_kpi_insights(self, report: Dict[str, Any]) -> str:
        """
        KPI分析の洞察を生成
        
        Args:
            report: レポートデータ
        
        Returns:
            KPI分析コメント
        """
        if not self.claude_client and not self.gemini_client:
            return ""
        
        kpis = report["section_3_kpis"]["metrics"]
        achievement = report["section_2_achievement"]
        
        # 前期間比較データ
        comparison_data = ""
        if "comparison" in report and report["comparison"]["has_comparison"]:
            comp = report["comparison"]["comparisons"]
            comparison_data = f"""
            前期間比較:
            - CPA変化: {comp.get('cpa', {}).get('change_rate', 0) * 100:.1f}%
            - CVR変化: {comp.get('cvr', {}).get('change_rate', 0) * 100:.1f}%
            - CTR変化: {comp.get('ctr', {}).get('change_rate', 0) * 100:.1f}%
            """
                    
            prompt = f"""
            以下のKPIデータを分析し、事実ベースの洞察を提供してください。

            # データ
            CPA: ¥{kpis['cpa']:,.0f}
            CVR: {kpis['cvr']:.2%}
            CTR: {kpis['ctr']:.2%}
            CPC: ¥{kpis['cpc']:,.0f}

            {comparison_data}

            # 出力形式（2-3文）:
            📊 **事実**: (数値から読み取れること)
            💡 **解釈**: (論理的に導かれること)
            🔍 **注目点**: (特に注意すべき指標があれば)

            注意: データに基づく事実のみを記述してください。
            """
                    
            try:
                if self.claude_client:
                    response = self.claude_client.messages.create(
                        model="claude-sonnet-4-20250514",
                        max_tokens=500,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    return response.content[0].text
                elif self.gemini_client:
                    response = self.gemini_client.generate_content(prompt)
                    return response.text
            except Exception as e:
                st.warning(f"⚠️ KPI洞察の生成に失敗: {e}")
                return ""
    
    def _generate_highlights_insights(self, report: Dict[str, Any]) -> str:
        """
        ハイライト分析の洞察を生成
        
        Args:
            report: レポートデータ
        
        Returns:
            ハイライト分析コメント
        """
        if not self.claude_client and not self.gemini_client:
            return ""
        
        highlights = report["section_5_highlights"]
        
        if not highlights.get("best_campaign") or not highlights.get("worst_campaign"):
            return ""
        
        best = highlights["best_campaign"]
        worst = highlights["worst_campaign"]
        
        cpa_diff = ((worst['cpa'] - best['cpa']) / best['cpa'] * 100) if best['cpa'] > 0 else 0
        
        prompt = f"""
        高パフォーマンスと低パフォーマンスのキャンペーンを分析してください。

        # データ
        ## 最高パフォーマンス
        - キャンペーン: {best['campaign_name']}
        - CPA: ¥{best['cpa']:,.0f}
        - CV数: {best['conversions']:.0f}件

        ## 最低パフォーマンス
        - キャンペーン: {worst['campaign_name']}
        - CPA: ¥{worst['cpa']:,.0f}
        - CV数: {worst['conversions']:.0f}件

        ## 差異
        - CPA差: {cpa_diff:+.1f}%

        # 出力形式（2-3文）:
        📊 **事実**: (CPAの差とその規模)
        🔍 **仮説**: (差が生じている可能性のある要因 - A-Bレベル)
        ✅ **検証方法**: (仮説を確認する方法)

        注意: 
        - 「可能性がある」「考えられる」を必ず付ける
        - 検証可能な仮説のみ提示
        """
        
        try:
            if self.claude_client:
                response = self.claude_client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=500,
                    messages=[{"role": "user", "content": prompt}]
                )
                return response.content[0].text
            elif self.gemini_client:
                response = self.gemini_client.generate_content(prompt)
                return response.text
        except Exception as e:
            st.warning(f"⚠️ ハイライト洞察の生成に失敗: {e}")
            return ""
    
    def _prepare_campaign_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        生のデータフレームからキャンペーン単位のデータを準備
        
        Args:
            df: BigQueryから取得したデータフレーム
            
        Returns:
            キャンペーン単位のデータリスト
        """
        try:
            # データフレームのコピーを作成
            df_copy = df.copy()
            
            # 数値型に変換
            numeric_columns = ['cost', 'conversions', 'clicks', 'impressions']
            for col in numeric_columns:
                if col in df_copy.columns:
                    df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)
            
            # キャンペーン名が存在しない場合のチェック
            if 'campaign_name' not in df_copy.columns:
                st.warning("⚠️ campaign_nameカラムが見つかりません。比較分析をスキップします。")
                return []
            
            # キャンペーン単位で集計
            campaign_agg = df_copy.groupby('campaign_name').agg({
                'cost': 'sum',
                'conversions': 'sum',
                'clicks': 'sum',
                'impressions': 'sum'
            }).reset_index()
            
            # CPA、ROAS、CVR、CTRを計算
            campaign_agg['cpa'] = campaign_agg.apply(
                lambda row: float(row['cost'] / row['conversions']) if row['conversions'] > 0 else 0.0,
                axis=1
            )
            
            # ROASの計算（仮に1コンバージョンあたり10,000円の価値と仮定）
            # ※実際の実装では、conversion_valueを使用してください
            campaign_agg['roas'] = campaign_agg.apply(
                lambda row: float(row['conversions'] * 10000 / row['cost']) if row['cost'] > 0 else 0.0,
                axis=1
            )
            
            campaign_agg['conversion_rate'] = campaign_agg.apply(
                lambda row: float(row['conversions'] / row['clicks']) if row['clicks'] > 0 else 0.0,
                axis=1
            )
            
            campaign_agg['click_rate'] = campaign_agg.apply(
                lambda row: float(row['clicks'] / row['impressions']) if row['impressions'] > 0 else 0.0,
                axis=1
            )
            
            # NaN/Infを0に置換
            campaign_agg = campaign_agg.fillna(0)
            campaign_agg = campaign_agg.replace([float('inf'), -float('inf')], 0)
            
            # 辞書のリストに変換
            campaigns_data = []
            for _, row in campaign_agg.iterrows():
                campaigns_data.append({
                    'campaign_name': str(row['campaign_name']),
                    'cost': float(row['cost']),
                    'conversions': int(row['conversions']),
                    'clicks': int(row['clicks']),
                    'impressions': int(row['impressions']),
                    'cpa': float(row['cpa']),
                    'roas': float(row['roas']),
                    'conversion_rate': float(row['conversion_rate']),
                    'click_rate': float(row['click_rate'])
                })
            
            st.info(f"✅ {len(campaigns_data)}件のキャンペーンデータを準備しました")
            return campaigns_data
            
        except Exception as e:
            st.error(f"❌ キャンペーンデータ準備エラー: {e}")
            import traceback
            st.code(traceback.format_exc())
            return []


    def _generate_comparative_analysis(
        self,
        campaigns_data: List[Dict[str, Any]],
        min_campaigns: int
    ) -> 'ComparativeAnalysis':
        """
        セクション6: パフォーマンス比較分析を生成
        
        Args:
            campaigns_data: キャンペーンデータのリスト
            min_campaigns: 比較分析に必要な最低キャンペーン数
            
        Returns:
            ComparativeAnalysis: 比較分析結果
        """
        from comparative_analyzer import ComparativeAnalyzer
        
        try:
            # Analyzerを初期化
            self.comparative_analyzer = ComparativeAnalyzer(
                min_campaigns_per_group=min_campaigns
            )
            
            # 分析実行
            analysis = self.comparative_analyzer.analyze(campaigns_data)
            
            # 分析がスキップされた場合
            if analysis.skipped:
                st.warning(f"⚠️ {analysis.skip_reason}")
                return analysis
            
            # AI分析が必要な場合（スキップされていない場合のみ）
            if self.claude_client or self.gemini_client:
                try:
                    st.info("🤖 比較分析のAI洞察を生成中...")
                    ai_prompt = self.comparative_analyzer.generate_ai_prompt(analysis)
                    
                    # Claudeを優先的に使用
                    if self.claude_client:
                        response = self.claude_client.messages.create(
                            model="claude-sonnet-4-20250514",
                            max_tokens=1500,
                            messages=[{"role": "user", "content": ai_prompt}]
                        )
                        analysis.ai_insights = response.content[0].text
                    
                    # Geminiをフォールバック
                    elif self.gemini_client:
                        response = self.gemini_client.generate_content(ai_prompt)
                        analysis.ai_insights = response.text
                    
                    st.success("✅ AI洞察を生成しました")
                        
                except Exception as e:
                    st.warning(f"⚠️ AI分析生成に失敗しました: {e}")
                    analysis.ai_insights = None
            
            return analysis
            
        except Exception as e:
            st.error(f"❌ 比較分析生成エラー: {e}")
            import traceback
            st.code(traceback.format_exc())
            
            # エラー時は空の分析結果を返す
            from comparative_analyzer import ComparativeAnalysis
            return ComparativeAnalysis(
                high_performers=[],
                low_performers=[],
                significant_differences=[],
                analysis_summary={},
                skipped=True,
                skip_reason=f"エラーが発生しました: {str(e)}"
            )


    def _generate_action_recommendations(
        self,
        comparative_analysis: 'ComparativeAnalysis',
        overall_metrics: Dict[str, Any]
    ) -> 'ActionRecommendations':
        """
        セクション7: アクション提案を生成
        
        Args:
            comparative_analysis: 比較分析結果
            overall_metrics: 全体指標
            
        Returns:
            ActionRecommendations: アクション提案セット
        """
        try:
            # アクション提案を生成
            recommendations = self.action_recommender.generate_recommendations(
                comparative_analysis,
                overall_metrics
            )
            
            # 比較分析がスキップされた場合
            if comparative_analysis.skipped:
                st.warning("⚠️ 比較分析がスキップされたため、アクション提案も制限されます")
                return recommendations
            
            # アクションが存在しない場合
            if not recommendations.actions:
                st.info("ℹ️ 生成されたアクション提案はありません")
                return recommendations
            
            # AI分析が必要な場合（アクションが存在する場合のみ）
            if self.claude_client or self.gemini_client:
                try:
                    st.info("🤖 アクション提案のAI洞察を生成中...")
                    ai_prompt = self.action_recommender.generate_ai_prompt(
                        recommendations,
                        comparative_analysis
                    )
                    
                    # Claudeを優先的に使用
                    if self.claude_client:
                        response = self.claude_client.messages.create(
                            model="claude-sonnet-4-20250514",
                            max_tokens=1500,
                            messages=[{"role": "user", "content": ai_prompt}]
                        )
                        recommendations.ai_insights = response.content[0].text
                    
                    # Geminiをフォールバック
                    elif self.gemini_client:
                        response = self.gemini_client.generate_content(ai_prompt)
                        recommendations.ai_insights = response.text
                    
                    st.success("✅ AI洞察を生成しました")
                        
                except Exception as e:
                    st.warning(f"⚠️ AI分析生成に失敗しました: {e}")
                    recommendations.ai_insights = None
            
            return recommendations
            
        except Exception as e:
            st.error(f"❌ アクション提案生成エラー: {e}")
            import traceback
            st.code(traceback.format_exc())
            
            # エラー時は空の提案を返す
            from action_recommender import ActionRecommendations
            return ActionRecommendations(
                actions=[],
                summary=f"エラーが発生しました: {str(e)}",
                high_priority_count=0,
                medium_priority_count=0,
                low_priority_count=0
            )


# テスト用コード
if __name__ == "__main__":
    print("✅ summary_report_generator.py のインポート成功")
    print("📝 実際の動作確認はStreamlitアプリから行ってください")