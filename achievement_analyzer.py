"""
achievement_analyzer.py
達成状況分析モジュール

機能:
- 予算消化ペース分析
- KPI達成率計算
- 判定ロジック（アンダー/オン/オーバー）
- 目標未設定時の適切な処理
"""

from typing import Dict, Optional, Any, Tuple
from datetime import datetime
import calendar


class AchievementAnalyzer:
    """達成状況分析クラス"""
    
    # ペース判定の閾値（±10%以内ならオンペース）
    PACE_THRESHOLD = 0.10
    
    def __init__(self):
        """初期化"""
        pass
    
    def analyze_budget_pacing(
        self,
        target_budget: Optional[float],
        actual_cost: float,
        current_day: int,
        total_days: int
    ) -> Dict[str, Any]:
        """
        予算消化ペースを分析
        
        Args:
            target_budget: 月間目標予算（Noneの場合は目標未設定）
            actual_cost: 現在までの実コスト
            current_day: 現在の日（1-31）
            total_days: 月の総日数
        
        Returns:
            ペース分析結果
        """
        result = {
            "actual_cost": actual_cost,
            "target_budget": target_budget,
            "has_target": target_budget is not None
        }
        
        # 目標未設定の場合
        if target_budget is None:
            result.update({
                "progress_rate": None,
                "expected_progress_rate": None,
                "pace_status": "no_target",
                "pace_status_text": "目標未設定",
                "daily_average": actual_cost / current_day if current_day > 0 else 0,
                "projected_month_end": (actual_cost / current_day * total_days) if current_day > 0 else 0
            })
            return result
        
        # 期待進捗率（例: 10月6日/31日 = 19.4%）
        expected_progress = current_day / total_days
        
        # 実際の進捗率
        actual_progress = actual_cost / target_budget if target_budget > 0 else 0
        
        # ペース差異
        pace_difference = actual_progress - expected_progress
        
        # ペース判定
        if pace_difference < -self.PACE_THRESHOLD:
            pace_status = "under"
            pace_status_text = "🟢 アンダーペース"
        elif pace_difference > self.PACE_THRESHOLD:
            pace_status = "over"
            pace_status_text = "🔴 オーバーペース"
        else:
            pace_status = "on_track"
            pace_status_text = "🟡 オンペース"
        
        # 日次平均と月末予測
        daily_average = actual_cost / current_day if current_day > 0 else 0
        projected_month_end = daily_average * total_days
        
        result.update({
            "progress_rate": actual_progress,
            "expected_progress_rate": expected_progress,
            "pace_difference": pace_difference,
            "pace_status": pace_status,
            "pace_status_text": pace_status_text,
            "daily_average": daily_average,
            "projected_month_end": projected_month_end,
            "remaining_budget": target_budget - actual_cost,
            "days_remaining": total_days - current_day
        })
        
        return result
    
    def calculate_kpi_achievement(
        self,
        targets: Optional[Dict[str, Any]],
        actuals: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        KPI達成率を計算
        
        Args:
            targets: 目標値（None or 各KPIの目標）
            actuals: 実績値（cost, conversions, cpa, cvr, ctr等）
        
        Returns:
            KPI達成率の詳細
        """
        result = {
            "has_target": targets is not None and any(targets.values()) if targets else False,
            "kpis": {}
        }
        
        # 目標未設定の場合
        if not result["has_target"]:
            for kpi in ["conversions", "cpa", "cvr", "ctr"]:
                result["kpis"][kpi] = {
                    "target": None,
                    "actual": actuals.get(kpi),
                    "achievement_rate": None,
                    "status": "no_target",
                    "status_text": "-"
                }
            return result
        
        # CV数の達成率（多いほど良い）
        if targets.get("target_conversions"):
            cv_actual = actuals.get("conversions", 0)
            cv_target = targets["target_conversions"]
            cv_rate = cv_actual / cv_target if cv_target > 0 else 0
            
            result["kpis"]["conversions"] = {
                "target": cv_target,
                "actual": cv_actual,
                "achievement_rate": cv_rate,
                "status": self._judge_achievement_status(cv_rate, is_higher_better=True),
                "status_text": self._format_achievement_text(cv_rate)
            }
        
        # CPAの達成率（低いほど良い）
        if targets.get("target_cpa"):
            cpa_actual = actuals.get("cpa", 0)
            cpa_target = targets["target_cpa"]
            # CPAは低い方が良いので、逆算（target/actual）
            cpa_rate = cpa_target / cpa_actual if cpa_actual > 0 else 0
            
            result["kpis"]["cpa"] = {
                "target": cpa_target,
                "actual": cpa_actual,
                "achievement_rate": cpa_rate,
                "status": self._judge_achievement_status(cpa_rate, is_higher_better=True),
                "status_text": self._format_achievement_text(cpa_rate),
                "is_lower_better": True
            }
        
        # CVRの達成率（高いほど良い）
        if targets.get("target_cvr"):
            cvr_actual = actuals.get("cvr", 0)
            cvr_target = targets["target_cvr"]
            cvr_rate = cvr_actual / cvr_target if cvr_target > 0 else 0
            
            result["kpis"]["cvr"] = {
                "target": cvr_target,
                "actual": cvr_actual,
                "achievement_rate": cvr_rate,
                "status": self._judge_achievement_status(cvr_rate, is_higher_better=True),
                "status_text": self._format_achievement_text(cvr_rate)
            }
        
        # CTRの達成率（高いほど良い）
        if targets.get("target_ctr"):
            ctr_actual = actuals.get("ctr", 0)
            ctr_target = targets["target_ctr"]
            ctr_rate = ctr_actual / ctr_target if ctr_target > 0 else 0
            
            result["kpis"]["ctr"] = {
                "target": ctr_target,
                "actual": ctr_actual,
                "achievement_rate": ctr_rate,
                "status": self._judge_achievement_status(ctr_rate, is_higher_better=True),
                "status_text": self._format_achievement_text(ctr_rate)
            }
        
        return result
    
    def _judge_achievement_status(
        self,
        rate: float,
        is_higher_better: bool = True,
        threshold_good: float = 0.95,
        threshold_warning: float = 0.85
    ) -> str:
        """
        達成率からステータスを判定
        
        Args:
            rate: 達成率（0.0-1.0以上）
            is_higher_better: 高い方が良いKPIか
            threshold_good: 良好と判定する閾値
            threshold_warning: 警告と判定する閾値
        
        Returns:
            ステータス（"good", "warning", "poor"）
        """
        if is_higher_better:
            if rate >= threshold_good:
                return "good"
            elif rate >= threshold_warning:
                return "warning"
            else:
                return "poor"
        else:
            # 低い方が良い場合（現在は使用していない）
            if rate <= 1 / threshold_good:
                return "good"
            elif rate <= 1 / threshold_warning:
                return "warning"
            else:
                return "poor"
    
    def _format_achievement_text(self, rate: Optional[float]) -> str:
        """
        達成率を表示用テキストに変換
        
        Args:
            rate: 達成率
        
        Returns:
            表示用テキスト
        """
        if rate is None:
            return "-"
        
        percentage = rate * 100
        
        if percentage >= 95:
            return f"✅ {percentage:.1f}%"
        elif percentage >= 85:
            return f"⚠️ {percentage:.1f}%"
        else:
            return f"❌ {percentage:.1f}%"
    
    def compare_with_previous_period(
        self,
        current_actuals: Dict[str, float],
        previous_actuals: Optional[Dict[str, float]]
    ) -> Dict[str, Any]:
        """
        前期間との比較分析
        
        Args:
            current_actuals: 今期の実績
            previous_actuals: 前期の実績（Noneの場合は比較不可）
        
        Returns:
            比較結果
        """
        result = {
            "has_comparison": previous_actuals is not None,
            "comparisons": {}
        }
        
        if not previous_actuals:
            return result
        
        metrics = ["cost", "conversions", "cpa", "cvr", "ctr"]
        
        for metric in metrics:
            current = current_actuals.get(metric, 0)
            previous = previous_actuals.get(f"actual_{metric}", 0)
            
            if previous > 0:
                change_rate = (current - previous) / previous
                change_abs = current - previous
                
                # 改善判定（CPAは低い方が良い）
                is_improved = change_rate < 0 if metric == "cpa" else change_rate > 0
                
                result["comparisons"][metric] = {
                    "current": current,
                    "previous": previous,
                    "change_rate": change_rate,
                    "change_abs": change_abs,
                    "is_improved": is_improved,
                    "trend_text": self._format_trend_text(change_rate, is_improved)
                }
            else:
                result["comparisons"][metric] = {
                    "current": current,
                    "previous": previous,
                    "change_rate": None,
                    "change_abs": None,
                    "is_improved": None,
                    "trend_text": "比較不可"
                }
        
        return result
    
    def _format_trend_text(self, change_rate: float, is_improved: bool) -> str:
        """
        トレンドテキストのフォーマット
        
        Args:
            change_rate: 変化率
            is_improved: 改善しているか
        
        Returns:
            表示用テキスト
        """
        percentage = abs(change_rate * 100)
        direction = "↑" if change_rate > 0 else "↓"
        
        if is_improved:
            return f"✅ {direction}{percentage:.1f}%"
        else:
            return f"⚠️ {direction}{percentage:.1f}%"
    
    def get_days_in_month(self, year: int, month: int) -> int:
        """
        指定月の日数を取得
        
        Args:
            year: 年
            month: 月
        
        Returns:
            日数
        """
        return calendar.monthrange(year, month)[1]
    
    def parse_year_month(self, year_month: str) -> Tuple[int, int]:
        """
        年月文字列をパース
        
        Args:
            year_month: "2025-10"形式の文字列
        
        Returns:
            (year, month)のタプル
        """
        year, month = map(int, year_month.split('-'))
        return year, month


# 使用例・テスト用コード
if __name__ == "__main__":
    analyzer = AchievementAnalyzer()
    
    print("📊 テスト1: 予算消化ペース分析")
    print("=" * 50)
    
    # ケース1: オンペース
    pace_result = analyzer.analyze_budget_pacing(
        target_budget=500000,
        actual_cost=96774,  # 6日目で約19.4%消化
        current_day=6,
        total_days=31
    )
    print(f"ステータス: {pace_result['pace_status_text']}")
    print(f"進捗率: {pace_result['progress_rate']:.1%}")
    print(f"期待進捗率: {pace_result['expected_progress_rate']:.1%}")
    print(f"月末予測: ¥{pace_result['projected_month_end']:,.0f}")
    
    print("\n📊 テスト2: KPI達成率計算")
    print("=" * 50)
    
    targets = {
        "target_conversions": 500,
        "target_cpa": 10000,
        "target_cvr": 0.03,
        "target_ctr": 0.025
    }
    
    actuals = {
        "conversions": 95,
        "cpa": 10186,
        "cvr": 0.029,
        "ctr": 0.024
    }
    
    kpi_result = analyzer.calculate_kpi_achievement(targets, actuals)
    
    for kpi_name, kpi_data in kpi_result["kpis"].items():
        print(f"\n{kpi_name.upper()}:")
        print(f"  目標: {kpi_data['target']}")
        print(f"  実績: {kpi_data['actual']}")
        print(f"  達成率: {kpi_data['status_text']}")
    
    print("\n📊 テスト3: 前期間比較")
    print("=" * 50)
    
    current = {
        "cost": 96774,
        "conversions": 95,
        "cpa": 10186,
        "cvr": 0.029,
        "ctr": 0.024
    }
    
    previous = {
        "actual_cost": 475000,
        "actual_conversions": 485,
        "actual_cpa": 9794,
        "actual_cvr": 0.029,
        "actual_ctr": 0.024
    }
    
    comparison = analyzer.compare_with_previous_period(current, previous)
    
    if comparison["has_comparison"]:
        for metric, data in comparison["comparisons"].items():
            if data["change_rate"] is not None:
                print(f"{metric}: {data['trend_text']}")
    
    print("\n✅ すべてのテスト完了")