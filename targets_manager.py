"""
targets_manager.py
目標・予算管理モジュール

機能:
- 月次目標・予算の設定・保存・取得
- 前月実績の参照
- JSON形式でのデータ永続化
"""

import json
import os
from datetime import datetime
from typing import Dict, Optional, Any
from pathlib import Path


class TargetsManager:
    """目標・予算管理クラス"""
    
    def __init__(self, config_path: str = "targets_config.json"):
        """
        初期化
        
        Args:
            config_path: 目標設定ファイルのパス（デフォルト: ルート直下）
        """
        self.config_path = Path(config_path)
        self.data = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        設定ファイルを読み込み
        
        Returns:
            設定データ（存在しない場合は初期化）
        """
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                print(f"⚠️ 設定ファイルの読み込みに失敗しました。初期化します。")
                return self._initialize_config()
        else:
            return self._initialize_config()
    
    def _initialize_config(self) -> Dict[str, Any]:
        """
        初期設定データを作成
        
        Returns:
            初期化された設定データ
        """
        return {
            "targets": {},
            "history": {},
            "last_updated": datetime.now().isoformat()
        }
    
    def _save_config(self) -> bool:
        """
        設定をファイルに保存
        
        Returns:
            保存成功時True
        """
        try:
            self.data["last_updated"] = datetime.now().isoformat()
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"❌ 設定の保存に失敗しました: {e}")
            return False
    
    def set_targets(
        self,
        year_month: str,
        budget: Optional[float] = None,
        target_conversions: Optional[int] = None,
        target_cpa: Optional[float] = None,
        target_cvr: Optional[float] = None,
        target_ctr: Optional[float] = None,
        notes: str = ""
    ) -> bool:
        """
        目標を設定
        
        Args:
            year_month: 対象年月（例: "2025-10"）
            budget: 月間予算（円）
            target_conversions: 目標CV数
            target_cpa: 目標CPA（円）
            target_cvr: 目標CVR（0.03 = 3%）
            target_ctr: 目標CTR（0.025 = 2.5%）
            notes: メモ
        
        Returns:
            設定成功時True
        """
        if "targets" not in self.data:
            self.data["targets"] = {}
        
        # 既存の目標があれば上書き警告
        if year_month in self.data["targets"]:
            print(f"⚠️ {year_month}の目標は既に設定されています。上書きします。")
        
        self.data["targets"][year_month] = {
            "budget": budget,
            "target_conversions": target_conversions,
            "target_cpa": target_cpa,
            "target_cvr": target_cvr,
            "target_ctr": target_ctr,
            "set_date": datetime.now().isoformat(),
            "notes": notes
        }
        
        return self._save_config()
    
    def get_targets(self, year_month: str) -> Optional[Dict[str, Any]]:
        """
        目標を取得
        
        Args:
            year_month: 対象年月（例: "2025-10"）
        
        Returns:
            目標データ（存在しない場合はNone）
        """
        return self.data.get("targets", {}).get(year_month)
    
    def has_targets(self, year_month: str) -> bool:
        """
        目標が設定されているか確認
        
        Args:
            year_month: 対象年月
        
        Returns:
            設定済みの場合True
        """
        targets = self.get_targets(year_month)
        if not targets:
            return False
        
        # いずれかの目標が設定されていればTrue
        return any([
            targets.get("budget"),
            targets.get("target_conversions"),
            targets.get("target_cpa"),
            targets.get("target_cvr"),
            targets.get("target_ctr")
        ])
    
    def save_actual_results(
        self,
        year_month: str,
        actual_cost: float,
        actual_conversions: int,
        actual_cpa: float,
        actual_cvr: float,
        actual_ctr: float,
        additional_metrics: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        実績を保存（月次レポート生成時に自動保存）
        
        Args:
            year_month: 対象年月
            actual_cost: 実際のコスト
            actual_conversions: 実際のCV数
            actual_cpa: 実際のCPA
            actual_cvr: 実際のCVR
            actual_ctr: 実際のCTR
            additional_metrics: その他の指標
        
        Returns:
            保存成功時True
        """
        if "history" not in self.data:
            self.data["history"] = {}
        
        self.data["history"][year_month] = {
            "actual_cost": actual_cost,
            "actual_conversions": actual_conversions,
            "actual_cpa": actual_cpa,
            "actual_cvr": actual_cvr,
            "actual_ctr": actual_ctr,
            "recorded_date": datetime.now().isoformat()
        }
        
        if additional_metrics:
            self.data["history"][year_month].update(additional_metrics)
        
        return self._save_config()
    
    def get_previous_actual(self, year_month: str) -> Optional[Dict[str, Any]]:
        """
        前月の実績を取得
        
        Args:
            year_month: 基準年月（例: "2025-10"）
        
        Returns:
            前月の実績データ（存在しない場合はNone）
        """
        try:
            year, month = map(int, year_month.split('-'))
            
            # 前月を計算
            if month == 1:
                prev_year = year - 1
                prev_month = 12
            else:
                prev_year = year
                prev_month = month - 1
            
            prev_year_month = f"{prev_year}-{prev_month:02d}"
            
            return self.data.get("history", {}).get(prev_year_month)
        
        except Exception as e:
            print(f"⚠️ 前月実績の取得に失敗しました: {e}")
            return None
    
    def calculate_daily_pace(
        self,
        year_month: str,
        current_day: int,
        total_days: int
    ) -> Optional[Dict[str, float]]:
        """
        日次ペースを計算
        
        Args:
            year_month: 対象年月
            current_day: 現在の日（1-31）
            total_days: 月の総日数
        
        Returns:
            ペース情報（目標未設定の場合はNone）
        """
        targets = self.get_targets(year_month)
        if not targets or not targets.get("budget"):
            return None
        
        expected_progress = current_day / total_days
        daily_budget_target = targets["budget"] / total_days
        
        return {
            "expected_progress_rate": expected_progress,
            "daily_budget_target": daily_budget_target,
            "expected_budget_to_date": daily_budget_target * current_day
        }
    
    def delete_targets(self, year_month: str) -> bool:
        """
        目標を削除
        
        Args:
            year_month: 対象年月
        
        Returns:
            削除成功時True
        """
        if year_month in self.data.get("targets", {}):
            del self.data["targets"][year_month]
            return self._save_config()
        return False
    
    def list_all_targets(self) -> Dict[str, Any]:
        """
        すべての目標を取得
        
        Returns:
            全目標データ
        """
        return self.data.get("targets", {})
    
    def list_all_history(self) -> Dict[str, Any]:
        """
        すべての実績履歴を取得
        
        Returns:
            全実績データ
        """
        return self.data.get("history", {})


# 使用例・テスト用コード
if __name__ == "__main__":
    # インスタンス作成
    manager = TargetsManager()
    
    # 目標設定
    print("📝 目標を設定します...")
    success = manager.set_targets(
        year_month="2025-10",
        budget=500000,
        target_conversions=500,
        target_cpa=10000,
        target_cvr=0.03,
        target_ctr=0.025,
        notes="10月の目標設定"
    )
    print(f"✅ 設定完了: {success}")
    
    # 目標取得
    print("\n📊 目標を取得します...")
    targets = manager.get_targets("2025-10")
    print(f"取得結果: {targets}")
    
    # 目標確認
    print(f"\n🔍 目標設定済み: {manager.has_targets('2025-10')}")
    
    # 日次ペース計算
    print("\n⏱️ 日次ペースを計算します...")
    pace = manager.calculate_daily_pace("2025-10", current_day=6, total_days=31)
    print(f"ペース情報: {pace}")
    
    # 実績保存（サンプル）
    print("\n💾 実績を保存します...")
    manager.save_actual_results(
        year_month="2025-09",
        actual_cost=475000,
        actual_conversions=485,
        actual_cpa=9794,
        actual_cvr=0.029,
        actual_ctr=0.024
    )
    
    # 前月実績取得
    print("\n📈 前月実績を取得します...")
    prev_actual = manager.get_previous_actual("2025-10")
    print(f"前月実績: {prev_actual}")
    
    print("\n✅ すべてのテスト完了")