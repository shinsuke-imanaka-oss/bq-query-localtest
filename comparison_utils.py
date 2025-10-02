# comparison_utils.py - 過去比較機能のコア関数群
"""
統合分析レポートの過去比較機能
現在期間と過去期間のデータを比較し、変化率や改善/悪化を分析
"""

import pandas as pd
from datetime import date, timedelta
from typing import Dict, Any, Tuple, Optional
import streamlit as st


# ============================================================
# 1. 比較期間の計算
# ============================================================

def calculate_comparison_period(
    current_start: date, 
    current_end: date, 
    comparison_type: str
) -> Tuple[date, date]:
    """
    比較期間の開始日・終了日を計算
    
    Args:
        current_start: 現在期間の開始日
        current_end: 現在期間の終了日
        comparison_type: "1week", "1month", "3month", "1year"
    
    Returns:
        (比較開始日, 比較終了日) のタプル
    
    Example:
        >>> calculate_comparison_period(date(2024, 11, 1), date(2024, 11, 30), "1month")
        (datetime.date(2024, 10, 1), datetime.date(2024, 10, 30))
    """
    # 比較タイプに応じた日数オフセット
    offset_map = {
        "1week": 7,
        "1month": 30,
        "3month": 90,
        "1year": 365
    }
    
    if comparison_type not in offset_map:
        raise ValueError(
            f"Unknown comparison type: {comparison_type}. "
            f"Valid options: {list(offset_map.keys())}"
        )
    
    days_offset = offset_map[comparison_type]
    
    compare_start = current_start - timedelta(days=days_offset)
    compare_end = current_end - timedelta(days=days_offset)
    
    return compare_start, compare_end


def comparison_type_to_label(comparison_type: str) -> str:
    """
    比較タイプを日本語ラベルに変換
    
    Args:
        comparison_type: "1week", "1month", "3month", "1year"
    
    Returns:
        日本語ラベル文字列
    """
    label_map = {
        "1week": "1週間前",
        "1month": "1ヶ月前",
        "3month": "3ヶ月前",
        "1year": "前年同期"
    }
    
    return label_map.get(comparison_type, comparison_type)


def ui_option_to_comparison_type(ui_option: str) -> str:
    """
    UIの選択肢を内部の比較タイプに変換
    
    Args:
        ui_option: "vs 1週間前", "vs 1ヶ月前", etc.
    
    Returns:
        内部比較タイプ: "1week", "1month", etc.
    """
    mapping = {
        "vs 1週間前": "1week",
        "vs 1ヶ月前": "1month",
        "vs 3ヶ月前": "3month",
        "vs 前年同期": "1year"
    }
    
    return mapping.get(ui_option, "1month")


# ============================================================
# 2. 変化率の計算と判定
# ============================================================

def calculate_metric_change(
    current_value: float, 
    previous_value: float,
    metric_name: str
) -> Dict[str, Any]:
    """
    単一指標の変化を計算
    
    Args:
        current_value: 現在の値
        previous_value: 過去の値
        metric_name: 指標名（コスト系かどうかの判定に使用）
    
    Returns:
        変化情報を含む辞書
    """
    # 0除算の回避
    if previous_value == 0:
        if current_value == 0:
            change_rate = 0.0
        else:
            change_rate = 100.0  # 0から値が発生した場合は+100%
    else:
        change = current_value - previous_value
        change_rate = (change / previous_value) * 100
    
    # 変化方向の判定
    direction = classify_change_direction(change_rate)
    
    # コスト系指標かどうか（減少が良い指標）
    is_cost_metric = metric_name.lower() in ["cost", "cpa", "cpc", "costincludingfees"]
    
    # 良い変化か悪い変化か
    if is_cost_metric:
        is_improvement = change_rate < 0  # コストは減少が良い
    else:
        is_improvement = change_rate > 0  # その他は増加が良い
    
    return {
        "current": current_value,
        "previous": previous_value,
        "change": current_value - previous_value,
        "change_rate": round(change_rate, 1),
        "direction": direction,
        "is_improvement": is_improvement,
        "is_cost_metric": is_cost_metric
    }


def classify_change_direction(change_rate: float) -> str:
    """
    変化率から変化の方向を分類
    
    Args:
        change_rate: 変化率（%）
    
    Returns:
        方向分類: "major_up", "up", "stable", "down", "major_down"
    """
    if change_rate >= 10:
        return "major_up"
    elif change_rate >= 3:
        return "up"
    elif change_rate <= -10:
        return "major_down"
    elif change_rate <= -3:
        return "down"
    else:
        return "stable"


def get_direction_icon(direction: str, is_improvement: bool = None) -> str:
    """
    変化方向に応じたアイコンを取得
    
    Args:
        direction: 変化方向
        is_improvement: 改善かどうか（Noneの場合は方向のみで判定）
    
    Returns:
        アイコン文字列
    """
    if is_improvement is not None:
        # 改善/悪化で色分け
        if is_improvement:
            if direction == "major_up":
                return "🚀"
            elif direction == "up":
                return "↗️"
            elif direction == "major_down":
                return "✅"
            elif direction == "down":
                return "↘️"
        else:
            if direction == "major_up":
                return "⚠️"
            elif direction == "up":
                return "↗️"
            elif direction == "major_down":
                return "📉"
            elif direction == "down":
                return "↘️"
    
    # デフォルトのアイコン
    icon_map = {
        "major_up": "🚀",
        "up": "↗️",
        "stable": "→",
        "down": "↘️",
        "major_down": "⚠️"
    }
    
    return icon_map.get(direction, "→")


# ============================================================
# 3. データフレーム全体の比較
# ============================================================

def calculate_dataframe_changes(
    current_df: pd.DataFrame,
    comparison_df: pd.DataFrame,
    metric_columns: list = None
) -> Dict[str, Any]:
    """
    データフレーム全体の変化を計算
    
    Args:
        current_df: 現在期間のデータフレーム
        comparison_df: 比較期間のデータフレーム
        metric_columns: 比較する指標のカラム名リスト（Noneの場合は数値列すべて）
    
    Returns:
        指標ごとの変化情報を含む辞書
    """
    if current_df is None or comparison_df is None:
        return {}
    
    if current_df.empty or comparison_df.empty:
        return {}
    
    # 比較する指標の決定
    if metric_columns is None:
        # 数値型のカラムを自動検出
        metric_columns = current_df.select_dtypes(include=['number']).columns.tolist()
    
    changes = {}
    
    for metric in metric_columns:
        if metric not in current_df.columns or metric not in comparison_df.columns:
            continue
        
        try:
            # 平均値または合計値を使用（指標によって適切な方を選択）
            if metric.lower() in ['cvr', 'ctr', 'roas', 'cpa', 'cpc']:
                # 率やコスト系は平均
                current_val = current_df[metric].mean()
                previous_val = comparison_df[metric].mean()
            else:
                # 絶対数は合計
                current_val = current_df[metric].sum()
                previous_val = comparison_df[metric].sum()
            
            # 変化の計算
            change_info = calculate_metric_change(current_val, previous_val, metric)
            changes[metric] = change_info
            
        except Exception as e:
            st.warning(f"指標 {metric} の比較計算でエラー: {e}")
            continue
    
    return changes


# ============================================================
# 4. 改善/悪化指標の分類
# ============================================================

def classify_metrics_by_performance(changes: Dict[str, Any]) -> Dict[str, list]:
    """
    指標を改善/悪化/横ばいに分類
    
    Args:
        changes: calculate_dataframe_changes() の戻り値
    
    Returns:
        improved, declined, stable の3つのリストを含む辞書
    """
    improved = []
    declined = []
    stable = []
    
    for metric, data in changes.items():
        if data["direction"] == "stable":
            stable.append(metric)
        elif data["is_improvement"]:
            improved.append(metric)
        else:
            declined.append(metric)
    
    return {
        "improved": improved,
        "declined": declined,
        "stable": stable
    }


# ============================================================
# 5. サマリー生成
# ============================================================

def generate_comparison_summary(
    changes: Dict[str, Any],
    classification: Dict[str, list]
) -> str:
    """
    比較結果の簡潔なサマリーテキストを生成
    
    Args:
        changes: 変化情報
        classification: 分類結果
    
    Returns:
        サマリーテキスト
    """
    improved_count = len(classification["improved"])
    declined_count = len(classification["declined"])
    stable_count = len(classification["stable"])
    
    summary_parts = []
    
    if improved_count > 0:
        summary_parts.append(f"📈 {improved_count}個の指標が改善")
    
    if declined_count > 0:
        summary_parts.append(f"📉 {declined_count}個の指標が悪化")
    
    if stable_count > 0:
        summary_parts.append(f"→ {stable_count}個の指標が横ばい")
    
    return " / ".join(summary_parts) if summary_parts else "データなし"


def create_comparison_table_data(changes: Dict[str, Any]) -> list:
    """
    比較テーブル表示用のデータを作成
    
    Args:
        changes: 変化情報
    
    Returns:
        テーブル表示用のリスト
    """
    table_data = []
    
    for metric, data in changes.items():
        icon = get_direction_icon(data["direction"], data["is_improvement"])
        
        # 値のフォーマット（小数点の桁数は指標によって調整）
        if metric.lower() in ['cvr', 'ctr']:
            current_str = f"{data['current']:.2%}"
            previous_str = f"{data['previous']:.2%}"
        elif metric.lower() in ['cost', 'costincludingfees', 'cpa']:
            current_str = f"¥{data['current']:,.0f}"
            previous_str = f"¥{data['previous']:,.0f}"
        else:
            current_str = f"{data['current']:,.1f}"
            previous_str = f"{data['previous']:,.1f}"
        
        table_data.append({
            "指標": metric.upper(),
            "現在": current_str,
            "前回": previous_str,
            "変化": f"{icon} {data['change_rate']:+.1f}%"
        })
    
    return table_data


# ============================================================
# 6. エクスポート用データ生成
# ============================================================

def prepare_comparison_export_data(
    current_period: str,
    comparison_period: str,
    changes: Dict[str, Any],
    classification: Dict[str, list]
) -> Dict[str, Any]:
    """
    エクスポート用の比較データを準備
    
    Args:
        current_period: 現在期間のラベル
        comparison_period: 比較期間のラベル
        changes: 変化情報
        classification: 分類結果
    
    Returns:
        エクスポート用データ辞書
    """
    return {
        "comparison_metadata": {
            "current_period": current_period,
            "comparison_period": comparison_period,
            "generated_at": pd.Timestamp.now().isoformat()
        },
        "metric_changes": changes,
        "classification": classification,
        "summary": generate_comparison_summary(changes, classification)
    }


# ============================================================
# 7. ユーティリティ関数
# ============================================================

def format_change_rate(change_rate: float) -> str:
    """
    変化率を読みやすくフォーマット
    
    Args:
        change_rate: 変化率（%）
    
    Returns:
        フォーマット済み文字列
    """
    if change_rate > 0:
        return f"+{change_rate:.1f}%"
    else:
        return f"{change_rate:.1f}%"


def validate_comparison_data(
    current_data: Any,
    comparison_data: Any
) -> Tuple[bool, str]:
    """
    比較データの妥当性を検証
    
    Args:
        current_data: 現在期間のデータ
        comparison_data: 比較期間のデータ
    
    Returns:
        (検証OK, エラーメッセージ) のタプル
    """
    # データの存在チェック
    if current_data is None:
        return False, "現在期間のデータがありません"
    
    if comparison_data is None:
        return False, "比較期間のデータがありません"
    
    # DataFrameの場合は空チェック
    if isinstance(current_data, pd.DataFrame) and current_data.empty:
        return False, "現在期間のデータが空です"
    
    if isinstance(comparison_data, pd.DataFrame) and comparison_data.empty:
        return False, "比較期間のデータが空です"
    
    return True, ""


# ============================================================
# 8. テスト用関数
# ============================================================

if __name__ == "__main__":
    # 簡易テスト
    print("🧪 comparison_utils.py テスト開始")
    
    # テスト1: 期間計算
    current_start = date(2024, 11, 1)
    current_end = date(2024, 11, 30)
    compare_start, compare_end = calculate_comparison_period(
        current_start, current_end, "1month"
    )
    print(f"✅ 期間計算: {compare_start} 〜 {compare_end}")
    
    # テスト2: 変化率計算
    change_info = calculate_metric_change(234, 198, "cvr")
    print(f"✅ 変化率計算: {change_info['change_rate']}%")
    
    # テスト3: 方向分類
    direction = classify_change_direction(15.5)
    print(f"✅ 方向分類: {direction}")
    
    # テスト4: アイコン取得
    icon = get_direction_icon("major_up", True)
    print(f"✅ アイコン: {icon}")
    
    print("🎉 すべてのテストが完了しました！")