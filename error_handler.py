# error_handler.py - 構文エラー修正版
"""
強化エラーハンドリング機能
- 詳細なエラー分析と分類
- 具体的な解決策の提示
- エラーの自動修正機能
- エラー履歴の追跡
"""

import streamlit as st
import pandas as pd
import re
import traceback
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import json

class EnhancedErrorHandler:
    """強化されたエラーハンドリングクラス"""
    
    def __init__(self):
        # エラーパターンと解決策のマッピング
        self.error_patterns = {
            # BigQuery SQLエラー
            "syntax_error": {
                "patterns": ["Syntax error", "syntax", "Expected", "Unexpected"],
                "category": "SQL構文エラー",
                "severity": "high",
                "solutions": [
                    "SQL構文を確認してください",
                    "カンマ、括弧、クォートの対応を確認してください",
                    "予約語の使用方法を確認してください"
                ]
            },
            
            "table_not_found": {
                "patterns": ["not found", "does not exist", "Table", "Dataset"],
                "category": "テーブル・データセットエラー",
                "severity": "high",
                "solutions": [
                    "テーブル名のスペルを確認してください",
                    "データセット名が正しいか確認してください",
                    "テーブルへのアクセス権限を確認してください"
                ]
            },
            
            "column_not_found": {
                "patterns": ["Unrecognized name", "Column", "field"],
                "category": "列名エラー",
                "severity": "medium",
                "solutions": [
                    "列名のスペルを確認してください",
                    "列名は大文字小文字を区別します",
                    "利用可能な列名をDESCRIBEクエリで確認してください"
                ]
            },
            
            "type_mismatch": {
                "patterns": ["type", "cannot be coerced", "CAST", "conversion"],
                "category": "データ型エラー",
                "severity": "medium",
                "solutions": [
                    "CAST関数を使用して適切な型に変換してください",
                    "NULL値の処理を追加してください",
                    "SAFE_CAST関数の使用を検討してください"
                ]
            },
            
            "aggregate_error": {
                "patterns": ["GROUP BY", "aggregate", "must be grouped"],
                "category": "集約関数エラー",
                "severity": "medium",
                "solutions": [
                    "GROUP BY句に必要な列を追加してください",
                    "集約関数を適切に使用してください",
                    "非集約列をGROUP BY句に含めてください"
                ]
            },
            
            "quota_exceeded": {
                "patterns": ["quota", "exceeded", "limit", "timeout"],
                "category": "リソース制限エラー",
                "severity": "high",
                "solutions": [
                    "クエリの範囲を狭めてください（日付条件の追加）",
                    "LIMIT句を使用して結果数を制限してください",
                    "必要な列のみを選択してください"
                ]
            },
            
            "permission_error": {
                "patterns": ["permission", "access", "denied", "forbidden"],
                "category": "権限エラー",
                "severity": "high",
                "solutions": [
                    "データセットへのアクセス権限を確認してください",
                    "BigQueryプロジェクトの設定を確認してください",
                    "管理者に権限の付与を依頼してください"
                ]
            },
            
            "api_error": {
                "patterns": ["API", "connection", "network", "timeout"],
                "category": "API・接続エラー",
                "severity": "high",
                "solutions": [
                    "ネットワーク接続を確認してください",
                    "API認証情報を確認してください",
                    "しばらく時間をおいて再試行してください"
                ]
            }
        }
        
        # エラー履歴
        if "error_history" not in st.session_state:
            st.session_state.error_history = []
    
    def analyze_error(self, error_message: str, error_context: Dict = None) -> Dict[str, Any]:
        """エラーの詳細分析"""
        error_context = error_context or {}
        
        # エラーパターンのマッチング
        matched_pattern = self._match_error_pattern(error_message)
        
        # エラーの分類と詳細分析
        analysis = {
            "original_message": error_message,
            "pattern_match": matched_pattern,
            "category": matched_pattern["category"] if matched_pattern else "不明なエラー",
            "severity": matched_pattern["severity"] if matched_pattern else "medium",
            "timestamp": datetime.now(),
            "context": error_context,
            "solutions": matched_pattern["solutions"] if matched_pattern else ["エラーメッセージを確認し、一般的なSQL構文ルールに従ってください"],
            "auto_fix_suggestions": self._generate_auto_fix_suggestions(error_message, error_context)
        }
        
        # エラー履歴に追加
        self._add_to_history(analysis)
        
        return analysis
    
    def _match_error_pattern(self, error_message: str) -> Optional[Dict]:
        """エラーメッセージとパターンのマッチング"""
        error_lower = error_message.lower()
        
        for pattern_name, pattern_info in self.error_patterns.items():
            for pattern in pattern_info["patterns"]:
                if pattern.lower() in error_lower:
                    return pattern_info
        
        return None
    
    def _generate_auto_fix_suggestions(self, error_message: str, context: Dict) -> List[Dict]:
        """自動修正提案の生成"""
        suggestions = []
        
        # SQL文の取得
        sql = context.get("sql", "")
        user_input = context.get("user_input", "")
        
        # 一般的な修正パターン
        if "syntax error" in error_message.lower():
            suggestions.extend(self._suggest_syntax_fixes(sql))
        
        if "not found" in error_message.lower() and "table" in error_message.lower():
            suggestions.extend(self._suggest_table_fixes(sql))
        
        if "unrecognized name" in error_message.lower():
            suggestions.extend(self._suggest_column_fixes(sql, error_message))
        
        if "group by" in error_message.lower():
            suggestions.extend(self._suggest_groupby_fixes(sql))
        
        if "cast" in error_message.lower() or "type" in error_message.lower():
            suggestions.extend(self._suggest_type_fixes(sql))
        
        return suggestions
    
    def _suggest_syntax_fixes(self, sql: str) -> List[Dict]:
        """構文エラーの修正提案"""
        suggestions = []
        
        # 一般的な構文問題のチェック
        issues = []
        
        # カンマの問題
        if sql.count("SELECT") > 0:
            # SELECT句での末尾カンマチェック
            select_match = re.search(r'SELECT\s+.*?FROM', sql, re.IGNORECASE | re.DOTALL)
            if select_match:
                select_part = select_match.group()
                if re.search(r',\s*FROM', select_part, re.IGNORECASE):
                    issues.append("SELECT句の末尾に不要なカンマがあります")
        
        # 括弧の対応チェック
        open_parens = sql.count('(')
        close_parens = sql.count(')')
        if open_parens != close_parens:
            issues.append(f"括弧の対応が取れていません（開括弧: {open_parens}, 閉括弧: {close_parens}）")
        
        # クォートの対応チェック
        single_quotes = sql.count("'")
        if single_quotes % 2 != 0:
            issues.append("シングルクォートの対応が取れていません")
        
        # 修正提案の生成
        for issue in issues:
            suggestions.append({
                "type": "syntax_fix",
                "description": issue,
                "priority": "high",
                "auto_fixable": False
            })
        
        return suggestions
    
    def _suggest_table_fixes(self, sql: str) -> List[Dict]:
        """テーブル関連エラーの修正提案"""
        suggestions = []
        
        # テーブル名のパターン抽出
        table_patterns = re.findall(r'FROM\s+`?([^`\s]+)`?', sql, re.IGNORECASE)
        
        for table in table_patterns:
            # 正しいテーブル名の推測
            if "." not in table:
                suggestions.append({
                    "type": "table_fix",
                    "description": f"テーブル名 '{table}' にプロジェクト・データセット名が不足している可能性があります",
                    "suggested_fix": f"FROM `project.dataset.{table}`",
                    "priority": "high",
                    "auto_fixable": True
                })
            
            # 一般的なテーブル名の間違い
            common_fixes = {
                "campaign": "LookerStudio_report_campaign",
                "report": "LookerStudio_report_campaign"
            }
            
            for wrong, correct in common_fixes.items():
                if wrong in table.lower():
                    suggestions.append({
                        "type": "table_fix",
                        "description": f"テーブル名 '{table}' は '{correct}' の可能性があります",
                        "suggested_fix": table.replace(wrong, correct),
                        "priority": "medium",
                        "auto_fixable": True
                    })
        
        return suggestions
    
    def _suggest_column_fixes(self, sql: str, error_message: str) -> List[Dict]:
        """列名エラーの修正提案"""
        suggestions = []
        
        # エラーメッセージから列名を抽出
        column_match = re.search(r"name:\s*([^\s;]+)", error_message)
        if column_match:
            problematic_column = column_match.group(1)
            
            # 一般的な列名の修正提案
            common_column_fixes = {
                "cost": "CostIncludingFees",
                "click": "Clicks",
                "impression": "Impressions",
                "conversion": "Conversions",
                "ctr": "Clicks / Impressions * 100",
                "cpa": "CostIncludingFees / Conversions",
                "cpc": "CostIncludingFees / Clicks",
                "cvr": "Conversions / Clicks * 100",
                "roas": "ConversionValue / CostIncludingFees"
            }
            
            for wrong, correct in common_column_fixes.items():
                if wrong.lower() in problematic_column.lower():
                    suggestions.append({
                        "type": "column_fix",
                        "description": f"列名 '{problematic_column}' は '{correct}' の可能性があります",
                        "suggested_fix": correct,
                        "priority": "high",
                        "auto_fixable": True
                    })
        
        return suggestions
    
    def _suggest_groupby_fixes(self, sql: str) -> List[Dict]:
        """GROUP BY関連エラーの修正提案"""
        suggestions = []
        
        # SELECT句の列を抽出
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_clause = select_match.group(1)
            
            # 集約関数以外の列を特定
            non_aggregate_columns = []
            aggregate_functions = ['SUM', 'COUNT', 'AVG', 'MAX', 'MIN', 'STDDEV']
            
            # 簡易的な列抽出（実際はより複雑な解析が必要）
            columns = [col.strip() for col in select_clause.split(',')]
            
            for col in columns:
                col_upper = col.upper()
                is_aggregate = any(func in col_upper for func in aggregate_functions)
                
                if not is_aggregate and 'AS ' not in col_upper:
                    # 計算列でない場合はGROUP BYが必要
                    # 修正: column_name = col.split('.')[- ] → column_name = col.split('.')[-1]
                    column_name = col.split('.')[-1]  # 最後の要素を取得
                    if column_name not in ['*']:
                        non_aggregate_columns.append(column_name)
            
            if non_aggregate_columns:
                suggestions.append({
                    "type": "groupby_fix",
                    "description": "GROUP BY句に非集約列を追加する必要があります",
                    "suggested_fix": f"GROUP BY {', '.join(non_aggregate_columns)}",
                    "priority": "high",
                    "auto_fixable": True
                })
        
        return suggestions
    
    def _suggest_type_fixes(self, sql: str) -> List[Dict]:
        """データ型関連エラーの修正提案"""
        suggestions = []
        
        # SAFE_DIVIDE等の安全な関数の使用を提案
        if "/" in sql:
            suggestions.append({
                "type": "type_fix",
                "description": "除算でゼロ除算エラーが発生している可能性があります",
                "suggested_fix": "SAFE_DIVIDE(分子, 分母) を使用してください",
                "priority": "medium",
                "auto_fixable": False
            })
        
        # NULL値処理の提案
        suggestions.append({
            "type": "type_fix",
            "description": "NULL値が原因でデータ型エラーが発生している可能性があります",
            "suggested_fix": "COALESCE(列名, デフォルト値) や IS NOT NULL を使用してください",
            "priority": "medium",
            "auto_fixable": False
        })
        
        return suggestions
    
    def _add_to_history(self, analysis: Dict):
        """エラー履歴への追加"""
        st.session_state.error_history.append(analysis)
        
        # 履歴の上限制御
        if len(st.session_state.error_history) > 50:
            st.session_state.error_history = st.session_state.error_history[-50:]
    
    def apply_auto_fix(self, original_sql: str, fix_suggestion: Dict) -> str:
        """自動修正の適用"""
        if not fix_suggestion.get("auto_fixable", False):
            return original_sql
        
        fix_type = fix_suggestion["type"]
        suggested_fix = fix_suggestion.get("suggested_fix", "")
        
        if fix_type == "table_fix":
            # テーブル名の修正
            if "." not in original_sql and "`" not in original_sql:
                # プロジェクト・データセット名の追加
                corrected_sql = re.sub(
                    r'FROM\s+(\w+)',
                    f'FROM `vorn-digi-mktg-poc-635a.toki_air.\\1`',
                    original_sql,
                    flags=re.IGNORECASE
                )
                return corrected_sql
        
        elif fix_type == "groupby_fix":
            # GROUP BY句の追加
            if "GROUP BY" not in original_sql.upper():
                return original_sql + "\n" + suggested_fix
        
        return original_sql
    
    def get_error_statistics(self) -> Dict[str, Any]:
        """エラー統計の取得"""
        if not st.session_state.error_history:
            return {"total_errors": 0}
        
        history = st.session_state.error_history
        
        # カテゴリ別エラー数
        category_counts = {}
        severity_counts = {}
        
        for error in history:
            category = error["category"]
            severity = error["severity"]
            
            category_counts[category] = category_counts.get(category, 0) + 1
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
        
        # 最頻出エラー
        most_common_category = max(category_counts.items(), key=lambda x: x[1]) if category_counts else None
        
        return {
            "total_errors": len(history),
            "category_distribution": category_counts,
            "severity_distribution": severity_counts,
            "most_common_error": most_common_category,
            "recent_errors": history[-5:]  # 最新5件
        }

# =========================================================================
# エラー表示・処理用のStreamlit関数
# =========================================================================

def show_enhanced_error_message(error: Exception, context: Dict = None):
    """強化されたエラーメッセージの表示"""
    handler = EnhancedErrorHandler()
    
    # エラーの分析
    error_analysis = handler.analyze_error(str(error), context)
    
    # エラーの重要度に応じた表示
    severity = error_analysis["severity"]
    category = error_analysis["category"]
    
    if severity == "high":
        st.error(f"🔴 **{category}**")
    elif severity == "medium":
        st.warning(f"🟡 **{category}**")
    else:
        st.info(f"ℹ️ **{category}**")
    
    # エラーメッセージの表示
    with st.expander("📋 エラー詳細", expanded=True):
        st.text(error_analysis["original_message"])
    
    # 解決策の表示
    st.markdown("### 💡 解決策")
    for i, solution in enumerate(error_analysis["solutions"], 1):
        st.markdown(f"{i}. {solution}")
    
    # 自動修正提案の表示
    if error_analysis["auto_fix_suggestions"]:
        st.markdown("### 🔧 自動修正提案")
        
        for suggestion in error_analysis["auto_fix_suggestions"]:
            priority_icon = "🔴" if suggestion["priority"] == "high" else "🟡" if suggestion["priority"] == "medium" else "🟢"
            
            st.markdown(f"**{priority_icon} {suggestion['description']}**")
            
            if suggestion.get("suggested_fix"):
                st.code(suggestion["suggested_fix"], language="sql")
            
            # 自動修正ボタン
            if suggestion.get("auto_fixable", False) and context and context.get("sql"):
                if st.button(f"🚀 この修正を適用", key=f"fix_{hash(suggestion['description'])}"):
                    fixed_sql = handler.apply_auto_fix(context["sql"], suggestion)
                    st.session_state.sql = fixed_sql
                    st.session_state.editable_sql = fixed_sql
                    st.success("✅ 修正が適用されました！SQLを再実行してください。")
                    st.rerun()

def show_error_dashboard():
    """エラーダッシュボードの表示"""
    handler = EnhancedErrorHandler()
    stats = handler.get_error_statistics()
    
    if stats["total_errors"] == 0:
        st.success("🎉 エラー履歴はありません！")
        return
    
    st.markdown("### 📊 エラー統計ダッシュボード")
    
    # 基本統計
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("総エラー数", stats["total_errors"])
    
    with col2:
        if stats.get("most_common_error"):
            st.metric("最頻出エラー", stats["most_common_error"][0])
    
    with col3:
        high_severity = stats.get("severity_distribution", {}).get("high", 0)
        st.metric("重要エラー数", high_severity)
    
    # エラーカテゴリ分布
    if stats.get("category_distribution"):
        st.markdown("#### 📋 エラーカテゴリ分布")
        
        try:
            import plotly.express as px
            
            categories = list(stats["category_distribution"].keys())
            counts = list(stats["category_distribution"].values())
            
            fig = px.pie(
                values=counts,
                names=categories,
                title="エラーカテゴリ別分布"
            )
            st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            # plotlyが利用できない場合の代替表示
            for category, count in stats["category_distribution"].items():
                st.text(f"{category}: {count}件")
    
    # 最近のエラー履歴
    if stats.get("recent_errors"):
        st.markdown("#### 🕒 最近のエラー履歴")
        
        for error in reversed(stats["recent_errors"]):
            timestamp = error["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            severity_icon = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(error["severity"], "❓")
            
            with st.expander(f"{severity_icon} {timestamp} - {error['category']}"):
                st.text(error["original_message"])
                
                if error.get("solutions"):
                    st.markdown("**提案された解決策:**")
                    for solution in error["solutions"]:
                        st.markdown(f"• {solution}")

# グローバルインスタンス
error_handler = EnhancedErrorHandler()

# =========================================================================
# 既存コードとの互換性を保つための関数
# =========================================================================

def handle_analysis_error(error: Exception, sql: str = "", operation: str = "") -> bool:
    """分析処理でのエラーハンドリング（既存コードとの統合用）"""
    context = {
        "sql": sql,
        "operation": operation,
        "timestamp": datetime.now()
    }
    
    # 強化されたエラー表示を使用
    show_enhanced_error_message(error, context)
    
    # エラーが重大かどうかを判定（処理続行の可否）
    handler = EnhancedErrorHandler()
    error_analysis = handler.analyze_error(str(error), context)
    
    # critical または high の場合は処理を停止すべき
    return error_analysis.get("severity") not in ["critical", "high"]

def log_error_to_history(error_message: str, error_category: str = "一般エラー", solutions: List[str] = None):
    """エラーを履歴に追加（既存コードとの互換性用）"""
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
# レガシー関数サポート（analysis_logic.pyとの互換性）
# =========================================================================

def show_error_with_solutions(error: Exception, context: Dict[str, Any] = None):
    """エラーを解決策付きで表示（レガシー互換）"""
    # 新しい強化されたエラー表示を使用
    show_enhanced_error_message(error, context)

# =========================================================================
# その他のユーティリティ関数
# =========================================================================

def get_error_summary() -> Dict[str, Any]:
    """エラーサマリーの取得"""
    handler = EnhancedErrorHandler()
    return handler.get_error_statistics()

def clear_error_history():
    """エラー履歴のクリア"""
    if "error_history" in st.session_state:
        st.session_state.error_history = []
        st.success("✅ エラー履歴をクリアしました")

def export_error_log() -> str:
    """エラーログのエクスポート"""
    if not st.session_state.get("error_history"):
        return json.dumps({"message": "エラー履歴がありません"}, ensure_ascii=False, indent=2)
    
    export_data = {
        "export_timestamp": datetime.now().isoformat(),
        "total_errors": len(st.session_state.error_history),
        "errors": [
            {
                "timestamp": error.get("timestamp", datetime.now()).isoformat(),
                "category": error.get("category", "不明"),
                "message": error.get("original_message", ""),
                "solutions": error.get("solutions", [])
            }
            for error in st.session_state.error_history
        ]
    }
    
    return json.dumps(export_data, ensure_ascii=False, indent=2, default=str)