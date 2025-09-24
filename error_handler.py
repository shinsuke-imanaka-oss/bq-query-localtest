# error_handler.py - 完全修正版
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
                "patterns": ["API", "authentication", "key", "unauthorized"],
                "category": "API認証エラー",
                "severity": "high",
                "solutions": [
                    "APIキーが正しく設定されているか確認してください",
                    "認証情報の有効期限を確認してください",
                    "ネットワーク接続を確認してください"
                ]
            }
        }
    
    def analyze_error(self, error_message: str, context: Dict = None) -> Dict:
        """エラーの詳細分析"""
        if context is None:
            context = {}
            
        # エラーパターンのマッチング
        pattern_match = self._match_error_pattern(error_message)
        
        # 自動修正提案の生成
        auto_fix_suggestions = self._generate_auto_fix_suggestions(error_message, context)
        
        # エラー重要度の判定
        severity = pattern_match["severity"] if pattern_match else "medium"
        
        return {
            "original_message": error_message,
            "category": pattern_match["category"] if pattern_match else "未分類エラー",
            "severity": severity,
            "solutions": pattern_match["solutions"] if pattern_match else ["エラーメッセージを確認してください"],
            "auto_fix_suggestions": auto_fix_suggestions,
            "context": context,
            "timestamp": datetime.now()
        }
    
    def _match_error_pattern(self, error_message: str) -> Optional[Dict]:
        """エラーパターンのマッチング"""
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
                    "description": f"テーブル名 '{table}' にプロジェクトIDとデータセット名を追加してください",
                    "suggestion": f"`project_id.dataset_id.{table}`",
                    "priority": "high",
                    "auto_fixable": True
                })
            
            # 一般的なテーブル名の修正提案
            common_corrections = {
                "campaign": "LookerStudio_report_campaign",
                "age_group": "LookerStudio_report_age_group",
                "gender": "LookerStudio_report_gender"
            }
            
            for wrong, correct in common_corrections.items():
                if wrong in table.lower():
                    suggestions.append({
                        "type": "table_fix",
                        "description": f"テーブル名を '{correct}' に修正することを推奨します",
                        "suggestion": f"`vorn-digi-mktg-poc-635a.toki_air.{correct}`",
                        "priority": "medium",
                        "auto_fixable": True
                    })
        
        return suggestions
    
    def _suggest_column_fixes(self, sql: str, error_message: str) -> List[Dict]:
        """列名エラーの修正提案"""
        suggestions = []
        
        # エラーメッセージから未認識の列名を抽出
        column_match = re.search(r'Unrecognized name:\s*([^\s;]+)', error_message, re.IGNORECASE)
        if column_match:
            unrecognized_column = column_match.group(1)
            
            # 一般的な列名の修正提案
            common_columns = {
                "cost": "CostIncludingFees",
                "clicks": "Clicks", 
                "impressions": "Impressions",
                "conversions": "Conversions",
                "campaign": "CampaignName",
                "date": "Date"
            }
            
            for wrong, correct in common_columns.items():
                if wrong.lower() in unrecognized_column.lower():
                    suggestions.append({
                        "type": "column_fix",
                        "description": f"列名 '{unrecognized_column}' を '{correct}' に修正してください",
                        "suggestion": correct,
                        "priority": "high",
                        "auto_fixable": True
                    })
        
        return suggestions
    
    def _suggest_groupby_fixes(self, sql: str) -> List[Dict]:
        """GROUP BY関連エラーの修正提案"""
        suggestions = []
        
        # SELECT句の列とGROUP BY句の列を比較
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        groupby_match = re.search(r'GROUP\s+BY\s+(.*?)(?:\s+ORDER|\s+LIMIT|\s*$)', sql, re.IGNORECASE | re.DOTALL)
        
        if select_match and groupby_match:
            select_columns = [col.strip() for col in select_match.group(1).split(',')]
            groupby_columns = [col.strip() for col in groupby_match.group(1).split(',')]
            
            # 集約関数を使用していない列をチェック
            non_aggregate_columns = []
            for col in select_columns:
                if not re.search(r'(SUM|COUNT|AVG|MAX|MIN|STDDEV)\s*\(', col, re.IGNORECASE):
                    non_aggregate_columns.append(col)
            
            # GROUP BYに含まれていない非集約列を特定
            missing_columns = []
            for col in non_aggregate_columns:
                col_clean = re.sub(r'\s+AS\s+\w+', '', col, flags=re.IGNORECASE).strip()
                if col_clean not in groupby_columns:
                    missing_columns.append(col_clean)
            
            if missing_columns:
                suggestions.append({
                    "type": "groupby_fix",
                    "description": f"以下の列をGROUP BY句に追加してください: {', '.join(missing_columns)}",
                    "suggestion": f"GROUP BY {', '.join(groupby_columns + missing_columns)}",
                    "priority": "high",
                    "auto_fixable": True
                })
        
        return suggestions
    
    def _suggest_type_fixes(self, sql: str) -> List[Dict]:
        """データ型エラーの修正提案"""
        suggestions = []
        
        # CAST関数の使用を提案
        suggestions.append({
            "type": "type_fix",
            "description": "データ型の不一致がある場合は、CAST関数またはSAFE_CAST関数を使用してください",
            "suggestion": "SAFE_CAST(column_name AS STRING) または CAST(column_name AS INT64)",
            "priority": "medium",
            "auto_fixable": False
        })
        
        # NULL値の処理を提案
        suggestions.append({
            "type": "type_fix",
            "description": "NULL値が原因の場合は、COALESCE関数を使用してください",
            "suggestion": "COALESCE(column_name, default_value)",
            "priority": "medium",
            "auto_fixable": False
        })
        
        return suggestions

# =========================================================================
# エラー表示機能
# =========================================================================

def show_enhanced_error_message(error, context: Dict = None):
    """強化されたエラーメッセージの表示"""
    if context is None:
        context = {}
    
    error_handler = EnhancedErrorHandler()
    error_analysis = error_handler.analyze_error(str(error), context)
    
    # エラーの重要度に応じて表示色を変更
    severity = error_analysis["severity"]
    if severity == "high":
        st.error(f"🔴 **{error_analysis['category']}**")
    elif severity == "medium":
        st.warning(f"🟡 **{error_analysis['category']}**")
    else:
        st.info(f"ℹ️ **{error_analysis['category']}**")
    
    # 元のエラーメッセージ
    with st.expander("📋 詳細エラーメッセージ", expanded=False):
        st.code(error_analysis["original_message"])
    
    # 解決策の表示
    st.markdown("### 💡 推奨解決策")
    for i, solution in enumerate(error_analysis["solutions"], 1):
        st.markdown(f"{i}. {solution}")
    
    # 自動修正提案
    if error_analysis["auto_fix_suggestions"]:
        st.markdown("### 🔧 自動修正提案")
        
        for suggestion in error_analysis["auto_fix_suggestions"]:
            priority_emoji = "🔴" if suggestion["priority"] == "high" else "🟡" if suggestion["priority"] == "medium" else "🟢"
            
            with st.expander(f"{priority_emoji} {suggestion['description']}", expanded=suggestion["priority"] == "high"):
                if suggestion.get("suggestion"):
                    st.code(suggestion["suggestion"], language="sql")
                
                if suggestion.get("auto_fixable", False):
                    if st.button(f"自動修正を適用", key=f"fix_{hash(suggestion['description'])}"):
                        st.info("自動修正機能は開発中です")
    
    # エラー履歴への追加
    try:
        if "error_history" not in st.session_state:
            st.session_state.error_history = []
        
        st.session_state.error_history.append({
            "timestamp": datetime.now(),
            "category": error_analysis["category"],
            "original_message": error_analysis["original_message"],
            "severity": error_analysis["severity"],
            "solutions": error_analysis["solutions"]
        })
        
        # 履歴の最大数を制限
        if len(st.session_state.error_history) > 50:
            st.session_state.error_history = st.session_state.error_history[-50:]
            
    except Exception as e:
        pass  # エラー履歴の追加に失敗してもアプリケーションを停止させない

# =========================================================================
# エラー回復機能
# =========================================================================

def suggest_error_recovery(error_analysis: Dict, original_sql: str = "", user_input: str = "") -> List[Dict]:
    """エラー回復のための具体的な提案"""
    recovery_options = []
    
    category = error_analysis.get("category", "")
    
    if "構文エラー" in category:
        recovery_options.extend([
            {
                "title": "SQL構文チェッカー使用",
                "description": "オンラインのSQL構文チェッカーでSQLを検証",
                "action_type": "external_tool",
                "url": "https://www.eversql.com/sql-syntax-check-validator/"
            },
            {
                "title": "プロンプト再実行", 
                "description": "より具体的な指示でAIにSQL再生成を依頼",
                "action_type": "regenerate",
                "suggestion": f"以下の指示をより詳細に書き換えてください: {user_input}"
            }
        ])
    
    elif "テーブル" in category:
        recovery_options.extend([
            {
                "title": "テーブル存在確認",
                "description": "BigQueryコンソールでテーブルの存在を確認",
                "action_type": "manual_check",
                "sql_suggestion": "SELECT table_name FROM `vorn-digi-mktg-poc-635a.toki_air.INFORMATION_SCHEMA.TABLES`"
            },
            {
                "title": "権限確認",
                "description": "データセットへのアクセス権限を管理者に確認依頼",
                "action_type": "permission_check"
            }
        ])
    
    elif "列名" in category:
        recovery_options.extend([
            {
                "title": "スキーマ確認",
                "description": "テーブルの列構造を確認",
                "action_type": "schema_check",
                "sql_suggestion": "SELECT column_name, data_type FROM `vorn-digi-mktg-poc-635a.toki_air.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'LookerStudio_report_campaign'"
            }
        ])
    
    return recovery_options

def create_error_report(error_history: List[Dict]) -> str:
    """エラー履歴からレポートを生成"""
    if not error_history:
        return "エラー履歴はありません。"
    
    # エラー統計の計算
    total_errors = len(error_history)
    error_categories = {}
    severity_counts = {"high": 0, "medium": 0, "low": 0}
    
    for error in error_history:
        category = error.get("category", "未分類")
        severity = error.get("severity", "medium")
        
        error_categories[category] = error_categories.get(category, 0) + 1
        severity_counts[severity] = severity_counts.get(severity, 0) + 1
    
    # レポートの生成
    report = f"""
# エラー分析レポート

## 📊 統計情報
- **総エラー数**: {total_errors}
- **高重要度**: {severity_counts['high']}
- **中重要度**: {severity_counts['medium']}  
- **低重要度**: {severity_counts['low']}

## 📋 エラーカテゴリ別集計
"""
    
    for category, count in sorted(error_categories.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total_errors) * 100
        report += f"- **{category}**: {count}回 ({percentage:.1f}%)\n"
    
    report += f"""
## 🔄 最近のエラー（最新5件）
"""
    
    recent_errors = error_history[-5:]
    for i, error in enumerate(reversed(recent_errors), 1):
        timestamp = error["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
        report += f"{i}. **{error['category']}** ({timestamp})\n   {error['original_message'][:100]}...\n\n"
    
    return report

# =========================================================================
# デバッグ支援機能
# =========================================================================

def debug_sql_execution(sql: str, bq_client) -> Dict:
    """SQL実行のデバッグ情報を収集"""
    debug_info = {
        "sql_length": len(sql),
        "sql_complexity": _analyze_sql_complexity(sql),
        "estimated_cost": "不明",
        "syntax_valid": False,
        "dry_run_result": None
    }
    
    try:
        # 基本的な構文チェック
        from analysis_controller import validate_basic_sql_syntax
        debug_info["syntax_valid"] = validate_basic_sql_syntax(sql)
    except:
        debug_info["syntax_valid"] = False
    
    try:
        # BigQueryのドライラン実行
        if bq_client:
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            query_job = bq_client.query(sql, job_config=job_config)
            
            debug_info["estimated_cost"] = f"{query_job.total_bytes_processed / 1024 / 1024:.2f} MB"
            debug_info["dry_run_result"] = "成功"
    except Exception as e:
        debug_info["dry_run_result"] = f"エラー: {str(e)}"
    
    return debug_info

def _analyze_sql_complexity(sql: str) -> str:
    """SQL複雑度の分析"""
    complexity_factors = {
        "joins": len(re.findall(r'\bJOIN\b', sql, re.IGNORECASE)),
        "subqueries": len(re.findall(r'\bSELECT\b', sql, re.IGNORECASE)) - 1,
        "aggregations": len(re.findall(r'\b(SUM|COUNT|AVG|MAX|MIN)\s*\(', sql, re.IGNORECASE)),
        "window_functions": len(re.findall(r'\bOVER\s*\(', sql, re.IGNORECASE))
    }
    
    total_complexity = sum(complexity_factors.values())
    
    if total_complexity == 0:
        return "シンプル"
    elif total_complexity <= 3:
        return "普通"
    elif total_complexity <= 7:
        return "複雑"
    else:
        return "非常に複雑"

# =========================================================================
# エクスポート機能
# =========================================================================

def export_error_data(error_history: List[Dict], format_type: str = "json") -> str:
    """エラーデータのエクスポート"""
    if format_type == "json":
        return json.dumps(error_history, ensure_ascii=False, indent=2, default=str)
    elif format_type == "csv":
        import pandas as pd
        df = pd.DataFrame(error_history)
        return df.to_csv(index=False)
    else:
        return create_error_report(error_history)

# =========================================================================
# 統合エラーハンドリング関数
# =========================================================================

def handle_application_error(error: Exception, context: Dict = None, show_ui: bool = True) -> Dict:
    """アプリケーション全体で使用する統合エラーハンドリング"""
    try:
        error_handler = EnhancedErrorHandler()
        error_analysis = error_handler.analyze_error(str(error), context or {})
        
        if show_ui:
            show_enhanced_error_message(error, context)
        
        # ログ記録
        error_log = {
            "timestamp": datetime.now().isoformat(),
            "error_type": type(error).__name__,
            "error_analysis": error_analysis,
            "context": context or {},
            "traceback": traceback.format_exc()
        }
        
        return error_log
        
    except Exception as handling_error:
        # エラーハンドリング自体でエラーが発生した場合
        fallback_message = f"エラーハンドリング中にエラーが発生: {str(handling_error)}"
        if show_ui:
            st.error(f"❌ {fallback_message}")
            st.error(f"元のエラー: {str(error)}")
        
        return {
            "timestamp": datetime.now().isoformat(),
            "error_type": "ErrorHandlingFailure",
            "original_error": str(error),
            "handling_error": fallback_message
        }