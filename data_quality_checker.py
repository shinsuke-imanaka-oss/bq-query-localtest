# data_quality_checker.py
"""
データ品質チェック機能（コア機能のみ）
- 包括的品質評価
- 異常値・NULL値・重複データの検出
- 品質スコア算出
- 改善提案生成
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional
import warnings
warnings.filterwarnings('ignore')

class DataQualityChecker:
    """データ品質をチェックし、問題点と改善提案を提供するクラス"""
    
    def __init__(self):
        self.quality_thresholds = {
            'null_rate_warning': 0.1,  # NULL値が10%以上で警告
            'null_rate_critical': 0.3,  # NULL値が30%以上で重要警告
            'duplicate_rate_warning': 0.05,  # 重複が5%以上で警告
            'outlier_rate_warning': 0.1,  # 外れ値が10%以上で警告
            'min_rows_warning': 10,  # 行数が10未満で警告
            'max_unique_ratio': 0.8  # ユニーク値率が80%以上でカテゴリ変数として不適切
        }
    
    def comprehensive_quality_check(self, df: pd.DataFrame, analysis_context: str = "") -> Dict[str, Any]:
        """包括的なデータ品質チェックを実行"""
        if df.empty:
            return {
                'overall_score': 0,
                'status': 'critical',
                'summary': 'データが空です',
                'issues': [{'severity': 'critical', 'message': 'データフレームが空です'}],
                'suggestions': ['有効なSQLクエリを実行してデータを取得してください']
            }
        
        # 各種品質チェックの実行
        checks = {
            'null_values': self._check_null_values(df),
            'duplicates': self._check_duplicates(df),
            'outliers': self._check_outliers(df),
            'data_types': self._check_data_types(df),
            'value_ranges': self._check_value_ranges(df),
            'data_consistency': self._check_data_consistency(df)
        }
        
        # 総合スコアの計算
        overall_score = self._calculate_overall_score(checks)
        
        # ステータスの決定
        status = self._determine_status(overall_score)
        
        # 問題点とサマリーの生成
        issues = self._compile_issues(checks)
        suggestions = self._generate_suggestions(checks, df)
        summary = self._generate_summary(df, overall_score, len(issues))
        
        return {
            'overall_score': overall_score,
            'status': status,
            'summary': summary,
            'issues': issues,
            'suggestions': suggestions,
            'detailed_checks': checks
        }
    
    def _check_null_values(self, df: pd.DataFrame) -> Dict[str, Any]:
        """NULL値のチェック"""
        null_counts = df.isnull().sum()
        null_rates = (null_counts / len(df)) * 100
        
        problematic_columns = []
        for col, rate in null_rates.items():
            if rate > self.quality_thresholds['null_rate_critical']:
                problematic_columns.append({
                    'column': col,
                    'null_rate': rate,
                    'severity': 'critical'
                })
            elif rate > self.quality_thresholds['null_rate_warning']:
                problematic_columns.append({
                    'column': col,
                    'null_rate': rate,
                    'severity': 'warning'
                })
        
        return {
            'score': max(0, 100 - len(problematic_columns) * 15),
            'problematic_columns': problematic_columns,
            'total_null_rate': null_rates.mean()
        }
    
    def _check_duplicates(self, df: pd.DataFrame) -> Dict[str, Any]:
        """重複データのチェック"""
        duplicate_count = df.duplicated().sum()
        duplicate_rate = (duplicate_count / len(df)) * 100
        
        severity = 'none'
        if duplicate_rate > self.quality_thresholds['duplicate_rate_warning']:
            severity = 'warning'
        
        return {
            'score': max(0, 100 - duplicate_rate * 10),
            'duplicate_count': duplicate_count,
            'duplicate_rate': duplicate_rate,
            'severity': severity
        }
    
    def _check_outliers(self, df: pd.DataFrame) -> Dict[str, Any]:
        """外れ値のチェック"""
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        outlier_info = []
        
        for col in numeric_columns:
            if df[col].notna().sum() == 0:
                continue
                
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
            outlier_rate = len(outliers) / len(df) * 100
            
            if outlier_rate > self.quality_thresholds['outlier_rate_warning']:
                outlier_info.append({
                    'column': col,
                    'outlier_count': len(outliers),
                    'outlier_rate': outlier_rate,
                    'severity': 'warning' if outlier_rate < 20 else 'critical'
                })
        
        return {
            'score': max(0, 100 - len(outlier_info) * 10),
            'outlier_columns': outlier_info
        }
    
    def _check_data_types(self, df: pd.DataFrame) -> Dict[str, Any]:
        """データ型の適切性チェック"""
        type_issues = []
        
        for col in df.columns:
            # 数値であるべきなのに文字列になっている可能性
            if 'cost' in col.lower() or 'click' in col.lower() or 'impression' in col.lower():
                if df[col].dtype == 'object':
                    # 数値に変換可能かチェック
                    try:
                        pd.to_numeric(df[col], errors='raise')
                    except:
                        type_issues.append({
                            'column': col,
                            'issue': '数値列ですが文字列型になっています',
                            'severity': 'warning'
                        })
            
            # 日付であるべきなのに文字列になっている可能性
            if 'date' in col.lower() and df[col].dtype == 'object':
                type_issues.append({
                    'column': col,
                    'issue': '日付列ですが文字列型になっています',
                    'severity': 'info'
                })
        
        return {
            'score': max(0, 100 - len(type_issues) * 10),
            'type_issues': type_issues
        }
    
    def _check_value_ranges(self, df: pd.DataFrame) -> Dict[str, Any]:
        """値の範囲の妥当性チェック"""
        range_issues = []
        
        for col in df.columns:
            if df[col].dtype in [np.int64, np.float64]:
                # 負の値チェック（通常負であるべきでない指標）
                if any(keyword in col.lower() for keyword in ['click', 'impression', 'view', 'conversion']):
                    negative_count = (df[col] < 0).sum()
                    if negative_count > 0:
                        range_issues.append({
                            'column': col,
                            'issue': f'{negative_count}行で負の値を検出',
                            'severity': 'warning'
                        })
                
                # 異常に大きな値のチェック
                if col.lower().endswith('rate') or 'rate' in col.lower():
                    # レート系は通常0-100%の範囲
                    out_of_range = ((df[col] < 0) | (df[col] > 100)).sum()
                    if out_of_range > 0:
                        range_issues.append({
                            'column': col,
                            'issue': f'{out_of_range}行で異常なレート値（0-100%範囲外）',
                            'severity': 'critical'
                        })
        
        return {
            'score': max(0, 100 - len(range_issues) * 15),
            'range_issues': range_issues
        }
    
    def _check_data_consistency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """データの一貫性チェック"""
        consistency_issues = []
        
        # クリック数がインプレッション数を超えていないかチェック
        if 'Clicks' in df.columns and 'Impressions' in df.columns:
            invalid_ctr = (df['Clicks'] > df['Impressions']).sum()
            if invalid_ctr > 0:
                consistency_issues.append({
                    'issue': f'{invalid_ctr}行でクリック数がインプレッション数を超過',
                    'severity': 'critical'
                })
        
        # コンバージョン数がクリック数を超えていないかチェック
        if 'Conversions' in df.columns and 'Clicks' in df.columns:
            invalid_cvr = (df['Conversions'] > df['Clicks']).sum()
            if invalid_cvr > 0:
                consistency_issues.append({
                    'issue': f'{invalid_cvr}行でコンバージョン数がクリック数を超過',
                    'severity': 'critical'
                })
        
        return {
            'score': max(0, 100 - len(consistency_issues) * 20),
            'consistency_issues': consistency_issues
        }
    
    def _calculate_overall_score(self, checks: Dict) -> int:
        """総合スコアの計算"""
        weights = {
            'null_values': 0.25,
            'duplicates': 0.15,
            'outliers': 0.20,
            'data_types': 0.15,
            'value_ranges': 0.15,
            'data_consistency': 0.10
        }
        
        total_score = 0
        for check_name, weight in weights.items():
            if check_name in checks:
                total_score += checks[check_name]['score'] * weight
        
        return min(100, max(0, int(total_score)))
    
    def _determine_status(self, score: int) -> str:
        """スコアに基づくステータスの決定"""
        if score >= 85:
            return 'excellent'
        elif score >= 70:
            return 'good'
        elif score >= 50:
            return 'warning'
        else:
            return 'critical'
    
    def _compile_issues(self, checks: Dict) -> List[Dict]:
        """問題点の一覧化"""
        issues = []
        
        # NULL値の問題
        for col_info in checks['null_values']['problematic_columns']:
            issues.append({
                'severity': col_info['severity'],
                'message': f"列 '{col_info['column']}' でNULL値が{col_info['null_rate']:.1f}%"
            })
        
        # 重複の問題
        if checks['duplicates']['severity'] != 'none':
            issues.append({
                'severity': checks['duplicates']['severity'],
                'message': f"重複行が{checks['duplicates']['duplicate_count']}行（{checks['duplicates']['duplicate_rate']:.1f}%）"
            })
        
        # 外れ値の問題
        for outlier_info in checks['outliers']['outlier_columns']:
            issues.append({
                'severity': outlier_info['severity'],
                'message': f"列 '{outlier_info['column']}' で外れ値が{outlier_info['outlier_count']}行（{outlier_info['outlier_rate']:.1f}%）"
            })
        
        # データ型の問題
        for type_issue in checks['data_types']['type_issues']:
            issues.append({
                'severity': type_issue['severity'],
                'message': f"列 '{type_issue['column']}': {type_issue['issue']}"
            })
        
        # 値の範囲の問題
        for range_issue in checks['value_ranges']['range_issues']:
            issues.append({
                'severity': range_issue['severity'],
                'message': f"列 '{range_issue['column']}': {range_issue['issue']}"
            })
        
        # 一貫性の問題
        for consistency_issue in checks['data_consistency']['consistency_issues']:
            issues.append({
                'severity': consistency_issue['severity'],
                'message': consistency_issue['issue']
            })
        
        return issues
    
    def _generate_suggestions(self, checks: Dict, df: pd.DataFrame) -> List[str]:
        """改善提案の生成"""
        suggestions = []
        
        # NULL値への対応提案
        if checks['null_values']['problematic_columns']:
            suggestions.append("NULL値の多い列は分析から除外するか、適切なデフォルト値での補完を検討してください")
        
        # 重複データへの対応提案
        if checks['duplicates']['duplicate_count'] > 0:
            suggestions.append("重複行を確認し、必要に応じてDISTINCTやGROUP BYでの集約を検討してください")
        
        # 外れ値への対応提案
        if checks['outliers']['outlier_columns']:
            suggestions.append("外れ値が多い列は分析結果に大きく影響する可能性があります。要因分析や除外を検討してください")
        
        # データ型の改善提案
        if checks['data_types']['type_issues']:
            suggestions.append("数値列が文字列型になっている場合、CAST関数での型変換を検討してください")
        
        # 値の範囲の改善提案
        if checks['value_ranges']['range_issues']:
            suggestions.append("異常な値の範囲がある列は、WHERE句での条件絞り込みを検討してください")
        
        # 一貫性の改善提案
        if checks['data_consistency']['consistency_issues']:
            suggestions.append("データの一貫性に問題があります。元データの品質確認とクリーニングが必要です")
        
        # データサイズに関する提案
        if len(df) < self.quality_thresholds['min_rows_warning']:
            suggestions.append("データ量が少ないため、統計的な信頼性が低い可能性があります。分析期間の延長を検討してください")
        
        return suggestions
    
    def _generate_summary(self, df: pd.DataFrame, score: int, issue_count: int) -> str:
        """品質サマリーの生成"""
        status_messages = {
            'excellent': '優秀なデータ品質です',
            'good': '良好なデータ品質です',
            'warning': '注意が必要なデータ品質です',
            'critical': '重大な品質問題があります'
        }
        
        status = self._determine_status(score)
        base_message = status_messages[status]
        
        return f"{base_message}（スコア: {score}/100、問題: {issue_count}件、データ行数: {len(df):,}）"

# =========================================================================
# データプロファイリング機能
# =========================================================================

class DataProfiler:
    """データプロファイリング機能"""
    
    @staticmethod
    def generate_profile_report(df: pd.DataFrame) -> Dict[str, Any]:
        """データプロファイルレポートの生成"""
        profile = {
            'basic_info': DataProfiler._get_basic_info(df),
            'column_profiles': DataProfiler._get_column_profiles(df),
            'correlations': DataProfiler._get_correlations(df),
            'data_patterns': DataProfiler._get_data_patterns(df)
        }
        return profile
    
    @staticmethod
    def _get_basic_info(df: pd.DataFrame) -> Dict:
        """基本情報の取得"""
        return {
            'row_count': len(df),
            'column_count': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'numeric_columns': len(df.select_dtypes(include=[np.number]).columns),
            'categorical_columns': len(df.select_dtypes(include=['object', 'category']).columns),
            'datetime_columns': len(df.select_dtypes(include=['datetime64']).columns)
        }
    
    @staticmethod
    def _get_column_profiles(df: pd.DataFrame) -> Dict:
        """列ごとのプロファイル"""
        profiles = {}
        
        for col in df.columns:
            col_profile = {
                'dtype': str(df[col].dtype),
                'null_count': df[col].isnull().sum(),
                'null_rate': (df[col].isnull().sum() / len(df)) * 100,
                'unique_count': df[col].nunique(),
                'unique_rate': (df[col].nunique() / len(df)) * 100
            }
            
            # 数値列の場合の追加情報
            if df[col].dtype in [np.int64, np.float64]:
                col_profile.update({
                    'min': df[col].min(),
                    'max': df[col].max(),
                    'mean': df[col].mean(),
                    'std': df[col].std(),
                    'zeros_count': (df[col] == 0).sum()
                })
            
            # カテゴリ列の場合の追加情報
            elif df[col].dtype == 'object':
                col_profile.update({
                    'top_values': df[col].value_counts().head(5).to_dict()
                })
            
            profiles[col] = col_profile
        
        return profiles
    
    @staticmethod
    def _get_correlations(df: pd.DataFrame) -> Dict:
        """相関関係の分析"""
        numeric_df = df.select_dtypes(include=[np.number])
        
        if len(numeric_df.columns) < 2:
            return {'message': '数値列が2列未満のため相関分析不可'}
        
        corr_matrix = numeric_df.corr()
        
        # 高い相関を持つペアを特定
        high_corr_pairs = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr_value = corr_matrix.iloc[i, j]
                if abs(corr_value) > 0.7:  # 高い相関の閾値
                    high_corr_pairs.append({
                        'column1': corr_matrix.columns[i],
                        'column2': corr_matrix.columns[j],
                        'correlation': corr_value
                    })
        
        return {
            'correlation_matrix': corr_matrix.to_dict(),
            'high_correlation_pairs': high_corr_pairs
        }
    
    @staticmethod
    def _get_data_patterns(df: pd.DataFrame) -> Dict:
        """データパターンの分析"""
        patterns = {
            'constant_columns': [],
            'high_cardinality_columns': [],
            'suspicious_columns': []
        }
        
        for col in df.columns:
            # 定数列の検出
            if df[col].nunique() <= 1:
                patterns['constant_columns'].append(col)
            
            # 高カーディナリティ列の検出
            elif df[col].nunique() / len(df) > 0.95:
                patterns['high_cardinality_columns'].append(col)
            
            # 疑わしい列の検出（例：すべて同じパターン）
            if df[col].dtype == 'object':
                # 文字列長がすべて同じかチェック
                str_lengths = df[col].dropna().astype(str).str.len()
                if str_lengths.nunique() == 1 and len(str_lengths) > 10:
                    patterns['suspicious_columns'].append({
                        'column': col,
                        'pattern': f'全て文字数{str_lengths.iloc[0]}文字'
                    })
        
        return patterns

# =========================================================================
# データクリーニング機能
# =========================================================================

class DataCleaner:
    """データクリーニング機能"""
    
    def __init__(self):
        self.cleaning_history = []
    
    def clean_data(self, df: pd.DataFrame, cleaning_options: List[str]) -> pd.DataFrame:
        """データクリーニングの実行"""
        cleaned_df = df.copy()
        applied_cleanings = []
        
        for option in cleaning_options:
            if "NULL値の多い列を除去" in option:
                cleaned_df, removed_cols = self._remove_high_null_columns(cleaned_df)
                if removed_cols:
                    applied_cleanings.append(f"NULL値30%以上の列を除去: {removed_cols}")
            
            elif "完全重複行を除去" in option:
                before_count = len(cleaned_df)
                cleaned_df = cleaned_df.drop_duplicates()
                removed_count = before_count - len(cleaned_df)
                if removed_count > 0:
                    applied_cleanings.append(f"重複行を{removed_count}行除去")
            
            elif "数値列の外れ値を除去" in option:
                cleaned_df, outlier_count = self._remove_outliers(cleaned_df)
                if outlier_count > 0:
                    applied_cleanings.append(f"外れ値を{outlier_count}行除去")
            
            elif "定数列を除去" in option:
                cleaned_df, constant_cols = self._remove_constant_columns(cleaned_df)
                if constant_cols:
                    applied_cleanings.append(f"定数列を除去: {constant_cols}")
        
        # クリーニング履歴に記録
        if applied_cleanings:
            self.cleaning_history.append({
                'timestamp': pd.Timestamp.now(),
                'original_shape': df.shape,
                'cleaned_shape': cleaned_df.shape,
                'applied_cleanings': applied_cleanings
            })
        
        return cleaned_df
    
    def _remove_high_null_columns(self, df: pd.DataFrame, threshold: float = 0.3) -> Tuple[pd.DataFrame, List[str]]:
        """NULL値の多い列を除去"""
        null_rates = df.isnull().sum() / len(df)
        cols_to_drop = null_rates[null_rates > threshold].index.tolist()
        
        cleaned_df = df.drop(columns=cols_to_drop) if cols_to_drop else df
        return cleaned_df, cols_to_drop
    
    def _remove_outliers(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        """外れ値の除去（IQR基準）"""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        mask = pd.Series([True] * len(df))
        
        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            mask = mask & (df[col] >= lower_bound) & (df[col] <= upper_bound)
        
        cleaned_df = df[mask]
        outlier_count = len(df) - len(cleaned_df)
        
        return cleaned_df, outlier_count
    
    def _remove_constant_columns(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """定数列の除去"""
        constant_cols = [col for col in df.columns if df[col].nunique() <= 1]
        cleaned_df = df.drop(columns=constant_cols) if constant_cols else df
        
        return cleaned_df, constant_cols
    
    def get_cleaning_history(self) -> List[Dict]:
        """クリーニング履歴の取得"""
        return self.cleaning_history

# =========================================================================
# SQL生成支援機能
# =========================================================================

class DataQualitySQL:
    """データ品質チェック用SQL生成"""
    
    @staticmethod
    def generate_cleaning_sql(df: pd.DataFrame, issues: List[Dict]) -> List[str]:
        """品質問題に基づくクリーニングSQLの生成"""
        cleaning_sqls = []
        
        for issue in issues:
            if 'NULL値' in issue['message']:
                # NULL値対応のSQL
                column = issue['message'].split("'")[1]
                cleaning_sqls.append(f"""
-- {column}のNULL値を除外
SELECT *
FROM your_table
WHERE {column} IS NOT NULL;

-- または平均値で補完する場合:
-- SELECT *, 
--   COALESCE({column}, AVG({column}) OVER()) AS {column}_cleaned
-- FROM your_table;
""")
            
            elif '重複行' in issue['message']:
                # 重複除去のSQL
                cleaning_sqls.append(f"""
-- 重複行の除去
SELECT DISTINCT *
FROM your_table;

-- または最新レコードのみ保持する場合:
-- SELECT * EXCEPT(row_num)
-- FROM (
--   SELECT *, ROW_NUMBER() OVER(PARTITION BY key_column ORDER BY date_column DESC) as row_num
--   FROM your_table
-- )
-- WHERE row_num = 1;
""")
            
            elif '外れ値' in issue['message']:
                # 外れ値除去のSQL
                column = issue['message'].split("'")[1]
                cleaning_sqls.append(f"""
-- {column}の外れ値を除去
WITH stats AS (
  SELECT 
    PERCENTILE_CONT({column}, 0.25) OVER() AS Q1,
    PERCENTILE_CONT({column}, 0.75) OVER() AS Q3
  FROM your_table
  WHERE {column} IS NOT NULL
),
bounds AS (
  SELECT 
    Q1 - 1.5 * (Q3 - Q1) AS lower_bound,
    Q3 + 1.5 * (Q3 - Q1) AS upper_bound
  FROM stats
  LIMIT 1
)
SELECT *
FROM your_table, bounds
WHERE {column} BETWEEN bounds.lower_bound AND bounds.upper_bound;
""")
        
        return cleaning_sqls
    
    @staticmethod
    def generate_quality_check_sql(table_name: str) -> str:
        """データ品質チェック用SQLの生成"""
        return f"""
-- データ品質チェッククエリ
WITH quality_metrics AS (
  SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT *) AS unique_rows,
    
    -- NULL値チェック
    COUNTIF(Impressions IS NULL) AS null_impressions,
    COUNTIF(Clicks IS NULL) AS null_clicks,
    COUNTIF(CostIncludingFees IS NULL) AS null_cost,
    
    -- 異常値チェック
    COUNTIF(Clicks > Impressions) AS invalid_ctr_rows,
    COUNTIF(Conversions > Clicks) AS invalid_cvr_rows,
    COUNTIF(CostIncludingFees < 0) AS negative_cost_rows,
    
    -- 基本統計
    AVG(SAFE_DIVIDE(Clicks, Impressions) * 100) AS avg_ctr,
    AVG(SAFE_DIVIDE(CostIncludingFees, Conversions)) AS avg_cpa,
    AVG(SAFE_DIVIDE(ConversionValue, CostIncludingFees)) AS avg_roas
    
  FROM `{table_name}`
)
SELECT 
  *,
  ROUND((unique_rows / total_rows) * 100, 2) AS uniqueness_rate,
  ROUND(((total_rows - null_impressions - null_clicks - null_cost) / total_rows) * 100, 2) AS completeness_rate,
  CASE 
    WHEN invalid_ctr_rows + invalid_cvr_rows + negative_cost_rows = 0 THEN 'GOOD'
    WHEN invalid_ctr_rows + invalid_cvr_rows + negative_cost_rows < total_rows * 0.05 THEN 'WARNING'
    ELSE 'CRITICAL'
  END AS data_quality_status
FROM quality_metrics;
"""

# =========================================================================
# 統計分析支援機能
# =========================================================================

class StatisticalAnalyzer:
    """統計分析支援機能"""
    
    @staticmethod
    def detect_distribution_type(series: pd.Series) -> str:
        """分布の種類を検出"""
        if series.dtype not in [np.int64, np.float64]:
            return "non-numeric"
        
        # 基本統計量
        skewness = series.skew()
        kurtosis = series.kurtosis()
        
        # 分布の判定
        if abs(skewness) < 0.5 and abs(kurtosis) < 0.5:
            return "normal"
        elif skewness > 1:
            return "right_skewed"
        elif skewness < -1:
            return "left_skewed"
        elif kurtosis > 1:
            return "heavy_tailed"
        else:
            return "unknown"
    
    @staticmethod
    def calculate_advanced_statistics(df: pd.DataFrame) -> Dict[str, Any]:
        """高度な統計量の計算"""
        numeric_df = df.select_dtypes(include=[np.number])
        
        if numeric_df.empty:
            return {"message": "数値列がありません"}
        
        stats = {}
        
        for col in numeric_df.columns:
            series = numeric_df[col].dropna()
            
            if len(series) == 0:
                continue
            
            stats[col] = {
                'count': len(series),
                'mean': series.mean(),
                'median': series.median(),
                'std': series.std(),
                'min': series.min(),
                'max': series.max(),
                'skewness': series.skew(),
                'kurtosis': series.kurtosis(),
                'distribution_type': StatisticalAnalyzer.detect_distribution_type(series),
                'quartiles': {
                    'Q1': series.quantile(0.25),
                    'Q2': series.quantile(0.50),
                    'Q3': series.quantile(0.75)
                },
                'outlier_bounds': StatisticalAnalyzer._calculate_outlier_bounds(series)
            }
        
        return stats
    
    @staticmethod
    def _calculate_outlier_bounds(series: pd.Series) -> Dict[str, float]:
        """外れ値の境界値を計算"""
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        
        return {
            'lower_bound': Q1 - 1.5 * IQR,
            'upper_bound': Q3 + 1.5 * IQR,
            'extreme_lower_bound': Q1 - 3 * IQR,
            'extreme_upper_bound': Q3 + 3 * IQR
        }

# =========================================================================
# 外部呼び出し用の便利関数
# =========================================================================

def quick_quality_check(df: pd.DataFrame) -> Dict[str, Any]:
    """クイック品質チェック"""
    checker = DataQualityChecker()
    return checker.comprehensive_quality_check(df)

def generate_quality_report(df: pd.DataFrame) -> Dict[str, Any]:
    """包括的な品質レポート生成"""
    checker = DataQualityChecker()
    profiler = DataProfiler()
    analyzer = StatisticalAnalyzer()
    
    return {
        'quality_check': checker.comprehensive_quality_check(df),
        'profile': profiler.generate_profile_report(df),
        'statistics': analyzer.calculate_advanced_statistics(df)
    }

def auto_clean_data(df: pd.DataFrame, aggressive: bool = False) -> pd.DataFrame:
    """自動データクリーニング"""
    cleaner = DataCleaner()
    
    # 基本的なクリーニングオプション
    basic_options = [
        "完全重複行を除去",
        "定数列を除去"
    ]
    
    # アグレッシブクリーニングオプション
    aggressive_options = basic_options + [
        "NULL値の多い列を除去 (30%以上)",
        "数値列の外れ値を除去 (IQR基準)"
    ]
    
    options = aggressive_options if aggressive else basic_options
    return cleaner.clean_data(df, options)