# data_quality_checker.py - 最小版
"""
データ品質チェック機能
"""

import pandas as pd
import streamlit as st
from typing import Dict, List, Any

def check_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """データ品質チェック"""
    if df is None or df.empty:
        return {"status": "empty", "message": "データが空です"}
    
    try:
        quality_report = {
            "status": "success",
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "missing_data": {},
            "data_types": {},
            "warnings": [],
            "recommendations": []
        }
        
        # NULL値チェック
        for column in df.columns:
            null_count = df[column].isnull().sum()
            null_percentage = (null_count / len(df)) * 100
            
            quality_report["missing_data"][column] = {
                "count": int(null_count),
                "percentage": float(null_percentage)
            }
            
            if null_percentage > 50:
                quality_report["warnings"].append(f"列'{column}'に50%以上のNULL値があります")
            
            # データ型情報
            quality_report["data_types"][column] = str(df[column].dtype)
        
        # 基本的な推奨事項
        if quality_report["total_rows"] < 10:
            quality_report["recommendations"].append("データ量が少ないため、統計的な分析には注意が必要です")
        
        if quality_report["total_rows"] > 10000:
            quality_report["recommendations"].append("大量のデータです。分析時間がかかる可能性があります")
        
        return quality_report
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"品質チェック中にエラーが発生: {str(e)}"
        }

def show_data_quality_summary(quality_report: Dict[str, Any]):
    """データ品質サマリーの表示"""
    if quality_report["status"] == "empty":
        st.warning("⚠️ データが空のため、品質チェックできません")
        return
    
    if quality_report["status"] == "error":
        st.error(f"❌ {quality_report['message']}")
        return
    
    # 基本統計
    col1, col2 = st.columns(2)
    with col1:
        st.metric("総行数", f"{quality_report['total_rows']:,}")
    with col2:
        st.metric("総列数", quality_report['total_columns'])
    
    # 警告表示
    if quality_report["warnings"]:
        st.warning("⚠️ **品質に関する警告:**")
        for warning in quality_report["warnings"]:
            st.write(f"- {warning}")
    
    # 推奨事項
    if quality_report["recommendations"]:
        st.info("💡 **推奨事項:**")
        for rec in quality_report["recommendations"]:
            st.write(f"- {rec}")