# looker_handler.py - 設定管理システム統合版
"""
Looker Studio連携・フィルター管理 - 設定管理システム対応
"""

import streamlit as st
import json
from urllib.parse import quote
import datetime
import os
import pandas as pd
from typing import Dict, List, Optional
from troubleshooter import display_troubleshooting_guide

# =========================================================================
# BigQueryライブラリのインポート
# =========================================================================

try:
    from google.cloud import bigquery
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    st.warning("⚠️ BigQueryライブラリが見つかりません")

# =========================================================================
# 設定管理システムの読み込み
# =========================================================================

try:
    from bq_tool_config import config_manager, settings
    CONFIG_AVAILABLE = True
    print("✅ 設定管理システム読み込み成功")
except ImportError:
    CONFIG_AVAILABLE = False
    settings = None
    print("⚠️ 設定管理システムが見つかりません - フォールバックモード")

# =========================================================================
# 設定取得関数（統合版）
# =========================================================================

def get_looker_config() -> Dict[str, any]:
    """Looker Studio設定の取得（設定管理統合版）"""
    if CONFIG_AVAILABLE and settings:
        return {
            "report_id": settings.looker.report_id,
            "base_url": settings.looker.get_embed_url(),
            "sheets": settings.looker.default_sheets
        }
    else:
        # フォールバック設定（環境変数から取得）
        report_id = os.environ.get("LOOKER_REPORT_ID")
        if not report_id:
            # Streamlit Secretsからの取得を試行
            try:
                report_id = st.secrets.get("LOOKER_REPORT_ID")
            except:
                pass
        
        return {
            "report_id": report_id,
            "base_url": f"https://lookerstudio.google.com/embed/reporting/{report_id}" if report_id else "",
            "sheets": get_default_report_sheets()
        }

def get_bigquery_table_config() -> Dict[str, str]:
    """BigQueryテーブル設定の取得（設定管理統合版）"""
    if CONFIG_AVAILABLE and settings:
        return {
            "project_id": settings.bigquery.project_id,
            "dataset": settings.bigquery.dataset,
            "table_prefix": settings.bigquery.table_prefix,
            "timeout": settings.bigquery.timeout,
            "full_table_name": settings.bigquery.get_full_table_name("campaign")
        }
    else:
        # フォールバック設定
        project_id = os.environ.get("GCP_PROJECT_ID", "vorn-digi-mktg-poc-635a")
        dataset = os.environ.get("BQ_DATASET", "toki_air")
        table_prefix = os.environ.get("BQ_TABLE_PREFIX", "")
        
        return {
            "project_id": project_id,
            "dataset": dataset,
            "table_prefix": table_prefix,
            "timeout": 300,
            "full_table_name": f"{project_id}.{dataset}.LookerStudio_report_campaign"
        }

def get_default_report_sheets() -> Dict[str, str]:
    """デフォルトレポートシート定義"""
    return {
        "予算管理": "Gcf9", "サマリー01": "6HI9", "サマリー02": "IH29",
        "メディア": "GTrk", "デバイス": "kovk", "月別": "Bsvk",
        "日別": "40vk", "曜日": "hsv3", "キャンペーン": "cYwk",
        "広告グループ": "1ZWq", "テキストCR": "NfWq", "ディスプレイCR": "p_grkcjbbytd",
        "キーワード": "imWq", "地域": "ZNdq", "時間": "bXdq",
        "最終ページURL": "7xXq", "性別": "ctdq", "年齢": "fX53",
    }

def get_sheet_param_sets() -> Dict[str, Dict[str, List[str]]]:
    """シートごとのパラメータ設定"""
    return {
        "予算管理": {
            "date": ["budget.p_start_date", "budget.p_end_date"], 
            "media": ["budget.p_media"], 
            "campaign": ["budget.p_campaign"]
        },
        "サマリー01": {
            "date": ["campaign.p_start_date", "campaign.p_end_date"], 
            "media": ["campaign.p_media"], 
            "campaign": ["campaign.p_campaign"]
        },
        "サマリー02": {
            "date": [
                "campaign.p_start_date", "campaign.p_end_date", 
                "device.p_start_date", "device.p_end_date", 
                "geo.p_start_date", "geo.p_end_date", 
                "gender.p_start_date", "gender.p_end_date", 
                "campaign_hourly.p_start_date", "campaign_hourly.p_end_date", 
                "age_range.p_start_date", "age_range.p_end_date"
            ],
            "media": [
                "campaign.p_media", "device.p_media", "geo.p_media", 
                "gender.p_media", "campaign_hourly.p_media", "age_range.p_media"
            ],
            "campaign": [
                "campaign.p_campaign", "device.p_campaign", "geo.p_campaign", 
                "gender.p_campaign", "campaign_hourly.p_campaign", "age_range.p_campaign"
            ]
        },
        "メディア": {
            "date": ["campaign.p_start_date", "campaign.p_end_date"], 
            "media": ["campaign.p_media"], 
            "campaign": ["campaign.p_campaign"]
        },
        "デバイス": {
            "date": ["device.p_start_date", "device.p_end_date"], 
            "media": ["device.p_media"], 
            "campaign": ["device.p_campaign"]
        },
        "月別": {
            "date": ["campaign.p_start_date", "campaign.p_end_date"], 
            "media": ["campaign.p_media"], 
            "campaign": ["campaign.p_campaign"]
        },
        "日別": {
            "date": ["campaign.p_start_date", "campaign.p_end_date"], 
            "media": ["campaign.p_media"], 
            "campaign": ["campaign.p_campaign"]
        },
        "曜日": {
            "date": ["campaign.p_start_date", "campaign.p_end_date"], 
            "media": ["campaign.p_media"], 
            "campaign": ["campaign.p_campaign"]
        },
        "キャンペーン": {
            "date": ["campaign.p_start_date", "campaign.p_end_date"], 
            "media": ["campaign.p_media"], 
            "campaign": ["campaign.p_campaign"]
        },
        "広告グループ": {
            "date": ["adgroup.p_start_date", "adgroup.p_end_date"], 
            "media": ["adgroup.p_media"], 
            "campaign": ["adgroup.p_campaign"]
        },
        "テキストCR": {
            "date": ["ad.p_start_date", "ad.p_end_date"], 
            "media": ["ad.p_media"], 
            "campaign": ["ad.p_campaign"]
        },
        "ディスプレイCR": {
            "date": ["ad.p_start_date", "ad.p_end_date"], 
            "media": ["ad.p_media"], 
            "campaign": ["ad.p_campaign"]
        },
        "キーワード": {
            "date": ["keyword.p_start_date", "keyword.p_end_date"], 
            "media": ["keyword.p_media"], 
            "campaign": ["keyword.p_campaign"]
        },
        "地域": {
            "date": ["geo.p_start_date", "geo.p_end_date"], 
            "media": ["geo.p_media"], 
            "campaign": ["geo.p_campaign"]
        },
        "時間": {
            "date": ["campaign_hourly.p_start_date", "campaign_hourly.p_end_date"], 
            "media": ["campaign_hourly.p_media"], 
            "campaign": ["campaign_hourly.p_campaign"]
        },
        "最終ページURL": {
            "date": ["final_url.p_start_date", "final_url.p_end_date"], 
            "media": ["final_url.p_media"], 
            "campaign": ["final_url.p_campaign"]
        },
        "性別": {
            "date": ["gender.p_start_date", "gender.p_end_date"], 
            "media": ["gender.p_media"], 
            "campaign": ["gender.p_campaign"]
        },
        "年齢": {
            "date": ["age_range.p_start_date", "age_range.p_end_date"], 
            "media": ["age_range.p_media"], 
            "campaign": ["age_range.p_campaign"]
        },
    }

# =========================================================================
# フィルター機能（設定管理統合版）
# =========================================================================

@st.cache_data(ttl=43200)
def get_filter_options(_bq_client, table_id: str, column_name: str) -> List[str]:
    """BigQueryからフィルタの選択肢を取得する（設定管理統合版）"""
    if not BIGQUERY_AVAILABLE:
        return []
        
    try:
        # 設定からタイムアウト値を取得
        table_config = get_bigquery_table_config()
        timeout = table_config.get("timeout", 300)
        
        query = f"""
        SELECT DISTINCT {column_name} 
        FROM `{table_id}` 
        WHERE {column_name} IS NOT NULL 
        ORDER BY {column_name}
        """
        
        # クエリ実行設定
        job_config = bigquery.QueryJobConfig(
            use_query_cache=True
        )
        
        result = _bq_client.query(query, job_config=job_config, timeout=timeout).to_dataframe()
        return result[column_name].tolist()
        
    except Exception as e:
        st.error(f"フィルターオプション取得エラー ({column_name}): {e}")

        # トラブルシューティング支援機能を追加
        display_troubleshooting_guide(e)

        return []

def init_filters():
    """filtersセッションの初期化（設定管理統合版）"""
    if "filters" not in st.session_state:
        st.session_state.filters = {}

    # デフォルト値の設定（設定管理システムから取得）
    if CONFIG_AVAILABLE and settings:
        default_days = settings.app.default_date_range_days
    else:
        default_days = 30

    defaults = {
        "sheet": "サマリー01",
        "start_date": datetime.date.today() - datetime.timedelta(days=default_days),
        "end_date": datetime.date.today(),
        "media": [],
        "campaigns": []
    }

    for key, value in defaults.items():
        if key not in st.session_state.filters:
            st.session_state.filters[key] = value

def show_filter_ui(bq_client):
    """サイドバーフィルタUI（設定管理統合版）"""
    # 設定確認
    looker_config = get_looker_config()
    if not looker_config["report_id"]:
        st.sidebar.error("❌ Looker Report IDが設定されていません")
        
        # 設定管理システムが利用可能な場合は設定パネルへの誘導
        if CONFIG_AVAILABLE:
            st.sidebar.markdown("""
            ### 🔧 設定管理
            メインページの「⚙️ システム設定」から設定してください。
            """)
        else:
            st.sidebar.markdown("""
            ### 🔧 設定方法
            環境変数またはStreamlit Secretsで以下を設定:
            ```
            LOOKER_REPORT_ID=your_report_id_here
            ```
            """)
        return
    
    init_filters()
    old_filters = st.session_state.filters.copy()

    # シート選択
    sheet_names = list(looker_config["sheets"].keys())
    selected_sheet_name = st.sidebar.selectbox(
        "表示するレポートシート:",
        sheet_names,
        index=sheet_names.index(st.session_state.filters.get("sheet", "メディア")),
    )
    st.session_state.filters["sheet"] = selected_sheet_name
    
    st.sidebar.markdown("---")

    # 日付範囲
    start_date = st.sidebar.date_input(
        "開始日", 
        value=st.session_state.filters["start_date"],
        help="分析対象期間の開始日"
    )
    end_date = st.sidebar.date_input(
        "終了日", 
        value=st.session_state.filters["end_date"],
        help="分析対象期間の終了日"
    )

    # BigQueryテーブル設定の取得
    table_config = get_bigquery_table_config()
    table_id = table_config["full_table_name"]

    # フィルターオプション取得
    if bq_client and BIGQUERY_AVAILABLE:
        try:
            media_placeholder = st.sidebar.empty()
            campaign_placeholder = st.sidebar.empty()
            
            # 設定管理システムが利用可能な場合はキャッシュTTL調整
            if CONFIG_AVAILABLE and settings:
                cache_ttl = settings.app.cache_ttl
            else:
                cache_ttl = 43200
            
            with st.spinner("フィルターオプション取得中..."):
                media_options = get_filter_options(bq_client, table_id, "ServiceNameJA_Media")
                campaign_options = get_filter_options(bq_client, table_id, "CampaignName")
            
            with media_placeholder.container():
                selected_media = st.multiselect(
                    "メディア", 
                    options=media_options, 
                    default=st.session_state.filters.get("media", []),
                    help="分析対象のメディアを選択（複数選択可）"
                )
            
            with campaign_placeholder.container():
                selected_campaigns = st.multiselect(
                    "キャンペーン", 
                    options=campaign_options, 
                    default=st.session_state.filters.get("campaigns", []),
                    help="分析対象のキャンペーンを選択（複数選択可）"
                )
                
        except Exception as e:
            st.sidebar.error(f"フィルター取得エラー: {e}")
            selected_media = st.sidebar.multiselect("メディア", options=[], default=[])
            selected_campaigns = st.sidebar.multiselect("キャンペーン", options=[], default=[])
    else:
        st.sidebar.warning("⚠️ BigQuery接続が必要です")
        selected_media = st.sidebar.multiselect("メディア", options=[], default=[])
        selected_campaigns = st.sidebar.multiselect("キャンペーン", options=[], default=[])

    # フィルター更新
    st.session_state.filters.update({
        "start_date": start_date, 
        "end_date": end_date,
        "media": selected_media, 
        "campaigns": selected_campaigns
    })
    
    # 変更時の再描画
    if st.session_state.filters != old_filters:
        st.rerun()

# =========================================================================
# Looker Studio統合表示（設定管理統合版）
# =========================================================================

def show_looker_studio_integration(bq_client=None, model=None, key_prefix="", sheet_analysis_queries=None):
    """Looker Studio統合表示（設定管理統合版）"""
    # Looker設定の取得
    looker_config = get_looker_config()
    
    if not looker_config["report_id"]:
        st.error("❌ Looker Studio レポートIDが設定されていません")
        
        # 設定管理システムが利用可能な場合の案内
        if CONFIG_AVAILABLE:
            st.info("💡 システム設定パネルから Looker Report ID を設定してください")
            if st.button("⚙️ 設定パネルを開く"):
                st.session_state.show_config_panel = True
                st.rerun()
        else:
            st.markdown("""
            ### 🔧 設定方法
            
            **方法1: 環境変数**
            ```bash
            export LOOKER_REPORT_ID=your_report_id_here
            ```
            
            **方法2: Streamlit Secrets**
            ```toml
            # .streamlit/secrets.toml
            LOOKER_REPORT_ID = "your_report_id_here"
            ```
            """)
        return
    
    # フィルター初期化
    init_filters()
    
    # 現在の選択値を取得
    selected_sheet_name = st.session_state.filters["sheet"]
    filters = st.session_state.filters
    
    # シート設定の取得
    report_sheets = looker_config["sheets"]
    selected_page_id = report_sheets.get(selected_sheet_name)
    
    if not selected_page_id:
        st.error(f"シート '{selected_sheet_name}' のページIDが見つかりません")
        return
    
    # パラメータ設定の構築
    sheet_param_sets = get_sheet_param_sets()
    param_sets = sheet_param_sets.get(selected_sheet_name, {})
    params = {}

    # Streamlitフィルターの適用
    if st.session_state.get("apply_streamlit_filters", True):
        # 日付パラメータ
        date_params = param_sets.get("date", [])
        if date_params and filters.get("start_date") and filters.get("end_date"):
            start_date_str = filters["start_date"].strftime("%Y%m%d")
            end_date_str = filters["end_date"].strftime("%Y%m%d")
            for param_name in date_params:
                if "start_date" in param_name: 
                    params[param_name] = start_date_str
                elif "end_date" in param_name: 
                    params[param_name] = end_date_str

        # メディアパラメータ
        media_params = param_sets.get("media", [])
        media_value = ",".join(filters["media"]) if filters.get("media") else ""
        for param_name in media_params:
            params[param_name] = media_value

        # キャンペーンパラメータ
        campaign_params = param_sets.get("campaign", [])
        campaign_value = ",".join(filters["campaigns"]) if filters.get("campaigns") else ""
        for param_name in campaign_params:
            params[param_name] = campaign_value
    
    # URL構築
    params_json = json.dumps(params)
    encoded_params = quote(params_json)
    base_url = looker_config["base_url"]
    final_url = f"{base_url}/page/{selected_page_id}?params={encoded_params}"

    # フィルター表示の制御
    hide_filters = st.session_state.get("apply_streamlit_filters", True)
    final_url += "&hideFilters=true" if hide_filters else "&hideFilters=false"

    # Looker Studio埋め込み表示
    st.components.v1.iframe(final_url, height=600, scrolling=True)
    
    # デバッグ情報の表示（設定に応じて）
    if CONFIG_AVAILABLE and settings and settings.app.debug_mode:
        with st.expander("🔍 デバッグ情報", expanded=False):
            st.json({
                "config_available": CONFIG_AVAILABLE,
                "report_id": looker_config["report_id"],
                "selected_sheet": selected_sheet_name,
                "page_id": selected_page_id,
                "params": params,
                "final_url": final_url
            })
    
    st.markdown("---")

    # AI分析サマリー
    st.subheader("🤖 AIによる分析サマリー")
    
    if model and bq_client:
        with st.spinner("AIが現在の表示内容を分析中です..."):
            try:
                # インポートと実行を一つのtryブロックで行う
                from dashboard_analyzer import get_ai_dashboard_comment
                
                comment = get_ai_dashboard_comment(
                    _bq_client=bq_client, 
                    _model=model,
                    sheet_name=selected_sheet_name,
                    filters=filters,
                    sheet_analysis_queries=sheet_analysis_queries
                )
                st.info(comment)
                
            except ImportError:
                # インポートに失敗した場合のエラー
                st.info("📊 AI分析機能は一時的に利用できません（dashboard_analyzerが見つかりません）")
                
            except Exception as e:
                # その他の実行時エラー
                st.error(f"AI分析中に予期せぬエラーが発生しました: {e}")
                # 必要であれば、ここでAIエラーハンドラを呼び出すことも可能です
                # from error_handler import handle_error_with_ai
                # handle_error_with_ai(e, model, context={...})
                
        if st.button("最新の情報で再生成", key=f"{key_prefix}_regenerate_summary"):
            st.rerun()
    else:
        st.warning("⚠️ AI分析にはBigQueryとGeminiの接続が必要です")

# =========================================================================
# システム情報・診断機能（設定管理統合版）
# =========================================================================

def get_looker_system_info() -> Dict[str, any]:
    """Looker連携システム情報の取得（設定管理統合版）"""
    looker_config = get_looker_config()
    table_config = get_bigquery_table_config()
    
    system_info = {
        "config_system_available": CONFIG_AVAILABLE,
        "bigquery_available": BIGQUERY_AVAILABLE,
        "report_id": looker_config["report_id"] or "未設定",
        "sheets_count": len(looker_config["sheets"]),
        "table_name": table_config["full_table_name"],
        "supported_sheets": list(looker_config["sheets"].keys())
    }
    
    # 設定管理システムが利用可能な場合は詳細情報追加
    if CONFIG_AVAILABLE and settings:
        validation = settings.get_validation_status()
        system_info.update({
            "settings_version": settings.version,
            "environment": settings.environment,
            "debug_mode": settings.app.debug_mode,
            "validation_status": validation["valid"],
            "validation_errors": validation["errors"],
            "validation_warnings": validation["warnings"]
        })
    
    return system_info

def validate_looker_config() -> Dict[str, any]:
    """Looker設定の検証（設定管理統合版）"""
    validation_result = {
        "status": "success",
        "errors": [],
        "warnings": []
    }
    
    # 設定管理システムが利用可能な場合は詳細検証
    if CONFIG_AVAILABLE and settings:
        settings_validation = settings.get_validation_status()
        validation_result["errors"].extend(settings_validation["errors"])
        validation_result["warnings"].extend(settings_validation["warnings"])
    else:
        validation_result["warnings"].append("設定管理システムが利用できません - 基本設定のみ")
    
    # BigQueryライブラリの確認
    if not BIGQUERY_AVAILABLE:
        validation_result["errors"].append("BigQueryライブラリが見つかりません")
    
    # Looker設定の確認
    looker_config = get_looker_config()
    if not looker_config["report_id"]:
        validation_result["errors"].append("Looker Report IDが設定されていません")
    
    # BigQuery設定の確認
    table_config = get_bigquery_table_config()
    if not table_config["project_id"]:
        validation_result["errors"].append("BigQuery Project IDが設定されていません")
    
    # シート設定の確認
    if len(looker_config["sheets"]) == 0:
        validation_result["warnings"].append("レポートシートが設定されていません")
    
    # ステータス判定
    if validation_result["errors"]:
        validation_result["status"] = "error"
    elif validation_result["warnings"]:
        validation_result["status"] = "warning"
    
    return validation_result

# =========================================================================
# 設定UI統合機能
# =========================================================================

def show_looker_settings_panel():
    """Looker設定パネル（設定管理統合版）"""
    st.subheader("📈 Looker Studio 設定")
    
    # 設定管理システムが利用可能な場合
    if CONFIG_AVAILABLE:
        st.info("💡 統合設定管理システムを使用中")
        
        # 現在の設定表示
        looker_config = get_looker_config()
        st.markdown("### 📋 現在の設定")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("レポートID", looker_config["report_id"] or "未設定")
            st.metric("シート数", len(looker_config["sheets"]))
        with col2:
            validation = validate_looker_config()
            status_color = {"success": "🟢", "warning": "🟡", "error": "🔴"}
            st.metric("設定ステータス", f"{status_color.get(validation['status'], '⚪')} {validation['status']}")
        
        # 設定パネル統合ボタン
        if st.button("⚙️ 統合設定パネルを開く"):
            st.session_state.show_config_panel = True
            st.rerun()
            
    else:
        st.warning("⚠️ 設定管理システムが利用できません")
        
        # フォールバック設定表示
        looker_config = get_looker_config()
        table_config = get_bigquery_table_config()
        
        st.code(f"""
Report ID: {looker_config['report_id'] or '未設定'}
Base URL: {looker_config['base_url']}
シート数: {len(looker_config['sheets'])}
BigQuery利用可能: {BIGQUERY_AVAILABLE}
        """)
    
    # 検証結果表示
    validation = validate_looker_config()
    if validation["status"] == "error":
        st.error("❌ 設定エラー:")
        for error in validation["errors"]:
            st.error(f"- {error}")
    elif validation["status"] == "warning":
        st.warning("⚠️ 設定警告:")
        for warning in validation["warnings"]:
            st.warning(f"- {warning}")
    else:
        st.success("✅ 設定正常")
    
    # シート一覧
    st.markdown("### 📊 利用可能シート")
    looker_config = get_looker_config()
    for sheet_name, page_id in looker_config["sheets"].items():
        st.markdown(f"- **{sheet_name}**: `{page_id}`")
    
    # ユーティリティ機能
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 フィルターリセット"):
            reset_looker_filters()
            st.success("フィルターをリセットしました")
            st.rerun()
    
    with col2:
        if st.button("🧪 接続テスト"):
            test_results = test_looker_integration()
            if test_results.get("overall_success", False):
                st.success("✅ 接続テスト成功")
            else:
                st.error("❌ 接続テストで問題が発見されました")

# =========================================================================
# ユーティリティ関数
# =========================================================================

def reset_looker_filters():
    """Lookerフィルターのリセット"""
    if "filters" in st.session_state:
        del st.session_state.filters
        if CONFIG_AVAILABLE and settings and settings.app.debug_mode:
            print("🔄 Lookerフィルターをリセットしました")

def reload_looker_config():
    """Looker設定の再読み込み"""
    if CONFIG_AVAILABLE:
        config_manager.reload_settings()
        st.success("✅ 設定を再読み込みしました")
    else:
        st.warning("⚠️ 設定管理システムが利用できません")

# =========================================================================
# テスト・統合機能
# =========================================================================

def test_looker_integration():
    """Looker統合テスト（設定管理統合版）"""
    test_results = {
        "config_system": CONFIG_AVAILABLE,
        "bigquery_available": BIGQUERY_AVAILABLE,
        "settings_valid": False,
        "report_id_set": False,
        "sheets_count": 0,
        "overall_success": False,
        "details": {}
    }
    
    try:
        # 設定検証
        validation = validate_looker_config()
        test_results["settings_valid"] = validation["status"] != "error"
        test_results["details"]["validation"] = validation
        
        # 基本設定確認
        looker_config = get_looker_config()
        test_results["report_id_set"] = bool(looker_config["report_id"])
        test_results["sheets_count"] = len(looker_config["sheets"])
        
        # システム情報取得
        system_info = get_looker_system_info()
        test_results["details"]["system_info"] = system_info
        
        # 総合判定
        test_results["overall_success"] = (
            test_results["settings_valid"] and 
            test_results["report_id_set"] and 
            test_results["sheets_count"] > 0
        )
        
    except Exception as e:
        test_results["details"]["error"] = str(e)
        print(f"❌ Looker統合テストエラー: {e}")
    
    return test_results

# =========================================================================
# 互換性維持
# =========================================================================

# 既存コードとの互換性のため、グローバル変数を維持
looker_config = get_looker_config()
REPORT_ID = looker_config["report_id"]
REPORT_SHEETS = looker_config["sheets"]
SHEET_PARAM_SETS = get_sheet_param_sets()

if __name__ == "__main__":
    # テスト実行
    print("🧪 Looker統合システム（設定管理統合版）テスト")
    results = test_looker_integration()
    
    print(f"✅ 設定システム: {results['config_system']}")
    print(f"✅ BigQuery利用可能: {results['bigquery_available']}")
    print(f"✅ 設定有効: {results['settings_valid']}")
    print(f"✅ レポートID設定: {results['report_id_set']}")
    print(f"✅ シート数: {results['sheets_count']}")
    print(f"🎯 総合判定: {results['overall_success']}")
    
    if results["details"].get("error"):
        print(f"❌ エラー: {results['details']['error']}")
    
    print("🎉 テスト完了")