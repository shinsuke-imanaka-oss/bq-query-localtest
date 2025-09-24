# looker_handler.py - 完全版 SHEET_PARAM_SETS（全18シート対応）

# シートごとのパラメータ名のセット（完全版 - 全18シート対応）
SHEET_PARAM_SETS = {
    # ========================================
    # 管理・サマリー系シート
    # ========================================
    "予算管理": {
        "date": ["budget.p_start_date", "budget.p_end_date"],
        "media": ["budget.p_media"],
        "campaign": ["budget.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_budget"
    },
    "サマリー01": {
        "date": ["campaign.p_start_date", "campaign.p_end_date"],
        "media": ["campaign.p_media"],
        "campaign": ["campaign.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign"
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
        ],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign"
    },

    # ========================================
    # 基本分析系シート
    # ========================================
    "メディア": {
        "date": ["campaign.p_start_date", "campaign.p_end_date"],
        "media": ["campaign.p_media"],
        "campaign": ["campaign.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign"
    },
    "デバイス": {
        "date": ["device.p_start_date", "device.p_end_date"],
        "media": ["device.p_media"],
        "campaign": ["device.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign_device"
    },
    "キャンペーン": {
        "date": ["campaign.p_start_date", "campaign.p_end_date"],
        "media": ["campaign.p_media"],
        "campaign": ["campaign.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign"
    },
    "広告グループ": {
        "date": ["adgroup.p_start_date", "adgroup.p_end_date"],
        "media": ["adgroup.p_media"],
        "campaign": ["adgroup.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_adgroup"
    },

    # ========================================
    # 時間軸分析系シート
    # ========================================
    "月別": {
        "date": ["campaign.p_start_date", "campaign.p_end_date"],
        "media": ["campaign.p_media"],
        "campaign": ["campaign.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign"
    },
    "日別": {
        "date": ["campaign.p_start_date", "campaign.p_end_date"],
        "media": ["campaign.p_media"],
        "campaign": ["campaign.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign"
    },
    "曜日": {
        "date": ["campaign.p_start_date", "campaign.p_end_date"],
        "media": ["campaign.p_media"],
        "campaign": ["campaign.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign"
    },
    "時間": {
        "date": ["campaign_hourly.p_start_date", "campaign_hourly.p_end_date"],
        "media": ["campaign_hourly.p_media"],
        "campaign": ["campaign_hourly.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign_hourly"
    },

    # ========================================
    # クリエイティブ分析系シート
    # ========================================
    "テキストCR": {
        "date": ["text_ad.p_start_date", "text_ad.p_end_date"],
        "media": ["text_ad.p_media"],
        "campaign": ["text_ad.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_text_ad"
    },
    "ディスプレイCR": {
        "date": ["display_ad.p_start_date", "display_ad.p_end_date"],
        "media": ["display_ad.p_media"],
        "campaign": ["display_ad.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_display_ad"
    },

    # ========================================
    # 詳細分析系シート
    # ========================================
    "キーワード": {
        "date": ["keyword.p_start_date", "keyword.p_end_date"],
        "media": ["keyword.p_media"],
        "campaign": ["keyword.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_keyword"
    },
    "地域": {
        "date": ["geo.p_start_date", "geo.p_end_date"],
        "media": ["geo.p_media"],
        "campaign": ["geo.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_geo"
    },
    "最終ページURL": {
        "date": ["landing_page.p_start_date", "landing_page.p_end_date"],
        "media": ["landing_page.p_media"],
        "campaign": ["landing_page.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_landing_page"
    },

    # ========================================
    # オーディエンス分析系シート
    # ========================================
    "性別": {
        "date": ["gender.p_start_date", "gender.p_end_date"],
        "media": ["gender.p_media"],
        "campaign": ["gender.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_gender"
    },
    "年齢": {
        "date": ["age_range.p_start_date", "age_range.p_end_date"],
        "media": ["age_range.p_media"],
        "campaign": ["age_range.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_age_range"
    }
}

# ========================================
# データソース別のパラメータパターン定義
# ========================================

# 各データソースの標準的なパラメータ命名パターン
DATA_SOURCE_PARAM_PATTERNS = {
    "budget": {
        "date": ["budget.p_start_date", "budget.p_end_date"],
        "media": ["budget.p_media"],
        "campaign": ["budget.p_campaign"]
    },
    "campaign": {
        "date": ["campaign.p_start_date", "campaign.p_end_date"],
        "media": ["campaign.p_media"],
        "campaign": ["campaign.p_campaign"]
    },
    "device": {
        "date": ["device.p_start_date", "device.p_end_date"],
        "media": ["device.p_media"],
        "campaign": ["device.p_campaign"]
    },
    "geo": {
        "date": ["geo.p_start_date", "geo.p_end_date"],
        "media": ["geo.p_media"],
        "campaign": ["geo.p_campaign"]
    },
    "gender": {
        "date": ["gender.p_start_date", "gender.p_end_date"],
        "media": ["gender.p_media"],
        "campaign": ["gender.p_campaign"]
    },
    "age_range": {
        "date": ["age_range.p_start_date", "age_range.p_end_date"],
        "media": ["age_range.p_media"],
        "campaign": ["age_range.p_campaign"]
    },
    "campaign_hourly": {
        "date": ["campaign_hourly.p_start_date", "campaign_hourly.p_end_date"],
        "media": ["campaign_hourly.p_media"],
        "campaign": ["campaign_hourly.p_campaign"]
    },
    "keyword": {
        "date": ["keyword.p_start_date", "keyword.p_end_date"],
        "media": ["keyword.p_media"],
        "campaign": ["keyword.p_campaign"]
    },
    "adgroup": {
        "date": ["adgroup.p_start_date", "adgroup.p_end_date"],
        "media": ["adgroup.p_media"],
        "campaign": ["adgroup.p_campaign"]
    },
    "text_ad": {
        "date": ["text_ad.p_start_date", "text_ad.p_end_date"],
        "media": ["text_ad.p_media"],
        "campaign": ["text_ad.p_campaign"]
    },
    "display_ad": {
        "date": ["display_ad.p_start_date", "display_ad.p_end_date"],
        "media": ["display_ad.p_media"],
        "campaign": ["display_ad.p_campaign"]
    },
    "landing_page": {
        "date": ["landing_page.p_start_date", "landing_page.p_end_date"],
        "media": ["landing_page.p_media"],
        "campaign": ["landing_page.p_campaign"]
    }
}

# ========================================
# フォールバック機能付きパラメータ取得関数
# ========================================

def get_sheet_params_with_fallback(sheet_name: str) -> dict:
    """
    シート名に対応するパラメータセットを取得（フォールバック機能付き）
    
    Args:
        sheet_name: シート名
    
    Returns:
        パラメータ辞書
    """
    # 1. 直接定義されているパラメータセットを確認
    if sheet_name in SHEET_PARAM_SETS:
        return SHEET_PARAM_SETS[sheet_name]
    
    # 2. シート名からデータソースを推定してフォールバック
    sheet_lower = sheet_name.lower()
    
    # データソース推定ロジック
    if "予算" in sheet_name or "budget" in sheet_lower:
        data_source_key = "budget"
        table_suffix = "budget"
    elif "デバイス" in sheet_name or "device" in sheet_lower:
        data_source_key = "device"
        table_suffix = "campaign_device"
    elif "地域" in sheet_name or "geo" in sheet_lower:
        data_source_key = "geo"
        table_suffix = "geo"
    elif "性別" in sheet_name or "gender" in sheet_lower:
        data_source_key = "gender"
        table_suffix = "gender"
    elif "年齢" in sheet_name or "age" in sheet_lower:
        data_source_key = "age_range"
        table_suffix = "age_range"
    elif "時間" in sheet_name or "hourly" in sheet_lower or "hour" in sheet_lower:
        data_source_key = "campaign_hourly"
        table_suffix = "campaign_hourly"
    elif "キーワード" in sheet_name or "keyword" in sheet_lower:
        data_source_key = "keyword"
        table_suffix = "keyword"
    elif "広告グループ" in sheet_name or "adgroup" in sheet_lower:
        data_source_key = "adgroup"
        table_suffix = "adgroup"
    elif "テキスト" in sheet_name or "text" in sheet_lower:
        data_source_key = "text_ad"
        table_suffix = "text_ad"
    elif "ディスプレイ" in sheet_name or "display" in sheet_lower:
        data_source_key = "display_ad"
        table_suffix = "display_ad"
    elif "ページ" in sheet_name or "landing" in sheet_lower or "url" in sheet_lower:
        data_source_key = "landing_page"
        table_suffix = "landing_page"
    else:
        # 最終フォールバック: キャンペーンベーステーブル
        data_source_key = "campaign"
        table_suffix = "campaign"
    
    # パラメータパターンを取得
    if data_source_key in DATA_SOURCE_PARAM_PATTERNS:
        params = DATA_SOURCE_PARAM_PATTERNS[data_source_key].copy()
        params["data_source"] = f"vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_{table_suffix}"
        
        # 警告メッセージ
        st.warning(f"⚠️ シート '{sheet_name}' のパラメータが未定義のため、推定設定を使用します: {data_source_key}")
        
        return params
    
    # 最終フォールバック
    st.error(f"❌ シート '{sheet_name}' のパラメータを決定できませんでした。デフォルト設定を使用します。")
    return {
        "date": ["campaign.p_start_date", "campaign.p_end_date"],
        "media": ["campaign.p_media"],
        "campaign": ["campaign.p_campaign"],
        "data_source": "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign"
    }

# ========================================
# パラメータ検証・デバッグ用関数
# ========================================

def validate_all_sheet_params():
    """
    全シートのパラメータ定義を検証
    """
    st.subheader("🔍 パラメータ定義検証結果")
    
    total_sheets = len(REPORT_SHEETS)
    defined_sheets = len(SHEET_PARAM_SETS)
    
    st.info(f"📊 総シート数: {total_sheets} / 定義済み: {defined_sheets} / 未定義: {total_sheets - defined_sheets}")
    
    # 未定義シートの一覧
    undefined_sheets = set(REPORT_SHEETS.keys()) - set(SHEET_PARAM_SETS.keys())
    if undefined_sheets:
        st.warning(f"⚠️ 未定義シート: {', '.join(sorted(undefined_sheets))}")
    
    # 定義済みシートの詳細
    with st.expander("📋 定義済みシート詳細"):
        for sheet_name, params in SHEET_PARAM_SETS.items():
            st.write(f"**{sheet_name}**")
            st.write(f"  - データソース: `{params.get('data_source', '未設定')}`")
            st.write(f"  - 日付パラメータ: {len(params.get('date', []))}個")
            st.write(f"  - メディアパラメータ: {len(params.get('media', []))}個")
            st.write(f"  - キャンペーンパラメータ: {len(params.get('campaign', []))}個")

def debug_sheet_params(sheet_name: str):
    """
    特定シートのパラメータをデバッグ表示
    """
    st.subheader(f"🔍 {sheet_name} のパラメータ詳細")
    
    if sheet_name in SHEET_PARAM_SETS:
        params = SHEET_PARAM_SETS[sheet_name]
        st.success(f"✅ 直接定義されています")
    else:
        params = get_sheet_params_with_fallback(sheet_name)
        st.warning(f"⚠️ フォールバック設定を使用")
    
    st.json(params)
    
    # パラメータの妥当性チェック
    required_keys = ["date", "media", "campaign", "data_source"]
    missing_keys = [key for key in required_keys if key not in params]
    
    if missing_keys:
        st.error(f"❌ 必須キーが不足: {missing_keys}")
    else:
        st.success("✅ 必須キーが全て存在")

# 使用例:
if __name__ == "__main__":
    # 全パラメータの検証
    validate_all_sheet_params()
    
    # 特定シートのデバッグ
    debug_sheet_params("キーワード")