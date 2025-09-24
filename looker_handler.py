# looker_handler.py

import streamlit as st
import json
from urllib.parse import quote
import datetime
import pandas as pd
from dashboard_analyzer import get_ai_dashboard_comment
import os

# --- レポート基本情報 ---
REPORT_ID = os.environ.get("LOOKER_REPORT_ID")
if not REPORT_ID:
    st.error("環境変数LOOKER_REPORT_IDが設定されていません。")
    st.stop()
    
REPORT_SHEETS = {
    "予算管理": "Gcf9",
    "サマリー01": "6HI9",
    "サマリー02": "IH29",
    "メディア": "GTrk",
    "デバイス": "kovk",
    "月別": "Bsvk",
    "日別": "40vk",
    "曜日": "hsv3",
    "キャンペーン": "cYwk",
    "広告グループ": "1ZWq",
    "テキストCR": "NfWq",
    "ディスプレイCR": "p_grkcjbbytd",
    "キーワード": "imWq",
    "地域": "ZNdq",
    "時間": "bXdq",
    "最終ページURL": "7xXq",
    "性別": "ctdq",
    "年齢": "fX53",
}

# シートごとのパラメータ名のセットを定義する辞書
# 複数のデータソースを持つシートの場合、ここにすべてのパラメータ名を追加
SHEET_PARAM_SETS = {
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
        "date": ["campaign.p_start_date", "campaign.p_end_date", "device.p_start_date", "device.p_end_date", "geo.p_start_date", "geo.p_end_date", "gender.p_start_date", "gender.p_end_date", "campaign_hourly.p_start_date", "campaign_hourly.p_end_date", "age_range.p_start_date", "age_range.p_end_date"],
        "media": ["campaign.p_media", "device.p_media", "geo.p_media", "gender.p_media", "campaign_hourly.p_media", "age_range.p_media"],
        "campaign": ["campaign.p_campaign", "device.p_campaign", "geo.p_campaign", "gender.p_campaign", "campaign_hourly.p_campaign", "age_range.p_campaign"]
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

@st.cache_data(ttl=43200)
def get_filter_options(_bq_client, table_id, column_name):
    """BigQueryからフィルタの選択肢を取得する"""
    try:
        query = f"SELECT DISTINCT {column_name} FROM `{table_id}` WHERE {column_name} IS NOT NULL ORDER BY {column_name}"
        return _bq_client.query(query).to_dataframe()[column_name].tolist()
    except Exception as e:
        st.error(f"フィルタオプションの取得中にエラーが発生しました ({column_name}): {e}")
        return []

def init_filters():
    """filtersセッションの初期化"""
    if "filters" not in st.session_state:
        st.session_state.filters = {}

    defaults = {
        "sheet": "メディア",
        "start_date": datetime.date.today() - datetime.timedelta(days=30),
        "end_date": datetime.date.today(),
        "media": [],
        "campaigns": []
    }

    for key, value in defaults.items():
        if key not in st.session_state.filters:
            st.session_state.filters[key] = value

def show_filter_ui(bq_client):
    """サイドバーに表示するフィルタUIを構築し、結果をsession_stateに保存する"""
    init_filters()

    # フィルタの変更を追跡するための古い状態を保存
    old_filters = st.session_state.filters.copy()

    # シート選択
    sheet_names = list(REPORT_SHEETS.keys())
    selected_sheet_name = st.selectbox(
        "表示するレポートシートを選択:",
        sheet_names,
        index=sheet_names.index(st.session_state.filters.get("sheet", "メディア")),
    )
    st.session_state.filters["sheet"] = selected_sheet_name
    
    st.markdown("---")

    # 日付入力
    start_date = st.date_input("開始日", value=st.session_state.filters["start_date"])
    end_date = st.date_input("終了日", value=st.session_state.filters["end_date"])

    table_id_for_filters = "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign"
    media_options = get_filter_options(bq_client, table_id_for_filters, "ServiceNameJA_Media")
    campaign_options = get_filter_options(bq_client, table_id_for_filters, "CampaignName")

    selected_media = st.multiselect(
        "メディア",
        options=media_options,
        default=st.session_state.filters["media"]
    )
    selected_campaigns = st.multiselect(
        "キャンペーン",
        options=campaign_options,
        default=st.session_state.filters["campaigns"]
    )

    # 選択状態を保存
    st.session_state.filters.update({
        "start_date": start_date,
        "end_date": end_date,
        "media": selected_media,
        "campaigns": selected_campaigns
    })
    
    # フィルタの変更を検出して再実行
    if st.session_state.filters != old_filters:
        st.rerun()

def show_looker_studio_integration(bq_client, model, key_prefix="", sheet_analysis_queries=None):
    # 既存のinit_filters()呼び出し
    init_filters()

    # --- 修正箇所 ---
    # selected_sheet_nameをセッションステートから取得
    selected_sheet_name = st.session_state.filters["sheet"]

    # filters変数をセッションステートから取得
    filters = st.session_state.filters

    selected_page_id = REPORT_SHEETS[selected_sheet_name]

    # 選択されたシートのパラメータセットを取得。ない場合は空の辞書を使用
    param_sets = SHEET_PARAM_SETS.get(selected_sheet_name, {})
    params = {}


    # Streamlitのフィルタ適用がONの場合のみパラメータを渡す
    if st.session_state.get("apply_streamlit_filters", True):
        # 日付フィルタ
        date_params = param_sets.get("date", [])
        if date_params and filters.get("start_date") and filters.get("end_date"):
            start_date_str = filters["start_date"].strftime("%Y%m%d")
            end_date_str = filters["end_date"].strftime("%Y%m%d")
            for param_name in date_params:
                if "start_date" in param_name:
                    params[param_name] = start_date_str
                elif "end_date" in param_name:
                    params[param_name] = end_date_str

        # メディアフィルタ
        media_params = param_sets.get("media", [])
        if filters.get("media"):
            media_value = ",".join(filters["media"])
            for param_name in media_params:
                params[param_name] = media_value
        else:
            for param_name in media_params:
                params[param_name] = ""

        # キャンペーンフィルタ
        campaign_params = param_sets.get("campaign", [])
        if filters.get("campaigns"):
            campaign_value = ",".join(filters["campaigns"])
            for param_name in campaign_params:
                params[param_name] = campaign_value
        else:
            for param_name in campaign_params:
                params[param_name] = ""
    
    # URL生成
    params_json = json.dumps(params)
    encoded_params = quote(params_json)
    base_url = f"https://lookerstudio.google.com/embed/reporting/{REPORT_ID}"
    final_url = f"{base_url}/page/{selected_page_id}?params={encoded_params}"

    # Looker Studioのフィルタを非表示にするパラメータを条件付きで追加
    if st.session_state.get("apply_streamlit_filters", True):
        final_url += "&hideFilters=true"
    else:
        final_url += "&hideFilters=false"

    # デバッグ情報
    #st.subheader("💡 デバッグ情報")
    #st.write(f"**生成されたURL:** `{final_url}`")
    #st.write(f"**パラメータ辞書:** `{params}`")
    #st.markdown("---")

    # iframeで表示
    st.components.v1.iframe(final_url, height=600, scrolling=True)
    st.markdown("---")

    st.subheader("🤖 AIによる分析サマリー")
    with st.spinner("AIが現在の表示内容を分析中です..."):
        comment = get_ai_dashboard_comment(
            _bq_client=bq_client,
            _model=model,
            sheet_name=st.session_state.filters["sheet"],
            filters=st.session_state.filters,
            sheet_analysis_queries=sheet_analysis_queries
        )
        st.info(comment)

    # 再生成ボタンも用意
    if st.button("最新の情報で再生成", key=f"{key_prefix}_regenerate_summary"):
        get_ai_dashboard_comment.clear()
        st.rerun()