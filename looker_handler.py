# looker_handler.py - フィルタ機能修正版

import streamlit as st
import json
from urllib.parse import quote
import datetime
import pandas as pd
from dashboard_analyzer import get_ai_dashboard_comment
import os

# --- レポート基本情報 ---
REPORT_ID = st.secrets.get("LOOKER_REPORT_ID") or os.environ.get("LOOKER_REPORT_ID")
if not REPORT_ID:
    st.error("環境変数またはsecrets.tomlにLOOKER_REPORT_IDが設定されていません。")
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

@st.cache_data(ttl=43200)  # 12時間キャッシュ
def get_filter_options(_bq_client, table_id, column_name):
    """BigQueryからフィルタの選択肢を取得する"""
    try:
        query = f"SELECT DISTINCT {column_name} FROM `{table_id}` WHERE {column_name} IS NOT NULL ORDER BY {column_name}"
        result = _bq_client.query(query).to_dataframe()
        return result[column_name].tolist()
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
    """サイドバーに表示するフィルタUIを構築し、結果をsession_stateに保存する（修正版）"""
    init_filters()

    # フィルタの変更を追跡するための古い状態を保存
    old_filters = st.session_state.filters.copy()

    # シート選択
    sheet_names = list(REPORT_SHEETS.keys())
    selected_sheet_name = st.selectbox(
        "📊 表示するレポートシートを選択:",
        sheet_names,
        index=sheet_names.index(st.session_state.filters.get("sheet", "メディア")),
        key="sheet_selector"
    )
    st.session_state.filters["sheet"] = selected_sheet_name
    
    st.markdown("---")
    st.markdown("### 🗓️ 期間設定")

    # 日付入力
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "開始日", 
            value=st.session_state.filters["start_date"],
            key="start_date_input"
        )
    with col2:
        end_date = st.date_input(
            "終了日", 
            value=st.session_state.filters["end_date"],
            key="end_date_input"
        )

    st.markdown("### 📺 メディア設定")
    
    # メディアフィルタ（BigQueryから実際のデータを取得）
    table_id_for_filters = "vorn-digi-mktg-poc-635a.toki_air.LookerStudio_report_campaign"
    
    try:
        # ServiceNameJA_Media の実際の値を取得
        media_options = get_filter_options(bq_client, table_id_for_filters, "ServiceNameJA_Media")
        
        if media_options:
            selected_media = st.multiselect(
                "メディアを選択",
                options=media_options,
                default=st.session_state.filters["media"],
                key="media_selector",
                help="ServiceNameJA_Media列の実際の値から選択"
            )
        else:
            st.warning("⚠️ メディアオプションの取得に失敗しました")
            selected_media = st.multiselect(
                "メディアを選択（手動入力）",
                options=["Google広告", "Facebook広告", "Yahoo!広告", "LINE広告"],
                default=st.session_state.filters["media"],
                key="media_selector_manual"
            )
    except Exception as e:
        st.error(f"メディアフィルタの初期化エラー: {e}")
        selected_media = []

    st.markdown("### 📋 キャンペーン設定")
    
    try:
        # CampaignName の実際の値を取得（個別のキャンペーン名）
        campaign_options = get_filter_options(bq_client, table_id_for_filters, "CampaignName")
        
        if campaign_options:
            # 選択可能な数を制限（パフォーマンス考慮）
            if len(campaign_options) > 100:
                st.info(f"💡 {len(campaign_options)}個のキャンペーンがあります。検索機能を使って絞り込んでください。")
            
            selected_campaigns = st.multiselect(
                "キャンペーンを選択",
                options=campaign_options,
                default=st.session_state.filters["campaigns"],
                key="campaign_selector",
                help="CampaignName列の実際の値から選択"
            )
        else:
            st.warning("⚠️ キャンペーンオプションの取得に失敗しました")
            selected_campaigns = st.text_input(
                "キャンペーン名を入力（カンマ区切り）",
                value=",".join(st.session_state.filters["campaigns"]),
                key="campaign_text_input"
            )
            selected_campaigns = [c.strip() for c in selected_campaigns.split(",") if c.strip()]
    except Exception as e:
        st.error(f"キャンペーンフィルタの初期化エラー: {e}")
        selected_campaigns = []

    # フィルタの適用設定
    st.markdown("---")
    apply_filters = st.checkbox(
        "🔗 Looker Studioにフィルタを適用",
        value=st.session_state.get("apply_streamlit_filters", True),
        key="apply_filters_checkbox",
        help="チェックを外すとLooker Studio側の元のフィルタを使用します"
    )

    # 選択状態を保存
    st.session_state.filters.update({
        "start_date": start_date,
        "end_date": end_date,
        "media": selected_media,
        "campaigns": selected_campaigns
    })
    st.session_state.apply_streamlit_filters = apply_filters
    
    # デバッグ情報の表示（オプション）
    if st.checkbox("🔍 デバッグ情報を表示", key="show_debug_info"):
        with st.expander("デバッグ情報"):
            st.write("**現在のフィルタ設定:**")
            st.json(st.session_state.filters)
            st.write("**フィルタ適用:**", apply_filters)

    # フィルタの変更を検出して再実行
    if st.session_state.filters != old_filters or apply_filters != st.session_state.get("prev_apply_filters", True):
        st.session_state.prev_apply_filters = apply_filters
        st.rerun()

def show_looker_studio_integration(bq_client, model, key_prefix="", sheet_analysis_queries=None):
    """Looker Studio統合表示（修正版）"""
    # フィルタ初期化
    init_filters()

    # 現在の設定を取得
    selected_sheet_name = st.session_state.filters["sheet"]
    filters = st.session_state.filters

    # 選択されたシートのページIDを取得
    selected_page_id = REPORT_SHEETS.get(selected_sheet_name)
    if not selected_page_id:
        st.error(f"シート '{selected_sheet_name}' のページIDが見つかりません")
        return

    # パラメータセットを取得
    param_sets = SHEET_PARAM_SETS.get(selected_sheet_name, {})
    params = {}

    # Streamlitのフィルタ適用がONの場合のみパラメータを渡す
    if st.session_state.get("apply_streamlit_filters", True):
        # 日付フィルタの適用
        date_params = param_sets.get("date", [])
        if date_params and filters.get("start_date") and filters.get("end_date"):
            start_date_str = filters["start_date"].strftime("%Y%m%d")
            end_date_str = filters["end_date"].strftime("%Y%m%d")
            
            for param_name in date_params:
                if "start_date" in param_name or "p_start_date" in param_name:
                    params[param_name] = start_date_str
                elif "end_date" in param_name or "p_end_date" in param_name:
                    params[param_name] = end_date_str

        # メディアフィルタの適用
        media_params = param_sets.get("media", [])
        if media_params and filters.get("media"):
            media_value = ",".join(filters["media"])
            for param_name in media_params:
                params[param_name] = media_value

        # キャンペーンフィルタの適用
        campaign_params = param_sets.get("campaign", [])
        if campaign_params and filters.get("campaigns"):
            campaign_value = ",".join(filters["campaigns"])
            for param_name in campaign_params:
                params[param_name] = campaign_value

    # URL生成
    try:
        params_json = json.dumps(params, ensure_ascii=False)
        encoded_params = quote(params_json)
        base_url = f"https://lookerstudio.google.com/embed/reporting/{REPORT_ID}"
        final_url = f"{base_url}/page/{selected_page_id}"
        
        # パラメータがある場合のみ追加
        if params:
            final_url += f"?params={encoded_params}"
            
        # フィルタ表示/非表示の設定
        separator = "&" if "?" in final_url else "?"
        if st.session_state.get("apply_streamlit_filters", True):
            final_url += f"{separator}hideFilters=true"
        else:
            final_url += f"{separator}hideFilters=false"
            
    except Exception as e:
        st.error(f"URL生成エラー: {e}")
        final_url = f"https://lookerstudio.google.com/embed/reporting/{REPORT_ID}/page/{selected_page_id}"

    # デバッグ情報（設定されている場合のみ表示）
    if st.session_state.get("show_debug_info", False):
        with st.expander("🔧 URL生成デバッグ情報"):
            st.write("**生成されたURL:**")
            st.code(final_url)
            st.write("**パラメータ辞書:**")
            st.json(params)
            st.write("**選択されたシート:**", selected_sheet_name)
            st.write("**ページID:**", selected_page_id)

    # Looker Studio表示
    try:
        st.markdown(f"### 📊 {selected_sheet_name} レポート")
        
        # 現在のフィルタ状況を表示
        if st.session_state.get("apply_streamlit_filters", True):
            filter_summary = []
            if filters.get("start_date") and filters.get("end_date"):
                filter_summary.append(f"📅 {filters['start_date']} ～ {filters['end_date']}")
            if filters.get("media"):
                filter_summary.append(f"📺 {', '.join(filters['media'][:3])}{'...' if len(filters['media']) > 3 else ''}")
            if filters.get("campaigns"):
                filter_summary.append(f"📋 {len(filters['campaigns'])}個のキャンペーン")
            
            if filter_summary:
                st.info(f"🔍 適用中のフィルタ: {' | '.join(filter_summary)}")
        else:
            st.info("🔗 Looker Studio側のフィルタを使用中")
        
        # iframe表示
        st.components.v1.iframe(final_url, height=600, scrolling=True)
        
    except Exception as e:
        st.error(f"Looker Studio表示エラー: {e}")
        st.info("💡 URL を直接開いてください:")
        st.code(final_url)

    st.markdown("---")

    # AI分析サマリー
    st.subheader("🤖 AIによる分析サマリー")
    with st.spinner("AIが現在の表示内容を分析中です..."):
        try:
            comment = get_ai_dashboard_comment(
                _bq_client=bq_client,
                _model=model,
                sheet_name=selected_sheet_name,
                filters=filters,
                sheet_analysis_queries=sheet_analysis_queries or {}
            )
            st.info(comment)
        except Exception as e:
            st.warning(f"AI分析の生成に失敗しました: {e}")
            st.info("💡 データが正常に表示されている場合、手動で分析を行うか、しばらく後に再試行してください。")

    # 再生成ボタン
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 最新の情報で再生成", key=f"{key_prefix}_regenerate_summary"):
            get_ai_dashboard_comment.clear()
            st.rerun()
    
    with col2:
        if st.button("📊 新しいタブで開く", key=f"{key_prefix}_open_new_tab"):
            st.markdown(f"[Looker Studioで開く]({final_url})")

# =========================================================================
# ユーティリティ関数
# =========================================================================

def clear_filter_cache():
    """フィルタキャッシュをクリア"""
    get_filter_options.clear()
    st.success("フィルタキャッシュをクリアしました")

def export_current_filters():
    """現在のフィルタ設定をエクスポート"""
    import json
    filter_export = {
        "filters": st.session_state.get("filters", {}),
        "apply_streamlit_filters": st.session_state.get("apply_streamlit_filters", True),
        "export_timestamp": datetime.datetime.now().isoformat()
    }
    return json.dumps(filter_export, ensure_ascii=False, indent=2, default=str)

def import_filters(import_data: str):
    """フィルタ設定をインポート"""
    try:
        import json
        data = json.loads(import_data)
        
        if "filters" in data:
            st.session_state.filters = data["filters"]
        if "apply_streamlit_filters" in data:
            st.session_state.apply_streamlit_filters = data["apply_streamlit_filters"]
            
        st.success("✅ フィルタ設定をインポートしました")
        st.rerun()
    except Exception as e:
        st.error(f"❌ インポートに失敗しました: {e}")