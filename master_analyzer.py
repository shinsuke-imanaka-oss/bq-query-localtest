# master_analyzer.py - 統合分析レポート機能（過去比較対応版）
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from typing import Optional, Dict, Any

# 過去比較ユーティリティのインポート
try:
    from comparison_utils import (
        calculate_comparison_period,
        ui_option_to_comparison_type,
        comparison_type_to_label,
        calculate_dataframe_changes,
        classify_metrics_by_performance,
        generate_comparison_summary,
        create_comparison_table_data,
        validate_comparison_data,
        get_direction_icon
    )
    COMPARISON_UTILS_AVAILABLE = True
except ImportError:
    COMPARISON_UTILS_AVAILABLE = False
    st.warning("⚠️ comparison_utils.pyが見つかりません。過去比較機能が制限されます。")

# 依存モジュールの動的インポート
try:
    from performance_analyzer import get_performance_data, calculate_kpis
    PERF_AVAILABLE = True
except ImportError:
    PERF_AVAILABLE = False

try:
    from forecast_analyzer import get_daily_kpi_data, get_forecast_data
    FORECAST_AVAILABLE = True
except ImportError:
    FORECAST_AVAILABLE = False

try:
    from insight_miner import find_key_drivers_safe
    INSIGHT_AVAILABLE = True
except ImportError:
    INSIGHT_AVAILABLE = False


# ============================================================
# 📊 データ収集関数（過去比較対応版）
# ============================================================

def calculate_differences(current_data, comparison_data):
    """
    現在期間と比較期間の差分を計算
    
    Args:
        current_data: 現在期間の分析結果
        comparison_data: 比較期間の分析結果
    
    Returns:
        差分データ (パーセンテージと絶対値)
    """
    differences = {}
    
    # 各分析カテゴリーごとに差分を計算
    for category in current_data.keys():
        if category not in comparison_data:
            continue
        
        current_val = current_data[category]
        compare_val = comparison_data[category]
        
        # エラーがある場合はスキップ
        if isinstance(current_val, dict) and "error" in current_val:
            continue
        if isinstance(compare_val, dict) and "error" in compare_val:
            continue
        
        differences[category] = {}
        
        # DataFrameの場合
        if isinstance(current_val, pd.DataFrame) and isinstance(compare_val, pd.DataFrame):
            differences[category] = calculate_dataframe_differences(
                current_val, compare_val
            )
        
        # 辞書の場合
        elif isinstance(current_val, dict) and isinstance(compare_val, dict):
            differences[category] = calculate_dict_differences(
                current_val, compare_val
            )
        
        # 数値の場合
        elif isinstance(current_val, (int, float)) and isinstance(compare_val, (int, float)):
            differences[category] = calculate_numeric_difference(
                current_val, compare_val
            )
    
    return differences


def calculate_numeric_difference(current_val, compare_val):
    """数値の差分計算"""
    if compare_val != 0:
        change_rate = ((current_val - compare_val) / compare_val) * 100
    else:
        change_rate = 0 if current_val == 0 else float('inf')
    
    return {
        "current": current_val,
        "comparison": compare_val,
        "change": current_val - compare_val,
        "change_rate": round(change_rate, 2)
    }


def calculate_dict_differences(current_dict, compare_dict):
    """辞書の差分計算"""
    result = {}
    
    for key in current_dict.keys():
        if key not in compare_dict:
            continue
        
        current_val = current_dict[key]
        compare_val = compare_dict[key]
        
        if isinstance(current_val, (int, float)) and isinstance(compare_val, (int, float)):
            result[key] = calculate_numeric_difference(current_val, compare_val)
        elif isinstance(current_val, dict) and isinstance(compare_val, dict):
            result[key] = calculate_dict_differences(current_val, compare_val)
    
    return result


def calculate_dataframe_differences(current_df, compare_df):
    """DataFrameの差分計算"""
    result = {
        "summary": {},
        "row_count_change": {
            "current": len(current_df),
            "comparison": len(compare_df),
            "change": len(current_df) - len(compare_df)
        }
    }
    
    # 数値カラムのみ抽出
    numeric_cols = current_df.select_dtypes(include=['number']).columns
    
    for col in numeric_cols:
        if col in compare_df.columns:
            current_sum = current_df[col].sum()
            compare_sum = compare_df[col].sum()
            
            result["summary"][col] = calculate_numeric_difference(
                current_sum, compare_sum
            )
    
    return result

def gather_all_analyses(bq_client, start_date, end_date):
    """
    指定期間の分析データを取得（比較機能なし）
    
    Args:
        bq_client: BigQueryクライアント
        start_date: 開始日
        end_date: 終了日
    
    Returns:
        分析結果を含む辞書
    """
    results = {}
    
    # パフォーマンス診断
    if PERF_AVAILABLE:
        try:
            perf_data = get_performance_data(bq_client, start_date, end_date)
            results["performance"] = calculate_kpis(perf_data)
        except Exception as e:
            print(f"パフォーマンス診断エラー: {e}")
            results["performance"] = {"error": str(e)}
    else:
        results["performance"] = {"error": "performance_analyzer not available"}
    
    # 予測分析
    if FORECAST_AVAILABLE:
        try:
            results["prediction"] = get_forecast_data(
                bq_client, start_date, end_date
            )
        except Exception as e:
            print(f"予測分析エラー: {e}")
            results["prediction"] = {"error": str(e)}
    else:
        results["prediction"] = {"error": "forecast_analyzer not available"}
    
    # 自動インサイト
    if INSIGHT_AVAILABLE:
        try:
            results["insights"] = find_key_drivers_safe(
                bq_client, target_kpi_en='cvr', start_date = start_date, end_date = end_date
            )
        except Exception as e:
            print(f"自動インサイトエラー: {e}")
            results["insights"] = {"error": str(e)}
    else:
        results["insights"] = {"error": "insight_miner not available"}
    
    return results

def gather_all_analyses_with_comparison(
    bq_client,
    current_start: date,
    current_end: date,
    comparison_type: str = None
) -> Dict[str, Any]:
    """
    現在期間と比較期間の両方のデータを取得・統合（メイン関数）
    
    Args:
        bq_client: BigQueryクライアント
        current_start: 現在期間の開始日
        current_end: 現在期間の終了日
        comparison_type: "1week", "1month", "3month", "1year", または None
    
    Returns:
        現在・比較・差分データを含む辞書
    """
    # 現在期間のデータ取得
    with st.spinner("ステップ1: 現在期間のデータを収集中..."):
        current_results = gather_all_analyses(bq_client, current_start, current_end)
    
    if current_results.get("error"):
        return current_results
    
    # 比較機能が無効な場合
    if not comparison_type or not COMPARISON_UTILS_AVAILABLE:
        return {
            "current": current_results,
            "comparison": None,
            "differences": None,
            "comparison_enabled": False,
            "current_period": f"{current_start} 〜 {current_end}",
            "comparison_period": None
        }
    
    # 比較期間の計算
    try:
        compare_start, compare_end = calculate_comparison_period(
            current_start, current_end, comparison_type
        )
    except Exception as e:
        st.warning(f"比較期間の計算でエラー: {e}")
        return {
            "current": current_results,
            "comparison": None,
            "differences": None,
            "comparison_enabled": False
        }
    
    # 比較期間のデータ取得
    with st.spinner(f"ステップ2: 比較期間（{compare_start} 〜 {compare_end}）のデータを収集中..."):
        comparison_results = gather_all_analyses(bq_client, compare_start, compare_end)
    
    if comparison_results.get("error"):
        st.warning(f"⚠️ 比較期間のデータ取得に失敗: {comparison_results['error']}")
        return {
            "current": current_results,
            "comparison": None,
            "differences": None,
            "comparison_enabled": False,
            "current_period": f"{current_start} 〜 {current_end}"
        }
    
    # 差分の計算
    with st.spinner("ステップ3: 変化率を計算中..."):
        differences = calculate_all_differences(current_results, comparison_results)
    
    # ラベル生成
    comparison_label = f"{comparison_type_to_label(comparison_type)} ({compare_start} 〜 {compare_end})"
    
    return {
        "current": current_results,
        "comparison": comparison_results,
        "differences": differences,
        "comparison_enabled": True,
        "comparison_type": comparison_type,
        "comparison_period_label": comparison_label,
        "current_period": f"{current_start} 〜 {current_end}",
        "comparison_period": f"{compare_start} 〜 {compare_end}"
    }


def calculate_all_differences(
    current_results: Dict[str, Any],
    comparison_results: Dict[str, Any]
) -> Dict[str, Any]:
    """
    すべての分析結果の差分を計算
    
    Args:
        current_results: 現在期間の分析結果
        comparison_results: 比較期間の分析結果
    
    Returns:
        差分情報を含む辞書
    """
    differences = {
        "performance": {},
        "summary": {}
    }
    
    # パフォーマンス指標の比較
    current_perf = current_results.get("performance")
    compare_perf = comparison_results.get("performance")
    
    is_valid, error_msg = validate_comparison_data(current_perf, compare_perf)
    
    if is_valid and isinstance(current_perf, pd.DataFrame):
        perf_changes = calculate_dataframe_changes(current_perf, compare_perf)
        differences["performance"] = perf_changes
        
        # 改善/悪化の分類
        classification = classify_metrics_by_performance(perf_changes)
        differences["summary"] = {
            **classification,
            "text": generate_comparison_summary(perf_changes, classification)
        }
    else:
        if error_msg:
            st.warning(f"パフォーマンス比較: {error_msg}")
    
    return differences


# ============================================================
# 🤖 AIレポート生成関数（過去比較対応版）
# ============================================================

def generate_executive_summary(
    analysis_results: Dict, 
    model_choice: str, 
    gemini_model, 
    claude_client, 
    claude_model_name
) -> str:
    """
    収集した分析結果を基にAIがエグゼクティブサマリーを生成（過去比較対応）
    
    Args:
        analysis_results: gather_all_analyses_with_comparison() の戻り値
        model_choice: "Gemini" または "Claude"
        gemini_model: Geminiモデルインスタンス
        claude_client: Claudeクライアント
        claude_model_name: Claudeモデル名
    
    Returns:
        AIが生成したサマリーテキスト
    """
    # 現在期間のデータ整形
    current_data = analysis_results.get("current", {})
    perf_summary = "データなし"
    if current_data.get("performance") is not None:
        perf_df = current_data["performance"]
        if isinstance(perf_df, pd.DataFrame):
            perf_summary = perf_df.to_string()
    
    # 基本プロンプト
    prompt_parts = [
        "あなたは経営層に報告を行う優秀なデジタルマーケティングアナリストです。",
        "以下の分析データを統合し、簡潔で示唆に富んだエグゼクティブサマリーを作成してください。",
        "",
        "## 分析データ",
        "",
        "### 1. パフォーマンス診断（現在期間）",
        perf_summary,
        ""
    ]
    
    # 過去比較が有効な場合は追加情報を含める
    if analysis_results.get("comparison_enabled"):
        comparison_label = analysis_results.get("comparison_period_label", "")
        differences = analysis_results.get("differences", {})
        
        # 比較期間のデータ
        compare_data = analysis_results.get("comparison", {})
        compare_perf_summary = "データなし"
        if compare_data.get("performance") is not None:
            compare_perf = compare_data["performance"]
            if isinstance(compare_perf, pd.DataFrame):
                compare_perf_summary = compare_perf.to_string()
        
        prompt_parts.extend([
            f"### 2. パフォーマンス診断（{comparison_label}）",
            compare_perf_summary,
            "",
            "### 3. 主要指標の変化",
        ])
        
        # 指標ごとの変化を追加
        perf_changes = differences.get("performance", {})
        for metric, change_data in perf_changes.items():
            direction = "改善" if change_data.get("is_improvement") else "悪化"
            prompt_parts.append(
                f"- {metric.upper()}: {change_data['change_rate']:+.1f}% ({direction})"
            )
        
        prompt_parts.extend([
            "",
            f"### 4. サマリー",
            differences.get("summary", {}).get("text", ""),
            ""
        ])
    
    # 出力形式の指定
    prompt_parts.extend([
        "## 出力形式",
        "",
        "以下の構成で、各セクション3-4文程度で簡潔にまとめてください：",
        "",
        "**📊 現状の要約**"
    ])
    
    if analysis_results.get("comparison_enabled"):
        prompt_parts.append("（アカウント全体の健全性と、前回からの主要な変化について）")
    else:
        prompt_parts.append("（アカウント全体の健全性について）")
    
    prompt_parts.extend([
        "",
        "**🔮 将来の見通し**",
        "（予測分析の結果を踏まえて）",
        "",
        "**💡 成功と課題の要因**"
    ])
    
    if analysis_results.get("comparison_enabled"):
        prompt_parts.append("（変化が起きた要因と、パフォーマンス分析を結びつけて）")
    else:
        prompt_parts.append("（パフォーマンスと要因分析を結びつけて）")
    
    prompt_parts.extend([
        "",
        "**🎯 推奨アクション**",
        "（最もインパクトの大きい施策を1-2つ提案）"
    ])
    
    prompt = "\n".join(prompt_parts)
    
    # AIでサマリー生成
    try:
        with st.spinner(f"ステップ4/4: {model_choice}が最終レポートを作成中..."):
            if model_choice == "Gemini" and gemini_model:
                response = gemini_model.generate_content(prompt)
                return response.text
            elif model_choice == "Claude" and claude_client:
                response = claude_client.messages.create(
                    model=claude_model_name,
                    max_tokens=2000,
                    messages=[{"role": "user", "content": prompt}]
                )
                return response.content[0].text
            else:
                return "選択したAIモデルが利用できません。"
    except Exception as e:
        return f"❌ AIレポート生成中にエラーが発生: {str(e)}"


# ============================================================
# 📈 過去比較詳細表示関数
# ============================================================

def show_comparison_details(report: Dict):
    """
    過去比較の詳細を表示
    
    Args:
        report: セッション状態に保存されたレポートデータ
    """
    if not report.get("comparison_enabled"):
        st.info("💡 過去比較は有効になっていません。設定で有効化してください。")
        return
    
    st.subheader("📊 期間比較サマリー")
    
    # 期間表示
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            "📅 現在期間", 
            report.get("current_period", "N/A")
        )
    with col2:
        st.metric(
            "📅 比較期間", 
            report.get("comparison_period_label", "N/A")
        )
    
    st.markdown("---")
    
    # 変化サマリー
    differences = report.get("details", {}).get("differences", {})
    summary_text = differences.get("summary", {}).get("text", "データなし")
    
    st.info(f"**変化の概要**: {summary_text}")
    
    st.markdown("---")
    
    # 指標比較テーブル
    st.subheader("📈 主要指標の変化")
    
    perf_changes = differences.get("performance", {})
    
    if perf_changes:
        table_data = create_comparison_table_data(perf_changes)
        
        if table_data:
            comparison_df = pd.DataFrame(table_data)
            st.dataframe(comparison_df, use_container_width=True, hide_index=True)
        else:
            st.warning("比較データが生成できませんでした")
    else:
        st.warning("パフォーマンス比較データがありません")
    
    # 改善/悪化サマリー
    st.markdown("---")
    st.subheader("📊 指標の分類")
    
    col1, col2, col3 = st.columns(3)
    
    summary = differences.get("summary", {})
    improved = summary.get("improved", [])
    declined = summary.get("declined", [])
    stable = summary.get("stable", [])
    
    with col1:
        st.success(f"**📈 改善 ({len(improved)})**")
        if improved:
            for metric in improved:
                change_data = perf_changes.get(metric, {})
                rate = change_data.get("change_rate", 0)
                st.write(f"✅ {metric.upper()}: {rate:+.1f}%")
        else:
            st.write("なし")
    
    with col2:
        st.error(f"**📉 悪化 ({len(declined)})**")
        if declined:
            for metric in declined:
                change_data = perf_changes.get(metric, {})
                rate = change_data.get("change_rate", 0)
                st.write(f"⚠️ {metric.upper()}: {rate:+.1f}%")
        else:
            st.write("なし")
    
    with col3:
        st.info(f"**→ 横ばい ({len(stable)})**")
        if stable:
            for metric in stable:
                change_data = perf_changes.get(metric, {})
                rate = change_data.get("change_rate", 0)
                st.write(f"→ {metric.upper()}: {rate:+.1f}%")
        else:
            st.write("なし")


# ============================================================
# 🎨 メインUI表示関数（過去比較対応版）
# ============================================================

def show_comprehensive_report_mode():
    """
    統合分析レポートモードのUIを表示し、分析フローを制御する
    main.pyから呼び出される主要な関数
    """
    st.header("📊 統合分析レポート")
    st.markdown("複数のAI分析を連携させ、アカウント全体の状況を一つのレポートに統合します。")
    
    # セッション状態から必要なクライアントを取得
    bq_client = st.session_state.get("bq_client")
    gemini_model = st.session_state.get("gemini_model")
    claude_client = st.session_state.get("claude_client")
    claude_model_name = st.session_state.get("claude_model_name")
    
    # 必須クライアントの存在チェック
    if not bq_client:
        st.error("❌ BigQueryに接続してください。")
        return
    
    if not gemini_model and not claude_client:
        st.error("❌ この機能を利用するには、GeminiまたはClaudeのいずれかに接続してください。")
        return
    
    # 利用可能なモジュールの確認
    available_modules = []
    if PERF_AVAILABLE:
        available_modules.append("パフォーマンス診断")
    if FORECAST_AVAILABLE:
        available_modules.append("予測分析")
    if INSIGHT_AVAILABLE:
        available_modules.append("要因分析")
    
    if not available_modules:
        st.error("❌ 利用可能な分析モジュールがありません。")
        return
    
    st.info(f"✅ 利用可能な分析: {', '.join(available_modules)}")
    
    if COMPARISON_UTILS_AVAILABLE:
        st.success("✅ 過去比較機能が利用可能です")
    
    # --- 1. コントロールパネルUI ---
    with st.expander("📋 分析設定", expanded=True):
        # AIモデル選択
        st.subheader("1️⃣ レポート生成AI")
        model_options = []
        if gemini_model: 
            model_options.append("Gemini")
        if claude_client: 
            model_options.append("Claude")
        
        model_choice = st.selectbox(
            "AIモデルを選択",
            options=model_options,
            help="レポートのサマリーを生成するAIモデルを選択します"
        )
        
        st.markdown("---")
        
        # 日付範囲選択
        st.subheader("2️⃣ 分析期間")
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "開始日", 
                value=date.today() - timedelta(days=90)
            )
        with col2:
            end_date = st.date_input(
                "終了日", 
                value=date.today() - timedelta(days=1)
            )
        
        st.markdown("---")
        
        # 過去比較設定
        st.subheader("3️⃣ 過去比較設定")
        
        comparison_enabled = st.checkbox(
            "過去データとの比較を有効にする",
            value=True,
            help="過去の同期間データと比較して、変化率や改善点を分析します",
            disabled=not COMPARISON_UTILS_AVAILABLE
        )
        
        comparison_type = None
        if comparison_enabled and COMPARISON_UTILS_AVAILABLE:
            comparison_option = st.selectbox(
                "比較期間を選択",
                options=[
                    "vs 1週間前",
                    "vs 1ヶ月前",
                    "vs 3ヶ月前",
                    "vs 前年同期"
                ],
                index=1,
                help="選択した期間前のデータと比較します"
            )
            comparison_type = ui_option_to_comparison_type(comparison_option)
        
        st.info("💡 このレポートは、利用可能な分析モジュールを統合して生成します。")
    
    # --- 2. レポート生成ボタンとロジック呼び出し ---
    if st.button("🚀 最新データで統合分析レポートを生成", type="primary"):
        # データ収集（過去比較対応）
        analysis_results = gather_all_analyses_with_comparison(
            bq_client, 
            start_date, 
            end_date,
            comparison_type if comparison_enabled else None
        )
        
        if "error" in analysis_results.get("current", {}):
            st.error(analysis_results["current"]["error"])
            return
        
        # AIサマリー生成
        summary = generate_executive_summary(
            analysis_results, 
            model_choice, 
            gemini_model, 
            claude_client, 
            claude_model_name
        )
        
        # 結果をセッション状態に保存
        st.session_state.comprehensive_report = {
            "summary": summary,
            "details": analysis_results,
            "model_used": model_choice,
            "generated_at": date.today().isoformat(),
            "comparison_enabled": comparison_enabled and analysis_results.get("comparison_enabled", False),
            "current_period": analysis_results.get("current_period"),
            "comparison_period_label": analysis_results.get("comparison_period_label")
        }
        
        st.success("✅ レポート生成完了！")
        st.rerun()
    
    # --- 3. レポート表示エリア ---
    if "comprehensive_report" in st.session_state:
        report = st.session_state.comprehensive_report
        
        st.markdown("---")
        st.subheader(f"🤖 エグゼクティブサマリー (by {report['model_used']})")
        
        # 期間情報の表示
        if report.get("comparison_enabled"):
            col1, col2 = st.columns(2)
            with col1:
                st.caption(f"📅 現在期間: {report.get('current_period')}")
            with col2:
                st.caption(f"📅 比較期間: {report.get('comparison_period_label')}")
        else:
            st.caption(f"📅 分析期間: {report.get('current_period')}")
        
        st.markdown(report["summary"])
        
        # 詳細データをタブで表示
        st.markdown("---")
        st.subheader("📈 詳細データ")
        
        # 過去比較が有効な場合はタブを追加
        if report.get("comparison_enabled"):
            tab1, tab2, tab3, tab4 = st.tabs([
                "📊 パフォーマンス診断", 
                "🔮 予測分析", 
                "🧠 要因分析",
                "📈 過去比較"
            ])
        else:
            tab1, tab2, tab3 = st.tabs([
                "📊 パフォーマンス診断", 
                "🔮 予測分析", 
                "🧠 要因分析"
            ])
        
        # 現在期間のデータ表示
        current_data = report["details"].get("current", {})
        
        with tab1:
            if current_data.get("performance") is not None:
                st.dataframe(
                    current_data["performance"], 
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("パフォーマンス診断データがありません")
        
        with tab2:
            if current_data.get("forecast") is not None:
                st.dataframe(
                    current_data["forecast"].head(50), 
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("予測分析データがありません")
        
        with tab3:
            if current_data.get("drivers") is not None:
                st.dataframe(
                    current_data["drivers"], 
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("要因分析データがありません")
        
        # 過去比較タブ
        if report.get("comparison_enabled"):
            with tab4:
                show_comparison_details(report)
        
        # エクスポート機能
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                label="📥 サマリーをテキスト形式で保存",
                data=report["summary"],
                file_name=f"comprehensive_report_{report['generated_at']}.txt",
                mime="text/plain"
            )
        with col2:
            if st.button("🔄 新しいレポートを生成"):
                del st.session_state.comprehensive_report
                st.rerun()