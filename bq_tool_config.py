# config.py - 統合設定管理システム
"""
デジタルマーケティング分析プラットフォーム - 設定管理
環境別設定・動的設定変更・設定検証機能を提供
"""

import os
import streamlit as st
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import json
from pathlib import Path

# =========================================================================
# 設定データクラス定義
# =========================================================================

@dataclass
class BigQuerySettings:
    """BigQuery関連設定"""
    project_id: str = "vorn-digi-mktg-poc-635a"
    dataset: str = "toki_air"
    table_prefix: str = ""
    timeout: int = 300
    location: str = "asia-northeast1"

    @property
    def full_dataset_id(self) -> str:
        """プロジェクトIDとデータセットIDを結合した完全なIDを返す"""
        return f"{self.project_id}.{self.dataset}"

    def get_full_table_name(self, table_name: str) -> str:
        """完全なテーブル名を取得"""
        prefix = f"{self.table_prefix}_" if self.table_prefix else ""
        return f"{self.project_id}.{self.dataset}.{prefix}{table_name}"

@dataclass
class LookerSettings:
    """Looker Studio関連設定"""
    report_id: str = ""
    base_url_template: str = "https://lookerstudio.google.com/embed/reporting/{report_id}"
    default_sheets: Dict[str, str] = field(default_factory=lambda: {
        "予算管理": "Gcf9", "サマリー01": "6HI9", "サマリー02": "IH29",
        "メディア": "GTrk", "デバイス": "kovk", "月別": "Bsvk",
        "日別": "40vk", "曜日": "hsv3", "キャンペーン": "cYwk",
        "広告グループ": "1ZWq", "テキストCR": "NfWq", "ディスプレイCR": "p_grkcjbbytd",
        "キーワード": "imWq", "地域": "ZNdq", "時間": "bXdq",
        "最終ページURL": "7xXq", "性別": "ctdq", "年齢": "fX53",
    })
    
    def get_embed_url(self) -> str:
        """埋め込み用URL取得"""
        if not self.report_id:
            return ""
        return self.base_url_template.format(report_id=self.report_id)

@dataclass
class AISettings:
    """AI関連設定"""
    gemini_model: str = "gemini-2.0-flash-001"
    claude_model: str = "claude-3-5-sonnet-20241022"
    max_tokens: int = 8000
    temperature: float = 0.1
    timeout: int = 60
    
@dataclass
class AppSettings:
    """アプリケーション設定"""
    debug_mode: bool = False
    auto_claude_analysis: bool = True
    cache_ttl: int = 43200  # 12時間
    max_dataframe_rows: int = 10000
    default_date_range_days: int = 30
    timezone: str = "Asia/Tokyo"
    
@dataclass
class UISettings:
    """UI関連設定"""
    theme: str = "light"
    sidebar_expanded: bool = True
    show_advanced_options: bool = False
    enable_animations: bool = True

# =========================================================================
# 統合設定クラス
# =========================================================================

@dataclass
class Settings:
    """統合設定管理クラス"""
    bigquery: BigQuerySettings = field(default_factory=BigQuerySettings)
    looker: LookerSettings = field(default_factory=LookerSettings)
    ai: AISettings = field(default_factory=AISettings)
    app: AppSettings = field(default_factory=AppSettings)
    ui: UISettings = field(default_factory=UISettings)
    
    # メタデータ
    version: str = "1.0.0"
    environment: str = "development"
    last_updated: str = ""
    
    def __post_init__(self):
        """初期化後処理"""
        self._load_from_environment()
        self._load_from_secrets()
        self._validate_settings()
    
    def _load_from_environment(self):
        """環境変数から設定を読み込み"""
        # BigQuery設定
        if os.getenv("GCP_PROJECT_ID"):
            self.bigquery.project_id = os.getenv("GCP_PROJECT_ID")
        if os.getenv("BQ_DATASET"):
            self.bigquery.dataset = os.getenv("BQ_DATASET")
        if os.getenv("BQ_TABLE_PREFIX"):
            self.bigquery.table_prefix = os.getenv("BQ_TABLE_PREFIX")
        if os.getenv("BQ_TIMEOUT"):
            self.bigquery.timeout = int(os.getenv("BQ_TIMEOUT"))
            
        # Looker設定
        if os.getenv("LOOKER_REPORT_ID"):
            self.looker.report_id = os.getenv("LOOKER_REPORT_ID")
            
        # AI設定
        if os.getenv("GEMINI_MODEL"):
            self.ai.gemini_model = os.getenv("GEMINI_MODEL")
        if os.getenv("CLAUDE_MODEL"):
            self.ai.claude_model = os.getenv("CLAUDE_MODEL")
            
        # アプリ設定
        if os.getenv("DEBUG_MODE"):
            self.app.debug_mode = os.getenv("DEBUG_MODE").lower() == "true"
        if os.getenv("ENVIRONMENT"):
            self.environment = os.getenv("ENVIRONMENT")
    
    def _load_from_secrets(self):
        """Streamlit Secretsから設定を読み込み"""
        try:
            # BigQuery設定
            if "GCP_PROJECT_ID" in st.secrets:
                self.bigquery.project_id = st.secrets["GCP_PROJECT_ID"]
            if "BQ_DATASET" in st.secrets:
                self.bigquery.dataset = st.secrets["BQ_DATASET"]
            if "BQ_TABLE_PREFIX" in st.secrets:
                self.bigquery.table_prefix = st.secrets["BQ_TABLE_PREFIX"]
                
            # Looker設定
            if "LOOKER_REPORT_ID" in st.secrets:
                self.looker.report_id = st.secrets["LOOKER_REPORT_ID"]
                
            # AI設定
            if "GEMINI_MODEL" in st.secrets:
                self.ai.gemini_model = st.secrets["GEMINI_MODEL"]
            if "CLAUDE_MODEL" in st.secrets:
                self.ai.claude_model = st.secrets["CLAUDE_MODEL"]
                
        except Exception as e:
            # Secrets読み込みエラーは無視（環境変数優先）
            if self.app.debug_mode:
                print(f"Secrets読み込みエラー: {e}")
    
    def _validate_settings(self):
        """設定値の検証"""
        errors = []
        warnings = []
        
        # 必須設定の確認
        if not self.looker.report_id:
            errors.append("LOOKER_REPORT_ID が設定されていません")
            
        if not self.bigquery.project_id:
            errors.append("GCP_PROJECT_ID が設定されていません")
            
        # 範囲チェック
        if self.bigquery.timeout < 30 or self.bigquery.timeout > 3600:
            warnings.append("BQ_TIMEOUT は 30-3600 秒の範囲で設定してください")
            
        if self.ai.max_tokens < 1000 or self.ai.max_tokens > 32000:
            warnings.append("AI max_tokens は 1000-32000 の範囲で設定してください")
        
        # エラー・警告の保存
        self._validation_errors = errors
        self._validation_warnings = warnings
        
        if errors and self.app.debug_mode:
            print(f"設定エラー: {errors}")
        if warnings and self.app.debug_mode:
            print(f"設定警告: {warnings}")
    
    def get_validation_status(self) -> Dict[str, Any]:
        """検証ステータス取得"""
        return {
            "valid": len(self._validation_errors) == 0,
            "errors": getattr(self, '_validation_errors', []),
            "warnings": getattr(self, '_validation_warnings', [])
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """辞書形式で設定を出力"""
        import dataclasses
        return dataclasses.asdict(self)
    
    def save_to_file(self, filepath: str):
        """設定をファイルに保存"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)
    
    def load_from_file(self, filepath: str):
        """ファイルから設定を読み込み"""
        if not Path(filepath).exists():
            return
            
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # 各設定セクションを更新
        if 'bigquery' in data:
            for key, value in data['bigquery'].items():
                if hasattr(self.bigquery, key):
                    setattr(self.bigquery, key, value)
                    
        if 'looker' in data:
            for key, value in data['looker'].items():
                if hasattr(self.looker, key):
                    setattr(self.looker, key, value)
                    
        if 'ai' in data:
            for key, value in data['ai'].items():
                if hasattr(self.ai, key):
                    setattr(self.ai, key, value)
                    
        if 'app' in data:
            for key, value in data['app'].items():
                if hasattr(self.app, key):
                    setattr(self.app, key, value)
                    
        if 'ui' in data:
            for key, value in data['ui'].items():
                if hasattr(self.ui, key):
                    setattr(self.ui, key, value)
        
        # メタデータの更新
        self.version = data.get('version', self.version)
        self.environment = data.get('environment', self.environment)
        self.last_updated = data.get('last_updated', self.last_updated)
        
        # 再検証
        self._validate_settings()
    
    def update_setting(self, section: str, key: str, value: Any):
        """動的設定更新"""
        if section == "bigquery" and hasattr(self.bigquery, key):
            setattr(self.bigquery, key, value)
        elif section == "looker" and hasattr(self.looker, key):
            setattr(self.looker, key, value)
        elif section == "ai" and hasattr(self.ai, key):
            setattr(self.ai, key, value)
        elif section == "app" and hasattr(self.app, key):
            setattr(self.app, key, value)
        elif section == "ui" and hasattr(self.ui, key):
            setattr(self.ui, key, value)
        else:
            raise ValueError(f"不正な設定: {section}.{key}")

    def get_api_key(self, service_name: str) -> Optional[str]:
        """APIキーを Secrets / 環境変数から取得"""
        service_name = service_name.lower()
        key = None

        # Streamlit Secretsを優先
        try:
            if service_name == "gemini":
                key = st.secrets.get("GEMINI_API_KEY") or st.secrets.get("GOOGLE_API_KEY")
            elif service_name == "claude":
                key = st.secrets.get("CLAUDE_API_KEY") or st.secrets.get("ANTHROPIC_API_KEY")
        except Exception:
            # st.secrets が利用できない環境でもエラーにしない
            pass

        # 環境変数でフォールバック
        if not key:
            if service_name == "gemini":
                key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
            elif service_name == "claude":
                key = os.environ.get("CLAUDE_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")

        return key    

        # 再検証
        self._validate_settings()
        
        # デバッグログ
        if self.app.debug_mode:
            print(f"設定更新: {section}.{key} = {value}")

# =========================================================================
# 設定管理ユーティリティ
# =========================================================================

class ConfigManager:
    """設定管理マネージャー"""
    
    def __init__(self):
        self._settings = None
        self._config_path = "config/settings.json"
    
    def get_settings(self) -> Settings:
        """設定インスタンス取得（シングルトン）"""
        if self._settings is None:
            self._settings = Settings()
            # ファイルからの読み込み試行
            if Path(self._config_path).exists():
                self._settings.load_from_file(self._config_path)
        return self._settings
    
    def reload_settings(self) -> Settings:
        """設定強制再読み込み"""
        self._settings = None
        return self.get_settings()
    
    def save_current_settings(self):
        """現在の設定をファイル保存"""
        if self._settings:
            # ディレクトリ作成
            Path(self._config_path).parent.mkdir(parents=True, exist_ok=True)
            self._settings.save_to_file(self._config_path)
    
    def get_environment_info(self) -> Dict[str, Any]:
        """環境情報取得"""
        settings = self.get_settings()
        return {
            "environment": settings.environment,
            "version": settings.version,
            "debug_mode": settings.app.debug_mode,
            "validation_status": settings.get_validation_status(),
            "config_source": {
                "environment_vars": bool(os.environ.get("LOOKER_REPORT_ID")),
                "streamlit_secrets": "LOOKER_REPORT_ID" in (st.secrets if hasattr(st, 'secrets') else {}),
                "config_file": Path(self._config_path).exists()
            }
        }

# =========================================================================
# グローバル設定インスタンス
# =========================================================================

# グローバル設定マネージャー
config_manager = ConfigManager()

# 設定インスタンス（互換性のため）
settings = config_manager.get_settings()

# =========================================================================
# 設定UI機能
# =========================================================================

def show_config_panel():
    """設定パネル表示"""
    st.subheader("⚙️ システム設定")
    
    settings = config_manager.get_settings()
    validation = settings.get_validation_status()
    
    # 検証状況表示
    if validation["valid"]:
        st.success("✅ 設定は正常です")
    else:
        st.error("❌ 設定エラーがあります:")
        for error in validation["errors"]:
            st.error(f"- {error}")
    
    if validation["warnings"]:
        st.warning("⚠️ 設定警告:")
        for warning in validation["warnings"]:
            st.warning(f"- {warning}")
    
    # 環境情報表示
    env_info = config_manager.get_environment_info()
    with st.expander("🌍 環境情報", expanded=False):
        st.json(env_info)
    
    # 設定編集
    with st.expander("✏️ 設定編集", expanded=False):
        # BigQuery設定
        st.markdown("### 📊 BigQuery設定")
        new_project_id = st.text_input("プロジェクトID", value=settings.bigquery.project_id)
        new_dataset = st.text_input("データセット", value=settings.bigquery.dataset)
        new_timeout = st.number_input("タイムアウト(秒)", value=settings.bigquery.timeout, min_value=30, max_value=3600)
        
        # Looker設定
        st.markdown("### 📈 Looker Studio設定")
        new_report_id = st.text_input("レポートID", value=settings.looker.report_id)
        
        # 設定更新ボタン
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("💾 設定保存"):
                try:
                    settings.update_setting("bigquery", "project_id", new_project_id)
                    settings.update_setting("bigquery", "dataset", new_dataset)
                    settings.update_setting("bigquery", "timeout", new_timeout)
                    settings.update_setting("looker", "report_id", new_report_id)
                    config_manager.save_current_settings()
                    st.success("設定を保存しました")
                    st.rerun()
                except Exception as e:
                    st.error(f"設定保存エラー: {e}")
        
        with col2:
            if st.button("🔄 設定リロード"):
                config_manager.reload_settings()
                st.success("設定をリロードしました")
                st.rerun()
        
        with col3:
            if st.button("📋 設定エクスポート"):
                config_json = json.dumps(settings.to_dict(), ensure_ascii=False, indent=2)
                st.download_button(
                    label="💾 設定ファイルダウンロード",
                    data=config_json,
                    file_name="settings.json",
                    mime="application/json"
                )

# =========================================================================
# 互換性関数
# =========================================================================

def get_bigquery_config():
    """BigQuery設定取得（互換性）"""
    settings = config_manager.get_settings()
    return {
        "project_id": settings.bigquery.project_id,
        "dataset": settings.bigquery.dataset,
        "table_prefix": settings.bigquery.table_prefix,
        "timeout": settings.bigquery.timeout,
        "location": settings.bigquery.location
    }

def get_looker_config():
    """Looker設定取得（互換性）"""
    settings = config_manager.get_settings()
    return {
        "report_id": settings.looker.report_id,
        "base_url": settings.looker.get_embed_url(),
        "sheets": settings.looker.default_sheets
    }

def get_ai_config():
    """AI設定取得（互換性）"""
    settings = config_manager.get_settings()
    return {
        "gemini_model": settings.ai.gemini_model,
        "claude_model": settings.ai.claude_model,
        "max_tokens": settings.ai.max_tokens,
        "temperature": settings.ai.temperature,
        "timeout": settings.ai.timeout
    }

# =========================================================================
# テスト・デバッグ機能
# =========================================================================

def test_config_system():
    """設定システムテスト"""
    print("🧪 設定システムテスト開始")
    
    try:
        settings = config_manager.get_settings()
        validation = settings.get_validation_status()
        env_info = config_manager.get_environment_info()
        
        print(f"✅ 設定読み込み: 成功")
        print(f"✅ 検証ステータス: {validation['valid']}")
        print(f"✅ 環境: {env_info['environment']}")
        print(f"✅ バージョン: {env_info['version']}")
        
        if validation["errors"]:
            print(f"❌ エラー: {validation['errors']}")
        if validation["warnings"]:
            print(f"⚠️ 警告: {validation['warnings']}")
        
        print("🎉 設定システムテスト完了")
        return True
        
    except Exception as e:
        print(f"❌ 設定システムテストエラー: {e}")
        return False

if __name__ == "__main__":
    test_config_system()