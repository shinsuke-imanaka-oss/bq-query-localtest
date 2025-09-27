# context_glossary.py
"""
AIのコンテキスト理解を強化するためのビジネス用語集
"""

BUSINESS_CONTEXT_GLOSSARY = {
    # =========================================================================
    # 重要KPI (Key Performance Indicator)
    # =========================================================================
    "ROAS": {
        "full_name": "Return On Advertising Spend (広告費用対効果)",
        "definition": "広告費に対してどれだけの売上があったかを示す指標。高いほど良い。",
        "calculation": "SUM(ConversionValue) / SUM(CostIncludingFees)",
        "analysis_point": "ROASが業界ベンチマーク（例: 4.2倍）を上回っているか、キャンペーン毎のROASを比較して予算配分の最適化を検討する。"
    },
    "F2転換率": {
        "full_name": "F2 Transition Rate (初回リピート率)",
        "definition": "新規顧客が2回目の購入に至った割合。顧客の定着度を示す。",
        "calculation": "(2回目の購入者数 / 新規顧客数) * 100",
        "analysis_point": "どの広告経由の顧客がF2転換率が高いかを分析し、LTV（顧客生涯価値）の高い顧客層を特定する。"
    },
    "LTV": {
        "full_name": "Life Time Value (顧客生涯価値)",
        "definition": "一人の顧客が取引期間中に企業にもたらす総利益。",
        "calculation": "平均顧客単価 * 平均購買頻度 * 平均継続期間",
        "analysis_point": "LTVの高い顧客を獲得できているキャンペーンを特定し、その広告クリエイティブやターゲティングを分析する。"
    },
    
    # =========================================================================
    # 社内用語・略語
    # =========================================================================
    "春本番CP": {
        "full_name": "春の本番セールキャンペーン",
        "definition": "毎年3月〜4月に実施される大規模な販売促進キャンペーン。",
        "analysis_point": "昨年の同キャンペーンと比較して、今年の成果がどうだったかを分析する。特に新規顧客獲得数に着目する。"
    },
    "指名検索": {
        "full_name": "Branded Search",
        "definition": "社名や商品名など、固有名詞での検索流入。",
        "analysis_point": "指名検索によるCVRは非常に高い傾向にある。広告費を増やした時期に、指名検索数も増加しているか（広告の認知効果）を確認する。"
    },

    # =========================================================================
    # 分析の視点
    # =========================================================================
    "深掘り分析": {
        "definition": "単なる数値の羅列ではなく、その数値の背景にある「なぜそうなったのか？」という原因を探る分析アプローチ。",
        "analysis_point": "例えばCPAが悪化した場合、どのキャンペーンの、どのキーワードの、どの広告文のクリック率が下がったのか？のようにドリルダウンして要因を特定する。"
    }
}

def get_glossary_for_prompt() -> str:
    """プロンプトに埋め込むための整形済み用語集テキストを生成する"""
    prompt_text = "## 📖 ビジネス用語集\n"
    prompt_text += "ユーザーの指示を解釈する際は、以下の定義を最優先で参考にしてください。\n\n"

    for term, data in BUSINESS_CONTEXT_GLOSSARY.items():
        prompt_text += f"### {term} ({data.get('full_name', '')})\n"
        prompt_text += f"- **定義**: {data['definition']}\n"
        if 'calculation' in data:
            prompt_text += f"- **計算式**: `{data['calculation']}`\n"
        if 'analysis_point' in data:
            prompt_text += f"- **分析のポイント**: {data['analysis_point']}\n"
        prompt_text += "\n"
        
    return prompt_text