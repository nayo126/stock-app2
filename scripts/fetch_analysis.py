#!/usr/bin/env python3
"""
重要60銘柄(MAIN+UNDER3000+TENBAGGER)について:
- 過去60日の history 取得
- 上昇/中立/下落シナリオ計算
- tier 根拠 / リスク要因 を構造化生成
data/analysis.json に保存
"""
import json
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import yfinance as yf

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

JST = timezone(timedelta(hours=9))


def load_meta():
    with open(DATA_DIR / "stocks-meta.json", encoding="utf-8") as f:
        return json.load(f)


# tier ごとの定型評価（根拠生成用）
TIER_DESCRIPTION = {
    "S": "🌟 最優先級 / 超強気評価",
    "A": "💎 優良 / 強気評価",
    "B": "✨ 良好 / 中立寄り強気",
    "C": "⚪ 普通 / 中立",
    "D": "⚠️ 注意 / 弱気",
}


def build_rationale(item):
    """tier 評価の根拠を生成"""
    tier = item.get("tier", "")
    name = item.get("name", "")
    desc = item.get("desc", "")
    tags = item.get("tags", [])

    bull_points = []
    bear_points = []

    # tags ベースの強み
    for tag in tags:
        if any(k in tag for k in ["AI", "NVIDIA", "半導体", "DC", "HBM"]):
            bull_points.append(f"📈 {tag} - 構造的成長テーマ、長期資金流入")
        elif "防衛" in tag or "国策" in tag:
            bull_points.append(f"📈 {tag} - 政府支援政策、需要安定")
        elif "高配当" in tag or "累進配当" in tag:
            bull_points.append(f"📈 {tag} - インカムゲイン魅力、下値堅い")
        elif "上方修正" in tag or "受注最高" in tag:
            bull_points.append(f"📈 {tag} - 業績モメンタム強い")
        elif "独占" in tag or "シェア" in tag:
            bull_points.append(f"📈 {tag} - 競争優位、高利益率")
        elif "ロボット" in tag:
            bull_points.append(f"📈 {tag} - 自動化需要拡大")
        else:
            bull_points.append(f"📈 {tag}")

    # tier ベースのリスク
    if tier == "S":
        bear_points = [
            "⚠️ 既に高値圏、調整リスクあり",
            "⚠️ 期待先行で材料出尽くし懸念",
            "⚠️ 為替・金利変動の影響大",
        ]
    elif tier == "A":
        bear_points = [
            "⚠️ 競合増加で成長鈍化リスク",
            "⚠️ マクロ環境悪化で下押し",
        ]
    elif tier == "B":
        bear_points = [
            "⚠️ 業績進捗の遅れ",
            "⚠️ セクター物色の逆風",
        ]
    elif tier == "C":
        bear_points = [
            "⚠️ ボラティリティ高い",
            "⚠️ 流動性リスク",
        ]
    else:
        bear_points = ["⚠️ 個別材料依存"]

    # tier 評価コメント
    tier_comment = TIER_DESCRIPTION.get(tier, "中立評価")

    return {
        "tier_label": tier_comment,
        "summary": desc,
        "bull_points": bull_points,
        "bear_points": bear_points,
    }


def fetch_history_and_scenarios(code):
    """60日 history + 3シナリオ"""
    try:
        t = yf.Ticker(f"{code}.T")
        h = t.history(period="60d")
        if h.empty:
            return None

        # daily prices
        history = []
        for idx, row in h.iterrows():
            history.append({
                "d": idx.strftime("%Y-%m-%d"),
                "p": round(float(row["Close"]), 2),
                "v": int(row["Volume"]) if row["Volume"] == row["Volume"] else 0,
            })

        current = float(h["Close"].iloc[-1])
        max_60 = float(h["High"].max())
        min_60 = float(h["Low"].min())
        avg_60 = float(h["Close"].mean())

        # ボラティリティ計算（標準偏差）
        import statistics
        prices = [float(p) for p in h["Close"].tolist() if p == p]
        std = statistics.stdev(prices) if len(prices) > 1 else 0
        vol_pct = (std / avg_60 * 100) if avg_60 else 0  # ボラティリティ%

        # 3シナリオ目標価格
        # 強気: 60日高値 × 1.1 (10%上抜けまで)
        # 中立: 60日平均
        # 弱気: 60日安値 × 0.9 (10%下抜けまで)
        bull_target = round(max_60 * 1.1, 0)
        neutral_target = round(avg_60, 0)
        bear_target = round(min_60 * 0.9, 0)

        bull_pct = round((bull_target - current) / current * 100, 1) if current else 0
        neutral_pct = round((neutral_target - current) / current * 100, 1) if current else 0
        bear_pct = round((bear_target - current) / current * 100, 1) if current else 0

        # 確率推定（ボラティリティから簡易）
        if vol_pct > 5:
            bull_prob, bear_prob = "高", "高"
        elif vol_pct > 3:
            bull_prob, bear_prob = "中", "中"
        else:
            bull_prob, bear_prob = "低", "低"

        return {
            "current": round(current, 2),
            "history": history,
            "stats": {
                "max60": round(max_60, 2),
                "min60": round(min_60, 2),
                "avg60": round(avg_60, 2),
                "volatility_pct": round(vol_pct, 2),
            },
            "scenarios": {
                "bull": {
                    "label": "🚀 強気シナリオ",
                    "target": bull_target,
                    "change_pct": bull_pct,
                    "prob": bull_prob,
                    "comment": f"60日高値{round(max_60,0)}円を10%上抜けが目標。最良ケースで+{bull_pct}%上昇余地。",
                },
                "neutral": {
                    "label": "➡️ 中立シナリオ",
                    "target": neutral_target,
                    "change_pct": neutral_pct,
                    "prob": "高",
                    "comment": f"60日平均{round(avg_60,0)}円水準。レンジ内推移なら{'+' if neutral_pct >= 0 else ''}{neutral_pct}%。",
                },
                "bear": {
                    "label": "📉 弱気シナリオ",
                    "target": bear_target,
                    "change_pct": bear_pct,
                    "prob": bear_prob,
                    "comment": f"60日安値{round(min_60,0)}円を10%下抜けが最悪。下落リスク{bear_pct}%。",
                },
            },
        }
    except Exception as e:
        print(f"[scenario] err {code}: {e}", file=sys.stderr)
        return None


def main():
    meta = load_meta()
    out = {
        "generated_at": datetime.now(JST).isoformat(),
        "stocks": {},
    }

    all_items = []
    for category in ["STOCKS_MAIN", "STOCKS_UNDER3000", "STOCKS_TENBAGGER"]:
        for item in meta.get(category, []):
            all_items.append((category, item))

    print(f"[main] target {len(all_items)} stocks")

    for category, item in all_items:
        code = item["code"]
        if not code.isdigit():
            continue
        print(f"  {category} {code} {item['name']}...", end=" ", flush=True)
        scenarios = fetch_history_and_scenarios(code)
        rationale = build_rationale(item)
        out["stocks"][code] = {
            "category": category,
            "rank": item.get("rank"),
            "tier": item.get("tier"),
            "name": item.get("name"),
            "tags": item.get("tags", []),
            "rationale": rationale,
            **(scenarios or {}),
        }
        print("OK" if scenarios else "✗")
        time.sleep(0.3)  # rate limit

    out_path = DATA_DIR / "analysis.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    print(f"\n[done] {out_path} ({out_path.stat().st_size / 1024:.1f} KB)")
    print(f"  stocks analyzed: {len(out['stocks'])}")


if __name__ == "__main__":
    main()
