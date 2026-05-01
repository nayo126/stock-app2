#!/usr/bin/env python3
"""
株価とニュースを取得して data/snapshot.json に保存
GitHub Actions で1時間毎に実行される
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import yfinance as yf
import feedparser

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

JST = timezone(timedelta(hours=9))


def load_stocks():
    """data/stocks-list.json から監視銘柄を読む"""
    with open(DATA_DIR / "stocks-list.json", encoding="utf-8") as f:
        return json.load(f)


def fetch_stocks_prices(codes):
    """日本株の現在値・前日比・出来高をまとめて取得"""
    tickers = [f"{c}.T" for c in codes]
    print(f"[stocks] fetching {len(tickers)} tickers...")
    # yf.download で一括取得（高速）
    data = yf.download(
        tickers=tickers,
        period="5d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
    )
    out = {}
    for code in codes:
        sym = f"{code}.T"
        try:
            if len(tickers) == 1:
                df = data
            else:
                df = data[sym] if sym in data.columns.get_level_values(0) else None
            if df is None or df.empty:
                continue
            df = df.dropna()
            if df.empty:
                continue
            last = df.iloc[-1]
            prev = df.iloc[-2] if len(df) >= 2 else None
            entry = {
                "price": round(float(last["Close"]), 2),
                "open": round(float(last["Open"]), 2),
                "high": round(float(last["High"]), 2),
                "low": round(float(last["Low"]), 2),
                "volume": int(last["Volume"]) if last["Volume"] == last["Volume"] else 0,
            }
            if prev is not None:
                pc = float(prev["Close"])
                entry["prev_close"] = round(pc, 2)
                entry["change"] = round(entry["price"] - pc, 2)
                entry["change_pct"] = round((entry["price"] - pc) / pc * 100, 2)
            out[code] = entry
        except Exception as e:
            print(f"[stocks] err {code}: {e}", file=sys.stderr)
    print(f"[stocks] got {len(out)}/{len(codes)} prices")
    return out


def fetch_indices():
    """主要指数（日経・TOPIX・ダウ・S&P500・ナスダック）"""
    indices = {
        "日経平均": "^N225",
        "TOPIX": "^TPX",
        "ダウ": "^DJI",
        "S&P500": "^GSPC",
        "ナスダック": "^IXIC",
        "VIX": "^VIX",
        "ドル円": "JPY=X",
    }
    out = {}
    for name, sym in indices.items():
        try:
            t = yf.Ticker(sym)
            h = t.history(period="5d")
            if h.empty:
                continue
            last = h.iloc[-1]
            entry = {"price": round(float(last["Close"]), 2)}
            if len(h) >= 2:
                pc = float(h.iloc[-2]["Close"])
                entry["prev_close"] = round(pc, 2)
                entry["change"] = round(entry["price"] - pc, 2)
                entry["change_pct"] = round((entry["price"] - pc) / pc * 100, 2)
            out[name] = entry
        except Exception as e:
            print(f"[indices] err {sym}: {e}", file=sys.stderr)
    print(f"[indices] got {len(out)}")
    return out


# RSS feed list - 投資・マーケット系を幅広く
FEEDS = [
    # 国内マーケット
    ("Yahoo!ファイナンス トピックス", "https://news.yahoo.co.jp/rss/topics/business.xml"),
    ("Yahoo!ファイナンス 経済ニュース", "https://news.yahoo.co.jp/rss/categories/business.xml"),
    ("Reuters Japan", "https://assets.wor.jp/rss/rdf/reuters/top.rdf"),
    ("Bloomberg Japan", "https://assets.wor.jp/rss/rdf/bloomberg/top.rdf"),
    ("日経 マーケット", "https://www.nikkei.com/markets/rss/"),
    ("ロイター ビジネス", "https://www.reuters.co.jp/feed/businessNews"),
    ("ZUU online", "https://zuuonline.com/feed"),
    ("マイナビニュース 経済", "https://news.mynavi.jp/rss/index_economy"),
    ("東洋経済 マーケット", "https://toyokeizai.net/list/feed/rss"),
    ("Diamond Online", "https://diamond.jp/list/feed/rss/dol"),
    ("マネックス証券", "https://media.monex.co.jp/index.xml"),
    ("みんかぶマガジン", "https://itf.minkabu.jp/news/rss/atom"),
    # 海外マーケット (英語)
    ("CNBC Markets", "https://www.cnbc.com/id/10000664/device/rss/rss.html"),
    ("CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("Reuters Business", "https://feeds.reuters.com/reuters/businessNews"),
    ("Investing.com", "https://www.investing.com/rss/news_25.rss"),
    ("Seeking Alpha", "https://seekingalpha.com/market_currents.xml"),
    ("MarketWatch Top Stories", "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
]


def fetch_news():
    """RSS 大量取得→平準化→マージ"""
    all_items = []
    for source_name, url in FEEDS:
        try:
            f = feedparser.parse(url)
            for entry in f.entries[:30]:
                # 日付parse
                pub = entry.get("published_parsed") or entry.get("updated_parsed")
                pub_iso = ""
                pub_ts = 0
                if pub:
                    try:
                        from time import mktime
                        dt = datetime.fromtimestamp(mktime(pub), tz=timezone.utc)
                        pub_iso = dt.isoformat()
                        pub_ts = int(dt.timestamp())
                    except Exception:
                        pass
                all_items.append({
                    "title": entry.get("title", "")[:300],
                    "link": entry.get("link", ""),
                    "source": source_name,
                    "published": pub_iso,
                    "ts": pub_ts,
                })
            print(f"[news] {source_name}: {len(f.entries)}")
        except Exception as e:
            print(f"[news] err {source_name}: {e}", file=sys.stderr)
    # 新しい順
    all_items.sort(key=lambda x: x["ts"], reverse=True)
    # 重複除去（タイトル基準）
    seen = set()
    deduped = []
    for it in all_items:
        key = it["title"][:80]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(it)
    print(f"[news] total {len(deduped)} after dedupe")
    return deduped[:200]  # 最大200件


def main():
    stocks_list = load_stocks()
    codes = stocks_list.get("stocks_jp", [])

    snapshot = {
        "generated_at": datetime.now(JST).isoformat(),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "stocks_count": len(codes),
        "indices": fetch_indices(),
        "stocks": fetch_stocks_prices(codes),
        "news": fetch_news(),
    }

    out_path = DATA_DIR / "snapshot.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    print(f"[done] {out_path} ({out_path.stat().st_size} bytes)")
    print(f"  stocks: {len(snapshot['stocks'])}/{len(codes)}")
    print(f"  indices: {len(snapshot['indices'])}")
    print(f"  news: {len(snapshot['news'])}")


if __name__ == "__main__":
    main()
