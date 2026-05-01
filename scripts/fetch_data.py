#!/usr/bin/env python3
"""
東証全銘柄（プライム+スタンダード+グロース）の株価＋指数＋ニュース全部取得
GitHub Actions で1時間毎に実行、data/snapshot.json に保存
"""
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

import yfinance as yf
import feedparser
import pyexcel

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

JST = timezone(timedelta(hours=9))

# JPX 公式: 東証上場全銘柄一覧
JPX_XLS_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
JPX_CACHE = DATA_DIR / "jpx_codes_cache.json"
JPX_CACHE_TTL = 24 * 3600  # 1日


def load_all_tse_codes():
    """JPX Excel から東証全銘柄コード取得（プライム/スタンダード/グロース）"""
    # キャッシュチェック
    if JPX_CACHE.exists():
        age = time.time() - JPX_CACHE.stat().st_mtime
        if age < JPX_CACHE_TTL:
            with open(JPX_CACHE) as f:
                cached = json.load(f)
                print(f"[jpx] cache hit: {len(cached)} codes (age {int(age)}s)")
                return cached

    print(f"[jpx] downloading {JPX_XLS_URL}")
    try:
        # User-Agent 必要 (JPX サイト)
        req = urllib.request.Request(JPX_XLS_URL, headers={"User-Agent": "Mozilla/5.0"})
        xls_path = DATA_DIR / "_jpx.xls"
        with urllib.request.urlopen(req, timeout=60) as r, open(xls_path, "wb") as f:
            f.write(r.read())
        # pyexcel で xls 読み込み（pandas依存なし）
        records = pyexcel.get_records(file_name=str(xls_path))
        codes = []
        for row in records:
            code = str(row.get("コード", "")).strip()
            market = str(row.get("市場・商品区分", "")).strip()
            if not code.isdigit() or len(code) != 4:
                continue
            if any(k in market for k in ["プライム", "スタンダード", "グロース"]):
                codes.append(code)
        codes = sorted(set(codes))
        # キャッシュ保存
        with open(JPX_CACHE, "w") as f:
            json.dump(codes, f)
        try:
            xls_path.unlink()
        except Exception:
            pass
        print(f"[jpx] got {len(codes)} TSE codes")
        return codes
    except Exception as e:
        print(f"[jpx] err: {e}", file=sys.stderr)
        # フォールバック: 旧 stocks-list.json
        fallback = DATA_DIR / "stocks-list.json"
        if fallback.exists():
            with open(fallback) as f:
                d = json.load(f)
                print(f"[jpx] fallback to stocks-list.json: {len(d.get('stocks_jp', []))} codes")
                return d.get("stocks_jp", [])
        return []


def fetch_stocks_prices(codes, chunk_size=200):
    """日本株を chunk 単位で bulk download"""
    out = {}
    total = len(codes)
    for i in range(0, total, chunk_size):
        chunk = codes[i:i + chunk_size]
        tickers = [f"{c}.T" for c in chunk]
        print(f"[stocks] chunk {i//chunk_size + 1}/{-(-total//chunk_size)}: {len(tickers)} tickers")
        try:
            data = yf.download(
                tickers=tickers,
                period="5d",
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
            )
            for code in chunk:
                sym = f"{code}.T"
                try:
                    if len(tickers) == 1:
                        df = data
                    else:
                        if sym not in data.columns.get_level_values(0):
                            continue
                        df = data[sym]
                    if df is None or df.empty:
                        continue
                    df = df.dropna()
                    if df.empty:
                        continue
                    last = df.iloc[-1]
                    prev = df.iloc[-2] if len(df) >= 2 else None
                    entry = {
                        "p": round(float(last["Close"]), 2),
                        "v": int(last["Volume"]) if last["Volume"] == last["Volume"] else 0,
                    }
                    if prev is not None:
                        pc = float(prev["Close"])
                        entry["pc"] = round(pc, 2)
                        entry["c"] = round(entry["p"] - pc, 2)
                        entry["cp"] = round((entry["p"] - pc) / pc * 100, 2) if pc else 0
                    out[code] = entry
                except Exception as e:
                    pass
        except Exception as e:
            print(f"[stocks] chunk err: {e}", file=sys.stderr)
        # rate limit回避
        time.sleep(1)
    print(f"[stocks] got {len(out)}/{total} prices")
    return out


def fetch_indices():
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


FEEDS = [
    ("Yahoo!ファイナンス トピックス", "https://news.yahoo.co.jp/rss/topics/business.xml"),
    ("Yahoo!ファイナンス 経済ニュース", "https://news.yahoo.co.jp/rss/categories/business.xml"),
    ("Reuters Japan", "https://assets.wor.jp/rss/rdf/reuters/top.rdf"),
    ("Bloomberg Japan", "https://assets.wor.jp/rss/rdf/bloomberg/top.rdf"),
    ("ZUU online", "https://zuuonline.com/feed"),
    ("東洋経済 マーケット", "https://toyokeizai.net/list/feed/rss"),
    ("Diamond Online", "https://diamond.jp/list/feed/rss/dol"),
    ("CNBC Markets", "https://www.cnbc.com/id/10000664/device/rss/rss.html"),
    ("CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("Investing.com", "https://www.investing.com/rss/news_25.rss"),
    ("Seeking Alpha", "https://seekingalpha.com/market_currents.xml"),
    ("MarketWatch Top Stories", "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
    ("MarketWatch RealTime", "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines"),
    ("Reuters Top News", "https://www.reutersagency.com/feed/?best-topics=business-finance"),
    ("Forbes Japan", "https://forbesjapan.com/feed"),
    ("ITmedia Business", "https://rss.itmedia.co.jp/rss/2.0/business.xml"),
    ("Newsweek Japan", "https://www.newsweekjapan.jp/rss/news.xml"),
]


def fetch_news():
    all_items = []
    for source_name, url in FEEDS:
        try:
            f = feedparser.parse(url)
            for entry in f.entries[:30]:
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
    all_items.sort(key=lambda x: x["ts"], reverse=True)
    seen = set()
    deduped = []
    for it in all_items:
        key = it["title"][:80]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(it)
    print(f"[news] total {len(deduped)} after dedupe")
    return deduped[:300]


def main():
    codes = load_all_tse_codes()
    print(f"[main] target {len(codes)} stocks")

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
        # 圧縮のため separators 詰める（インデント無し）
        json.dump(snapshot, f, ensure_ascii=False, separators=(",", ":"))
    size_kb = out_path.stat().st_size / 1024
    print(f"[done] {out_path} ({size_kb:.1f} KB)")
    print(f"  stocks: {len(snapshot['stocks'])}/{len(codes)}")
    print(f"  indices: {len(snapshot['indices'])}")
    print(f"  news: {len(snapshot['news'])}")


if __name__ == "__main__":
    main()
