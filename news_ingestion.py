"""
SRSID  —  news_ingestion.py
==============================
Fetches supplier news from multiple sources and writes to vendor_news table.

Sources:
    gdelt    GDELT GKG (free, no key needed) — global events
    newsapi  NewsAPI.org (requires NEWSAPI_KEY in .env)
    guardian The Guardian (requires GUARDIAN_KEY in .env)

Usage:
    python news_ingestion.py                          # all sources, 30 days
    python news_ingestion.py --source gdelt           # GDELT only
    python news_ingestion.py --source newsapi         # NewsAPI only
    python news_ingestion.py --days 7                 # last 7 days
    python news_ingestion.py --limit 100              # first 100 vendors only
    python news_ingestion.py --vendor "Siemens AG"    # single vendor
"""

import sys
import re
import json
import time
import logging
import argparse
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from db.db_client import DBClient

# ─────────────────────────────────────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/news_ingestion.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import os
NEWSAPI_KEY   = os.getenv("NEWSAPI_KEY", "")
GUARDIAN_KEY  = os.getenv("GUARDIAN_KEY", "")


# ─────────────────────────────────────────────────────────────────────────────
# SENTIMENT SCORING
# ─────────────────────────────────────────────────────────────────────────────
POSITIVE_WORDS = {
    "growth","expansion","profit","award","partnership","contract","strong",
    "increase","record","invest","innovative","sustainable","reliable","approved",
    "upgrade","acquisition","deal","surplus","outperform","recovery",
}
NEGATIVE_WORDS = {
    "delay","disruption","shortage","recall","lawsuit","fraud","bankruptcy",
    "fine","penalty","loss","shutdown","strike","sanction","risk","default",
    "decline","breach","contamination","accident","halt","suspend","warning",
    "downgrade","investigation","violation","outage","failure","defect","crash",
}
DISRUPTION_TYPES = {
    "shortage":     ["shortage","scarce","depleted","supply gap"],
    "delay":        ["delay","late","behind schedule","backlog","slow delivery"],
    "financial":    ["bankruptcy","default","insolvency","loss","receivership"],
    "geopolitical": ["sanction","tariff","trade war","embargo","geopolitical"],
    "natural":      ["earthquake","flood","hurricane","typhoon","disaster","fire"],
    "labour":       ["strike","walkout","labour dispute","worker","union"],
    "quality":      ["recall","defect","contamination","quality issue","rejection"],
    "cyber":        ["cyberattack","data breach","hack","ransomware","phishing"],
    "regulatory":   ["fine","penalty","investigation","violation","lawsuit","ban"],
}


def score_sentiment(text: str) -> float:
    """Return sentiment score -1 to +1 based on keyword presence."""
    if not text:
        return 0.0
    words  = set(re.sub(r"[^a-z\s]", "", text.lower()).split())
    pos    = len(words & POSITIVE_WORDS)
    neg    = len(words & NEGATIVE_WORDS)
    total  = pos + neg
    return round((pos - neg) / total, 3) if total else 0.0


def classify_disruption(text: str):
    """Return (disruption_type, disruption_flag) from article text."""
    tl = text.lower()
    for dtype, keywords in DISRUPTION_TYPES.items():
        if any(kw in tl for kw in keywords):
            return dtype, True
    return None, False


def make_article_id(vendor_id: str, url: str) -> str:
    """Stable deduplification ID."""
    return hashlib.md5(f"{vendor_id}|{url}".encode()).hexdigest()[:16]


# ─────────────────────────────────────────────────────────────────────────────
# GDELT (FREE — no key required)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_gdelt(vendor_name: str, days: int) -> list[dict]:
    """
    Query GDELT GKG API for vendor mentions.
    Returns list of article dicts.
    """
    query   = requests.utils.quote(f'"{vendor_name}"')
    since   = (datetime.now(timezone.utc) - timedelta(days=days)
               ).strftime("%Y%m%d%H%M%S")
    url     = (
        f"https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={query}&mode=artlist&maxrecords=20"
        f"&startdatetime={since}&format=json&sort=DateDesc"
    )
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return []
        data     = r.json()
        articles = data.get("articles", [])
        results  = []
        for a in articles:
            title   = a.get("title", "")
            full    = f"{title} {a.get('seendescription','')}"
            sent    = score_sentiment(full)
            dtype, dflag = classify_disruption(full)
            results.append({
                "title":          title[:500],
                "url":            a.get("url", ""),
                "source_name":    a.get("domain", "GDELT"),
                "published_at":   a.get("seendate", "")[:14],
                "sentiment_score":sent,
                "disruption_type":dtype,
                "disruption_flag":dflag,
                "api_source":     "gdelt",
                "raw_snippet":    a.get("seendescription","")[:1000],
            })
        return results
    except Exception as e:
        log.debug(f"    GDELT error for {vendor_name}: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# NEWSAPI
# ─────────────────────────────────────────────────────────────────────────────

def fetch_newsapi(vendor_name: str, days: int) -> list[dict]:
    if not NEWSAPI_KEY:
        return []
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    url   = "https://newsapi.org/v2/everything"
    params = {
        "q":        f'"{vendor_name}"',
        "from":     since,
        "sortBy":   "publishedAt",
        "pageSize": 10,
        "apiKey":   NEWSAPI_KEY,
        "language": "en",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code != 200:
            return []
        results = []
        for a in r.json().get("articles", []):
            full    = f"{a.get('title','')} {a.get('description','')}"
            sent    = score_sentiment(full)
            dtype, dflag = classify_disruption(full)
            results.append({
                "title":          (a.get("title") or "")[:500],
                "url":            a.get("url", ""),
                "source_name":    a.get("source", {}).get("name","NewsAPI"),
                "published_at":   (a.get("publishedAt") or "")[:19],
                "sentiment_score":sent,
                "disruption_type":dtype,
                "disruption_flag":dflag,
                "api_source":     "newsapi",
                "raw_snippet":    (a.get("description") or "")[:1000],
            })
        return results
    except Exception as e:
        log.debug(f"    NewsAPI error for {vendor_name}: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# THE GUARDIAN
# ─────────────────────────────────────────────────────────────────────────────

def fetch_guardian(vendor_name: str, days: int) -> list[dict]:
    if not GUARDIAN_KEY:
        return []
    since  = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    url    = "https://content.guardianapis.com/search"
    params = {
        "q":           vendor_name,
        "from-date":   since,
        "order-by":    "newest",
        "page-size":   10,
        "api-key":     GUARDIAN_KEY,
        "show-fields": "headline,trailText",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code != 200:
            return []
        results = []
        for a in r.json().get("response", {}).get("results", []):
            title   = a.get("fields", {}).get("headline", a.get("webTitle",""))
            snippet = a.get("fields", {}).get("trailText","")
            full    = f"{title} {snippet}"
            sent    = score_sentiment(full)
            dtype, dflag = classify_disruption(full)
            results.append({
                "title":          title[:500],
                "url":            a.get("webUrl",""),
                "source_name":    "The Guardian",
                "published_at":   a.get("webPublicationDate","")[:19],
                "sentiment_score":sent,
                "disruption_type":dtype,
                "disruption_flag":dflag,
                "api_source":     "guardian",
                "raw_snippet":    snippet[:1000],
            })
        return results
    except Exception as e:
        log.debug(f"    Guardian error for {vendor_name}: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# WRITE TO POSTGRES
# ─────────────────────────────────────────────────────────────────────────────

def write_articles(db: DBClient, articles: list[dict],
                   vendor_id: str, vendor_name: str) -> int:
    """Insert articles, skip duplicates by URL."""
    if not articles:
        return 0

    inserted = 0
    for a in articles:
        url = a.get("url","")
        if not url:
            continue

        # Parse published_at
        pub_raw = a.get("published_at","")
        try:
            if len(pub_raw) == 14:       # GDELT format YYYYMMDDHHMMSS
                pub = datetime.strptime(pub_raw, "%Y%m%d%H%M%S")
            elif "T" in pub_raw:
                pub = datetime.fromisoformat(pub_raw.replace("Z","+00:00"))
            else:
                pub = datetime.strptime(pub_raw[:10], "%Y-%m-%d")
        except Exception:
            pub = datetime.now(timezone.utc)

        try:
            db.execute("""
                INSERT INTO vendor_news (
                    vendor_id, supplier_name, title, url,
                    source_name, published_at,
                    sentiment_score, disruption_type, disruption_flag,
                    api_source, raw_snippet
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (
                vendor_id, vendor_name,
                a.get("title","")[:500],
                url[:2000],
                a.get("source_name","")[:200],
                pub,
                float(a.get("sentiment_score",0)),
                a.get("disruption_type"),
                bool(a.get("disruption_flag", False)),
                a.get("api_source",""),
                a.get("raw_snippet","")[:1000],
            ))
            inserted += 1
        except Exception as e:
            log.debug(f"    Insert error: {e}")
            db.conn.rollback()
            continue

    db.conn.commit()
    return inserted


def update_vendor_signals(db: DBClient):
    """
    Update vendors.news_sentiment_30d and disruption_count_30d
    from the vendor_news table.
    """
    log.info("Updating vendor news signals...")
    db.execute("""
        UPDATE vendors v
        SET
            news_sentiment_30d   = sub.avg_sentiment,
            disruption_count_30d = sub.disruption_count
        FROM (
            SELECT vendor_id,
                   ROUND(AVG(sentiment_score)::NUMERIC, 4) AS avg_sentiment,
                   COUNT(*) FILTER (WHERE disruption_flag) AS disruption_count
            FROM vendor_news
            WHERE published_at >= NOW() - INTERVAL '30 days'
            GROUP BY vendor_id
        ) sub
        WHERE v.vendor_id = sub.vendor_id
    """)
    updated = db.scalar(
        "SELECT COUNT(*) FROM vendors WHERE news_sentiment_30d IS NOT NULL"
    )
    log.info(f"  Updated signals for {updated:,} vendors")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SRSID News Ingestion")
    parser.add_argument("--source", choices=["gdelt","newsapi","guardian","all"],
                        default="all", help="News source to query")
    parser.add_argument("--days",   type=int, default=30,
                        help="Lookback window in days (default: 30)")
    parser.add_argument("--limit",  type=int, default=None,
                        help="Max vendors to process (default: all)")
    parser.add_argument("--vendor", type=str, default=None,
                        help="Process a single vendor name")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SRSID News Ingestion")
    log.info("=" * 60)
    log.info(f"  Source  : {args.source}")
    log.info(f"  Lookback: {args.days} days")

    # Check API keys
    if args.source in ("newsapi","all") and not NEWSAPI_KEY:
        log.warning("  NEWSAPI_KEY not set — skipping NewsAPI")
    if args.source in ("guardian","all") and not GUARDIAN_KEY:
        log.warning("  GUARDIAN_KEY not set — skipping Guardian")

    with DBClient() as db:
        # Ensure raw_snippet column exists (added in new script, may be missing)
        db.add_column_if_missing("vendor_news", "raw_snippet", "TEXT")
        db.conn.commit()

        # Ensure URL uniqueness constraint exists
        try:
            db.execute(
                "ALTER TABLE vendor_news ADD CONSTRAINT vendor_news_url_unique UNIQUE (url)"
            )
            db.conn.commit()
        except Exception:
            db.conn.rollback()   # already exists

        # Load vendor list
        if args.vendor:
            vendors = db.fetch_df(
                "SELECT vendor_id, supplier_name FROM vendors "
                "WHERE is_active = TRUE AND supplier_name ILIKE %s",
                (f"%{args.vendor}%",)
            )
        else:
            vendors = db.fetch_df(
                "SELECT vendor_id, supplier_name FROM vendors "
                "WHERE is_active = TRUE ORDER BY total_annual_spend DESC NULLS LAST"
            )

        if vendors.empty:
            log.error("No vendors found in database. Run sap_loader.py first.")
            sys.exit(1)

        if args.limit:
            vendors = vendors.head(args.limit)

        log.info(f"  Processing {len(vendors):,} vendors")

        # Source functions to call
        fetchers = []
        if args.source in ("gdelt","all"):
            fetchers.append(("GDELT", fetch_gdelt))
        if args.source in ("newsapi","all") and NEWSAPI_KEY:
            fetchers.append(("NewsAPI", fetch_newsapi))
        if args.source in ("guardian","all") and GUARDIAN_KEY:
            fetchers.append(("Guardian", fetch_guardian))

        if not fetchers:
            log.error("No valid news sources available. Check API keys in .env")
            sys.exit(1)

        # Process each vendor
        total_inserted = 0
        total_vendors  = 0

        for i, (_, row) in enumerate(vendors.iterrows(), 1):
            vid   = str(row["vendor_id"])
            vname = str(row["supplier_name"])

            if i % 50 == 0 or i <= 5:
                log.info(f"  [{i}/{len(vendors)}] {vname}")

            all_articles = []
            for src_name, fetcher in fetchers:
                articles = fetcher(vname, args.days)
                all_articles.extend(articles)
                if articles:
                    log.debug(f"    {src_name}: {len(articles)} articles")

            n = write_articles(db, all_articles, vid, vname)
            if n:
                total_inserted += n
                total_vendors  += 1

            # Polite rate limiting
            time.sleep(0.2)

        # Update vendor signals
        update_vendor_signals(db)

    log.info("\n" + "=" * 60)
    log.info("NEWS INGESTION COMPLETE")
    log.info("=" * 60)
    log.info(f"  Vendors with news : {total_vendors:,}")
    log.info(f"  Articles inserted : {total_inserted:,}")
    log.info(f"\n  Next: streamlit run app/dashboard.py")


if __name__ == "__main__":
    main()
