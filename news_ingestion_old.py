"""
News Ingestion for SRSID
==========================
Queries NewsAPI, Guardian API, and GDELT for every vendor in the
Postgres `vendors` table, scores sentiment, classifies disruption type,
and writes results to the `vendor_news` Postgres table.

This replaces the old fetch_newsapi_disruptions.py /
fetch_gdelt_disruptions.py scripts with a single, Postgres-aware module.

Features:
  - Reads vendor names directly from Postgres (no CSV needed)
  - Batches queries to stay within free API limits
  - Rate-limit safe (configurable delay between requests)
  - Deduplicates articles across sources
  - Scores sentiment (-1 to +1) and classifies disruption type
  - Writes results to vendor_news table

Usage:
    python news_ingestion.py                        # all vendors
    python news_ingestion.py --vendor "Siemens"     # single vendor test
    python news_ingestion.py --days 7               # last 7 days only
    python news_ingestion.py --source newsapi       # single source
    python news_ingestion.py --limit 50             # first 50 vendors only

Requirements:
    pip install requests psycopg2-binary pandas python-dotenv

Environment variables (or .env file):
    NEWSAPI_KEY=your_newsapi_key
    GUARDIAN_KEY=your_guardian_key
    DATABASE_URL=postgresql://user:pass@localhost:5432/srsid_db
"""

import os
import sys
import time
import json
import hashlib
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Iterator

import requests
import pandas as pd

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("news_ingestion.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Config ────────────────────────────────────────────────────────────────────
NEWSAPI_KEY  = os.environ.get("NEWSAPI_KEY",  "")
GUARDIAN_KEY = os.environ.get("GUARDIAN_KEY", "")
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://srsid_user:srsid_pass@localhost:5432/srsid_db"
)

# API rate limits (free tiers)
NEWSAPI_DELAY_SEC  = 1.0    # 1 req/sec → ~100 req/day free tier
GUARDIAN_DELAY_SEC = 0.5    # 12 req/sec free tier
GDELT_DELAY_SEC    = 2.0    # Be polite — no official rate limit

# Article limits per vendor per source
MAX_ARTICLES_PER_VENDOR = 5

# Disruption keywords by type
DISRUPTION_TYPES = {
    "bankruptcy":        ["bankrupt", "insolvency", "liquidat", "chapter 11",
                          "administration", "receivership", "default", "debt crisis"],
    "labor_strike":      ["strike", "walkout", "union dispute", "labor action",
                          "industrial action", "work stoppage", "picket"],
    "supply_shortage":   ["shortage", "stock out", "out of stock", "supply crunch",
                          "component shortage", "material shortage", "scarcity"],
    "geopolitical":      ["sanction", "trade war", "tariff", "embargo", "ban",
                          "export control", "geopolit", "conflict", "war", "invasion"],
    "natural_disaster":  ["earthquake", "flood", "hurricane", "typhoon", "tsunami",
                          "wildfire", "drought", "storm", "disaster", "climate"],
    "logistics_delay":   ["shipping delay", "port congestion", "freight",
                          "logistics disruption", "suez", "panama canal",
                          "container", "transit delay"],
    "recall":            ["recall", "safety alert", "product defect", "fda warning",
                          "quality issue", "contamination"],
    "cyber_incident":    ["cyberattack", "ransomware", "data breach", "hack",
                          "cybersecurity incident", "outage", "system failure"],
    "financial_risk":    ["credit downgrade", "rating cut", "profit warning",
                          "revenue miss", "earnings miss", "cash flow", "writedown"],
    "regulatory":        ["fine", "penalty", "investigation", "lawsuit", "compliance",
                          "regulatory action", "anti-trust", "violation"],
}

# Positive / negative sentiment keywords (simple lexicon)
POSITIVE_WORDS = {
    "growth", "expand", "record", "strong", "profit", "surge", "beat",
    "improve", "recovery", "rise", "gain", "award", "partnership", "invest",
    "innovate", "launch", "win", "success", "robust", "resilient",
}
NEGATIVE_WORDS = {
    "fall", "drop", "decline", "loss", "warn", "cut", "layoff", "close",
    "bankrupt", "shortage", "delay", "recall", "dispute", "risk", "concern",
    "crisis", "fail", "miss", "down", "weak", "problem", "issue", "halt",
    "suspend", "restrict", "sanction", "strike", "defect", "breach",
}


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────

def get_db_conn():
    """Return a Postgres connection. Raises clearly if unavailable."""
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except ImportError:
        log.error("psycopg2 not installed: pip install psycopg2-binary")
        sys.exit(1)
    except Exception as e:
        log.error(f"Cannot connect to Postgres: {e}")
        log.error(f"  DATABASE_URL = {DATABASE_URL}")
        log.error("  Run: python db/schema.sql first")
        sys.exit(1)


def ensure_news_table(conn):
    """Create vendor_news table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS vendor_news (
            id              SERIAL PRIMARY KEY,
            vendor_id       VARCHAR(50),
            supplier_name   VARCHAR(500),
            article_id      VARCHAR(64) UNIQUE,   -- SHA256 of title+source
            title           TEXT,
            description     TEXT,
            url             TEXT,
            source_name     VARCHAR(200),
            api_source      VARCHAR(50),           -- newsapi / guardian / gdelt
            published_at    TIMESTAMP,
            fetched_at      TIMESTAMP DEFAULT NOW(),
            sentiment_score FLOAT,                 -- -1 (negative) to +1 (positive)
            disruption_type VARCHAR(100),          -- primary disruption category
            disruption_flag BOOLEAN DEFAULT FALSE,
            days_lookback   INT
        );
        CREATE INDEX IF NOT EXISTS idx_vendor_news_vendor_id
            ON vendor_news(vendor_id);
        CREATE INDEX IF NOT EXISTS idx_vendor_news_published
            ON vendor_news(published_at DESC);
        CREATE INDEX IF NOT EXISTS idx_vendor_news_disruption
            ON vendor_news(disruption_flag) WHERE disruption_flag = TRUE;
        """)
        conn.commit()
    log.info("  vendor_news table ready")


def get_vendors_from_db(conn, limit: int | None = None,
                        single: str | None = None) -> list[dict]:
    """Fetch vendor names + IDs from Postgres."""
    with conn.cursor() as cur:
        if single:
            cur.execute(
                "SELECT vendor_id, supplier_name, country_code, industry "
                "FROM vendors WHERE supplier_name ILIKE %s LIMIT 10",
                (f"%{single}%",)
            )
        else:
            sql = ("SELECT vendor_id, supplier_name, country_code, industry "
                   "FROM vendors WHERE supplier_name IS NOT NULL "
                   "ORDER BY total_annual_spend DESC NULLS LAST")
            if limit:
                sql += f" LIMIT {limit}"
            cur.execute(sql)

        rows = cur.fetchall()

    vendors = [
        {"vendor_id": r[0], "supplier_name": r[1],
         "country": r[2], "industry": r[3]}   # keep dict key as 'country'
        for r in rows
    ]
    log.info(f"Loaded {len(vendors):,} vendors from Postgres")
    return vendors


def article_id(title: str, source: str) -> str:
    """Stable deduplification key."""
    return hashlib.sha256(f"{title}|{source}".encode()).hexdigest()[:24]


def upsert_articles(conn, articles: list[dict]):
    """Insert articles, skip duplicates (ON CONFLICT DO NOTHING)."""
    if not articles:
        return
    with conn.cursor() as cur:
        for a in articles:
            try:
                cur.execute("""
                INSERT INTO vendor_news
                    (vendor_id, supplier_name, article_id, title, description,
                     url, source_name, api_source, published_at,
                     sentiment_score, disruption_type, disruption_flag, days_lookback)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (article_id) DO NOTHING
                """, (
                    a.get("vendor_id"), a.get("supplier_name"),
                    a.get("article_id"), a.get("title"), a.get("description"),
                    a.get("url"), a.get("source_name"), a.get("api_source"),
                    a.get("published_at"), a.get("sentiment_score"),
                    a.get("disruption_type"), a.get("disruption_flag", False),
                    a.get("days_lookback"),
                ))
            except Exception as e:
                log.debug(f"    Skip duplicate/error: {e}")
        conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# SENTIMENT + DISRUPTION CLASSIFICATION
# ─────────────────────────────────────────────────────────────────────────────

def score_sentiment(text: str) -> float:
    """Simple lexicon-based sentiment score: -1 to +1."""
    if not text:
        return 0.0
    words  = set(text.lower().split())
    pos    = len(words & POSITIVE_WORDS)
    neg    = len(words & NEGATIVE_WORDS)
    total  = pos + neg
    if total == 0:
        return 0.0
    return round((pos - neg) / total, 3)


def classify_disruption(text: str) -> tuple[str | None, bool]:
    """
    Returns (disruption_type, is_disruption_flag).
    Checks title + description for disruption keyword matches.
    """
    if not text:
        return None, False
    tl = text.lower()
    for dtype, keywords in DISRUPTION_TYPES.items():
        if any(kw in tl for kw in keywords):
            return dtype, True
    return None, False


def make_article(vendor: dict, title: str, description: str,
                 url: str, source: str, api: str,
                 published: str | None, days: int) -> dict:
    combined  = f"{title} {description}"
    sentiment = score_sentiment(combined)
    disr_type, disr_flag = classify_disruption(combined)
    return {
        "vendor_id":       vendor["vendor_id"],
        "supplier_name":   vendor["supplier_name"],
        "article_id":      article_id(title, source),
        "title":           title[:500] if title else "",
        "description":     description[:1000] if description else "",
        "url":             url,
        "source_name":     source,
        "api_source":      api,
        "published_at":    published,
        "sentiment_score": sentiment,
        "disruption_type": disr_type,
        "disruption_flag": disr_flag,
        "days_lookback":   days,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1 — NEWSAPI
# ─────────────────────────────────────────────────────────────────────────────

def fetch_newsapi(vendor: dict, days: int) -> list[dict]:
    """Fetch up to MAX_ARTICLES_PER_VENDOR articles from NewsAPI for one vendor."""
    if not NEWSAPI_KEY:
        return []

    from_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    params = {
        "q":        f'"{vendor["supplier_name"]}"',
        "from":     from_date,
        "sortBy":   "relevancy",
        "pageSize": MAX_ARTICLES_PER_VENDOR,
        "language": "en",
        "apiKey":   NEWSAPI_KEY,
    }
    try:
        resp = requests.get(
            "https://newsapi.org/v2/everything",
            params=params, timeout=10
        )
        resp.raise_for_status()
        data = resp.json()

        articles = []
        for a in data.get("articles", [])[:MAX_ARTICLES_PER_VENDOR]:
            articles.append(make_article(
                vendor,
                title       = a.get("title", ""),
                description = a.get("description", "") or a.get("content", ""),
                url         = a.get("url", ""),
                source      = a.get("source", {}).get("name", "NewsAPI"),
                api         = "newsapi",
                published   = a.get("publishedAt"),
                days        = days,
            ))
        return articles

    except requests.exceptions.HTTPError as e:
        if "429" in str(e) or "rateLimited" in str(e):
            log.warning(f"  NewsAPI rate limit hit — sleeping 60s")
            time.sleep(60)
        else:
            log.debug(f"  NewsAPI error for {vendor['supplier_name']}: {e}")
        return []
    except Exception as e:
        log.debug(f"  NewsAPI exception for {vendor['supplier_name']}: {e}")
        return []
    finally:
        time.sleep(NEWSAPI_DELAY_SEC)


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2 — GUARDIAN API
# ─────────────────────────────────────────────────────────────────────────────

def fetch_guardian(vendor: dict, days: int) -> list[dict]:
    """Fetch articles from The Guardian API for one vendor."""
    key = GUARDIAN_KEY or "test"   # Guardian allows test key with limited results

    from_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    params = {
        "q":           vendor["supplier_name"],
        "from-date":   from_date,
        "page-size":   MAX_ARTICLES_PER_VENDOR,
        "show-fields": "headline,trailText,bodyText",
        "api-key":     key,
    }
    try:
        resp = requests.get(
            "https://content.guardianapis.com/search",
            params=params, timeout=10
        )
        resp.raise_for_status()
        data = resp.json()

        articles = []
        for a in data.get("response", {}).get("results", [])[:MAX_ARTICLES_PER_VENDOR]:
            fields = a.get("fields", {})
            articles.append(make_article(
                vendor,
                title       = fields.get("headline", a.get("webTitle", "")),
                description = fields.get("trailText", "") or fields.get("bodyText", "")[:500],
                url         = a.get("webUrl", ""),
                source      = "The Guardian",
                api         = "guardian",
                published   = a.get("webPublicationDate"),
                days        = days,
            ))
        return articles

    except Exception as e:
        log.debug(f"  Guardian error for {vendor['supplier_name']}: {e}")
        return []
    finally:
        time.sleep(GUARDIAN_DELAY_SEC)


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3 — GDELT
# ─────────────────────────────────────────────────────────────────────────────

def fetch_gdelt(vendor: dict, days: int) -> list[dict]:
    """
    Query GDELT 2.0 DOC API for articles mentioning the vendor.
    Uses the free, unauthenticated endpoint.
    """
    from_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y%m%d%H%M%S")
    params = {
        "query":   f'"{vendor["supplier_name"]}" sourcelang:english',
        "mode":    "ArtList",
        "maxrecords": MAX_ARTICLES_PER_VENDOR,
        "startdatetime": from_date,
        "format": "json",
    }
    try:
        resp = requests.get(
            "https://api.gdeltproject.org/api/v2/doc/doc",
            params=params, timeout=15
        )
        resp.raise_for_status()
        data = resp.json()

        articles = []
        for a in data.get("articles", [])[:MAX_ARTICLES_PER_VENDOR]:
            articles.append(make_article(
                vendor,
                title       = a.get("title", ""),
                description = a.get("seendescription", ""),
                url         = a.get("url", ""),
                source      = a.get("domain", "GDELT"),
                api         = "gdelt",
                published   = a.get("seendate"),
                days        = days,
            ))
        return articles

    except Exception as e:
        log.debug(f"  GDELT error for {vendor['supplier_name']}: {e}")
        return []
    finally:
        time.sleep(GDELT_DELAY_SEC)


# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def ingest_vendor(vendor: dict, sources: list[str], days: int) -> list[dict]:
    """Fetch from all requested sources for one vendor. Returns article list."""
    all_articles = []
    seen_ids     = set()

    fetchers = {
        "newsapi":  fetch_newsapi,
        "guardian": fetch_guardian,
        "gdelt":    fetch_gdelt,
    }

    for src in sources:
        if src not in fetchers:
            continue
        fetcher   = fetchers[src]
        articles  = fetcher(vendor, days)
        # Deduplicate
        for a in articles:
            if a["article_id"] not in seen_ids:
                seen_ids.add(a["article_id"])
                all_articles.append(a)

    return all_articles


def run_ingestion(vendors: list[dict], sources: list[str],
                  days: int, conn) -> dict:
    """Main ingestion loop over all vendors."""
    total_articles   = 0
    total_disruptions = 0
    vendor_coverage  = 0

    log.info(f"\nIngesting news for {len(vendors):,} vendors | "
             f"Sources: {sources} | Last {days} days")
    log.info("─" * 60)

    for i, vendor in enumerate(vendors, 1):
        name = vendor["supplier_name"]
        articles = ingest_vendor(vendor, sources, days)

        if articles:
            upsert_articles(conn, articles)
            disruptive = sum(1 for a in articles if a["disruption_flag"])
            total_articles    += len(articles)
            total_disruptions += disruptive
            vendor_coverage   += 1

            avg_sentiment = sum(a["sentiment_score"] for a in articles) / len(articles)
            sentiment_label = "🔴 neg" if avg_sentiment < -0.1 else \
                              "🟢 pos" if avg_sentiment >  0.1 else "⚪ neu"
            log.info(
                f"  [{i:>4}/{len(vendors)}] {name:<45} "
                f"{len(articles):>2} articles | {disruptive:>1} disruptions | {sentiment_label}"
            )
        else:
            log.debug(f"  [{i:>4}/{len(vendors)}] {name:<45} no articles")

        # Progress checkpoint every 50 vendors
        if i % 50 == 0:
            log.info(f"  ── Checkpoint: {total_articles:,} articles so far ──")

    return {
        "total_vendors":      len(vendors),
        "vendors_with_news":  vendor_coverage,
        "total_articles":     total_articles,
        "total_disruptions":  total_disruptions,
        "sources":            sources,
        "days_lookback":      days,
        "run_at":             datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# POST-INGESTION RISK UPDATE
# ─────────────────────────────────────────────────────────────────────────────

def update_news_risk_scores(conn):
    """
    After ingestion, update the vendors table with news-derived signals:
    - news_sentiment_score  (avg of last 30 days)
    - disruption_count_30d  (count of disruptive articles in 30 days)
    - last_disruption_type  (most recent disruption category)
    - last_news_at          (most recent article date)
    """
    log.info("\nUpdating vendor news risk scores in Postgres...")
    with conn.cursor() as cur:
        # Check columns exist, add if missing
        for col, dtype in [
            ("news_sentiment_30d", "FLOAT"),
            ("disruption_count_30d", "INT"),
            ("last_disruption_type", "VARCHAR(100)"),
            ("last_news_at", "TIMESTAMP"),
        ]:
            cur.execute(f"""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='vendors' AND column_name='{col}'
                ) THEN
                    ALTER TABLE vendors ADD COLUMN {col} {dtype};
                END IF;
            END $$;
            """)

        # Update each vendor
        cur.execute("""
        UPDATE vendors v
        SET
            news_sentiment_30d = sub.avg_sentiment,
            disruption_count_30d = sub.disr_count,
            last_disruption_type = sub.last_disr,
            last_news_at = sub.latest_date
        FROM (
            SELECT
                vendor_id,
                AVG(sentiment_score)        AS avg_sentiment,
                COUNT(*) FILTER (WHERE disruption_flag) AS disr_count,
                MAX(disruption_type) FILTER (WHERE disruption_flag) AS last_disr,
                MAX(published_at)           AS latest_date
            FROM vendor_news
            WHERE published_at >= NOW() - INTERVAL '30 days'
            GROUP BY vendor_id
        ) sub
        WHERE v.vendor_id = sub.vendor_id
        """)
        conn.commit()
    log.info("  ✅ vendor news scores updated")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fetch news for all SRSID vendors from NewsAPI/Guardian/GDELT")
    parser.add_argument("--vendor",  type=str,   default=None,
                        help="Test with a single vendor name")
    parser.add_argument("--days",    type=int,   default=30,
                        help="Lookback window in days (default: 30)")
    parser.add_argument("--source",  type=str,   default="all",
                        choices=["newsapi","guardian","gdelt","all"],
                        help="Which API to use (default: all)")
    parser.add_argument("--limit",   type=int,   default=None,
                        help="Max number of vendors to process")
    parser.add_argument("--no-update", action="store_true",
                        help="Skip updating vendor risk scores after ingestion")
    args = parser.parse_args()

    sources = ["newsapi","guardian","gdelt"] if args.source == "all" \
              else [args.source]

    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║   SRSID News Ingestion — Vendor Risk Intelligence               ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print()

    # API key status
    print("  API status:")
    print(f"    NewsAPI  : {'✅ key found' if NEWSAPI_KEY  else '❌ no key (set NEWSAPI_KEY)'}")
    print(f"    Guardian : {'✅ key found' if GUARDIAN_KEY else '⚠️  using test key (limited)'}")
    print(f"    GDELT    : ✅ free (no key needed)")
    print()

    if not NEWSAPI_KEY and not GUARDIAN_KEY and "gdelt" not in sources:
        log.error("No API keys configured and GDELT not in sources. Nothing to fetch.")
        log.error("Set NEWSAPI_KEY and/or GUARDIAN_KEY environment variables.")
        sys.exit(1)

    conn = get_db_conn()
    ensure_news_table(conn)

    vendors = get_vendors_from_db(
        conn,
        limit  = args.limit,
        single = args.vendor,
    )

    if not vendors:
        log.error("No vendors found in Postgres. Run sap_phase1_rebuild.py first.")
        sys.exit(1)

    summary = run_ingestion(vendors, sources, args.days, conn)

    if not args.no_update:
        update_news_risk_scores(conn)

    conn.close()

    # Print summary
    print()
    print("=" * 70)
    print("  NEWS INGESTION COMPLETE")
    print("=" * 70)
    print(f"  Vendors processed    : {summary['total_vendors']:,}")
    print(f"  Vendors with news    : {summary['vendors_with_news']:,} "
          f"({summary['vendors_with_news']/summary['total_vendors']*100:.1f}%)")
    print(f"  Total articles       : {summary['total_articles']:,}")
    print(f"  Disruption articles  : {summary['total_disruptions']:,}")
    print(f"  Sources used         : {', '.join(summary['sources'])}")
    print(f"  Lookback window      : {summary['days_lookback']} days")
    print()
    print("  Results stored in Postgres table: vendor_news")
    print("  Vendor risk scores updated in: vendors.news_sentiment_30d")
    print()
    print("  NEXT:")
    print("    python ml/features.py   → re-run feature engineering with news signals")
    print()

    # Save summary
    Path("reports").mkdir(exist_ok=True)
    with open("reports/news_ingestion_summary.json", "w") as f:
        json.dump(summary, f, indent=2)


if __name__ == "__main__":
    main()
