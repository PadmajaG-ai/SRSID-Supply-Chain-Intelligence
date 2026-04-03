"""
SRSID  —  news_ingestion_batch.py
===================================
Batch news ingestion using topic + industry queries instead of
one API call per vendor.

Strategy:
  OLD: 2,541 vendors × 1 API call each = 2,541 calls (~7 hours)
  NEW: ~60 topic queries → fetch articles → match to vendors locally
       = ~60 calls (~5-10 minutes)

How matching works:
  1. Build a set of topic queries from your industry + country combinations
     e.g. "semiconductor shortage supply chain", "chemical plant disruption"
  2. Fetch up to 50 articles per query from GDELT/NewsAPI/Guardian
  3. For each article, check which vendor names appear in the title/snippet
  4. Assign the article to every matching vendor
  5. Write all matches to vendor_news in one bulk operation

Coverage trade-off:
  - Per-vendor mode: precise but slow (finds "Tyranex Solution" news)
  - Batch mode: fast but only catches vendors mentioned by name in
    industry-level articles. Smaller/niche vendors may get no articles.
  - Recommended: run batch mode weekly + per-vendor mode for top 100
    vendors monthly.

Usage:
    python news_ingestion_batch.py                    # all topics, 30 days
    python news_ingestion_batch.py --days 7           # last 7 days
    python news_ingestion_batch.py --source gdelt     # GDELT only
    python news_ingestion_batch.py --source all       # all 3 sources
    python news_ingestion_batch.py --dry-run          # show queries, no fetch
"""

import sys
import re
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict

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
        logging.FileHandler("logs/news_batch.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

import os
NEWSAPI_KEY  = os.getenv("NEWSAPI_KEY",  "")
GUARDIAN_KEY = os.getenv("GUARDIAN_KEY", "")

# ─────────────────────────────────────────────────────────────────────────────
# TOPIC QUERY MAP
# Maps industry categories → list of search queries.
# Each query targets disruption signals relevant to that sector.
# Designed to return articles that will mention specific supplier names.
# ─────────────────────────────────────────────────────────────────────────────
INDUSTRY_QUERIES = {
    "Electronics & Semiconductors": [
        "semiconductor shortage supply chain 2024",
        "chip manufacturer disruption Taiwan",
        "electronics component supply delay",
        "TSMC Intel Samsung supply chain",
        "semiconductor fab production halt",
    ],
    "Chemicals": [
        "chemical plant disruption explosion 2024",
        "chemical supply shortage raw materials",
        "BASF Dow chemical production cut",
        "chemical industry supply chain risk",
        "petrochemical shortage refinery",
    ],
    "Automotive": [
        "automotive supplier bankruptcy 2024",
        "car parts shortage supply chain",
        "EV battery supplier disruption",
        "automotive production halt strike",
        "tier 1 supplier recall defect",
    ],
    "Pharma & Life Sciences": [
        "pharmaceutical drug shortage supply 2024",
        "API active pharmaceutical ingredient shortage",
        "pharma supplier quality recall FDA",
        "medical device supply chain disruption",
        "biotech manufacturing shutdown",
    ],
    "Energy & Utilities": [
        "energy supplier disruption outage 2024",
        "oil gas pipeline disruption shortage",
        "renewable energy supply chain solar wind",
        "power grid utility supply failure",
        "LNG natural gas shortage price spike",
    ],
    "Metals": [
        "steel aluminum metal shortage 2024",
        "mining disruption supply chain",
        "rare earth metal shortage China",
        "copper nickel supply disruption",
        "metal recycler smelter shutdown",
    ],
    "Food & Beverage": [
        "food supply chain disruption shortage 2024",
        "agricultural commodity shortage price spike",
        "food manufacturer recall contamination",
        "drought flood crop supply disruption",
        "food logistics cold chain failure",
    ],
    "Logistics & Transport": [
        "shipping logistics disruption port congestion 2024",
        "freight rate spike container shortage",
        "suez panama canal logistics delay",
        "trucking driver shortage supply chain",
        "warehouse logistics strike disruption",
    ],
    "IT Services": [
        "IT vendor outage cyberattack 2024",
        "cloud provider data breach outage",
        "software supplier ransomware security",
        "IT outsourcing vendor risk failure",
        "managed service provider breach",
    ],
    "Manufacturing": [
        "manufacturing plant shutdown strike 2024",
        "factory fire flood production halt",
        "industrial supplier bankruptcy default",
        "manufacturing supply chain reshoring",
        "OEM supplier quality defect recall",
    ],
    "Agriculture": [
        "agricultural supply chain disruption 2024",
        "fertilizer shortage crop production",
        "drought flood harvest supply impact",
        "agri commodity price spike shortage",
        "food ingredient supplier disruption",
    ],
    "Wholesale & Retail": [
        "wholesale distributor bankruptcy 2024",
        "retail supply chain out of stock",
        "distribution center disruption logistics",
        "wholesale supplier contract dispute",
        "retail vendor fraud compliance",
    ],
    "Financial Services": [
        "financial services vendor risk 2024",
        "bank fintech supplier compliance breach",
        "payment processor outage disruption",
        "financial data vendor security breach",
        "credit risk supplier default",
    ],
    "Consulting": [
        "consulting firm restructuring layoffs 2024",
        "professional services contract dispute",
        "advisory firm regulatory investigation",
    ],
}

# General disruption queries that apply across all industries
CROSS_INDUSTRY_QUERIES = [
    "supply chain disruption shortage 2024",
    "supplier bankruptcy insolvency default",
    "trade war tariff export control 2024",
    "natural disaster flood earthquake factory",
    "cyberattack ransomware supplier breach",
    "strike labour dispute walkout 2024",
    "port congestion shipping delay freight",
    "regulatory fine penalty investigation supplier",
    "recall product defect contamination 2024",
    "sanctions export restriction supply chain",
]


# ─────────────────────────────────────────────────────────────────────────────
# SENTIMENT + DISRUPTION (reused from news_ingestion.py)
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
    "shortage":     ["shortage","scarce","supply gap","depleted"],
    "delay":        ["delay","late","behind schedule","backlog"],
    "financial":    ["bankruptcy","default","insolvency","loss","receivership"],
    "geopolitical": ["sanction","tariff","trade war","embargo","geopolitical"],
    "natural":      ["earthquake","flood","hurricane","typhoon","disaster","fire"],
    "labour":       ["strike","walkout","labour dispute","worker","union"],
    "quality":      ["recall","defect","contamination","quality issue","rejection"],
    "cyber":        ["cyberattack","data breach","hack","ransomware","phishing"],
    "regulatory":   ["fine","penalty","investigation","violation","lawsuit","ban"],
}


def score_sentiment(text: str) -> float:
    if not text:
        return 0.0
    words = set(re.sub(r"[^a-z\s]", "", text.lower()).split())
    pos, neg = len(words & POSITIVE_WORDS), len(words & NEGATIVE_WORDS)
    total = pos + neg
    return round((pos - neg) / total, 3) if total else 0.0


def classify_disruption(text: str):
    tl = text.lower()
    for dtype, keywords in DISRUPTION_TYPES.items():
        if any(kw in tl for kw in keywords):
            return dtype, True
    return None, False


# ─────────────────────────────────────────────────────────────────────────────
# FETCHERS  (topic-level, not per-vendor)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_gdelt_topic(query: str, days: int,
                      max_articles: int = 50) -> list[dict]:
    """Fetch up to max_articles from GDELT for a topic query."""
    q      = requests.utils.quote(f'"{query}"')
    since  = (datetime.now(timezone.utc) - timedelta(days=days)
              ).strftime("%Y%m%d%H%M%S")
    url    = (
        f"https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={q}&mode=artlist&maxrecords={max_articles}"
        f"&startdatetime={since}&format=json&sort=DateDesc"
    )
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return []
        articles = r.json().get("articles", [])
        results  = []
        for a in articles:
            title   = a.get("title", "")
            snippet = a.get("seendescription", "")
            full    = f"{title} {snippet}"
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
                "raw_snippet":    snippet[:1000],
                "full_text":      full.lower(),  # used for vendor matching
            })
        return results
    except Exception as e:
        log.debug(f"  GDELT error for '{query[:40]}': {e}")
        return []


def fetch_newsapi_topic(query: str, days: int,
                        max_articles: int = 50) -> list[dict]:
    """Fetch articles from NewsAPI for a topic query."""
    if not NEWSAPI_KEY:
        return []
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={"q": query, "from": since, "sortBy": "publishedAt",
                    "pageSize": max_articles, "apiKey": NEWSAPI_KEY,
                    "language": "en"},
            timeout=15
        )
        if r.status_code != 200:
            return []
        results = []
        for a in r.json().get("articles", []):
            title   = a.get("title", "") or ""
            snippet = a.get("description", "") or ""
            full    = f"{title} {snippet}"
            sent    = score_sentiment(full)
            dtype, dflag = classify_disruption(full)
            results.append({
                "title":          title[:500],
                "url":            a.get("url", ""),
                "source_name":    a.get("source", {}).get("name", "NewsAPI"),
                "published_at":   (a.get("publishedAt") or "")[:19],
                "sentiment_score":sent,
                "disruption_type":dtype,
                "disruption_flag":dflag,
                "api_source":     "newsapi",
                "raw_snippet":    snippet[:1000],
                "full_text":      full.lower(),
            })
        return results
    except Exception as e:
        log.debug(f"  NewsAPI error for '{query[:40]}': {e}")
        return []


def fetch_guardian_topic(query: str, days: int,
                         max_articles: int = 50) -> list[dict]:
    """Fetch articles from Guardian for a topic query."""
    if not GUARDIAN_KEY:
        return []
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        r = requests.get(
            "https://content.guardianapis.com/search",
            params={"q": query, "from-date": since, "order-by": "newest",
                    "page-size": min(max_articles, 50), "api-key": GUARDIAN_KEY,
                    "show-fields": "headline,trailText"},
            timeout=15
        )
        if r.status_code != 200:
            return []
        results = []
        for a in r.json().get("response", {}).get("results", []):
            title   = a.get("fields", {}).get("headline", a.get("webTitle", ""))
            snippet = a.get("fields", {}).get("trailText", "")
            full    = f"{title} {snippet}"
            sent    = score_sentiment(full)
            dtype, dflag = classify_disruption(full)
            results.append({
                "title":          title[:500],
                "url":            a.get("webUrl", ""),
                "source_name":    "The Guardian",
                "published_at":   a.get("webPublicationDate", "")[:19],
                "sentiment_score":sent,
                "disruption_type":dtype,
                "disruption_flag":dflag,
                "api_source":     "guardian",
                "raw_snippet":    snippet[:1000],
                "full_text":      full.lower(),
            })
        return results
    except Exception as e:
        log.debug(f"  Guardian error for '{query[:40]}': {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# VENDOR MATCHING
# ─────────────────────────────────────────────────────────────────────────────

def build_vendor_index(vendors: pd.DataFrame) -> dict:
    """
    Build a lookup dict: normalised_name → (vendor_id, supplier_name).
    Also builds short aliases for matching — "Siemens AG" → "siemens".
    """
    index = {}
    for _, row in vendors.iterrows():
        vid   = str(row["vendor_id"])
        vname = str(row["supplier_name"])

        # Full name match (normalised)
        norm = vname.lower().strip()
        index[norm] = (vid, vname)

        # First significant word (≥5 chars) — catches "Siemens" from "Siemens AG"
        words = [w for w in re.split(r'\s+', norm) if len(w) >= 5
                 and w not in {"group","international","limited","company",
                               "corporation","services","solutions","global",
                               "holdings","industries","technology"}]
        if words:
            index[words[0]] = (vid, vname)

    return index


def match_vendors(article: dict, vendor_index: dict) -> list[tuple]:
    """
    Return list of (vendor_id, vendor_name) that appear in article text.
    Checks title + snippet against vendor name index.
    """
    text = article.get("full_text", "")
    if not text:
        return []

    matched = set()
    for name_key, (vid, vname) in vendor_index.items():
        if len(name_key) < 4:
            continue   # skip very short names — too many false matches
        # Use word-boundary match to avoid "Inc" matching "ince"
        if re.search(r'\b' + re.escape(name_key) + r'\b', text):
            matched.add((vid, vname))

    return list(matched)


# ─────────────────────────────────────────────────────────────────────────────
# WRITE TO POSTGRES  (batch version)
# ─────────────────────────────────────────────────────────────────────────────

def parse_date(pub_raw: str) -> datetime:
    try:
        if len(pub_raw) == 14:
            return datetime.strptime(pub_raw, "%Y%m%d%H%M%S")
        elif "T" in pub_raw:
            return datetime.fromisoformat(pub_raw.replace("Z", "+00:00"))
        else:
            return datetime.strptime(pub_raw[:10], "%Y-%m-%d")
    except Exception:
        return datetime.now(timezone.utc)


def bulk_write(db: DBClient,
               matches: list[tuple],   # [(vendor_id, vendor_name, article), ...]
               ) -> int:
    """Write all matched articles in a single transaction."""
    inserted = 0
    for vid, vname, article in matches:
        url = article.get("url", "")
        if not url:
            continue
        try:
            db.execute("""
                INSERT INTO vendor_news (
                    vendor_id, supplier_name, title, url,
                    source_name, published_at,
                    sentiment_score, disruption_type, disruption_flag,
                    api_source, raw_snippet
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (url) DO NOTHING
            """, (
                vid, vname,
                article.get("title","")[:500],
                url[:2000],
                article.get("source_name","")[:200],
                parse_date(article.get("published_at","")),
                float(article.get("sentiment_score", 0)),
                article.get("disruption_type"),
                bool(article.get("disruption_flag", False)),
                article.get("api_source",""),
                article.get("raw_snippet","")[:1000],
            ))
            inserted += 1
        except Exception as e:
            db.conn.rollback()
            log.debug(f"    Insert skip: {e}")
            continue

    db.conn.commit()
    return inserted


def update_vendor_signals(db: DBClient):
    log.info("Updating vendor news signals...")
    db.execute("""
        UPDATE vendors v
        SET news_sentiment_30d   = sub.avg_sentiment,
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
    db.conn.commit()
    updated = db.scalar(
        "SELECT COUNT(*) FROM vendors WHERE news_sentiment_30d IS NOT NULL"
    )
    log.info(f"  Updated signals for {updated:,} vendors")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="SRSID Batch News Ingestion — topic-level queries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--source", choices=["gdelt","newsapi","guardian","all"],
                        default="gdelt")
    parser.add_argument("--days",    type=int, default=30)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print queries without fetching")
    parser.add_argument("--cross-only", action="store_true",
                        help="Only run cross-industry queries (fastest)")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SRSID Batch News Ingestion")
    log.info("=" * 60)
    log.info(f"  Mode    : topic-batch (not per-vendor)")
    log.info(f"  Source  : {args.source}")
    log.info(f"  Lookback: {args.days} days")

    # Build query list
    queries = []
    if not args.cross_only:
        for industry, topic_list in INDUSTRY_QUERIES.items():
            for q in topic_list:
                queries.append((industry, q))
    for q in CROSS_INDUSTRY_QUERIES:
        queries.append(("cross-industry", q))

    log.info(f"  Queries : {len(queries)} total")

    if args.dry_run:
        log.info("\n  Queries that would run:")
        for industry, q in queries:
            log.info(f"    [{industry[:25]}] {q}")
        return

    # Load vendor list + build match index
    with DBClient() as db:
        vendors = db.fetch_df("""
            SELECT vendor_id, supplier_name, industry_category
            FROM vendors WHERE is_active = TRUE
        """)

    if vendors.empty:
        log.error("No vendors found. Run sap_loader.py first.")
        sys.exit(1)

    log.info(f"  Vendors : {len(vendors):,} loaded for matching")
    vendor_index = build_vendor_index(vendors)
    log.info(f"  Index   : {len(vendor_index):,} name keys built")

    # Select fetcher functions
    fetchers = []
    if args.source in ("gdelt", "all"):
        fetchers.append(("GDELT", fetch_gdelt_topic))
    if args.source in ("newsapi", "all") and NEWSAPI_KEY:
        fetchers.append(("NewsAPI", fetch_newsapi_topic))
    if args.source in ("guardian", "all") and GUARDIAN_KEY:
        fetchers.append(("Guardian", fetch_guardian_topic))

    if not fetchers:
        log.error("No news sources available. Check API keys in .env")
        sys.exit(1)

    # Run queries and accumulate all matches
    all_matches   = []      # list of (vid, vname, article)
    total_articles = 0
    seen_urls      = set()  # dedup within this run

    for i, (industry, query) in enumerate(queries, 1):
        log.info(f"  [{i:02d}/{len(queries)}] [{industry[:20]}] {query[:55]}")

        for src_name, fetcher in fetchers:
            articles = fetcher(query, args.days, max_articles=50)
            if not articles:
                continue

            log.debug(f"    {src_name}: {len(articles)} articles fetched")

            for article in articles:
                url = article.get("url", "")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                # Match to vendors
                matched_vendors = match_vendors(article, vendor_index)
                for vid, vname in matched_vendors:
                    all_matches.append((vid, vname, article))

            total_articles += len(articles)

        time.sleep(0.5)   # polite delay between queries

    log.info(f"\n  Fetched : {total_articles:,} articles across all queries")
    log.info(f"  Matched : {len(all_matches):,} vendor-article pairs")

    vendors_covered = len({vid for vid, _, _ in all_matches})
    log.info(f"  Coverage: {vendors_covered:,} / {len(vendors):,} vendors matched")

    if not all_matches:
        log.warning("  No matches found — vendors may not be mentioned in industry news")
        log.warning("  Consider running news_ingestion.py --source all --limit 200")
        return

    # Write to database
    log.info("  Writing to vendor_news table...")
    with DBClient() as db:
        # Ensure unique constraint
        try:
            db.execute(
                "ALTER TABLE vendor_news ADD CONSTRAINT "
                "vendor_news_url_unique UNIQUE (url)"
            )
            db.conn.commit()
        except Exception:
            db.conn.rollback()

        db.add_column_if_missing("vendor_news", "raw_snippet", "TEXT")
        db.conn.commit()

        inserted = bulk_write(db, all_matches)
        log.info(f"  Inserted: {inserted:,} new records")

        update_vendor_signals(db)

    log.info("\n" + "=" * 60)
    log.info("BATCH NEWS INGESTION COMPLETE")
    log.info("=" * 60)
    log.info(f"  Articles fetched   : {total_articles:,}")
    log.info(f"  Vendor-article pairs: {len(all_matches):,}")
    log.info(f"  Vendors with news  : {vendors_covered:,}")
    log.info(f"  Records inserted   : {inserted:,}")
    log.info(f"\n  For vendors not matched by batch mode, run:")
    log.info(f"  python news_ingestion.py --source all --limit 100")


if __name__ == "__main__":
    main()
