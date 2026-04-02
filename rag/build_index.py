"""
SRSID  —  rag/build_index.py
===============================
Builds the ChromaDB vector index from vendor data in Postgres.
Run once (or after every pipeline run to keep index fresh).

Each vendor becomes one text chunk:
  "[Name]. Country: [X]. Industry: [Y]. Risk: [tier] (score [N]).
   Spend: $[X]. OTIF: [X]%. OTTR: [X]%. Lead time variability: [X] days.
   Financial stability: [X]/100. News sentiment: [X]. Disruptions (30d): [N].
   Segment: [Kraljic]. Strategic action: [text]."

Usage:
    python rag/build_index.py               # build full index
    python rag/build_index.py --reset       # wipe and rebuild
    python rag/build_index.py --test        # build + run 3 test queries
"""

import sys, argparse, logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.db_client import DBClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

CHROMA_PATH = Path(__file__).parent / "chroma_store"
COLLECTION_NAME = "srsid_vendors"


def vendor_to_text(row: dict) -> str:
    """Convert a vendor record to a single text chunk for embedding."""
    def fmt(v, suffix="", default="N/A", scale=1, decimals=1):
        if v is None or (isinstance(v, float) and v != v):
            return default
        v = float(v) * scale
        return f"{v:.{decimals}f}{suffix}"

    parts = [
        f"{row.get('supplier_name', 'Unknown')}.",
        f"Vendor ID: {row.get('vendor_id', '?')}.",
        f"Country: {row.get('country_code', 'N/A')}.",
        f"Industry: {row.get('industry_category', 'N/A')}.",
        f"Risk tier: {row.get('risk_label', 'N/A')} (score {fmt(row.get('composite_risk_score'), decimals=3)}).",
        f"Annual spend: ${fmt(row.get('total_annual_spend'), decimals=0, scale=1)}.",
        f"Portfolio share: {fmt(row.get('spend_pct_of_portfolio'), suffix='%', scale=100)}.",
        f"OTIF rate: {fmt(row.get('otif_rate'), suffix='%', scale=100)}.",
        f"OTTR rate: {fmt(row.get('ottr_rate'), suffix='%', scale=100)}.",
        f"On-time delivery: {fmt(row.get('delivery_performance'), suffix='/100')}.",
        f"Avg delay: {fmt(row.get('avg_delay_days'), suffix=' days')}.",
        f"Lead time variability: {fmt(row.get('lead_time_variability'), suffix=' days')}.",
        f"Order accuracy: {fmt(row.get('order_accuracy_rate'), suffix='%', scale=100)}.",
        f"Price variance (PPV): {fmt(row.get('avg_price_variance_pct'), suffix='%')}.",
        f"Financial stability: {fmt(row.get('financial_stability'), suffix='/100')}.",
        f"News sentiment (30d): {fmt(row.get('news_sentiment_30d'), decimals=2)}.",
        f"Disruptions (30d): {int(row.get('disruption_count_30d') or 0)}.",
        f"Geo risk: {row.get('geo_risk', 'N/A')}.",
        f"Kraljic segment: {row.get('kraljic_segment', 'N/A')}.",
        f"ABC class: {row.get('abc_class', 'N/A')}.",
        f"Strategic action: {row.get('strategic_action', 'N/A')}.",
        f"Contract compliance (SUM%): {fmt(row.get('sum_percentage'), suffix='%')}.",
        f"Maverick spend: {'Yes' if row.get('is_maverick') else 'No'}.",
        f"Concentration risk: {row.get('concentration_risk', 'N/A')}.",
        f"ESG score: {fmt(row.get('esg_score'), suffix='/100', default='not scored')}.",
        f"Cybersecurity score: {fmt(row.get('cybersecurity_score'), suffix='/100', default='not scored')}.",
        f"Innovation score: {fmt(row.get('innovation_score'), suffix='/100', default='not scored')}.",
    ]
    return " ".join(parts)


def build_index(reset: bool = False):
    """Build (or rebuild) the ChromaDB vector index."""
    try:
        import chromadb
        from chromadb.utils import embedding_functions
    except ImportError:
        log.error("chromadb not installed. Run: pip install chromadb sentence-transformers")
        sys.exit(1)

    CHROMA_PATH.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(CHROMA_PATH))

    if reset:
        try:
            client.delete_collection(COLLECTION_NAME)
            log.info("Existing collection deleted.")
        except Exception:
            pass

    emb_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="all-MiniLM-L6-v2"
    )

    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        embedding_function=emb_fn,
        metadata={"hnsw:space": "cosine"}
    )

    existing_count = collection.count()
    log.info(f"Collection '{COLLECTION_NAME}': {existing_count} existing documents")

    # Load vendors with all evaluation metrics
    log.info("Loading vendor data from Postgres...")
    with DBClient() as db:
        vendors = db.fetch_df("""
            SELECT
                v.vendor_id, v.supplier_name, v.country_code,
                v.industry_category, v.risk_label,
                v.composite_risk_score, v.total_annual_spend,
                v.spend_pct_of_portfolio, v.delivery_performance,
                v.otif_rate, v.ottr_rate, v.avg_delay_days,
                v.lead_time_variability, v.order_accuracy_rate,
                v.avg_price_variance_pct, v.financial_stability,
                v.news_sentiment_30d, v.disruption_count_30d,
                v.geo_risk, v.sum_percentage, v.is_maverick,
                v.concentration_risk, v.esg_score,
                v.cybersecurity_score, v.innovation_score,
                s.kraljic_segment, s.abc_class, s.strategic_action
            FROM vendors v
            LEFT JOIN latest_segments s ON v.vendor_id = s.vendor_id
            WHERE v.is_active = TRUE
        """)

    if vendors.empty:
        log.error("No vendor data found. Run the pipeline first.")
        sys.exit(1)

    log.info(f"Loaded {len(vendors):,} vendors. Building text chunks...")

    # Build chunks
    docs, ids, metadatas = [], [], []
    for _, row in vendors.iterrows():
        vid  = str(row["vendor_id"])
        text = vendor_to_text(row.to_dict())
        docs.append(text)
        ids.append(vid)
        metadatas.append({
            "vendor_id":    vid,
            "name":         str(row.get("supplier_name", "")),
            "risk_label":   str(row.get("risk_label", "Unknown")),
            "country":      str(row.get("country_code", "")),
            "industry":     str(row.get("industry_category", "")),
            "risk_score":   float(row.get("composite_risk_score") or 0),
            "annual_spend": float(row.get("total_annual_spend") or 0),
        })

    # Upsert in batches of 100
    batch_size = 100
    total = len(docs)
    for i in range(0, total, batch_size):
        batch_docs  = docs[i:i+batch_size]
        batch_ids   = ids[i:i+batch_size]
        batch_meta  = metadatas[i:i+batch_size]
        collection.upsert(documents=batch_docs, ids=batch_ids, metadatas=batch_meta)
        log.info(f"  Indexed {min(i+batch_size, total):,}/{total:,} vendors...")

    log.info(f"\n✅ Index built: {collection.count():,} vendors in ChromaDB")
    log.info(f"   Stored at: {CHROMA_PATH}")
    return collection


def test_index():
    """Run 3 sample queries to verify the index works."""
    try:
        import chromadb
        from chromadb.utils import embedding_functions
    except ImportError:
        log.error("chromadb not installed.")
        return

    client = chromadb.PersistentClient(path=str(CHROMA_PATH))
    emb_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="all-MiniLM-L6-v2"
    )
    collection = client.get_collection(COLLECTION_NAME, embedding_function=emb_fn)

    test_queries = [
        "high risk electronics supplier",
        "best delivery performance vendor",
        "supplier with maverick spend no contract",
    ]
    log.info("\n--- Test Queries ---")
    for q in test_queries:
        results = collection.query(query_texts=[q], n_results=3)
        log.info(f"\nQuery: '{q}'")
        for name, meta in zip(results["documents"][0], results["metadatas"][0]):
            log.info(f"  {meta['name']} | {meta['risk_label']} | {meta['country']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build SRSID vendor vector index")
    parser.add_argument("--reset", action="store_true", help="Wipe and rebuild index")
    parser.add_argument("--test",  action="store_true", help="Run test queries after build")
    args = parser.parse_args()

    build_index(reset=args.reset)
    if args.test:
        test_index()
