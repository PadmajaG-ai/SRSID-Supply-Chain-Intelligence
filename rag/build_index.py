"""
SRSID  —  rag/build_index.py
===============================
Builds vendor embeddings using sentence-transformers and stores them
in Supabase using the pgvector extension.

No local ChromaDB files — embeddings live in the vendors table,
accessible from Streamlit Cloud and anywhere else.

Prerequisites:
  1. Run db/migrations/001_pgvector.sql in Supabase SQL Editor
  2. pip install sentence-transformers

Usage:
    python rag/build_index.py               # embed all vendors
    python rag/build_index.py --reset       # clear and re-embed all
    python rag/build_index.py --missing     # only embed vendors with no embedding
    python rag/build_index.py --test        # embed + run 3 test queries
"""

import sys, argparse, logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.db_client import DBClient

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

EMBED_MODEL = "all-MiniLM-L6-v2"
BATCH_SIZE  = 64
_encoder    = None


def vendor_to_text(row: dict) -> str:
    def fmt(v, suffix="", default="N/A", scale=1, decimals=1):
        if v is None or (isinstance(v, float) and v != v):
            return default
        return f"{float(v)*scale:.{decimals}f}{suffix}"
    return " ".join([
        f"{row.get('supplier_name','Unknown')}.",
        f"Vendor ID: {row.get('vendor_id','?')}.",
        f"Country: {row.get('country_code','N/A')}.",
        f"Industry: {row.get('industry_category','N/A')}.",
        f"Risk tier: {row.get('risk_label','N/A')} (score {fmt(row.get('composite_risk_score'),decimals=3)}).",
        f"Annual spend: ${fmt(row.get('total_annual_spend'),decimals=0)}.",
        f"OTIF rate: {fmt(row.get('otif_rate'),suffix='%',scale=100)}.",
        f"OTTR rate: {fmt(row.get('ottr_rate'),suffix='%',scale=100)}.",
        f"Delivery performance: {fmt(row.get('delivery_performance'),suffix='/100')}.",
        f"Avg delay: {fmt(row.get('avg_delay_days'),suffix=' days')}.",
        f"Lead time variability: {fmt(row.get('lead_time_variability'),suffix=' days')}.",
        f"Order accuracy: {fmt(row.get('order_accuracy_rate'),suffix='%',scale=100)}.",
        f"Price variance PPV: {fmt(row.get('avg_price_variance_pct'),suffix='%')}.",
        f"Financial stability: {fmt(row.get('financial_stability'),suffix='/100')}.",
        f"News sentiment: {fmt(row.get('news_sentiment_30d'),decimals=2)}.",
        f"Disruptions 30d: {int(row.get('disruption_count_30d') or 0)}.",
        f"Geo risk: {row.get('geo_risk','N/A')}.",
        f"Kraljic segment: {row.get('kraljic_segment','N/A')}.",
        f"ABC class: {row.get('abc_class','N/A')}.",
        f"Strategic action: {row.get('strategic_action','N/A')}.",
        f"Contract compliance SUM%: {fmt(row.get('sum_percentage'),suffix='%')}.",
        f"Maverick spend: {'Yes' if row.get('is_maverick') else 'No'}.",
    ])


def get_encoder():
    global _encoder
    if _encoder is None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise RuntimeError("pip install sentence-transformers")
        log.info(f"Loading model: {EMBED_MODEL}")
        _encoder = SentenceTransformer(EMBED_MODEL)
    return _encoder


def embed_texts(texts):
    return get_encoder().encode(texts, batch_size=32,
                                show_progress_bar=False).tolist()


def check_pgvector(db):
    try:
        has_ext = db.scalar(
            "SELECT COUNT(*) FROM pg_extension WHERE extname='vector'")
        if not has_ext:
            log.error("pgvector not enabled — run db/migrations/001_pgvector.sql")
            return False
        has_col = db.scalar("""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name='vendors' AND column_name='embedding'""")
        if not has_col:
            log.error("embedding column missing — run db/migrations/001_pgvector.sql")
            return False
        return True
    except Exception as e:
        log.error(f"pgvector check failed: {e}")
        return False


def build_index(reset=False, missing_only=False):
    with DBClient() as db:
        if not check_pgvector(db):
            return False
        if reset:
            db.execute("UPDATE vendors SET embedding=NULL, vendor_text=NULL")
            db.conn.commit()
            log.info("Cleared existing embeddings.")

        where = "WHERE v.is_active=TRUE"
        if missing_only:
            where += " AND v.embedding IS NULL"

        vendors = db.fetch_df(f"""
            SELECT v.vendor_id, v.supplier_name, v.country_code,
                   v.industry_category, v.risk_label,
                   v.composite_risk_score, v.total_annual_spend,
                   v.spend_pct_of_portfolio, v.delivery_performance,
                   v.otif_rate, v.ottr_rate, v.avg_delay_days,
                   v.lead_time_variability, v.order_accuracy_rate,
                   v.avg_price_variance_pct, v.financial_stability,
                   v.news_sentiment_30d, v.disruption_count_30d,
                   v.geo_risk, v.sum_percentage, v.is_maverick,
                   s.kraljic_segment, s.abc_class, s.strategic_action
            FROM vendors v
            LEFT JOIN latest_segments s ON v.vendor_id=s.vendor_id
            {where}
        """)

    if vendors.empty:
        log.error("No vendors found.")
        return False

    total    = len(vendors)
    embedded = 0
    log.info(f"Embedding {total:,} vendors into pgvector...")

    for start in range(0, total, BATCH_SIZE):
        batch   = vendors.iloc[start:start+BATCH_SIZE]
        texts   = [vendor_to_text(r.to_dict()) for _, r in batch.iterrows()]
        vids    = batch["vendor_id"].tolist()
        vectors = embed_texts(texts)

        with DBClient() as db:
            for vid, text, vec in zip(vids, texts, vectors):
                vec_str = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"
                db.execute("""
                    UPDATE vendors
                    SET embedding   = %s::vector,
                        vendor_text = %s
                    WHERE vendor_id = %s
                """, (vec_str, text, vid))
            db.conn.commit()

        embedded += len(batch)
        log.info(f"  {embedded:,}/{total:,}")

    log.info(f"\nDone — {embedded:,} vendors embedded in Supabase.")
    return True


def test_index():
    from rag.retriever import retrieve
    for q in ["high risk electronics supplier",
              "best delivery performance",
              "maverick spend no contract"]:
        results = retrieve(q, n=3)
        log.info(f"\nQuery: '{q}'")
        for r in results:
            m = r["metadata"]
            log.info(f"  {m['name']} | {m['risk_label']} | sim={r['similarity']:.3f}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--reset",   action="store_true")
    p.add_argument("--missing", action="store_true")
    p.add_argument("--test",    action="store_true")
    args = p.parse_args()
    ok = build_index(reset=args.reset, missing_only=args.missing)
    if ok and args.test:
        test_index()
