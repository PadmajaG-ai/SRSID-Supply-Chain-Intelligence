"""
SRSID  —  ingestion/unspsc_classifier.py
==========================================
Maps SAP material_group (MATKL) codes and vendor data to UNSPSC
(United Nations Standard Products and Services Code) taxonomy.

Two-stage approach:
  Stage 1 — Lookup table: maps known SAP MATKL codes to UNSPSC segments.
             Covers ~70–80% of standard SAP material group configurations.
  Stage 2 — TF-IDF classifier: for unmapped codes, uses supplier_name +
             material_group text to predict UNSPSC segment/family.

UNSPSC hierarchy:
  Segment  (2-digit)  — broadest, e.g. "43 Information Technology"
  Family   (4-digit)  — e.g. "4321 Computer Equipment"
  Class    (6-digit)  — e.g. "432112 Notebooks"
  Commodity(8-digit)  — most specific

For SRSID we map to Segment + Family level which is most useful for
spend analytics and risk scoring without over-engineering.

Usage:
    python ingestion/unspsc_classifier.py --dry-run    # preview mappings
    python ingestion/unspsc_classifier.py              # write to DB
    python ingestion/unspsc_classifier.py --retrain    # retrain ML model
"""

import sys
import json
import logging
import argparse
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.db_client import DBClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# UNSPSC SEGMENT REFERENCE  (top 45 segments most relevant to procurement)
# Source: UNSPSC v25 public taxonomy
# ─────────────────────────────────────────────────────────────────────────────
UNSPSC_SEGMENTS = {
    "10": ("10000000", "Live Plant and Animal Material and Accessories and Supplies"),
    "11": ("11000000", "Mineral and Textile and Inorganic Chemicals and Rubber and Plastics"),
    "12": ("12000000", "Chemicals including Bio Chemicals and Gas Materials"),
    "13": ("13000000", "Resin and Rosin and Rubber and Foam and Film and Elastomeric Materials"),
    "14": ("14000000", "Paper Materials and Products"),
    "15": ("15000000", "Fuels and Fuel Additives and Lubricants and Anti corrosive Materials"),
    "20": ("20000000", "Mining and Well Drilling Machinery and Accessories"),
    "21": ("21000000", "Farming and Fishing and Forestry and Wildlife Machinery"),
    "22": ("22000000", "Building and Construction Machinery and Accessories"),
    "23": ("23000000", "Industrial Manufacturing and Processing Machinery and Accessories"),
    "24": ("24000000", "Material Handling and Conditioning and Storage Machinery"),
    "25": ("25000000", "Vehicles"),
    "26": ("26000000", "Power Generation and Distribution Machinery and Accessories"),
    "27": ("27000000", "Tools and General Machinery"),
    "30": ("30000000", "Structures and Building and Construction and Manufacturing Components and Supplies"),
    "31": ("31000000", "Manufacturing Components and Supplies"),
    "32": ("32000000", "Electronic Components and Supplies"),
    "39": ("39000000", "Electrical Systems and Lighting and Components and Accessories and Supplies"),
    "40": ("40000000", "Distribution and Conditioning Systems and Equipment and Components"),
    "41": ("41000000", "Laboratory and Measuring and Observing and Testing Equipment"),
    "42": ("42000000", "Medical Equipment and Accessories and Supplies"),
    "43": ("43000000", "Information Technology Broadcasting and Telecommunications"),
    "44": ("44000000", "Office Equipment and Accessories and Supplies"),
    "45": ("45000000", "Printing and Publishing Equipment and Supplies"),
    "46": ("46000000", "Defence and Law Enforcement and Security and Safety Equipment"),
    "47": ("47000000", "Cleaning Equipment and Supplies"),
    "48": ("48000000", "Service Industry Machinery and Equipment and Supplies"),
    "49": ("49000000", "Sports and Recreational Equipment and Supplies"),
    "50": ("50000000", "Food Beverage and Tobacco Products"),
    "51": ("51000000", "Drugs and Pharmaceutical Products"),
    "52": ("52000000", "Domestic Appliances and Supplies and Consumer Electronic Products"),
    "53": ("53000000", "Apparel and Luggage and Personal Care Products"),
    "55": ("55000000", "Published Products"),
    "56": ("56000000", "Furniture and Furnishings"),
    "60": ("60000000", "Musical Instruments and Games and Toys and Arts and Crafts"),
    "70": ("70000000", "Farming and Fishing and Forestry and Wildlife Contracting Services"),
    "72": ("72000000", "Building and Construction and Maintenance Services"),
    "73": ("73000000", "Industrial Production and Manufacturing Services"),
    "76": ("76000000", "Industrial Cleaning Services"),
    "77": ("77000000", "Environmental Services"),
    "78": ("78000000", "Transportation and Storage and Mail Services"),
    "80": ("80000000", "Management and Business Professionals and Administrative Services"),
    "81": ("81000000", "Engineering and Research and Technology Based Services"),
    "82": ("82000000", "Editorial and Design and Graphic and Fine Art Services"),
    "83": ("83000000", "Public Utilities and Public Sector Related Services"),
    "84": ("84000000", "Financial and Insurance Services"),
    "85": ("85000000", "Healthcare Services"),
    "86": ("86000000", "Education and Training Services"),
    "90": ("90000000", "Travel and Food and Lodging and Entertainment Services"),
    "91": ("91000000", "Personal and Domestic Services"),
    "92": ("92000000", "National Defence and Public Order and Security and Safety Services"),
    "93": ("93000000", "Politics and Civic Affairs Services"),
    "94": ("94000000", "Organizations and Clubs"),
    "95": ("95000000", "Land and Buildings and Structures and Thoroughfares"),
}

# ─────────────────────────────────────────────────────────────────────────────
# SAP MATERIAL GROUP → UNSPSC LOOKUP TABLE
# Maps common SAP MATKL codes to UNSPSC segments.
# SAP material groups vary by company, but standard Customising uses
# MATKL as an internal code with a description.
# This covers the most common configurations found in SAP standard datasets.
# ─────────────────────────────────────────────────────────────────────────────
MATKL_TO_UNSPSC = {
    # Electronics / IT
    "001": "43", "ELEC": "43", "COMP": "43", "IT": "43", "HW": "43",
    "SW": "43", "SOFT": "43", "TELE": "43", "ELE": "43", "EDV": "43",
    # Chemicals
    "002": "12", "CHEM": "12", "CHE": "12", "LAB": "41",
    # Metals / Raw materials
    "003": "31", "METL": "31", "IRON": "31", "STEE": "31", "ALUM": "31",
    # Automotive parts
    "004": "25", "AUTO": "25", "VHCL": "25", "PART": "31",
    # Packaging
    "005": "14", "PACK": "14", "PAPER": "14",
    # Logistics / Transport services
    "006": "78", "LOG": "78", "TRAN": "78", "SHIP": "78",
    # MRO / Maintenance
    "007": "27", "MRO": "27", "MANT": "72", "REPR": "72",
    # Office supplies
    "008": "44", "OFF": "44", "OFFI": "44",
    # Food & Beverage
    "009": "50", "FOOD": "50", "BEV": "50",
    # Pharma / Medical
    "010": "51", "PHAR": "51", "MED": "42", "DRUG": "51",
    # Consulting / Professional services
    "011": "80", "CONS": "80", "SERV": "80", "PROF": "80", "MGMT": "80",
    # Construction
    "012": "72", "CONS": "72", "BLDG": "72", "CNST": "72",
    # Energy / Utilities
    "013": "26", "ENRG": "26", "FUEL": "15", "UTIL": "83",
    # Machinery
    "014": "23", "MACH": "23", "EQUIP": "27",
    # Agriculture
    "015": "10", "AGR": "10", "FARM": "10",
    # Financial services
    "016": "84", "FIN": "84", "INS": "84",
    # Textiles / Apparel
    "017": "53", "TEXT": "53", "APP": "53",
    # Plastics / Rubber
    "018": "13", "PLAS": "13", "RUBB": "13",
    # Cleaning
    "019": "47", "CLEN": "47",
    # Wholesale / Generic
    "020": "80", "WHL": "80", "GEN": "80",
}

# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD → UNSPSC SEGMENT  (for ML fallback + text matching)
# Used to classify vendor names and descriptions when MATKL lookup fails
# ─────────────────────────────────────────────────────────────────────────────
KEYWORD_SEGMENT_MAP = {
    "43": ["computer", "software", "hardware", "laptop", "server", "network",
           "telecom", "electronics", "semiconductor", "chip", "circuit",
           "IT", "technology", "digital", "cloud", "data center", "storage",
           "printer", "monitor", "mobile", "tablet"],
    "12": ["chemical", "reagent", "solvent", "adhesive", "paint", "coating",
           "acid", "polymer", "compound", "substance"],
    "31": ["metal", "steel", "aluminium", "iron", "copper", "component",
           "fastener", "bracket", "casting", "machined", "precision"],
    "25": ["vehicle", "automotive", "car", "truck", "spare part", "tyre",
           "engine", "transmission"],
    "78": ["logistics", "transport", "freight", "shipping", "courier",
           "delivery", "warehouse", "supply chain"],
    "80": ["consulting", "advisory", "professional service", "management",
           "staffing", "recruitment", "outsourc"],
    "72": ["construction", "building", "maintenance", "facility", "civil",
           "contractor", "installation"],
    "50": ["food", "beverage", "catering", "grocery", "packaging food"],
    "51": ["pharma", "drug", "medicine", "pharmaceutical", "biotech",
           "life science", "clinical"],
    "42": ["medical", "hospital", "surgical", "diagnostic", "healthcare"],
    "26": ["energy", "power", "electric", "generator", "solar", "wind",
           "utility", "gas", "oil", "fuel"],
    "15": ["lubricant", "petroleum", "diesel", "petrol", "hydraulic fluid"],
    "27": ["tool", "equipment", "machinery", "industrial", "plant",
           "manufacturing equipment"],
    "44": ["office", "stationery", "furniture", "supplies"],
    "84": ["insurance", "finance", "bank", "financial", "investment"],
    "86": ["training", "education", "learning", "certification"],
    "41": ["laboratory", "testing", "measurement", "instrument", "analytical"],
    "13": ["plastic", "rubber", "polymer", "foam", "packaging material"],
    "10": ["agriculture", "farm", "crop", "livestock", "seed", "fertiliser"],
    "47": ["cleaning", "hygiene", "sanitation", "janitorial"],
}


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1: LOOKUP
# ─────────────────────────────────────────────────────────────────────────────

def lookup_unspsc(material_group: str) -> str | None:
    """Try to map a SAP MATKL code to UNSPSC segment via lookup table."""
    if not material_group:
        return None
    mg = str(material_group).strip().upper()
    # Direct match
    if mg in MATKL_TO_UNSPSC:
        return MATKL_TO_UNSPSC[mg]
    # Numeric codes — try padding variations
    mg_stripped = mg.lstrip("0")
    if mg_stripped in MATKL_TO_UNSPSC:
        return MATKL_TO_UNSPSC[mg_stripped]
    return None


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 2: KEYWORD + TF-IDF CLASSIFIER
# ─────────────────────────────────────────────────────────────────────────────

def keyword_classify(text: str) -> str | None:
    """
    Simple keyword matching against UNSPSC segment keywords.
    Returns segment code or None if no confident match.
    """
    if not text:
        return None
    tl = text.lower()
    scores = {}
    for seg, keywords in KEYWORD_SEGMENT_MAP.items():
        score = sum(1 for kw in keywords if kw.lower() in tl)
        if score > 0:
            scores[seg] = score
    if not scores:
        return None
    return max(scores, key=scores.get)


def train_tfidf_classifier(transactions: pd.DataFrame):
    """
    Train a lightweight TF-IDF + Logistic Regression classifier
    on supplier_name + material_group → UNSPSC segment.
    Used for transactions not covered by the lookup table.
    """
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline
    except ImportError:
        log.warning("scikit-learn not available — skipping ML classifier")
        return None

    # Build training data from keyword map
    train_texts, train_labels = [], []
    for seg, keywords in KEYWORD_SEGMENT_MAP.items():
        for kw in keywords:
            train_texts.append(kw)
            train_labels.append(seg)
        # Add the segment description as training text
        if seg in UNSPSC_SEGMENTS:
            train_texts.append(UNSPSC_SEGMENTS[seg][1].lower())
            train_labels.append(seg)

    # Also use any already-mapped transactions as training signal
    if "unspsc_segment" in transactions.columns:
        mapped = transactions[transactions["unspsc_segment"].notna()]
        for _, row in mapped.iterrows():
            text = f"{row.get('supplier_name','')} {row.get('material_group','')}".strip()
            if text and row["unspsc_segment"]:
                train_texts.append(text.lower())
                train_labels.append(str(row["unspsc_segment"])[:2])

    if len(set(train_labels)) < 3:
        return None

    pipeline = Pipeline([
        ("tfidf", TfidfVectorizer(ngram_range=(1, 2), max_features=5000,
                                   sublinear_tf=True)),
        ("clf",   LogisticRegression(max_iter=500, C=1.0,
                                     class_weight="balanced")),
    ])
    pipeline.fit(train_texts, train_labels)
    log.info(f"  TF-IDF classifier trained on {len(train_texts)} samples, "
             f"{len(set(train_labels))} classes")
    return pipeline


def classify_transactions(transactions: pd.DataFrame,
                           classifier=None) -> pd.DataFrame:
    """
    Apply both lookup and ML classification to a transactions dataframe.
    Adds columns: unspsc_segment, unspsc_segment_name, unspsc_code,
                  unspsc_method (lookup / keyword / ml / unknown)
    """
    df = transactions.copy()

    segments, seg_names, codes, methods = [], [], [], []

    for _, row in df.iterrows():
        mg    = str(row.get("material_group", "") or "")
        sname = str(row.get("supplier_name",  "") or "")
        text  = f"{sname} {mg}".strip()

        seg    = None
        method = "unknown"

        # Stage 1: lookup
        seg = lookup_unspsc(mg)
        if seg:
            method = "lookup"

        # Stage 2a: keyword matching
        if not seg:
            seg = keyword_classify(text)
            if seg:
                method = "keyword"

        # Stage 2b: ML classifier
        if not seg and classifier is not None:
            try:
                pred = classifier.predict([text.lower()])[0]
                prob = classifier.predict_proba([text.lower()]).max()
                if prob >= 0.30:   # only use if reasonably confident
                    seg    = pred
                    method = f"ml({prob:.2f})"
            except Exception:
                pass

        # Resolve segment name
        if seg and seg in UNSPSC_SEGMENTS:
            code, name = UNSPSC_SEGMENTS[seg]
        elif seg:
            code, name = f"{seg}000000", f"Segment {seg}"
        else:
            code, name, seg = "00000000", "Unclassified", "00"
            method = "unknown"

        segments.append(seg)
        seg_names.append(name)
        codes.append(code)
        methods.append(method)

    df["unspsc_segment"]      = segments
    df["unspsc_segment_name"] = seg_names
    df["unspsc_code"]         = codes
    df["unspsc_method"]       = methods
    return df


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE OPS
# ─────────────────────────────────────────────────────────────────────────────

def ensure_columns(db: DBClient):
    """Add UNSPSC columns to transactions table if not present."""
    for col, dtype in [
        ("unspsc_segment",      "VARCHAR(10)"),
        ("unspsc_segment_name", "VARCHAR(500)"),
        ("unspsc_code",         "VARCHAR(20)"),
        ("unspsc_method",       "VARCHAR(30)"),
    ]:
        db.add_column_if_missing("transactions", col, dtype)
    db.conn.commit()


def save_unspsc_summary(db: DBClient, df: pd.DataFrame):
    """
    Write spend aggregated by UNSPSC segment to unspsc_spend_summary table.
    This is what the dashboard and chatbot read.
    """
    db.execute("""
        CREATE TABLE IF NOT EXISTS unspsc_spend_summary (
            id                  SERIAL PRIMARY KEY,
            unspsc_segment      VARCHAR(10),
            unspsc_code         VARCHAR(20),
            unspsc_segment_name TEXT,
            vendor_count        INT,
            transaction_count   INT,
            total_spend         FLOAT,
            spend_pct           FLOAT,
            maverick_spend      FLOAT,
            maverick_pct        FLOAT,
            high_risk_spend     FLOAT,
            high_risk_pct       FLOAT,
            savings_opportunity FLOAT,
            classification_method VARCHAR(30),
            run_date            TIMESTAMP DEFAULT NOW()
        )
    """)
    db.conn.commit()
    db.execute("DELETE FROM unspsc_spend_summary")

    total = df["transaction_amount"].sum() if "transaction_amount" in df.columns else 1

    summary = (
        df.groupby(["unspsc_segment", "unspsc_segment_name", "unspsc_code",
                    "unspsc_method"])
        .agg(
            vendor_count       = ("vendor_id",           "nunique"),
            transaction_count  = ("transaction_amount",  "count"),
            total_spend        = ("transaction_amount",  "sum"),
        )
        .reset_index()
        .sort_values("total_spend", ascending=False)
    )

    summary["spend_pct"] = (summary["total_spend"] / total * 100).round(2)

    for _, row in summary.iterrows():
        db.execute("""
            INSERT INTO unspsc_spend_summary (
                unspsc_segment, unspsc_code, unspsc_segment_name,
                vendor_count, transaction_count, total_spend, spend_pct,
                classification_method
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            str(row["unspsc_segment"]),
            str(row["unspsc_code"]),
            str(row["unspsc_segment_name"]),
            int(row["vendor_count"]),
            int(row["transaction_count"]),
            float(row["total_spend"]),
            float(row["spend_pct"]),
            str(row["unspsc_method"]),
        ))
    db.conn.commit()
    log.info(f"  Saved {len(summary)} UNSPSC segments to unspsc_spend_summary")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Map SAP transactions to UNSPSC taxonomy"
    )
    parser.add_argument("--dry-run",  action="store_true",
                        help="Preview mappings without writing to DB")
    parser.add_argument("--retrain",  action="store_true",
                        help="Force retrain the ML classifier")
    parser.add_argument("--limit",    type=int, default=None,
                        help="Process only N transactions (for testing)")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SRSID UNSPSC Classifier")
    log.info("=" * 60)

    with DBClient() as db:
        transactions = db.fetch_df("""
            SELECT t.vendor_id, t.supplier_name, t.material_group,
                   t.transaction_amount, t.po_number,
                   v.industry_category, v.risk_label,
                   CASE WHEN c.vendor_id IS NULL THEN TRUE ELSE FALSE END
                       AS is_maverick
            FROM transactions t
            LEFT JOIN vendors v ON t.vendor_id = v.vendor_id
            LEFT JOIN (
                SELECT DISTINCT vendor_id FROM contracts
                WHERE contract_status NOT IN ('Expired')
            ) c ON t.vendor_id = c.vendor_id
            WHERE t.transaction_amount > 0
        """ + (f" LIMIT {args.limit}" if args.limit else ""))

    if transactions.empty:
        log.error("No transactions found. Run sap_loader.py first.")
        return

    log.info(f"  Loaded {len(transactions):,} transactions")

    # Train classifier
    log.info("  Training TF-IDF classifier...")
    classifier = train_tfidf_classifier(transactions)

    # Classify
    log.info("  Classifying transactions...")
    classified = classify_transactions(transactions, classifier)

    # Coverage report
    method_counts = classified["unspsc_method"].value_counts()
    total = len(classified)
    log.info("\n  Classification coverage:")
    for method, count in method_counts.items():
        pct = count / total * 100
        bar = "█" * int(pct / 2)
        log.info(f"  {method:<20} {bar:<25} {count:>7,} ({pct:.1f}%)")

    unknown_pct = method_counts.get("unknown", 0) / total * 100
    log.info(f"\n  Covered: {100-unknown_pct:.1f}% of transactions mapped to UNSPSC")

    # Segment summary
    log.info("\n  Top UNSPSC segments by spend:")
    seg_spend = (classified.groupby("unspsc_segment_name")["transaction_amount"]
                 .sum().sort_values(ascending=False).head(10))
    total_spend = classified["transaction_amount"].sum()
    for seg, spend in seg_spend.items():
        log.info(f"  {seg[:55]:<55} ${spend/1e9:.1f}B ({spend/total_spend*100:.1f}%)")

    if args.dry_run:
        log.info("\n  DRY RUN — no changes written.")
        return

    # Write to DB
    log.info("\n  Writing UNSPSC codes to transactions table...")
    with DBClient() as db:
        ensure_columns(db)

        # Bulk update using a temporary VALUES table — one round trip per batch
        # instead of one UPDATE per row. ~100x faster over Supabase.
        updated   = 0
        CHUNK     = 2000   # rows per round trip

        rows = classified[["po_number", "unspsc_segment",
                            "unspsc_segment_name", "unspsc_code",
                            "unspsc_method"]].drop_duplicates("po_number")

        total_rows = len(rows)
        log.info(f"  {total_rows:,} unique PO numbers to update in chunks of {CHUNK}")

        for i in range(0, total_rows, CHUNK):
            chunk = rows.iloc[i:i + CHUNK]

            # Build VALUES list: (po_number, seg, seg_name, code, method), ...
            values_sql = ",".join(
                db.conn.cursor().mogrify(
                    "(%s,%s,%s,%s,%s)",
                    (str(r.po_number), str(r.unspsc_segment),
                     str(r.unspsc_segment_name), str(r.unspsc_code),
                     str(r.unspsc_method))
                ).decode()
                for r in chunk.itertuples(index=False)
            )

            try:
                db.execute(f"""
                    UPDATE transactions AS t
                    SET unspsc_segment      = v.seg,
                        unspsc_segment_name = v.seg_name,
                        unspsc_code         = v.code,
                        unspsc_method       = v.method
                    FROM (VALUES {values_sql})
                         AS v(po_number, seg, seg_name, code, method)
                    WHERE t.po_number = v.po_number
                """)
                db.conn.commit()
                updated += len(chunk)
                if updated % 10000 == 0 or updated == total_rows:
                    log.info(f"  {updated:,}/{total_rows:,} rows updated")
            except Exception as e:
                db.conn.rollback()
                log.warning(f"  Chunk {i}–{i+CHUNK} failed: {e}")
                continue

        log.info(f"  Updated {updated:,} transaction rows")

        # Write summary table
        save_unspsc_summary(db, classified)

    log.info("\n  Done. Next: python ml/spend_analytics.py")
    log.info("  The Spend tab will now show UNSPSC breakdown alongside material groups.")


if __name__ == "__main__":
    main()
