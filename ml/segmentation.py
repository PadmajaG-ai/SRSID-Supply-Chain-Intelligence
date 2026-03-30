"""
SRSID  ml/segmentation.py
==========================
Supplier segmentation adapted for Postgres.

Changes from phase3_supplier_segmentation.py:
  - Reads feature matrix from reports/supplier_features.csv
  - Writes segments to Postgres segments table
  - All K-Means, Kraljic, ABC logic unchanged

Run:
    python ml/segmentation.py
"""

import sys, json, logging, warnings
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import silhouette_score

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import ML_CONFIG, PATHS, SPEND_CONFIG
from db.db_client import DBClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/segmentation.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)
REPORT_DIR = PATHS["reports"]
RUN_DATE   = datetime.now()


# ─────────────────────────────────────────────────────────────────────────────
# SEGMENTATION LOGIC  (unchanged from phase3_supplier_segmentation.py)
# ─────────────────────────────────────────────────────────────────────────────

SEGMENT_FEATURES = [
    "composite_risk_score", "profit_impact_score",
    "supply_risk_score", "financial_stability",
    "delivery_performance", "total_annual_spend",
    "transaction_count", "news_sentiment_30d",
]


def build_kraljic(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Building Kraljic matrix...")

    sr = df["supply_risk_score"].fillna(0.5)
    pi = df["profit_impact_score"].fillna(0.5)
    sr_mid, pi_mid = sr.median(), pi.median()

    conditions = [
        (pi >= pi_mid) & (sr >= sr_mid),
        (pi >= pi_mid) & (sr <  sr_mid),
        (pi <  pi_mid) & (sr >= sr_mid),
        (pi <  pi_mid) & (sr <  sr_mid),
    ]
    labels = ["Strategic", "Leverage", "Bottleneck", "Tactical"]
    df["kraljic_segment"] = np.select(conditions, labels, default="Tactical")

    action_map = {
        "Strategic":  "Long-term contracts, dual-sourcing, safety stock",
        "Leverage":   "Competitive tenders, volume discounts, renegotiate",
        "Bottleneck": "Qualify alternatives urgently, reduce sole-source risk",
        "Tactical":   "Automate, e-catalog procurement, simplify orders",
    }
    df["strategic_action"] = df["kraljic_segment"].map(action_map)

    dist = df["kraljic_segment"].value_counts().to_dict()
    log.info(f"  Kraljic distribution: {dist}")
    return df


def build_abc(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Building ABC spend classification...")

    df = df.sort_values("total_annual_spend", ascending=False).reset_index(drop=True)
    df["spend_rank"] = df.index + 1

    total = df["total_annual_spend"].sum()
    df["cum_spend_pct"] = df["total_annual_spend"].cumsum() / total

    df["abc_class"] = "C"
    df.loc[df["cum_spend_pct"] <= SPEND_CONFIG["abc_a_cumulative"], "abc_class"] = "A"
    df.loc[
        (df["cum_spend_pct"] > SPEND_CONFIG["abc_a_cumulative"]) &
        (df["cum_spend_pct"] <= SPEND_CONFIG["abc_b_cumulative"]),
        "abc_class"
    ] = "B"

    dist = df["abc_class"].value_counts().to_dict()
    log.info(f"  ABC distribution: {dist}")
    return df


def build_kmeans(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Building K-Means clusters...")

    feat_cols = [c for c in SEGMENT_FEATURES if c in df.columns]
    X = df[feat_cols].copy()

    imputer = SimpleImputer(strategy="median")
    scaler  = StandardScaler()
    X_clean = scaler.fit_transform(imputer.fit_transform(X))

    k     = ML_CONFIG["kmeans_k"]
    kmeans = KMeans(
        n_clusters=k,
        max_iter=ML_CONFIG["kmeans_max_iter"],
        random_state=ML_CONFIG["random_state"],
        n_init=10,
    )
    df["cluster_id"] = kmeans.fit_predict(X_clean)

    # Try to compute silhouette (needs ≥2 clusters populated)
    try:
        sil = silhouette_score(X_clean, df["cluster_id"])
        log.info(f"  Silhouette score: {sil:.3f}")
    except Exception:
        pass

    # Label clusters by avg risk + spend
    cluster_stats = df.groupby("cluster_id").agg(
        avg_risk=("composite_risk_score", "mean"),
        avg_spend=("total_annual_spend",  "mean"),
    )
    def label_cluster(row):
        if row.avg_risk > 0.6:  return "High Risk"
        if row.avg_spend > df["total_annual_spend"].quantile(0.75): return "High Spend"
        if row.avg_risk < 0.3:  return "Low Risk"
        return "Medium Risk"

    cluster_labels = cluster_stats.apply(label_cluster, axis=1).to_dict()
    df["cluster_label"] = df["cluster_id"].map(cluster_labels)

    dist = df["cluster_label"].value_counts().to_dict()
    log.info(f"  Cluster distribution: {dist}")
    return df


def build_risk_spend_quadrant(df: pd.DataFrame) -> pd.DataFrame:
    """Simple 4-quadrant: High/Low Risk × High/Low Spend."""
    med_spend = df["total_annual_spend"].median()
    med_risk  = df["composite_risk_score"].median()

    def quadrant(row):
        hi_risk  = row["composite_risk_score"] >= med_risk
        hi_spend = row["total_annual_spend"]    >= med_spend
        if hi_risk  and hi_spend:  return "Watch Closely (High Risk, High Spend)"
        if hi_risk  and not hi_spend: return "Monitor (High Risk, Low Spend)"
        if not hi_risk and hi_spend:  return "Leverage (Low Risk, High Spend)"
        return "Routine (Low Risk, Low Spend)"

    df["risk_spend_quadrant"] = df.apply(quadrant, axis=1)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# WRITE TO POSTGRES
# ─────────────────────────────────────────────────────────────────────────────

def write_segments_to_postgres(df: pd.DataFrame, db: DBClient):
    log.info("Writing segments to Postgres...")

    seg_cols = [
        "vendor_id", "supplier_name",
        "kraljic_segment", "supply_risk_score", "profit_impact_score",
        "cluster_id", "cluster_label",
        "abc_class", "spend_rank",
        "risk_spend_quadrant", "strategic_action",
    ]
    seg_df = df[[c for c in seg_cols if c in df.columns]].copy()
    seg_df["run_date"] = RUN_DATE

    rows = db.bulk_insert_df(seg_df, "segments", if_exists="append")
    log.info(f"  Inserted {rows:,} rows into segments")

    # Save CSV for dashboard / chatbot
    seg_df.to_csv(REPORT_DIR / "supplier_segments.csv", index=False)

    # Save summary JSON
    summary = {
        "run_date": RUN_DATE.isoformat(),
        "total_vendors": len(df),
        "kraljic_distribution": df["kraljic_segment"].value_counts().to_dict()
                                 if "kraljic_segment" in df.columns else {},
        "abc_distribution": df["abc_class"].value_counts().to_dict()
                             if "abc_class" in df.columns else {},
        "cluster_distribution": df["cluster_label"].value_counts().to_dict()
                                  if "cluster_label" in df.columns else {},
    }
    with open(REPORT_DIR / "segmentation_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("SRSID Supplier Segmentation (Postgres)")
    log.info("=" * 60)

    # Load feature matrix
    path = REPORT_DIR / "supplier_features.csv"
    if not path.exists():
        log.error(f"Feature matrix not found. Run ml/features.py first.")
        sys.exit(1)

    df = pd.read_csv(path)
    log.info(f"Loaded {len(df):,} vendors from feature matrix")

    # Fill required columns
    for col, default in [
        ("total_annual_spend", 500_000),
        ("supply_risk_score", 0.5),
        ("profit_impact_score", 0.5),
        ("composite_risk_score", 0.5),
    ]:
        if col not in df.columns:
            df[col] = default

    # Build segmentation
    df = build_kraljic(df)
    df = build_abc(df)
    df = build_kmeans(df)
    df = build_risk_spend_quadrant(df)

    # Write to Postgres
    with DBClient() as db:
        summary = write_segments_to_postgres(df, db)

    log.info("\n" + "=" * 60)
    log.info("SEGMENTATION COMPLETE")
    log.info("=" * 60)
    log.info(f"  Vendors segmented: {summary['total_vendors']:,}")
    log.info(f"  Kraljic: {summary['kraljic_distribution']}")
    log.info(f"  ABC:     {summary['abc_distribution']}")
    log.info("\n  Next: python ml/explainability.py")


if __name__ == "__main__":
    main()
