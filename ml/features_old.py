"""
SRSID  ml/features.py
======================
Feature engineering adapted for Postgres.

Changes from phase3_feature_engineering.py:
  - Reads from Postgres (vendors, transactions, vendor_news)
  - Writes feature matrix back to Postgres (updates vendors table)
  - All ML feature logic kept identical

Run:
    python ml/features.py
"""

import sys, json, logging, warnings
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.impute import SimpleImputer

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import RISK_THRESHOLDS, SPEND_CONFIG, PATHS
from db.db_client import DBClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/features.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

OUTPUT_DIR = PATHS["reports"]

# ── Feature columns that go into ML models ────────────────────────────────────
NUMERIC_FEATURES = [
    "financial_stability", "delivery_performance",
    "supply_risk_score", "profit_impact_score",
    "total_annual_spend", "transaction_count",
    "spend_pct_of_portfolio", "geo_risk_numeric",
    "industry_risk_numeric", "news_sentiment_30d",
    "disruption_count_30d", "otif_rate",
    "avg_delay_days", "performance_composite",
    "composite_risk_score",
]

SPEND_CONCENTRATION_THRESHOLD = SPEND_CONFIG["top1_supplier_max"]


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADERS  (from Postgres)
# ─────────────────────────────────────────────────────────────────────────────

def load_from_postgres(db: DBClient) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load vendors, transactions, and news signals from Postgres."""

    log.info("Loading data from Postgres...")

    # Vendor master — all active vendors
    vendors = db.fetch_df("""
        SELECT
            vendor_id, supplier_name,
            country_code, industry_category,
            financial_stability, delivery_performance,
            supply_risk_score, composite_risk_score,
            total_annual_spend, transaction_count,
            avg_order_value, spend_pct_of_portfolio,
            otif_rate, avg_delay_days,
            geo_risk, risk_label
        FROM vendors
        WHERE is_active = TRUE
        ORDER BY total_annual_spend DESC NULLS LAST
    """)
    log.info(f"  Vendors: {len(vendors):,}")

    # Transactions — aggregated spend per vendor
    # First check if transactions table has data and diagnose vendor_id format
    tx_count = db.scalar("SELECT COUNT(*) FROM transactions")
    tx_vendors = db.scalar("SELECT COUNT(DISTINCT vendor_id) FROM transactions WHERE vendor_id IS NOT NULL")
    log.info(f"  Transactions in DB: {tx_count:,} rows, {tx_vendors:,} distinct vendors")

    if tx_count and tx_count > 0:
        # Sample vendor_ids from both tables to diagnose format mismatch
        sample_tx  = db.fetch_df("SELECT DISTINCT vendor_id FROM transactions WHERE vendor_id IS NOT NULL LIMIT 5")
        sample_v   = db.fetch_df("SELECT DISTINCT vendor_id FROM vendors LIMIT 5")
        log.info(f"  Sample tx vendor_ids : {sample_tx['vendor_id'].tolist()}")
        log.info(f"  Sample vendor ids    : {sample_v['vendor_id'].tolist()}")

    transactions = db.fetch_df("""
        SELECT
            vendor_id, supplier_name,
            SUM(transaction_amount)             AS total_spend,
            COUNT(*)                            AS tx_count,
            AVG(transaction_amount)             AS avg_tx_value,
            STDDEV(transaction_amount)          AS spend_std,
            MIN(po_date)                        AS first_po,
            MAX(po_date)                        AS last_po,
            COUNT(DISTINCT year_quarter)        AS active_quarters
        FROM transactions
        GROUP BY vendor_id, supplier_name
    """)
    log.info(f"  Transaction aggregates: {len(transactions):,} vendors")

    # News signals — sentiment and disruption counts per vendor
    news = db.fetch_df("""
        SELECT
            vendor_id,
            AVG(sentiment_score)                        AS avg_sentiment,
            COUNT(*)                                    AS article_count,
            COUNT(*) FILTER (WHERE disruption_flag)     AS disruption_count,
            MAX(published_at)                           AS latest_news
        FROM vendor_news
        WHERE published_at >= NOW() - INTERVAL '30 days'
        GROUP BY vendor_id
    """)
    log.info(f"  News signals: {len(news):,} vendors with recent news")

    return vendors, transactions, news


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE BUILDERS  (logic unchanged from phase3_feature_engineering.py)
# ─────────────────────────────────────────────────────────────────────────────

def build_performance_features(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Building performance features...")

    # Fill missing performance scores with medians
    for col in ["financial_stability", "delivery_performance"]:
        if col in df.columns:
            median = df[col].median()
            df[col] = df[col].fillna(median if pd.notna(median) else 60.0)
        else:
            df[col] = 60.0

    # Historical risk numeric (from risk_label)
    risk_map = {"high": 0.9, "medium": 0.5, "low": 0.1}
    if "risk_label" in df.columns:
        df["historical_risk_numeric"] = (
            df["risk_label"].str.lower().str.strip()
            .map(risk_map).fillna(0.5)
        )
    else:
        df["historical_risk_numeric"] = 0.5

    # Composite performance (0–1)
    df["performance_composite"] = (
        df["financial_stability"].fillna(50)  * 0.35 +
        df["delivery_performance"].fillna(50) * 0.40 +
        (1 - df["historical_risk_numeric"])   * 100 * 0.25
    ) / 100

    df["performance_tier"] = pd.cut(
        df["performance_composite"],
        bins=[0, 0.4, 0.7, 1.01],
        labels=["Poor", "Adequate", "Strong"],
    )
    return df


def build_spend_features(df: pd.DataFrame, tx: pd.DataFrame) -> pd.DataFrame:
    log.info("Building spend features...")

    if not tx.empty and "vendor_id" in tx.columns:
        tx = tx.rename(columns={
            "total_spend": "total_annual_spend",
            "tx_count":    "transaction_count",
            "avg_tx_value":"avg_transaction_value",
        })
        spend_cols = ["vendor_id","total_annual_spend","transaction_count",
                      "avg_transaction_value","spend_std",
                      "first_po","last_po","active_quarters"]
        tx_sub = tx[[c for c in spend_cols if c in tx.columns]]
        df = df.merge(tx_sub, on="vendor_id", how="left",
                      suffixes=("", "_tx"))
        for col in ["total_annual_spend","transaction_count"]:
            tx_col = f"{col}_tx"
            if tx_col in df.columns:
                df[col] = df[col].combine_first(df[tx_col])
                df.drop(columns=[tx_col], inplace=True)

        coverage = tx_sub["vendor_id"].nunique()
        log.info(f"  Transaction coverage: {coverage:,} of {len(df):,} vendors "
                 f"({coverage/len(df)*100:.1f}%)")
        if coverage < len(df) * 0.5:
            log.warning(f"  ⚠️  Low transaction coverage ({coverage} vendors). "
                        f"Re-run: python ingestion/sap_loader.py --table transactions")

    # Fill defaults for vendors with no transaction data
    df["total_annual_spend"]    = pd.to_numeric(
        df.get("total_annual_spend", pd.Series(dtype=float)), errors="coerce"
    ).fillna(500_000)
    df["transaction_count"]     = pd.to_numeric(
        df.get("transaction_count", pd.Series(dtype=float)), errors="coerce"
    ).fillna(10).astype(int)
    df["avg_transaction_value"] = df.get(
        "avg_transaction_value",
        df["total_annual_spend"] / df["transaction_count"]
    ).fillna(50_000)

    # Spend concentration
    total = df["total_annual_spend"].sum()
    df["spend_pct_of_portfolio"] = (
        df["total_annual_spend"] / total if total > 0 else 0.0
    )
    df["spend_concentration_flag"] = (
        df["spend_pct_of_portfolio"] > SPEND_CONCENTRATION_THRESHOLD
    ).astype(int)

    if "active_quarters" in df.columns:
        df["spend_trend_numeric"] = (
            df["active_quarters"].fillna(1) / 4
        ).clip(0, 1)
    else:
        df["spend_trend_numeric"] = 0.5

    log_spend = np.log1p(df["total_annual_spend"].clip(0))
    mn, mx = log_spend.min(), log_spend.max()
    df["profit_impact_score"] = (
        (log_spend - mn) / (mx - mn) if mx > mn else pd.Series(0.5, index=df.index)
    )
    return df


def build_news_features(df: pd.DataFrame, news: pd.DataFrame) -> pd.DataFrame:
    log.info("Building news / disruption features...")

    if not news.empty and "vendor_id" in news.columns:
        news_sub = news[["vendor_id","avg_sentiment","article_count",
                          "disruption_count"]].rename(columns={
            "avg_sentiment":   "news_sentiment_30d",
            "disruption_count":"disruption_count_30d",
        })
        df = df.merge(news_sub, on="vendor_id", how="left")

    # Fill missing news signals (vendors with no news = neutral)
    df["news_sentiment_30d"]   = df.get("news_sentiment_30d",
                                         pd.Series(0.0, index=df.index)).fillna(0.0)
    df["disruption_count_30d"] = df.get("disruption_count_30d",
                                         pd.Series(0,   index=df.index)).fillna(0)

    # News risk flag (negative sentiment OR recent disruption)
    df["news_risk_flag"] = (
        (df["news_sentiment_30d"] < -0.2) |
        (df["disruption_count_30d"] > 0)
    ).astype(int)

    # Historical disruption frequency (normalised)
    max_d = df["disruption_count_30d"].max()
    df["disruption_freq_norm"] = (
        df["disruption_count_30d"] / max_d if max_d > 0 else 0
    )
    return df


def build_geo_industry_features(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Building geo + industry risk features...")

    from config import GEO_RISK, GEO_RISK_SCORES, INDUSTRY_RISK

    # Geo risk numeric
    if "country_code" in df.columns:
        country_to_tier = {c: tier for tier, codes in GEO_RISK.items()
                           for c in codes}
        df["geo_risk_tier"]    = df["country_code"].map(country_to_tier).fillna("Medium")
        df["geo_risk_numeric"] = df["geo_risk_tier"].map(GEO_RISK_SCORES).fillna(0.5)
    else:
        df["geo_risk_tier"]    = "Medium"
        df["geo_risk_numeric"] = 0.5

    # Industry risk numeric
    if "industry_category" in df.columns:
        df["industry_risk_numeric"] = (
            df["industry_category"].map(INDUSTRY_RISK).fillna(0.5)
        )
    else:
        df["industry_risk_numeric"] = 0.5

    return df


def build_delivery_features(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Building delivery features...")

    df["otif_rate"]      = df.get("otif_rate",
                                   pd.Series(0.75, index=df.index)).fillna(0.75)
    df["avg_delay_days"] = df.get("avg_delay_days",
                                   pd.Series(0.0, index=df.index)).fillna(0.0)
    df["delivery_risk_numeric"] = (1 - df["otif_rate"]).clip(0, 1)

    # Significant delay flag (>5 days average)
    from config import DELIVERY_CONFIG
    df["significant_delay_flag"] = (
        df["avg_delay_days"] > DELIVERY_CONFIG["delay_threshold_days"]
    ).astype(int)

    return df


def build_composite_risk(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Building composite risk score + labels...")

    w = RISK_THRESHOLDS["weights"]
    df["composite_risk_score"] = (
        df["geo_risk_numeric"].fillna(0.4)          * w["geo_risk"] +
        df["industry_risk_numeric"].fillna(0.5)     * w["industry_risk"] +
        df["delivery_risk_numeric"].fillna(0.25)    * w["delivery_risk"] +
        df["spend_concentration_flag"].fillna(0)    * w["concentration_risk"]
    ).clip(0, 1).round(4)

    # Incorporate news risk (small boost if negative news)
    if "news_risk_flag" in df.columns:
        df["composite_risk_score"] = (
            df["composite_risk_score"] * 0.85 +
            df["disruption_freq_norm"].fillna(0) * 0.15
        ).clip(0, 1).round(4)

    # Supply risk score
    df["supply_risk_score"] = df["composite_risk_score"]

    # Risk label
    high_t, med_t = RISK_THRESHOLDS["high"], RISK_THRESHOLDS["medium"]
    df["risk_label_3class"] = pd.cut(
        df["composite_risk_score"],
        bins=[-0.001, med_t, high_t, 1.001],
        labels=["Low", "Medium", "High"],
    ).astype(str)

    dist = df["risk_label_3class"].value_counts().to_dict()
    log.info(f"  Risk distribution: {dist}")
    return df


def build_kraljic_features(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Building Kraljic quadrant features...")

    sr_mid = df["supply_risk_score"].median()
    pi_mid = df["profit_impact_score"].median()

    conditions = [
        (df["profit_impact_score"] >= pi_mid) & (df["supply_risk_score"] >= sr_mid),
        (df["profit_impact_score"] >= pi_mid) & (df["supply_risk_score"] <  sr_mid),
        (df["profit_impact_score"] <  pi_mid) & (df["supply_risk_score"] >= sr_mid),
        (df["profit_impact_score"] <  pi_mid) & (df["supply_risk_score"] <  pi_mid),
    ]
    labels = ["Strategic", "Leverage", "Bottleneck", "Tactical"]
    df["kraljic_quadrant"] = np.select(conditions, labels, default="Tactical")

    dist = pd.Series(df["kraljic_quadrant"]).value_counts().to_dict()
    log.info(f"  Kraljic distribution: {dist}")
    return df


def normalise_features(df: pd.DataFrame) -> pd.DataFrame:
    """MinMax-scale numeric features for ML models."""
    num_cols = [c for c in NUMERIC_FEATURES if c in df.columns]
    scaler   = MinMaxScaler()
    df_norm  = df.copy()
    valid    = df[num_cols].select_dtypes(include=[np.number])
    imputer  = SimpleImputer(strategy="median")
    df_norm[valid.columns] = scaler.fit_transform(
        imputer.fit_transform(valid)
    )
    return df_norm


# ─────────────────────────────────────────────────────────────────────────────
# WRITE BACK TO POSTGRES
# ─────────────────────────────────────────────────────────────────────────────

def write_features_to_postgres(df: pd.DataFrame, db: DBClient):
    """
    Update the vendors table with computed feature scores.
    Only writes columns that actually exist in the vendors table.
    Also save full feature matrix to CSV for ML model training.
    """
    log.info("Writing features back to Postgres...")

    # Map risk_label_3class → risk_label
    if "risk_label_3class" in df.columns:
        df["risk_label"] = df["risk_label_3class"]

    # Desired columns to update
    desired_cols = [
        "vendor_id",
        "composite_risk_score", "supply_risk_score",
        "profit_impact_score",  "risk_label",
        "financial_stability",  "delivery_performance",
        "otif_rate",            "avg_delay_days",
        "news_sentiment_30d",   "disruption_count_30d",
        "total_annual_spend",   "transaction_count",
        "spend_pct_of_portfolio",
    ]

    # Add any missing columns to vendors table dynamically
    col_type_map = {
        "profit_impact_score":    "FLOAT",
        "composite_risk_score":   "FLOAT",
        "supply_risk_score":      "FLOAT",
        "news_sentiment_30d":     "FLOAT",
        "disruption_count_30d":   "INT",
        "spend_pct_of_portfolio": "FLOAT",
        "otif_rate":              "FLOAT",
        "avg_delay_days":         "FLOAT",
        "transaction_count":      "INT",
        "total_annual_spend":     "FLOAT",
    }
    for col, dtype in col_type_map.items():
        if col in desired_cols:
            try:
                db.add_column_if_missing("vendors", col, dtype)
            except Exception as e:
                log.debug(f"  Column check {col}: {e}")

    # Only keep columns present in BOTH df and vendors table
    db_cols = set(db.fetch_df(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'vendors'"
    )["column_name"].tolist())

    update_cols = [c for c in desired_cols if c in df.columns and c in db_cols]
    log.info(f"  Updating {len(update_cols)} columns: {update_cols}")

    update_df = df[update_cols].copy()
    update_df = update_df.drop_duplicates("vendor_id")

    # Use UPDATE only — never INSERT new vendor rows from features
    # This avoids NOT NULL violations on required fields like supplier_name
    updated = 0
    set_cols = [c for c in update_cols if c != "vendor_id"]
    set_clause = ", ".join([f"{c} = %s" for c in set_cols])
    sql = f"UPDATE vendors SET {set_clause} WHERE vendor_id = %s"

    with db.conn.cursor() as cur:
        for _, row in update_df.iterrows():
            vals = tuple(
                None if (isinstance(row[c], float) and np.isnan(row[c]))
                else row[c]
                for c in set_cols
            )
            vals = vals + (str(row["vendor_id"]),)
            cur.execute(sql, vals)
            updated += cur.rowcount
    db.conn.commit()
    log.info(f"  Updated {updated:,} vendor records in Postgres")

    # Save full feature matrix as CSV (used by ML training scripts)
    output_path = OUTPUT_DIR / "supplier_features.csv"
    df.to_csv(output_path, index=False)
    log.info(f"  Saved feature matrix CSV → {output_path}")

    # Save feature summary JSON
    summary = {
        "generated_at":       datetime.now().isoformat(),
        "total_suppliers":    len(df),
        "total_features":     len(df.columns),
        "risk_distribution":  df["risk_label_3class"].value_counts().to_dict()
                              if "risk_label_3class" in df.columns else {},
        "avg_composite_risk": round(float(df["composite_risk_score"].mean()), 4)
                              if "composite_risk_score" in df.columns else 0,
        "high_risk_count":    int((df.get("risk_label_3class","") == "High").sum()),
        "geo_risk_count":     int((df.get("geo_risk_tier","") == "High").sum()),
        "spend_concentrated": int(df.get("spend_concentration_flag", 0).sum()),
        "quadrant_distribution": df["kraljic_quadrant"].value_counts().to_dict()
                                  if "kraljic_quadrant" in df.columns else {},
    }
    with open(OUTPUT_DIR / "feature_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    log.info("  Saved feature_summary.json")

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("SRSID Feature Engineering (Postgres)")
    log.info("=" * 60)

    with DBClient() as db:

        # 1. Load from Postgres
        vendors, transactions, news = load_from_postgres(db)

        if vendors.empty:
            log.error("No vendors in Postgres. Run ingestion/sap_loader.py first.")
            sys.exit(1)

        df = vendors.copy()

        # Ensure no duplicates
        df = df.drop_duplicates(subset=["vendor_id"]).reset_index(drop=True)
        log.info(f"Working with {len(df):,} unique vendors")

        # 2. Build all feature groups
        df = build_performance_features(df)
        df = build_spend_features(df, transactions)
        df = build_news_features(df, news)
        df = build_geo_industry_features(df)
        df = build_delivery_features(df)
        df = build_composite_risk(df)
        df = build_kraljic_features(df)

        # 3. Write results back to Postgres + save CSV
        summary = write_features_to_postgres(df, db)

    log.info("\n" + "=" * 60)
    log.info("FEATURE ENGINEERING COMPLETE")
    log.info("=" * 60)
    log.info(f"  Vendors processed   : {summary['total_suppliers']:,}")
    log.info(f"  Features created    : {summary['total_features']}")
    log.info(f"  Risk distribution   : {summary['risk_distribution']}")
    log.info(f"  Avg composite risk  : {summary['avg_composite_risk']}")
    log.info(f"  Kraljic distribution: {summary['quadrant_distribution']}")
    log.info("\n  Next: python ml/risk_model.py")


if __name__ == "__main__":
    main()
