"""
SRSID  ml/recommendations.py
==============================
Alternative supplier finder + anomaly detection, adapted for Postgres.

Module A — Recommendation Engine:
  For each High/Medium-risk vendor, find top-3 similar vendors
  in the same industry from a different country.
  Algorithm: cosine similarity on feature vectors.

Module B — Anomaly Detection:
  Isolation Forest + Z-score + rule-based flags.
  Flags vendors with unusual risk/spend/delivery patterns.

Changes from phase3_recommendation_anomaly.py:
  - Reads feature matrix from Postgres (vendors table)
  - Writes results to vendor_alternatives + vendor_anomalies tables
  - All ML logic identical

Run:
    python ml/recommendations.py
    python ml/recommendations.py --module recommendations
    python ml/recommendations.py --module anomalies
"""

import sys, json, logging, argparse, warnings
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.ensemble import IsolationForest
from scipy import stats

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PATHS, RISK_THRESHOLDS
from db.db_client import DBClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/recommendations.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

REPORT_DIR         = PATHS["reports"]
TOP_N_ALTERNATIVES = 3
RUN_DATE           = datetime.now()

SIMILARITY_FEATURES = [
    "supply_risk_score", "profit_impact_score",
    "financial_stability", "delivery_performance",
    "performance_composite", "industry_risk_numeric",
    "total_annual_spend", "geo_risk_numeric",
]

ANOMALY_FEATURES = [
    "composite_risk_score", "disruption_count_30d",
    "financial_stability", "delivery_performance",
    "total_annual_spend", "spend_pct_of_portfolio",
    "otif_rate", "avg_delay_days",
    "news_sentiment_30d",
]


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADER
# ─────────────────────────────────────────────────────────────────────────────

def load_features(db: DBClient) -> pd.DataFrame:
    """Load full vendor feature set from Postgres."""

    # Try feature CSV first (has all computed ML features)
    feat_path = REPORT_DIR / "supplier_features.csv"
    if feat_path.exists():
        df = pd.read_csv(feat_path)
        log.info(f"Loaded feature matrix CSV: {len(df):,} vendors")
        return df

    # Fall back to vendors table
    log.info("Feature CSV not found — loading from vendors table")
    df = db.fetch_df("""
        SELECT
            vendor_id, supplier_name,
            country_code, industry_category,
            composite_risk_score, supply_risk_score,
            financial_stability, delivery_performance,
            total_annual_spend, spend_pct_of_portfolio,
            otif_rate, avg_delay_days,
            news_sentiment_30d, disruption_count_30d,
            geo_risk, risk_label
        FROM vendors
        WHERE is_active = TRUE
    """)
    log.info(f"Loaded {len(df):,} vendors from Postgres")
    return df


def build_feature_matrix(df: pd.DataFrame, features: list) -> np.ndarray:
    """Scale + impute feature matrix for ML."""
    avail = [c for c in features if c in df.columns]
    missing = [c for c in features if c not in df.columns]
    if missing:
        log.debug(f"  Missing features (using 0): {missing}")
        for c in missing:
            df[c] = 0.0

    X = df[avail].apply(pd.to_numeric, errors="coerce")
    imp    = SimpleImputer(strategy="median")
    scaler = StandardScaler()
    return scaler.fit_transform(imp.fit_transform(X))


# ─────────────────────────────────────────────────────────────────────────────
# MODULE A — RECOMMENDATION ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def _recommendation_reason(src: pd.Series, alt: pd.Series,
                            df: pd.DataFrame) -> str:
    """Generate plain-English reason for recommending this alternative."""
    parts = []

    src_risk = float(src.get("composite_risk_score", 0.5))
    alt_risk = float(alt.get("composite_risk_score", 0.5))
    if alt_risk < src_risk:
        parts.append(f"Lower risk ({alt_risk:.2f} vs {src_risk:.2f})")

    src_c = str(src.get("country_code",""))
    alt_c = str(alt.get("country_code",""))
    if src_c != alt_c:
        parts.append(f"Geographic diversification ({alt_c})")

    if float(alt.get("delivery_performance", 0)) > float(src.get("delivery_performance", 0)):
        parts.append("Better delivery performance")

    if float(alt.get("financial_stability", 0)) > float(src.get("financial_stability", 0)):
        parts.append("Higher financial stability")

    return "; ".join(parts) if parts else "Similar capability profile"


def find_alternatives(df: pd.DataFrame,
                       X_sim: np.ndarray) -> pd.DataFrame:
    """
    For each High/Medium-risk vendor find TOP_N similar vendors:
      - Same industry category
      - Different country (geo diversification)
      - Lower risk score preferred
    """
    log.info("Computing cosine similarities for alternative recommendations...")

    sim_matrix = cosine_similarity(X_sim)

    # Which vendors need alternatives
    risk_threshold = RISK_THRESHOLDS["medium"]
    if "composite_risk_score" in df.columns:
        needs_alt = df["composite_risk_score"] >= risk_threshold
    elif "risk_label" in df.columns:
        needs_alt = df["risk_label"].isin(["High", "Medium"])
    else:
        needs_alt = pd.Series([True] * len(df))

    n_at_risk = needs_alt.sum()
    log.info(f"  Finding alternatives for {n_at_risk:,} at-risk vendors...")

    records = []
    df = df.reset_index(drop=True)

    for i, row in df.iterrows():
        if not needs_alt.iloc[i]:
            continue

        scores = sim_matrix[i].copy()
        scores[i] = -1  # exclude self

        # Prefer same industry
        ind_col = "industry_category" if "industry_category" in df.columns else None
        if ind_col:
            own_ind = str(row.get(ind_col, ""))
            ind_match = df[ind_col].astype(str).apply(
                lambda x: own_ind.split("/")[0].lower() in x.lower()
                          if own_ind else True
            ).values
            # Bonus for same industry, penalty for completely different
            scores = np.where(ind_match, scores * 1.2, scores * 0.8)

        # Prefer different country (geo diversification)
        ctry_col = "country_code" if "country_code" in df.columns else None
        if ctry_col:
            own_ctry = str(row.get(ctry_col, ""))
            diff_ctry = (df[ctry_col].astype(str) != own_ctry).values
            scores = np.where(diff_ctry, scores * 1.1, scores)

        # Prefer lower-risk alternatives
        if "composite_risk_score" in df.columns:
            own_r = float(row.get("composite_risk_score", 0.5))
            lower = (df["composite_risk_score"].fillna(0.5) < own_r).values
            scores = np.where(lower, scores * 1.15, scores)

        top_idx = np.argsort(scores)[::-1][:TOP_N_ALTERNATIVES]

        for rank, alt_idx in enumerate(top_idx, 1):
            if scores[alt_idx] <= 0:
                continue
            alt = df.iloc[alt_idx]
            records.append({
                "vendor_id":            str(row.get("vendor_id", "")),
                "supplier_name":        str(row.get("supplier_name", "")),
                "risk_score":           float(row.get("composite_risk_score", 0)),
                "risk_tier":            str(row.get("risk_label", "Unknown")),
                "alt_vendor_id":        str(alt.get("vendor_id", "")),
                "alt_supplier_name":    str(alt.get("supplier_name", "")),
                "alt_risk_score":       float(alt.get("composite_risk_score", 0)),
                "alt_risk_tier":        str(alt.get("risk_label", "Unknown")),
                "alt_country":          str(alt.get("country_code", "")),
                "alt_industry":         str(alt.get("industry_category", "")),
                "alternative_rank":     rank,
                "similarity_score":     round(float(sim_matrix[i, alt_idx]), 4),
                "recommendation_reason":_recommendation_reason(row, alt, df),
                "run_date":             RUN_DATE,
            })

    result = pd.DataFrame(records)
    log.info(f"  Generated {len(result):,} recommendations for "
             f"{result['supplier_name'].nunique():,} vendors")
    return result


def write_alternatives(df: pd.DataFrame, db: DBClient):
    """Write alternative recommendations to Postgres."""
    if df.empty:
        log.warning("No alternatives to write")
        return

    # Ensure table exists
    db.execute("""
        CREATE TABLE IF NOT EXISTS vendor_alternatives (
            id SERIAL PRIMARY KEY,
            vendor_id VARCHAR(20),
            supplier_name VARCHAR(500),
            risk_score FLOAT,
            risk_tier VARCHAR(20),
            alt_vendor_id VARCHAR(20),
            alt_supplier_name VARCHAR(500),
            alt_risk_score FLOAT,
            alt_risk_tier VARCHAR(20),
            alt_country VARCHAR(10),
            alt_industry VARCHAR(200),
            alternative_rank INT,
            similarity_score FLOAT,
            recommendation_reason TEXT,
            run_date TIMESTAMP DEFAULT NOW()
        )
    """)

    # Truncate old run and insert fresh
    db.execute("TRUNCATE TABLE vendor_alternatives RESTART IDENTITY")
    rows = db.bulk_insert_df(df, "vendor_alternatives", if_exists="append")
    log.info(f"  Wrote {rows:,} alternative recommendations to Postgres")

    # Save CSV
    df.to_csv(REPORT_DIR / "alternative_suppliers.csv", index=False)
    log.info(f"  Saved alternative_suppliers.csv")


# ─────────────────────────────────────────────────────────────────────────────
# MODULE B — ANOMALY DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def run_isolation_forest(df: pd.DataFrame, X: np.ndarray) -> pd.DataFrame:
    """Isolation Forest — flags statistically unusual vendors."""
    log.info("Running Isolation Forest...")
    iso = IsolationForest(
        n_estimators=200, contamination=0.1,
        random_state=42, n_jobs=-1,
    )
    df = df.copy()
    df["anomaly_if_flag"]  = (iso.fit_predict(X) == -1)
    df["anomaly_if_score"] = (-iso.score_samples(X)).round(4)

    n = df["anomaly_if_flag"].sum()
    log.info(f"  Isolation Forest: {n:,} anomalies ({n/len(df)*100:.1f}%)")
    return df


def run_zscore(df: pd.DataFrame) -> pd.DataFrame:
    """Z-score outlier detection — flags vendors >3 std from mean."""
    log.info("Running Z-score detection...")
    avail = [c for c in ANOMALY_FEATURES if c in df.columns]
    X     = df[avail].apply(pd.to_numeric, errors="coerce")
    imp   = SimpleImputer(strategy="median")
    X_imp = pd.DataFrame(imp.fit_transform(X), columns=avail)

    z_scores = X_imp.apply(lambda col: np.abs(stats.zscore(col, nan_policy="omit")))
    df = df.copy()
    df["max_zscore"]           = z_scores.max(axis=1).round(4)
    df["zscore_feature"]       = z_scores.idxmax(axis=1)
    df["anomaly_zscore_flag"]  = (df["max_zscore"] > 3.0)

    n = df["anomaly_zscore_flag"].sum()
    log.info(f"  Z-score: {n:,} outliers (|z| > 3)")
    return df


def run_rule_based(df: pd.DataFrame) -> pd.DataFrame:
    """Business rule flags for procurement-specific anomalies."""
    log.info("Running rule-based anomaly detection...")

    risk_p75  = df.get("composite_risk_score", pd.Series(0.5)).quantile(0.75)
    spend_p90 = df.get("spend_pct_of_portfolio", pd.Series(0)).quantile(0.90)
    fin_p10   = df.get("financial_stability", pd.Series(60)).quantile(0.10)
    deliv_p95 = df.get("delivery_performance", pd.Series(80)).quantile(0.95)
    news_real = df.get("disruption_count_30d", pd.Series(0)).sum() > 0

    flags, reasons = [], []
    for _, row in df.iterrows():
        row_flags = []
        risk      = float(row.get("composite_risk_score", 0))
        spend_pct = float(row.get("spend_pct_of_portfolio", 0))
        fin_stab  = float(row.get("financial_stability", 60))
        disrupts  = float(row.get("disruption_count_30d", 0))
        delivery  = float(row.get("delivery_performance", 80))

        # Rule 1: High risk + top spend concentration
        if risk >= risk_p75 and spend_pct >= spend_p90:
            row_flags.append("High risk + top-10% spend concentration")

        # Rule 2: News disruptions but low modelled risk (monitoring gap)
        if news_real and disrupts > 2 and risk < risk_p75:
            row_flags.append(f"Active news disruptions ({disrupts:.0f}) but low risk score")

        # Rule 3: Financial stability critically low
        if fin_stab <= fin_p10 and fin_stab < 40:
            row_flags.append(f"Financial stability critically low ({fin_stab:.0f}/100)")

        # Rule 4: Excellent delivery but high risk (data inconsistency)
        if delivery >= deliv_p95 and risk >= risk_p75:
            row_flags.append("Top delivery performance inconsistent with high risk score")

        # Rule 5: Zero OTIF recorded (no delivery data — blind spot)
        if row.get("otif_rate") is None or float(row.get("otif_rate") or 0) == 0:
            if risk >= risk_p75:
                row_flags.append("No OTIF data for high-risk vendor (monitoring gap)")

        flags.append(bool(row_flags))
        reasons.append("; ".join(row_flags))

    df = df.copy()
    df["rule_based_flag"]   = flags
    df["rule_based_reason"] = reasons
    n = sum(flags)
    log.info(f"  Rule-based: {n:,} flags ({n/len(df)*100:.1f}%)")
    return df


def build_anomaly_report(df: pd.DataFrame) -> pd.DataFrame:
    """Compile final anomaly report."""
    df = df.copy()
    df["total_anomaly_flags"] = (
        df["anomaly_if_flag"].astype(int) +
        df.get("anomaly_zscore_flag", pd.Series(False, index=df.index)).astype(int) +
        df.get("rule_based_flag",     pd.Series(False, index=df.index)).astype(int)
    )
    df["is_anomalous"] = df["total_anomaly_flags"] >= 1

    n_total = df["is_anomalous"].sum()
    log.info(f"  Total anomalous vendors: {n_total:,} ({n_total/len(df)*100:.1f}%)")
    return df


def write_anomalies(df: pd.DataFrame, db: DBClient):
    """Write anomaly results to Postgres."""

    db.execute("""
        CREATE TABLE IF NOT EXISTS vendor_anomalies (
            id SERIAL PRIMARY KEY,
            vendor_id VARCHAR(20),
            supplier_name VARCHAR(500),
            is_anomalous BOOLEAN DEFAULT FALSE,
            total_anomaly_flags INT DEFAULT 0,
            anomaly_if_score FLOAT,
            anomaly_if_flag BOOLEAN DEFAULT FALSE,
            max_zscore FLOAT,
            zscore_feature VARCHAR(100),
            anomaly_zscore_flag BOOLEAN DEFAULT FALSE,
            rule_based_flag BOOLEAN DEFAULT FALSE,
            rule_based_reason TEXT,
            composite_risk_score FLOAT,
            total_annual_spend FLOAT,
            financial_stability FLOAT,
            run_date TIMESTAMP DEFAULT NOW()
        )
    """)
    db.execute("TRUNCATE TABLE vendor_anomalies RESTART IDENTITY")

    out_cols = [
        "vendor_id", "supplier_name",
        "is_anomalous", "total_anomaly_flags",
        "anomaly_if_score", "anomaly_if_flag",
        "max_zscore", "zscore_feature", "anomaly_zscore_flag",
        "rule_based_flag", "rule_based_reason",
        "composite_risk_score", "total_annual_spend", "financial_stability",
    ]
    out_df = df[[c for c in out_cols if c in df.columns]].copy()
    out_df["run_date"] = RUN_DATE

    rows = db.bulk_insert_df(out_df, "vendor_anomalies", if_exists="append")
    log.info(f"  Wrote {rows:,} anomaly records to Postgres")

    # Save CSV + summary JSON
    out_df.to_csv(REPORT_DIR / "anomaly_report.csv", index=False)

    summary = {
        "run_date":             RUN_DATE.isoformat(),
        "total_vendors":        len(df),
        "total_anomalous":      int(df["is_anomalous"].sum()),
        "isolation_forest":     int(df["anomaly_if_flag"].sum()),
        "zscore_outliers":      int(df.get("anomaly_zscore_flag", pd.Series(False)).sum()),
        "rule_based_flags":     int(df.get("rule_based_flag",     pd.Series(False)).sum()),
        "top_anomalies": (
            df[df["is_anomalous"]]
            .sort_values("anomaly_if_score", ascending=False)
            .head(5)[["supplier_name","total_anomaly_flags","rule_based_reason"]]
            .to_dict("records")
        ),
    }
    with open(REPORT_DIR / "anomaly_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    log.info(f"  Saved anomaly_report.csv + anomaly_summary.json")
    return summary


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="SRSID Recommendations + Anomaly Detection")
    parser.add_argument("--module", choices=["recommendations","anomalies","all"],
                        default="all",
                        help="Which module to run (default: all)")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SRSID Recommendations + Anomaly Detection (Postgres)")
    log.info("=" * 60)

    with DBClient() as db:
        df = load_features(db)

        if df.empty:
            log.error("No feature data. Run ml/features.py first.")
            sys.exit(1)

        df = df.reset_index(drop=True)

        # ── Module A: Recommendations ──────────────────────────────────────
        if args.module in ("recommendations", "all"):
            log.info("\n--- MODULE A: ALTERNATIVE SUPPLIERS ---")
            X_sim    = build_feature_matrix(df, SIMILARITY_FEATURES)
            alt_df   = find_alternatives(df, X_sim)
            write_alternatives(alt_df, db)

        # ── Module B: Anomaly Detection ────────────────────────────────────
        if args.module in ("anomalies", "all"):
            log.info("\n--- MODULE B: ANOMALY DETECTION ---")
            X_anom = build_feature_matrix(df, ANOMALY_FEATURES)
            df     = run_isolation_forest(df, X_anom)
            df     = run_zscore(df)
            df     = run_rule_based(df)
            df     = build_anomaly_report(df)
            summary = write_anomalies(df, db)

    log.info("\n" + "=" * 60)
    log.info("RECOMMENDATIONS + ANOMALY DETECTION COMPLETE")
    log.info("=" * 60)
    if args.module in ("recommendations", "all"):
        log.info(f"  Alternatives : {len(alt_df):,} recommendations")
    if args.module in ("anomalies", "all"):
        log.info(f"  Anomalies    : {summary['total_anomalous']:,} vendors flagged")
    log.info("\n  Next: streamlit run app/dashboard.py")


if __name__ == "__main__":
    main()
