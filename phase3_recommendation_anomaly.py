"""
Phase 3: Recommendation Engine + Anomaly Detection
=====================================================

MODULE A — RECOMMENDATION ENGINE
  Find top-N alternative suppliers for each at-risk supplier.
  Algorithm: Cosine similarity on feature vectors (collaborative filtering approach).
  Filters: Same industry, different region (geographic diversification).

MODULE B — ANOMALY DETECTION
  Identify suppliers with unusual patterns using:
  - Isolation Forest (unsupervised)
  - Z-score outlier detection
  - Rule-based flags

Inputs:
    - phase3_features/supplier_features.csv
    - phase3_risk_predictions/risk_predictions.csv (optional)

Outputs:
    - phase3_recommendations/alternative_suppliers.csv
    - phase3_anomalies/anomaly_report.csv
    - phase3_recommendations/recommendation_summary.json
    - phase3_anomalies/anomaly_summary.json
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
import logging
import warnings
warnings.filterwarnings("ignore")

from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.ensemble import IsolationForest
from scipy import stats

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("phase3_recommendation_anomaly.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
FEATURES_DIR   = Path("phase3_features")
RISK_PRED_DIR  = Path("phase3_risk_predictions")
RECOM_DIR      = Path("phase3_recommendations")
ANOMALY_DIR    = Path("phase3_anomalies")
RECOM_DIR.mkdir(exist_ok=True)
ANOMALY_DIR.mkdir(exist_ok=True)

# ── Feature sets ──────────────────────────────────────────────────────────────
SIMILARITY_FEATURES = [
    "supply_risk_score", "profit_impact_score",
    "financial_stability", "delivery_performance",
    "performance_composite",
    "industry_risk_score",
    "total_annual_spend",
]

ANOMALY_FEATURES = [
    "composite_risk_score", "disruption_frequency",
    "financial_stability", "delivery_performance",
    "total_annual_spend", "spend_pct_of_portfolio",
    "days_since_last_disruption",
]

TOP_N_ALTERNATIVES = 3


# ─────────────────────────────────────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────────────────────────────────────

def load_data():
    feat_path = FEATURES_DIR / "supplier_features.csv"
    if not feat_path.exists():
        raise FileNotFoundError(f"{feat_path} not found. Run feature engineering first.")
    df = pd.read_csv(feat_path)
    log.info(f"Loaded features: {len(df)} suppliers")

    # Optionally merge risk predictions
    risk_path = RISK_PRED_DIR / "risk_predictions.csv"
    if risk_path.exists():
        risk_df = pd.read_csv(risk_path)
        log.info("Merging risk predictions...")
        name_col_r = next((c for c in risk_df.columns if "name" in c.lower()), None)
        name_col_d = next((c for c in df.columns if "name" in c.lower()), df.columns[0])
        if name_col_r:
            df = df.merge(risk_df[["supplier_name", "risk_tier", "risk_probability"]].rename(
                columns={"supplier_name": name_col_d}),
                on=name_col_d, how="left")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# MODULE A: RECOMMENDATION ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def build_feature_matrix(df: pd.DataFrame, features: list) -> np.ndarray:
    avail = [c for c in features if c in df.columns]
    for c in features:
        if c not in df.columns:
            df[c] = 0.0
    X = df[avail].apply(pd.to_numeric, errors="coerce")
    imp    = SimpleImputer(strategy="median")
    scaler = StandardScaler()
    return scaler.fit_transform(imp.fit_transform(X))


def find_alternatives(df: pd.DataFrame, X_sim: np.ndarray,
                      name_col: str, industry_col: str = None,
                      country_col: str = None) -> pd.DataFrame:
    """
    For each high/medium-risk supplier, find TOP_N_ALTERNATIVES
    similar suppliers in the same industry from different regions.
    Filters:
      - Not the same supplier
      - Same industry (if available)
      - Different country/region (geo diversification)
      - Lower risk (not replacing with equally risky supplier)
    """
    log.info("Computing supplier similarities and alternatives...")

    sim_matrix = cosine_similarity(X_sim)  # (n_suppliers, n_suppliers)

    risk_col = next((c for c in df.columns if "composite_risk" in c.lower()
                     or "risk_probability" in c.lower()), None)

    # Determine which suppliers need alternatives
    if "risk_tier" in df.columns:
        needs_alt = df["risk_tier"].isin(["High", "Medium"])
    elif risk_col:
        needs_alt = df[risk_col] >= 0.35
    else:
        needs_alt = pd.Series([True] * len(df))

    records = []
    for i, row in df.iterrows():
        if not needs_alt.iloc[i]:
            continue

        scores = sim_matrix[i].copy()
        scores[i] = -1  # exclude self

        # Filter: same industry
        if industry_col and industry_col in df.columns:
            own_industry = str(row.get(industry_col, ""))
            mask_industry = df[industry_col].astype(str).apply(
                lambda x: own_industry.split("/")[0].lower() in x.lower()
                         if own_industry else True
            ).values
            scores = np.where(mask_industry, scores, -1)

        # Filter: different country/region
        if country_col and country_col in df.columns:
            own_country = str(row.get(country_col, ""))
            mask_country = df[country_col].astype(str) != own_country
            scores = np.where(mask_country.values, scores, scores * 0.7)

        # Filter: prefer lower-risk alternatives
        if risk_col and risk_col in df.columns:
            own_risk = float(row.get(risk_col, 0.5))
            lower_risk = (df[risk_col].fillna(0.5) < own_risk).values
            scores = np.where(lower_risk, scores * 1.1, scores)

        top_idx = np.argsort(scores)[::-1][:TOP_N_ALTERNATIVES]
        for rank, alt_idx in enumerate(top_idx, 1):
            alt_row = df.iloc[alt_idx]
            records.append({
                "supplier":              row[name_col],
                "supplier_risk_score":   row.get(risk_col, None),
                "supplier_risk_tier":    row.get("risk_tier", "Unknown"),
                "alternative_rank":      rank,
                "alternative_supplier":  alt_row[name_col],
                "similarity_score":      round(float(sim_matrix[i, alt_idx]), 4),
                "alt_risk_score":        alt_row.get(risk_col, None),
                "alt_risk_tier":         alt_row.get("risk_tier", "Unknown"),
                "alt_industry":          alt_row.get(industry_col, "Unknown") if industry_col else "N/A",
                "alt_country":           alt_row.get(country_col, "Unknown") if country_col else "N/A",
                "recommendation_reason": _recommendation_reason(row, alt_row, risk_col, country_col, df),
            })

    result = pd.DataFrame(records)
    log.info(f"Generated {len(result)} alternative recommendations for {result['supplier'].nunique()} suppliers")
    return result


def _recommendation_reason(src_row, alt_row, risk_col, country_col, df) -> str:
    parts = []
    if risk_col and risk_col in df.columns:
        src_r = float(src_row.get(risk_col, 0.5))
        alt_r = float(alt_row.get(risk_col, 0.5))
        if alt_r < src_r:
            parts.append(f"Lower risk ({alt_r:.2f} vs {src_r:.2f})")
    if country_col and country_col in df.columns:
        if src_row.get(country_col) != alt_row.get(country_col):
            parts.append("Geographic diversification")
    if "performance_composite" in df.columns:
        if float(alt_row.get("performance_composite", 0)) > float(src_row.get("performance_composite", 0)):
            parts.append("Better performance score")
    return "; ".join(parts) if parts else "Similar capability profile"


# ─────────────────────────────────────────────────────────────────────────────
# MODULE B: ANOMALY DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def run_isolation_forest(df: pd.DataFrame, X: np.ndarray, contamination=0.1) -> pd.DataFrame:
    log.info("Running Isolation Forest anomaly detection...")
    iso = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
        n_jobs=-1,
    )
    df["anomaly_if"]    = iso.fit_predict(X)    # -1 = anomaly, 1 = normal
    df["anomaly_score"] = -iso.score_samples(X)  # higher = more anomalous

    n_anomalies = (df["anomaly_if"] == -1).sum()
    log.info(f"  Isolation Forest: {n_anomalies} anomalies detected "
             f"({n_anomalies/len(df)*100:.1f}%)")
    return df


def run_zscore_detection(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Running Z-score anomaly detection...")
    avail = [c for c in ANOMALY_FEATURES if c in df.columns]
    X = df[avail].apply(pd.to_numeric, errors="coerce")
    imp = SimpleImputer(strategy="median")
    X_imp = pd.DataFrame(imp.fit_transform(X), columns=avail)

    z_scores = X_imp.apply(lambda col: np.abs(stats.zscore(col, nan_policy="omit")))
    df["max_zscore"]    = z_scores.max(axis=1)
    df["anomaly_zscore"] = (df["max_zscore"] > 3.0).astype(int)
    df["zscore_feature"] = z_scores.idxmax(axis=1)

    n_zscore = df["anomaly_zscore"].sum()
    log.info(f"  Z-score: {n_zscore} outliers detected (|z| > 3)")
    return df


def rule_based_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flag unusual patterns with business rules.
    Only fires when there is genuine data variation to compare against.
    """
    # Check if disruption data is real (not all-zero defaults)
    disruptions_are_real = df.get("disruption_count", pd.Series(0)).sum() > 0

    flags = []
    reasons = []

    risk_p75   = df.get("composite_risk_score", pd.Series(0.5)).quantile(0.75)
    spend_p90  = df.get("spend_pct_of_portfolio", pd.Series(0.01)).quantile(0.90)
    fin_p10    = df.get("financial_stability", pd.Series(50)).quantile(0.10)
    deliv_p95  = df.get("delivery_performance", pd.Series(80)).quantile(0.95)

    for _, row in df.iterrows():
        row_flags = []
        risk      = float(row.get("composite_risk_score", 0))
        spend_pct = float(row.get("spend_pct_of_portfolio", 0))
        fin_stab  = float(row.get("financial_stability", 50))
        disruptions = float(row.get("disruption_count", 0))
        delivery  = float(row.get("delivery_performance", 80))

        # Rule 1: high risk AND top-10% spend concentration
        if risk >= risk_p75 and spend_pct >= spend_p90:
            row_flags.append("High risk + high spend concentration (top 10%)")

        # Rule 2: zero disruptions + high risk — only if disruption data is real
        if disruptions_are_real and disruptions == 0 and risk >= risk_p75:
            row_flags.append("No disruptions recorded but high risk score (monitoring gap?)")

        # Rule 3: financial stability in bottom 10%
        if fin_stab <= fin_p10 and fin_stab < 40:
            row_flags.append(f"Financial stability critically low ({fin_stab:.0f})")

        # Rule 4: delivery performance inconsistency (top 5% delivery but high risk)
        if delivery >= deliv_p95 and risk >= risk_p75:
            row_flags.append("Delivery performance inconsistent with high risk score")

        flags.append(1 if row_flags else 0)
        reasons.append("; ".join(row_flags) if row_flags else "")

    df["rule_based_flag"]   = flags
    df["rule_based_reason"] = reasons
    n_rules = sum(flags)
    log.info(f"  Rule-based: {n_rules} flags raised ({n_rules/len(df)*100:.1f}%)")
    return df


def build_anomaly_report(df: pd.DataFrame, name_col: str) -> pd.DataFrame:
    """Compile final anomaly report."""
    df["total_anomaly_flags"] = (
        (df["anomaly_if"] == -1).astype(int) +
        df.get("anomaly_zscore", pd.Series(0, index=df.index)).fillna(0).astype(int) +
        df.get("rule_based_flag", pd.Series(0, index=df.index)).fillna(0).astype(int)
    )
    df["is_anomalous"] = (df["total_anomaly_flags"] >= 1).astype(int)

    report_cols = [
        name_col, "is_anomalous", "total_anomaly_flags",
        "anomaly_score", "max_zscore", "zscore_feature",
        "rule_based_flag", "rule_based_reason",
        "composite_risk_score", "total_annual_spend",
        "financial_stability", "disruption_count",
    ]
    report_cols = [c for c in report_cols if c in df.columns]
    report = df[report_cols].sort_values("anomaly_score", ascending=False)
    return report


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("PHASE 3: RECOMMENDATION ENGINE + ANOMALY DETECTION")
    log.info("=" * 60)

    df = load_data()

    name_col     = next((c for c in df.columns if "name" in c.lower() or "supplier" in c.lower()),
                        df.columns[0])
    industry_col = next((c for c in df.columns if "industry" in c.lower()), None)
    country_col  = next((c for c in df.columns if "country" in c.lower() or "region" in c.lower()), None)

    # ══════════════════════════════════════════════════════════════
    # MODULE A: RECOMMENDATIONS
    # ══════════════════════════════════════════════════════════════
    log.info("\n--- MODULE A: RECOMMENDATIONS ---")
    X_sim = build_feature_matrix(df, SIMILARITY_FEATURES)
    alternatives = find_alternatives(df, X_sim, name_col, industry_col, country_col)

    alternatives.to_csv(RECOM_DIR / "alternative_suppliers.csv", index=False)
    log.info(f"Saved alternative_suppliers.csv ({len(alternatives)} rows)")

    rec_summary = {
        "suppliers_with_alternatives": int(alternatives["supplier"].nunique()),
        "total_alternatives_generated": len(alternatives),
        "avg_similarity_score": round(float(alternatives["similarity_score"].mean()), 4),
        "top_pairs": alternatives.head(5)[["supplier", "alternative_supplier", "similarity_score"]].to_dict("records"),
    }
    with open(RECOM_DIR / "recommendation_summary.json", "w") as f:
        json.dump(rec_summary, f, indent=2, default=str)

    # ══════════════════════════════════════════════════════════════
    # MODULE B: ANOMALY DETECTION
    # ══════════════════════════════════════════════════════════════
    log.info("\n--- MODULE B: ANOMALY DETECTION ---")
    X_anom = build_feature_matrix(df, ANOMALY_FEATURES)
    df = run_isolation_forest(df, X_anom)
    df = run_zscore_detection(df)
    df = rule_based_flags(df)
    anomaly_report = build_anomaly_report(df, name_col)

    anomaly_report.to_csv(ANOMALY_DIR / "anomaly_report.csv", index=False)
    log.info(f"Saved anomaly_report.csv ({len(anomaly_report)} rows)")

    anom_summary = {
        "total_suppliers":            len(df),
        "anomalies_isolation_forest": int((df["anomaly_if"] == -1).sum()),
        "anomalies_zscore":           int(df.get("anomaly_zscore", pd.Series(0)).sum()),
        "anomalies_rules":            int(df.get("rule_based_flag", pd.Series(0)).sum()),
        "total_flagged":              int(anomaly_report["is_anomalous"].sum()),
        "top_anomalies":              anomaly_report.head(5)[[name_col, "total_anomaly_flags", "rule_based_reason"]].to_dict("records"),
    }
    with open(ANOMALY_DIR / "anomaly_summary.json", "w") as f:
        json.dump(anom_summary, f, indent=2, default=str)

    # ── Final summary ─────────────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("RECOMMENDATION + ANOMALY DETECTION COMPLETE")
    log.info("=" * 60)
    log.info(f"  Recommendations  : {rec_summary['suppliers_with_alternatives']} suppliers, "
             f"{rec_summary['total_alternatives_generated']} alternatives")
    log.info(f"  Anomalies flagged: {anom_summary['total_flagged']} suppliers")
    log.info("\nOutputs:")
    log.info("  phase3_recommendations/alternative_suppliers.csv")
    log.info("  phase3_recommendations/recommendation_summary.json")
    log.info("  phase3_anomalies/anomaly_report.csv")
    log.info("  phase3_anomalies/anomaly_summary.json")
    log.info("Ready for Streamlit dashboard!")


if __name__ == "__main__":
    main()
