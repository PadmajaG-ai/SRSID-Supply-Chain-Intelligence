"""
Phase 3: Feature Engineering
============================
Purpose: Transform Phase 2 unified supplier master data into
         ML-ready feature sets for all downstream models.

Inputs:
    - unified_supplier_master.csv (from Phase 2)
    - raw_transactions.parquet / raw_supply_chain_transactions.csv
    - disruptions data (phase 2 outputs)

Outputs:
    - phase3_features/supplier_features.csv       (full feature matrix)
    - phase3_features/features_normalized.csv     (scaled for ML)
    - phase3_features/feature_summary.json        (stats + metadata)
    - phase3_features/correlation_matrix.csv      (for EDA)
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
import logging
from datetime import datetime, timedelta
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder
from sklearn.impute import SimpleImputer
import warnings
warnings.filterwarnings('ignore')

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("phase3_feature_engineering.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(".")
DATA_DIR = BASE_DIR / "phase1_tables"
PHASE2_DIR = BASE_DIR / "phase2_outputs"
OUTPUT_DIR = BASE_DIR / "phase3_features"
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Config ────────────────────────────────────────────────────────────────────
RISK_LABEL_THRESHOLDS = {
    "high":   0.65,   # risk_score >= 0.65 → High
    "medium": 0.35,   # 0.35 <= risk_score < 0.65 → Medium
    # below 0.35 → Low
}

SPEND_CONCENTRATION_THRESHOLD = 0.10  # >10% of total spend = concentrated
HIGH_DISRUPTION_FREQ = 2.0            # >2 disruptions/year = high freq

# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADERS
# ─────────────────────────────────────────────────────────────────────────────

def load_supplier_master() -> pd.DataFrame:
    """Load Phase 2 unified supplier master. Falls back to Phase 1 if needed."""
    candidates = [
        PHASE2_DIR / "unified_supplier_master.csv",
        BASE_DIR   / "unified_supplier_master.csv",
        DATA_DIR   / "raw_supplier_risk_assessment.csv",
    ]
    for path in candidates:
        if path.exists():
            df = pd.read_csv(path)
            log.info(f"Loaded supplier master from {path} ({len(df)} rows)")
            return df
    raise FileNotFoundError(
        "No supplier master found. Check phase2_outputs/ or phase1_tables/."
    )


def load_transactions() -> pd.DataFrame:
    """Load transaction data (parquet or csv)."""
    candidates = [
        DATA_DIR / "raw_supply_chain_transactions.parquet",
        DATA_DIR / "raw_supply_chain_transactions.csv",
        BASE_DIR / "raw_transactions.parquet",
        BASE_DIR / "transactions.csv",
    ]
    for path in candidates:
        if path.exists():
            df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
            log.info(f"Loaded transactions from {path} ({len(df)} rows)")
            return df
    log.warning("No transactions file found — spend features will use defaults.")
    return pd.DataFrame()


def load_disruptions() -> pd.DataFrame:
    """Load disruption events."""
    candidates = [
        PHASE2_DIR / "disruptions_mapped.csv",
        DATA_DIR   / "disruptions_combined.csv",
        BASE_DIR   / "disruptions.csv",
    ]
    for path in candidates:
        if path.exists():
            df = pd.read_csv(path)
            log.info(f"Loaded disruptions from {path} ({len(df)} rows)")
            return df
    log.warning("No disruptions file found — disruption features will use defaults.")
    return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE GROUPS
# ─────────────────────────────────────────────────────────────────────────────

def build_performance_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group A: Supplier Performance Features
    Sources: financial_stability, delivery_performance, quality scores
    """
    log.info("Building performance features...")

    col_map = {
        # (possible column names → standard name)
        "financial_stability":        ["financial_stability_score", "financial_stability", "fin_stability"],
        "delivery_performance":       ["delivery_performance_score", "delivery_performance", "on_time_delivery"],
        "historical_risk_raw":        ["historical_risk_category", "risk_category", "historical_risk"],
        "supply_risk_score":          ["supply_risk_score", "supply_risk"],
        "profit_impact_score":        ["profit_impact_score", "profit_impact"],
    }

    for target, sources in col_map.items():
        if target not in df.columns:
            for src in sources:
                if src in df.columns:
                    df[target] = df[src]
                    break
            else:
                df[target] = np.nan

    # Encode historical risk category → numeric
    risk_map = {"low": 0.1, "medium": 0.5, "high": 0.9,
                "l": 0.1,   "m": 0.5,    "h": 0.9}
    if df["historical_risk_raw"].dtype == object:
        df["historical_risk_numeric"] = (
            df["historical_risk_raw"].str.lower().str.strip().map(risk_map).fillna(0.5)
        )
    else:
        df["historical_risk_numeric"] = df["historical_risk_raw"].fillna(0.5)

    # Composite performance score (weighted average)
    df["performance_composite"] = (
        df["financial_stability"].fillna(50)   * 0.35 +
        df["delivery_performance"].fillna(50)  * 0.40 +
        (1 - df["historical_risk_numeric"])    * 100 * 0.25
    ) / 100  # normalise to 0-1

    # Performance tier
    df["performance_tier"] = pd.cut(
        df["performance_composite"],
        bins=[0, 0.4, 0.7, 1.01],
        labels=["Poor", "Adequate", "Strong"],
    )

    return df


def build_spend_features(df: pd.DataFrame, tx: pd.DataFrame) -> pd.DataFrame:
    """
    Group B: Spend Features
    Sources: transactions data
    """
    log.info("Building spend features...")

    if tx.empty:
        log.warning("No transaction data — using synthetic spend features.")
        np.random.seed(42)
        n = len(df)
        df["total_annual_spend"]        = np.random.lognormal(mean=14, sigma=1.5, size=n)
        df["transaction_count"]         = np.random.randint(5, 80, size=n)
        df["avg_transaction_value"]     = df["total_annual_spend"] / df["transaction_count"]
        df["spend_pct_of_portfolio"]    = df["total_annual_spend"] / df["total_annual_spend"].sum()
        df["spend_concentration_flag"]  = (df["spend_pct_of_portfolio"] > SPEND_CONCENTRATION_THRESHOLD).astype(int)
        df["spend_trend"]               = np.random.choice(["increasing", "stable", "decreasing"],
                                                           p=[0.3, 0.5, 0.2], size=n)
        return df

    # Identify supplier + amount columns
    sup_col = next((c for c in tx.columns if "supplier" in c.lower()), None)
    amt_col = next((c for c in tx.columns if any(k in c.lower() for k in ["amount", "spend", "value", "cost"])), None)
    date_col = next((c for c in tx.columns if "date" in c.lower()), None)

    if not sup_col or not amt_col:
        log.warning("Could not identify supplier/amount columns in transactions — using defaults.")
        df["total_annual_spend"]     = 1_000_000
        df["transaction_count"]      = 10
        df["avg_transaction_value"]  = 100_000
        df["spend_pct_of_portfolio"] = 1 / len(df)
        df["spend_concentration_flag"] = 0
        df["spend_trend"]            = "stable"
        return df

    tx[amt_col] = pd.to_numeric(tx[amt_col], errors="coerce").fillna(0)
    spend_agg = tx.groupby(sup_col).agg(
        total_annual_spend=(amt_col, "sum"),
        transaction_count=(amt_col, "count"),
        avg_transaction_value=(amt_col, "mean"),
        spend_std=(amt_col, "std"),
    ).reset_index().rename(columns={sup_col: "supplier_name"})

    # Spend trend via linear slope on monthly buckets
    if date_col:
        tx[date_col] = pd.to_datetime(tx[date_col], errors="coerce")
        tx["month"] = tx[date_col].dt.to_period("M")
        monthly = (tx.groupby([sup_col, "month"])[amt_col]
                   .sum().reset_index())
        monthly["month_num"] = monthly["month"].apply(lambda x: x.ordinal if pd.notna(x) else 0)

        def trend_label(grp):
            if len(grp) < 3:
                return "stable"
            slope = np.polyfit(grp["month_num"], grp[amt_col], 1)[0]
            mean_val = grp[amt_col].mean() + 1e-9
            if slope / mean_val > 0.05:
                return "increasing"
            elif slope / mean_val < -0.05:
                return "decreasing"
            return "stable"

        trend_df = (monthly.groupby(sup_col)
                    .apply(trend_label)
                    .reset_index()
                    .rename(columns={0: "spend_trend", sup_col: "supplier_name"}))
        spend_agg = spend_agg.merge(trend_df, on="supplier_name", how="left")
    else:
        spend_agg["spend_trend"] = "stable"

    total_spend = spend_agg["total_annual_spend"].sum()
    spend_agg["spend_pct_of_portfolio"] = spend_agg["total_annual_spend"] / (total_spend + 1e-9)
    spend_agg["spend_concentration_flag"] = (
        spend_agg["spend_pct_of_portfolio"] > SPEND_CONCENTRATION_THRESHOLD
    ).astype(int)

    # Merge into main df
    name_col = next((c for c in df.columns if "name" in c.lower() or "supplier" in c.lower()), df.columns[0])
    df = df.merge(spend_agg, left_on=name_col, right_on="supplier_name", how="left")

    # Fill missing with medians
    for col in ["total_annual_spend", "transaction_count", "avg_transaction_value",
                "spend_pct_of_portfolio"]:
        df[col] = df[col].fillna(df[col].median())
    df["spend_concentration_flag"] = df["spend_concentration_flag"].fillna(0)
    df["spend_trend"] = df["spend_trend"].fillna("stable")

    return df


def build_risk_features(df: pd.DataFrame, disruptions: pd.DataFrame) -> pd.DataFrame:
    """
    Group C: Risk & Disruption Features
    Sources: disruption events, historical risk categories
    """
    log.info("Building risk features...")

    if not disruptions.empty:
        sup_col = next((c for c in disruptions.columns if "supplier" in c.lower()), None)
        date_col = next((c for c in disruptions.columns if "date" in c.lower()), None)
        type_col = next((c for c in disruptions.columns if "type" in c.lower()), None)

        if sup_col:
            disr_agg = disruptions.groupby(sup_col).agg(
                disruption_count=(sup_col, "count"),
            ).reset_index().rename(columns={sup_col: "supplier_name"})

            if date_col:
                disruptions[date_col] = pd.to_datetime(disruptions[date_col], errors="coerce")
                latest = (disruptions.groupby(sup_col)[date_col]
                          .max().reset_index()
                          .rename(columns={sup_col: "supplier_name", date_col: "last_disruption_date"}))
                disr_agg = disr_agg.merge(latest, on="supplier_name", how="left")
                disr_agg["days_since_last_disruption"] = (
                    pd.Timestamp.now() - disr_agg["last_disruption_date"]
                ).dt.days.fillna(999)
            else:
                disr_agg["days_since_last_disruption"] = 999

            if type_col:
                # Most common disruption type per supplier
                mode_type = (disruptions.groupby(sup_col)[type_col]
                             .agg(lambda x: x.mode()[0] if len(x) > 0 else "Unknown")
                             .reset_index()
                             .rename(columns={sup_col: "supplier_name", type_col: "primary_disruption_type"}))
                disr_agg = disr_agg.merge(mode_type, on="supplier_name", how="left")
            else:
                disr_agg["primary_disruption_type"] = "Unknown"

            # Merge
            name_col = next((c for c in df.columns if "name" in c.lower()), df.columns[0])
            df = df.merge(disr_agg, left_on=name_col, right_on="supplier_name",
                          how="left", suffixes=("", "_disr"))
        else:
            log.warning("No supplier column found in disruptions data.")

    # Defaults if columns still missing
    defaults = {
        "disruption_count":          0,
        "days_since_last_disruption": 999,
        "primary_disruption_type":   "None",
    }
    for col, val in defaults.items():
        if col not in df.columns:
            df[col] = val
        else:
            df[col] = df[col].fillna(val)

    # Derived risk features
    df["disruption_frequency"] = df["disruption_count"] / 3  # assume 3-year window
    df["high_disruption_flag"]  = (df["disruption_frequency"] > HIGH_DISRUPTION_FREQ).astype(int)
    df["recency_risk"]          = np.where(df["days_since_last_disruption"] < 90, 1,
                                   np.where(df["days_since_last_disruption"] < 365, 0.5, 0))

    # Composite risk score (0-1, higher = riskier)
    sr = df.get("supply_risk_score", pd.Series(0.5, index=df.index)).fillna(0.5)
    pi = df.get("profit_impact_score", pd.Series(0.5, index=df.index)).fillna(0.5)

    df["composite_risk_score"] = (
        sr                          * 0.30 +
        df["historical_risk_numeric"] * 0.25 +
        df["disruption_frequency"].clip(0, 5) / 5 * 0.25 +
        df["recency_risk"]            * 0.20
    ).clip(0, 1)

    # Criticality index = risk × business impact
    df["criticality_index"] = (
        df["composite_risk_score"] * 0.5 +
        pi * 0.3 +
        df["spend_concentration_flag"].astype(float) * 0.2
    ).clip(0, 1)

    return df


def build_strategic_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group D: Strategic / Kraljic Features
    """
    log.info("Building strategic features...")

    # Encode Kraljic quadrant
    quadrant_col = next(
        (c for c in df.columns if "quadrant" in c.lower() or "kraljic" in c.lower()), None
    )
    if quadrant_col and df[quadrant_col].nunique() > 1:
        df["kraljic_quadrant"] = df[quadrant_col]
    else:
        # Derive from supply_risk + profit_impact if they exist and vary
        sr_col = next((c for c in df.columns if "supply_risk" in c.lower()), None)
        pi_col = next((c for c in df.columns if "profit_impact" in c.lower()), None)

        if sr_col and pi_col and df[sr_col].nunique() > 1:
            sr = pd.to_numeric(df[sr_col], errors="coerce").fillna(0.5)
            pi = pd.to_numeric(df[pi_col], errors="coerce").fillna(0.5)
        else:
            # Fall back: use composite_risk_score + spend_pct as proxies
            log.warning("supply_risk_score / profit_impact_score not found or constant — "
                        "deriving Kraljic from composite_risk + spend_pct.")
            sr = df.get("composite_risk_score", pd.Series(0.5, index=df.index)).fillna(0.5)
            pi = df.get("spend_pct_of_portfolio", pd.Series(0.01, index=df.index)).fillna(0.01)
            # Normalise pi to 0-1
            pi = (pi - pi.min()) / (pi.max() - pi.min() + 1e-9)

        sr_mid = sr.median()
        pi_mid = pi.median()

        conditions = [
            (pi >= pi_mid) & (sr >= sr_mid),
            (pi >= pi_mid) & (sr <  sr_mid),
            (pi <  pi_mid) & (sr >= sr_mid),
            (pi <  pi_mid) & (sr <  sr_mid),
        ]
        labels = ["Strategic", "Leverage", "Bottleneck", "Tactical"]
        df["kraljic_quadrant"] = np.select(conditions, labels, default="Tactical")

    quadrant_score = {
        "Strategic":   1.0,
        "Bottleneck":  0.75,
        "Leverage":    0.5,
        "Tactical":    0.25,
    }
    df["quadrant_score"] = (df["kraljic_quadrant"]
                            .map(quadrant_score)
                            .fillna(0.5))

    # Geographic risk
    country_col = next((c for c in df.columns if "country" in c.lower() or "region" in c.lower()), None)
    if country_col:
        high_risk_regions = ["China", "Russia", "Taiwan", "Ukraine", "Iran", "Venezuela"]
        df["geo_risk_flag"] = df[country_col].apply(
            lambda x: 1 if any(r.lower() in str(x).lower() for r in high_risk_regions) else 0
        )
    else:
        df["geo_risk_flag"] = 0

    # Industry risk encoding
    industry_col = next((c for c in df.columns if "industry" in c.lower()), None)
    if industry_col:
        industry_risk = {
            "Semiconductors": 0.9, "Electronics": 0.8, "Automotive": 0.75,
            "Chemicals": 0.7, "Pharma": 0.65, "Energy": 0.6,
            "Logistics": 0.55, "IT Services": 0.4, "Consulting": 0.3,
        }
        df["industry_risk_score"] = df[industry_col].apply(
            lambda x: next((v for k, v in industry_risk.items() if k.lower() in str(x).lower()), 0.5)
        )
    else:
        df["industry_risk_score"] = 0.5

    return df


def build_risk_labels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Target Variables for supervised ML models.
    """
    log.info("Building risk labels...")

    # --- Binary risk label (for binary classification) ---
    df["risk_label_binary"] = (
        df["composite_risk_score"] >= RISK_LABEL_THRESHOLDS["high"]
    ).astype(int)

    # --- 3-class risk label ---
    df["risk_label_3class"] = pd.cut(
        df["composite_risk_score"],
        bins=[-0.01, RISK_LABEL_THRESHOLDS["medium"], RISK_LABEL_THRESHOLDS["high"], 1.01],
        labels=["Low", "Medium", "High"],
    )

    # --- Disruption within 90 days (simulated for forecasting model) ---
    df["disruption_in_90d"] = (df["days_since_last_disruption"] < 90).astype(int)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# NORMALISATION & ENCODING
# ─────────────────────────────────────────────────────────────────────────────

NUMERIC_FEATURES = [
    "financial_stability", "delivery_performance",
    "supply_risk_score", "profit_impact_score",
    "performance_composite",
    "total_annual_spend", "transaction_count",
    "avg_transaction_value", "spend_pct_of_portfolio",
    "disruption_count", "disruption_frequency",
    "days_since_last_disruption",
    "composite_risk_score", "criticality_index",
    "quadrant_score", "industry_risk_score",
    "historical_risk_numeric", "recency_risk",
    "geo_risk_flag", "spend_concentration_flag",
    "high_disruption_flag",
]

CATEGORICAL_FEATURES = [
    "kraljic_quadrant", "spend_trend",
    "primary_disruption_type", "performance_tier",
]


def normalise_features(df: pd.DataFrame) -> pd.DataFrame:
    """Apply StandardScaler to numeric features, encode categoricals."""
    log.info("Normalising features...")

    df_norm = df.copy()

    # Only include columns that (a) are in NUMERIC_FEATURES, (b) exist, (c) can be numeric
    candidate_numeric = [c for c in NUMERIC_FEATURES if c in df_norm.columns]
    num_data = df_norm[candidate_numeric].apply(pd.to_numeric, errors="coerce")

    # Drop columns that are entirely NaN (can't impute or scale)
    valid_cols = [c for c in candidate_numeric if num_data[c].notna().any()]
    if len(valid_cols) < len(candidate_numeric):
        dropped = set(candidate_numeric) - set(valid_cols)
        log.warning(f"Dropping all-NaN numeric columns from normalisation: {dropped}")
    num_data = num_data[valid_cols]

    imputer = SimpleImputer(strategy="median")
    scaler  = StandardScaler()
    scaled  = scaler.fit_transform(imputer.fit_transform(num_data))
    df_norm[valid_cols] = scaled   # assign only valid_cols — shapes always match

    # Encode categoricals
    present_cat = [c for c in CATEGORICAL_FEATURES if c in df_norm.columns]
    le = LabelEncoder()
    for col in present_cat:
        df_norm[col + "_enc"] = le.fit_transform(df_norm[col].astype(str).fillna("Unknown"))

    return df_norm


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY & VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

def build_feature_summary(df: pd.DataFrame) -> dict:
    summary = {
        "generated_at":        datetime.now().isoformat(),
        "total_suppliers":     len(df),
        "total_features":      len(df.columns),
        "risk_distribution":   df["risk_label_3class"].value_counts().to_dict() if "risk_label_3class" in df.columns else {},
        "quadrant_distribution": df["kraljic_quadrant"].value_counts().to_dict() if "kraljic_quadrant" in df.columns else {},
        "avg_composite_risk":  round(float(df["composite_risk_score"].mean()), 4) if "composite_risk_score" in df.columns else None,
        "avg_criticality":     round(float(df["criticality_index"].mean()), 4) if "criticality_index" in df.columns else None,
        "high_risk_count":     int(df["risk_label_binary"].sum()) if "risk_label_binary" in df.columns else None,
        "spend_concentrated":  int(df["spend_concentration_flag"].sum()) if "spend_concentration_flag" in df.columns else None,
        "geo_risk_count":      int(df["geo_risk_flag"].sum()) if "geo_risk_flag" in df.columns else None,
        "missing_pct":         {c: round(df[c].isna().mean() * 100, 2) for c in df.columns if df[c].isna().any()},
        "numeric_features":    [c for c in NUMERIC_FEATURES if c in df.columns],
        "categorical_features": [c for c in CATEGORICAL_FEATURES if c in df.columns],
    }
    return summary


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("PHASE 3: FEATURE ENGINEERING")
    log.info("=" * 60)

    # 1. Load data
    df          = load_supplier_master()
    transactions = load_transactions()
    disruptions  = load_disruptions()

    # Ensure unique supplier-level df
    name_col = next((c for c in df.columns if "name" in c.lower() or "supplier" in c.lower()), df.columns[0])
    df = df.drop_duplicates(subset=[name_col]).reset_index(drop=True)
    log.info(f"Working with {len(df)} unique suppliers")

    # 2. Feature groups
    df = build_performance_features(df)
    df = build_spend_features(df, transactions)
    df = build_risk_features(df, disruptions)
    df = build_strategic_features(df)
    df = build_risk_labels(df)

    # 3. Save full feature matrix
    feature_path = OUTPUT_DIR / "supplier_features.csv"
    df.to_csv(feature_path, index=False)
    log.info(f"Saved feature matrix → {feature_path}  ({len(df)} rows × {len(df.columns)} cols)")

    # 4. Normalised version (for ML)
    df_norm = normalise_features(df)
    norm_path = OUTPUT_DIR / "features_normalized.csv"
    df_norm.to_csv(norm_path, index=False)
    log.info(f"Saved normalised features → {norm_path}")

    # 5. Correlation matrix (EDA)
    numeric_cols = [c for c in NUMERIC_FEATURES if c in df.columns]
    corr = df[numeric_cols].apply(pd.to_numeric, errors="coerce").corr()
    corr.to_csv(OUTPUT_DIR / "correlation_matrix.csv")
    log.info("Saved correlation matrix")

    # 6. Summary
    summary = build_feature_summary(df)
    with open(OUTPUT_DIR / "feature_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    # ── Print summary ─────────────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("FEATURE ENGINEERING COMPLETE")
    log.info("=" * 60)
    log.info(f"  Suppliers processed : {summary['total_suppliers']}")
    log.info(f"  Features created    : {summary['total_features']}")
    log.info(f"  Avg composite risk  : {summary['avg_composite_risk']}")
    log.info(f"  High-risk suppliers : {summary['high_risk_count']}")
    log.info(f"  Geo-risk flagged    : {summary['geo_risk_count']}")
    log.info(f"  Spend concentrated  : {summary['spend_concentrated']}")
    if summary["risk_distribution"]:
        log.info(f"  Risk distribution   : {summary['risk_distribution']}")
    if summary["quadrant_distribution"]:
        log.info(f"  Quadrant breakdown  : {summary['quadrant_distribution']}")
    log.info("\nOutputs saved to phase3_features/")
    log.info("  - supplier_features.csv    (full matrix)")
    log.info("  - features_normalized.csv  (ML-ready)")
    log.info("  - correlation_matrix.csv   (EDA)")
    log.info("  - feature_summary.json     (statistics)")
    log.info("Ready for Phase 3 ML models!")


if __name__ == "__main__":
    main()
