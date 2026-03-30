"""
Phase 3: Supplier Segmentation
================================
Purpose: Cluster suppliers into strategic segments using:
  1. Kraljic Matrix positioning (Supply Risk vs Profit Impact)
  2. K-Means clustering on full feature set
  3. Spend-based segmentation (Pareto / ABC analysis)
  4. Risk-Spend quadrant (composite view)

Inputs:
    - phase3_features/supplier_features.csv

Outputs:
    - phase3_segmentation/supplier_segments.csv
    - phase3_segmentation/cluster_profiles.csv
    - phase3_segmentation/abc_analysis.csv
    - phase3_segmentation/segmentation_summary.json
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
import logging
import warnings
warnings.filterwarnings("ignore")

from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score, davies_bouldin_score

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("phase3_segmentation.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
FEATURES_DIR = Path("phase3_features")
OUTPUT_DIR   = Path("phase3_segmentation")
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Cluster feature set ───────────────────────────────────────────────────────
CLUSTER_FEATURES = [
    "composite_risk_score",
    "criticality_index",
    "total_annual_spend",
    "spend_pct_of_portfolio",
    "performance_composite",
    "disruption_frequency",
    "supply_risk_score",
    "profit_impact_score",
    "industry_risk_score",
    "days_since_last_disruption",
]


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────

def load_features() -> pd.DataFrame:
    path = FEATURES_DIR / "supplier_features.csv"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run phase3_feature_engineering.py first.")
    df = pd.read_csv(path)
    log.info(f"Loaded features: {len(df)} suppliers")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# KRALJIC MATRIX SEGMENTATION
# ─────────────────────────────────────────────────────────────────────────────

def compute_kraljic_segments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign Kraljic quadrant based on Supply Risk and Profit Impact.
    Quadrants:
        Strategic  : High supply risk + High profit impact
        Bottleneck : High supply risk + Low profit impact
        Leverage   : Low supply risk  + High profit impact
        Tactical   : Low supply risk  + Low profit impact
    """
    log.info("Computing Kraljic segments...")

    sr = df.get("supply_risk_score",  pd.Series(0.5, index=df.index)).fillna(0.5)
    pi = df.get("profit_impact_score", pd.Series(0.5, index=df.index)).fillna(0.5)

    # Midpoint threshold
    sr_mid = sr.median()
    pi_mid = pi.median()

    conditions = [
        (pi >= pi_mid) & (sr >= sr_mid),
        (pi >= pi_mid) & (sr <  sr_mid),
        (pi <  pi_mid) & (sr >= sr_mid),
        (pi <  pi_mid) & (sr <  sr_mid),
    ]
    labels = ["Strategic", "Leverage", "Bottleneck", "Tactical"]

    df["kraljic_segment"] = np.select(conditions, labels, default="Tactical")

    # Strategic action recommendation
    action_map = {
        "Strategic":   "Develop long-term partnership, dual-source, safety stock",
        "Leverage":    "Consolidate spend, negotiate volume discounts, competitive tender",
        "Bottleneck":  "Qualify alternative suppliers, inventory buffer, risk mitigation plan",
        "Tactical":    "Automate procurement, reduce transaction cost, e-catalog",
    }
    df["strategic_action"] = df["kraljic_segment"].map(action_map)

    counts = df["kraljic_segment"].value_counts()
    for seg, cnt in counts.items():
        log.info(f"  Kraljic {seg:12s}: {cnt} suppliers")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# ABC / PARETO SPEND ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

def compute_abc_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Classify suppliers by cumulative spend contribution:
        A: Top suppliers contributing to first 70% of spend
        B: Next tier → 70-90%
        C: Tail → 90-100%
    """
    log.info("Computing ABC spend analysis...")

    spend_col = "total_annual_spend"
    if spend_col not in df.columns:
        df["abc_class"] = "C"
        return df

    df_sorted = df.sort_values(spend_col, ascending=False).copy()
    df_sorted["cumulative_spend"]     = df_sorted[spend_col].cumsum()
    df_sorted["cumulative_spend_pct"] = df_sorted["cumulative_spend"] / df_sorted[spend_col].sum()

    df_sorted["abc_class"] = np.where(
        df_sorted["cumulative_spend_pct"] <= 0.70, "A",
        np.where(df_sorted["cumulative_spend_pct"] <= 0.90, "B", "C")
    )

    abc_counts = df_sorted["abc_class"].value_counts()
    total_spend = df_sorted[spend_col].sum()
    for cls in ["A", "B", "C"]:
        cls_spend = df_sorted.loc[df_sorted["abc_class"] == cls, spend_col].sum()
        pct = cls_spend / total_spend * 100
        log.info(f"  ABC Class {cls}: {abc_counts.get(cls, 0)} suppliers | "
                 f"${cls_spend:,.0f} ({pct:.1f}% of spend)")

    # Merge back (preserve original order)
    abc_cols = ["abc_class", "cumulative_spend_pct"]
    name_col = next((c for c in df.columns if "name" in c.lower() or "supplier" in c.lower()), df.columns[0])
    df = df.merge(df_sorted[[name_col] + abc_cols], on=name_col, how="left")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# K-MEANS CLUSTERING
# ─────────────────────────────────────────────────────────────────────────────

def find_optimal_k(X_scaled: np.ndarray, k_range=range(2, 9)) -> int:
    """Use silhouette score to find optimal k."""
    if len(X_scaled) < 10:
        return 3

    scores = {}
    for k in k_range:
        if k >= len(X_scaled):
            continue
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X_scaled)
        if len(np.unique(labels)) < 2:
            continue
        scores[k] = silhouette_score(X_scaled, labels)

    if not scores:
        return 4

    best_k = max(scores, key=scores.get)
    log.info(f"Optimal k={best_k} (silhouette scores: {scores})")
    return best_k


def run_kmeans(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Running K-Means clustering...")

    avail = [c for c in CLUSTER_FEATURES if c in df.columns]
    for c in CLUSTER_FEATURES:
        if c not in df.columns:
            df[c] = 0.0

    X_raw = df[avail].apply(pd.to_numeric, errors="coerce")
    imp = SimpleImputer(strategy="median")
    X_imp = imp.fit_transform(X_raw)

    scaler  = StandardScaler()
    X_scaled = scaler.fit_transform(X_imp)

    # Optimal k
    k = find_optimal_k(X_scaled)

    km = KMeans(n_clusters=k, random_state=42, n_init=20)
    df["cluster_id"] = km.fit_predict(X_scaled)

    # Evaluate
    sil = silhouette_score(X_scaled, df["cluster_id"])
    db  = davies_bouldin_score(X_scaled, df["cluster_id"])
    log.info(f"K-Means: k={k}, silhouette={sil:.3f}, DB index={db:.3f}")

    # PCA for 2D representation
    pca = PCA(n_components=2, random_state=42)
    coords = pca.fit_transform(X_scaled)
    df["pca_x"] = coords[:, 0]
    df["pca_y"] = coords[:, 1]

    return df, k, sil, db


def label_clusters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign meaningful names to K-Means clusters based on their centroid profiles.
    """
    log.info("Labelling clusters...")

    cluster_profiles = df.groupby("cluster_id").agg(
        avg_risk         = ("composite_risk_score", "mean"),
        avg_spend        = ("total_annual_spend",   "mean"),
        avg_performance  = ("performance_composite", "mean"),
        avg_disruptions  = ("disruption_frequency", "mean"),
        supplier_count   = ("composite_risk_score", "count"),
    ).reset_index()

    def assign_label(row):
        high_risk  = row["avg_risk"]  > df["composite_risk_score"].median()
        high_spend = row["avg_spend"] > df["total_annual_spend"].median()
        high_perf  = row["avg_performance"] > df["performance_composite"].median()

        if high_risk and high_spend:
            return "Critical High-Spend", "Priority 1: Immediate risk mitigation + dual sourcing"
        elif high_risk and not high_spend:
            return "Vulnerable Low-Spend", "Priority 2: Monitor closely, qualify alternatives"
        elif not high_risk and high_spend:
            return "Stable Strategic", "Priority 3: Leverage spend, deepen partnership"
        else:
            if high_perf:
                return "Reliable Tactical", "Priority 4: Maintain, automate processes"
            else:
                return "Underperforming Tail", "Priority 5: Review, consolidate or exit"

    labels_actions = cluster_profiles.apply(assign_label, axis=1)
    cluster_profiles["cluster_label"]  = [la[0] for la in labels_actions]
    cluster_profiles["cluster_action"] = [la[1] for la in labels_actions]

    df = df.merge(
        cluster_profiles[["cluster_id", "cluster_label", "cluster_action"]],
        on="cluster_id", how="left"
    )

    for _, row in cluster_profiles.iterrows():
        log.info(f"  Cluster {int(row['cluster_id'])}: {row['cluster_label']} "
                 f"({int(row['supplier_count'])} suppliers, "
                 f"avg_risk={row['avg_risk']:.2f})")

    return df, cluster_profiles


# ─────────────────────────────────────────────────────────────────────────────
# RISK-SPEND QUADRANT
# ─────────────────────────────────────────────────────────────────────────────

def compute_risk_spend_quadrant(df: pd.DataFrame) -> pd.DataFrame:
    """
    2×2 grid: Risk (x) vs Spend (y)
        Q1 High Risk, High Spend → Critical
        Q2 Low  Risk, High Spend → Invest
        Q3 High Risk, Low  Spend → Monitor
        Q4 Low  Risk, Low  Spend → Optimise
    """
    log.info("Computing Risk-Spend quadrant...")

    risk  = df.get("composite_risk_score", pd.Series(0.5, index=df.index)).fillna(0.5)
    spend = df.get("total_annual_spend",   pd.Series(0,   index=df.index)).fillna(0)

    r_mid = risk.median()
    s_mid = spend.median()

    conditions = [
        (risk >= r_mid) & (spend >= s_mid),
        (risk <  r_mid) & (spend >= s_mid),
        (risk >= r_mid) & (spend <  s_mid),
        (risk <  r_mid) & (spend <  s_mid),
    ]
    labels = ["Critical", "Invest", "Monitor", "Optimise"]
    df["risk_spend_quadrant"] = np.select(conditions, labels, default="Optimise")

    for q in labels:
        cnt = (df["risk_spend_quadrant"] == q).sum()
        log.info(f"  {q:10s}: {cnt} suppliers")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("PHASE 3: SUPPLIER SEGMENTATION")
    log.info("=" * 60)

    df = load_features()

    # ── Segmentation modules ──────────────────────────────────────────────────
    df = compute_kraljic_segments(df)
    df = compute_abc_analysis(df)
    df, k, sil, db = run_kmeans(df)
    df, cluster_profiles = label_clusters(df)
    df = compute_risk_spend_quadrant(df)

    # ── Save full segmented supplier file ─────────────────────────────────────
    name_col = next((c for c in df.columns if "name" in c.lower() or "supplier" in c.lower()),
                    df.columns[0])

    out_cols = [name_col,
                "kraljic_segment", "strategic_action",
                "abc_class",
                "cluster_id", "cluster_label", "cluster_action",
                "risk_spend_quadrant",
                "composite_risk_score", "criticality_index",
                "total_annual_spend", "spend_pct_of_portfolio",
                "performance_composite",
                "pca_x", "pca_y",
                ]
    out_cols = [c for c in out_cols if c in df.columns]
    df[out_cols].to_csv(OUTPUT_DIR / "supplier_segments.csv", index=False)
    log.info(f"Saved supplier_segments.csv ({len(df)} rows)")

    # ── Cluster profiles ──────────────────────────────────────────────────────
    cluster_profiles.to_csv(OUTPUT_DIR / "cluster_profiles.csv", index=False)
    log.info("Saved cluster_profiles.csv")

    # ── ABC analysis standalone ───────────────────────────────────────────────
    abc_cols = [name_col, "abc_class", "total_annual_spend", "cumulative_spend_pct",
                "composite_risk_score"]
    abc_cols = [c for c in abc_cols if c in df.columns]
    df[abc_cols].sort_values("total_annual_spend", ascending=False).to_csv(
        OUTPUT_DIR / "abc_analysis.csv", index=False
    )

    # ── Summary JSON ──────────────────────────────────────────────────────────
    summary = {
        "total_suppliers": len(df),
        "kmeans_k": k,
        "silhouette_score": round(sil, 4),
        "davies_bouldin_score": round(db, 4),
        "kraljic_distribution": df["kraljic_segment"].value_counts().to_dict(),
        "abc_distribution": df["abc_class"].value_counts().to_dict() if "abc_class" in df.columns else {},
        "cluster_distribution": df["cluster_label"].value_counts().to_dict() if "cluster_label" in df.columns else {},
        "risk_spend_distribution": df["risk_spend_quadrant"].value_counts().to_dict(),
    }
    with open(OUTPUT_DIR / "segmentation_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    # ── Print summary ─────────────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("SEGMENTATION COMPLETE")
    log.info("=" * 60)
    log.info(f"  Suppliers        : {len(df)}")
    log.info(f"  K-Means clusters : {k} (silhouette={sil:.3f})")
    log.info(f"  Kraljic: {dict(df['kraljic_segment'].value_counts())}")
    if "abc_class" in df.columns:
        log.info(f"  ABC:     {dict(df['abc_class'].value_counts())}")
    log.info(f"  Risk-Spend: {dict(df['risk_spend_quadrant'].value_counts())}")
    log.info("\nOutputs saved to phase3_segmentation/")
    log.info("  - supplier_segments.csv    (all segmentation labels)")
    log.info("  - cluster_profiles.csv     (cluster statistics)")
    log.info("  - abc_analysis.csv         (spend ranking)")
    log.info("  - segmentation_summary.json")
    log.info("Ready for disruption forecasting model!")


if __name__ == "__main__":
    main()
