"""
SRSID  ml/explainability.py
=============================
SHAP explainability adapted for Postgres.

Changes from phase3_explainability.py:
  - Reads feature matrix + model from disk
  - Writes explanations to Postgres explanations table
  - SHAP logic unchanged

Run:
    python ml/explainability.py
"""

import sys, json, logging, warnings
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import joblib

warnings.filterwarnings("ignore")

from sklearn.impute import SimpleImputer

try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False
    logging.warning("shap not installed — pip install shap")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import ML_CONFIG, PATHS
from db.db_client import DBClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/explainability.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)
REPORT_DIR = PATHS["reports"]
MODEL_DIR  = PATHS["models"]
RUN_DATE   = datetime.now()

FEATURE_COLS = [
    "financial_stability", "delivery_performance",
    "supply_risk_score", "profit_impact_score",
    "total_annual_spend", "transaction_count",
    "spend_pct_of_portfolio", "geo_risk_numeric",
    "industry_risk_numeric", "news_sentiment_30d",
    "disruption_count_30d", "otif_rate",
    "avg_delay_days", "performance_composite",
    "composite_risk_score", "delivery_risk_numeric",
    "spend_concentration_flag", "news_risk_flag",
]

FEATURE_LABELS = {
    "financial_stability":    "Financial stability",
    "delivery_performance":   "On-time delivery",
    "supply_risk_score":      "Supply risk score",
    "profit_impact_score":    "Spend impact",
    "total_annual_spend":     "Annual spend",
    "geo_risk_numeric":       "Geographic risk",
    "industry_risk_numeric":  "Industry risk",
    "news_sentiment_30d":     "News sentiment (30d)",
    "disruption_count_30d":   "Recent disruptions",
    "otif_rate":              "OTIF rate",
    "avg_delay_days":         "Avg delivery delay",
    "composite_risk_score":   "Composite risk",
    "delivery_risk_numeric":  "Delivery risk",
    "spend_concentration_flag":"Spend concentration",
    "news_risk_flag":         "News risk flag",
}


# ─────────────────────────────────────────────────────────────────────────────
# SHAP EXPLANATIONS  (logic identical to phase3_explainability.py)
# ─────────────────────────────────────────────────────────────────────────────

def load_model_and_features():
    """Load best available model and feature matrix."""
    # Try XGBoost first, then RF, then GB
    for fname in ["risk_model_xgb.pkl", "risk_model_rf.pkl", "risk_model_gb.pkl"]:
        p = MODEL_DIR / fname
        if p.exists():
            model = joblib.load(p)
            model_type = fname.replace("risk_model_", "").replace(".pkl", "").upper()
            log.info(f"Loaded model: {fname}")
            break
    else:
        log.error("No trained model found. Run ml/risk_model.py first.")
        sys.exit(1)

    feat_path = REPORT_DIR / "supplier_features.csv"
    if not feat_path.exists():
        log.error("Feature matrix not found. Run ml/features.py first.")
        sys.exit(1)

    df = pd.read_csv(feat_path)
    return model, model_type, df


def compute_shap_values(model, X: np.ndarray, model_type: str):
    """Compute SHAP values — handles tree and linear models."""
    if not SHAP_AVAILABLE:
        return None

    sample_size = min(ML_CONFIG["shap_sample_size"], len(X))
    X_sample    = X[:sample_size]

    try:
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_sample)
        log.info(f"  SHAP TreeExplainer: {sample_size} samples")
        return shap_values, sample_size
    except Exception:
        try:
            explainer   = shap.LinearExplainer(model, X_sample)
            shap_values = explainer.shap_values(X_sample)
            log.info(f"  SHAP LinearExplainer: {sample_size} samples")
            return shap_values, sample_size
        except Exception as e:
            log.warning(f"  SHAP failed: {e} — using permutation importance instead")
            return None, sample_size


def build_explanation_row(vendor_row: pd.Series, shap_row,
                           feature_names: list, class_names: list,
                           predicted_tier: str) -> dict:
    """Build one explanation dict per vendor."""
    record = {
        "vendor_id":           str(vendor_row.get("vendor_id", "")),
        "supplier_name":       str(vendor_row.get("supplier_name", "")),
        "predicted_risk_tier": predicted_tier,
        "run_date":            RUN_DATE,
    }

    if shap_row is not None:
        # ── Flatten shap_row to a 1-D array of floats ─────────────────────
        # TreeExplainer output shapes vary by model type:
        #   Binary classification : (n_features,)
        #   Multi-class list      : list of (n_features,) — one per class
        #   Multi-class 2D array  : (n_features, n_classes)
        # We always want the values for the predicted class.

        tier_idx = class_names.index(predicted_tier) \
                   if predicted_tier in class_names else 0

        sv = shap_row  # start with whatever came in

        # Case 1: list of arrays (old SHAP multi-class format)
        if isinstance(sv, list):
            sv = sv[tier_idx] if tier_idx < len(sv) else sv[0]

        # Case 2: numpy 2D array (n_features, n_classes)
        if isinstance(sv, np.ndarray) and sv.ndim == 2:
            sv = sv[:, tier_idx] if sv.shape[1] > tier_idx else sv[:, 0]

        # Now sv should be 1-D; convert every element to plain Python float
        try:
            sv = [float(v) for v in sv]
        except (TypeError, ValueError):
            sv = [0.0] * len(feature_names)

        if len(sv) != len(feature_names):
            # Lengths mismatch — pad or truncate
            sv = (sv + [0.0] * len(feature_names))[:len(feature_names)]

        # ── Top 3 risk drivers (highest |SHAP|) ───────────────────────────
        fi_pairs = sorted(
            zip(feature_names, sv),
            key=lambda x: abs(x[1]),
            reverse=True,
        )
        for i, (feat, shap_val) in enumerate(fi_pairs[:3], 1):
            record[f"driver_{i}_label"] = FEATURE_LABELS.get(
                feat, feat.replace("_", " ").title()
            )
            record[f"driver_{i}_value"] = str(
                round(float(vendor_row.get(feat, 0)), 3)
            )
            record[f"driver_{i}_shap"] = round(float(shap_val), 4)

        # ── Main mitigator (most negative SHAP) ───────────────────────────
        mitigators = [(f, v) for f, v in fi_pairs if v < 0]
        if mitigators:
            mf, mv = mitigators[0]
            record["mitigator_label"] = FEATURE_LABELS.get(mf, mf)
            record["mitigator_value"] = str(
                round(float(vendor_row.get(mf, 0)), 3)
            )
            record["mitigator_shap"] = round(float(mv), 4)

        # ── Plain-English narrative ────────────────────────────────────────
        top_driver = record.get("driver_1_label", "overall risk factors")
        fin_stable = float(vendor_row.get("financial_stability", 60))
        deliv_perf = float(vendor_row.get("delivery_performance", 75))
        record["narrative"] = (
            f"This supplier is classified as {predicted_tier} risk, "
            f"primarily driven by {top_driver}. "
            f"Financial stability: {fin_stable:.0f}/100. "
            f"Delivery performance (OTIF): {deliv_perf:.0f}%."
            + (f" Mitigating factor: {record.get('mitigator_label', '')}."
               if mitigators else "")
        )

    return record


# ─────────────────────────────────────────────────────────────────────────────
# WRITE TO POSTGRES
# ─────────────────────────────────────────────────────────────────────────────

def write_explanations_to_postgres(records: list[dict], db: DBClient):
    log.info(f"Writing {len(records):,} explanations to Postgres...")

    expl_df = pd.DataFrame(records)

    # Keep only columns that exist in schema
    schema_cols = [
        "vendor_id","supplier_name","run_date",
        "driver_1_label","driver_1_value","driver_1_shap",
        "driver_2_label","driver_2_value","driver_2_shap",
        "driver_3_label","driver_3_value","driver_3_shap",
        "mitigator_label","mitigator_value","mitigator_shap",
        "narrative","predicted_risk_tier",
    ]
    expl_df = expl_df[[c for c in schema_cols if c in expl_df.columns]]

    rows = db.bulk_insert_df(expl_df, "explanations", if_exists="append")
    log.info(f"  Inserted {rows:,} explanations")

    # Save CSV for dashboard
    expl_df.to_csv(REPORT_DIR / "supplier_explanations.csv", index=False)
    log.info(f"  Saved supplier_explanations.csv")

    # Save global feature importance
    fi_path = REPORT_DIR / "feature_importance.csv"
    if fi_path.exists():
        fi_df = pd.read_csv(fi_path)
        fi_df["feature_label"] = fi_df["feature"].map(FEATURE_LABELS).fillna(
            fi_df["feature"].str.replace("_"," ").str.title()
        )
        fi_df.to_csv(fi_path, index=False)
        log.info(f"  Updated feature labels in feature_importance.csv")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("SRSID Explainability (Postgres)")
    log.info("=" * 60)

    model, model_type, df = load_model_and_features()

    feat_cols = [c for c in FEATURE_COLS if c in df.columns]
    X_raw     = df[feat_cols].copy()
    imputer   = SimpleImputer(strategy="median")
    X         = imputer.fit_transform(X_raw)

    # Predicted risk tiers
    try:
        y_pred = model.predict(X)
        # Try to get class names from model
        class_names = list(model.classes_) if hasattr(model, "classes_") else \
                      ["Low","Medium","High"]
        # Decode if numeric
        if len(y_pred) > 0 and isinstance(y_pred[0], (int, np.integer)):
            tiers = [class_names[p] if p < len(class_names) else "Low"
                     for p in y_pred]
        else:
            tiers = list(y_pred)
    except Exception:
        tiers = df.get("risk_label_3class",
                       pd.Series(["Low"] * len(df))).tolist()

    # Compute SHAP
    shap_vals, n_sample = compute_shap_values(model, X, model_type)

    # Build explanation per vendor
    records     = []
    class_names = ["Low","Medium","High"]

    for i, (_, row) in enumerate(df.iterrows()):
        tier = tiers[i] if i < len(tiers) else "Low"
        shap_row = None
        if shap_vals is not None and i < n_sample:
            shap_row = shap_vals[i] if not isinstance(shap_vals, list) \
                       else [sv[i] for sv in shap_vals]
        record = build_explanation_row(row, shap_row, feat_cols,
                                        class_names, str(tier))
        records.append(record)

    with DBClient() as db:
        write_explanations_to_postgres(records, db)

    log.info("\n" + "=" * 60)
    log.info("EXPLAINABILITY COMPLETE")
    log.info("=" * 60)
    log.info(f"  Explanations generated: {len(records):,}")
    log.info(f"  SHAP available: {SHAP_AVAILABLE}")
    log.info("\n  Next: python run_pipeline.py or streamlit run app/dashboard.py")


if __name__ == "__main__":
    main()
