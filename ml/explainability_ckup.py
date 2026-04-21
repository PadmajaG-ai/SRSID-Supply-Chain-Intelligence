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

try:
    from lime.lime_tabular import LimeTabularExplainer
    LIME_AVAILABLE = True
except ImportError:
    LIME_AVAILABLE = False
    logging.warning("lime not installed — pip install lime")

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
    "transaction_count":      "Transaction count",
    "spend_pct_of_portfolio": "Portfolio spend %",
    "geo_risk_numeric":       "Geographic risk",
    "industry_risk_numeric":  "Industry risk",
    "news_sentiment_30d":     "News sentiment (30d)",
    "disruption_count_30d":   "Recent disruptions",
    "otif_rate":              "OTIF rate",
    "avg_delay_days":         "Avg delivery delay",
    "ottr_rate":              "OTTR (On-Time to Request)",
    "lead_time_variability":  "Lead time variability",
    "order_accuracy_rate":    "Order accuracy",
    "avg_price_variance_pct": "Price variance (PPV%)",
    "performance_composite":  "Performance composite",
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
    """Compute SHAP values — handles tree and linear models.
    Falls back to permutation importance if SHAP fails."""
    sample_size = min(ML_CONFIG.get("shap_sample_size", 500), len(X))
    X_sample    = X[:sample_size]

    if not SHAP_AVAILABLE:
        log.warning("shap not installed — using permutation importance fallback")
        return _permutation_importance_fallback(model, X_sample), sample_size

    # Try TreeExplainer (works for XGBoost, RandomForest, GradientBoosting)
    try:
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_sample)
        log.info(f"  SHAP TreeExplainer OK: {sample_size} samples, "
                 f"output type={type(shap_values)}")
        return shap_values, sample_size
    except Exception as e:
        log.warning(f"  TreeExplainer failed: {e}")

    # Try KernelExplainer (model-agnostic, slower but universal)
    try:
        background  = shap.sample(X_sample, min(50, len(X_sample)))
        explainer   = shap.KernelExplainer(model.predict_proba, background)
        shap_values = explainer.shap_values(X_sample[:100])  # limit for speed
        log.info(f"  SHAP KernelExplainer OK: 100 samples")
        return shap_values, min(100, sample_size)
    except Exception as e:
        log.warning(f"  KernelExplainer failed: {e}")

    # Final fallback: permutation importance (always works)
    log.warning("  All SHAP methods failed — using permutation importance fallback")
    return _permutation_importance_fallback(model, X_sample), sample_size


def _permutation_importance_fallback(model, X: np.ndarray):
    """
    When SHAP is unavailable, approximate feature importance using
    the model's built-in feature_importances_ attribute.
    Returns a 2D array (n_samples, n_features) with identical rows
    so every vendor gets the same global importance scores.
    This is less accurate than per-vendor SHAP but always works.
    """
    try:
        importances = model.feature_importances_   # works for RF, XGB, GB
    except AttributeError:
        try:
            importances = np.abs(model.coef_[0])   # linear models
        except AttributeError:
            importances = np.ones(X.shape[1]) / X.shape[1]

    # Replace any NaN/inf in importances with 0
    importances = np.nan_to_num(importances, nan=0.0, posinf=0.0, neginf=0.0)
    max_imp = importances.max() if importances.max() > 0 else 1.0
    normalised = importances / max_imp

    # Broadcast to all samples (each vendor gets global importances)
    return np.tile(normalised, (len(X), 1))


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

        # Now sv should be 1-D; convert to plain Python float, replace NaN with 0
        try:
            sv = [0.0 if (v is None or np.isnan(float(v))) else float(v) for v in sv]
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
            raw_val = vendor_row.get(feat, 0)
            safe_val = 0.0 if (raw_val is None or (isinstance(raw_val, float) and np.isnan(raw_val))) else float(raw_val)
            record[f"driver_{i}_value"] = str(round(safe_val, 3))
            safe_shap = 0.0 if np.isnan(float(shap_val)) else float(shap_val)
            record[f"driver_{i}_shap"] = round(safe_shap, 4)

        # ── Main mitigator (most negative SHAP) ───────────────────────────
        mitigators = [(f, v) for f, v in fi_pairs if v < 0]
        if mitigators:
            mf, mv = mitigators[0]
            record["mitigator_label"] = FEATURE_LABELS.get(mf, mf)
            raw_mv = vendor_row.get(mf, 0)
            safe_mv = 0.0 if (raw_mv is None or (isinstance(raw_mv, float) and np.isnan(raw_mv))) else float(raw_mv)
            record["mitigator_value"] = str(round(safe_mv, 3))
            safe_ms = 0.0 if np.isnan(float(mv)) else float(mv)
            record["mitigator_shap"] = round(safe_ms, 4)

        # ── Plain-English narrative ────────────────────────────────────────
        top_driver = record.get("driver_1_label", "overall risk factors") or "overall risk factors"
        fin_raw = vendor_row.get("financial_stability", 60)
        del_raw = vendor_row.get("delivery_performance", 75)
        fin_stable = 60.0 if (fin_raw is None or (isinstance(fin_raw, float) and np.isnan(fin_raw))) else float(fin_raw)
        deliv_perf = 75.0 if (del_raw is None or (isinstance(del_raw, float) and np.isnan(del_raw))) else float(del_raw)
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
# LIME EXPLAINER
# Fits a local linear model around each vendor's prediction to explain
# which features — and in which direction — drove their specific score.
# Complements SHAP: SHAP gives global consistency, LIME gives local intuition.
# ─────────────────────────────────────────────────────────────────────────────

def build_lime_explainer(model, X_train: np.ndarray,
                          feature_names: list, class_names: list):
    """
    Build a LimeTabularExplainer trained on the feature matrix.
    Returns the explainer object, or None if LIME is unavailable.
    """
    if not LIME_AVAILABLE:
        log.warning("  LIME not available — pip install lime")
        return None
    try:
        explainer = LimeTabularExplainer(
            training_data  = X_train,
            feature_names  = feature_names,
            class_names    = class_names,
            mode           = "classification",
            discretize_continuous = True,
            random_state   = 42,
        )
        log.info(f"  LIME explainer built on {len(X_train)} training samples")
        return explainer
    except Exception as e:
        log.warning(f"  LIME explainer build failed: {e}")
        return None


def compute_lime_explanation(lime_explainer, model, x_instance: np.ndarray,
                              predicted_tier: str, class_names: list,
                              feature_names: list) -> dict:
    """
    Compute LIME explanation for a single vendor instance.
    Returns dict with lime_driver_1-3 labels/weights and lime_narrative.
    """
    if lime_explainer is None:
        return {}
    try:
        tier_idx = class_names.index(predicted_tier) \
                   if predicted_tier in class_names else 0

        exp = lime_explainer.explain_instance(
            data_row       = x_instance,
            predict_fn     = model.predict_proba,
            num_features   = 6,
            labels         = (tier_idx,),
            num_samples    = 100,    # small for speed across 2500 vendors
        )
        # Get feature weights for the predicted class
        lime_pairs = exp.as_list(label=tier_idx)

        # Sort by absolute weight descending
        lime_pairs_sorted = sorted(lime_pairs, key=lambda x: abs(x[1]), reverse=True)

        record = {}
        for i, (condition, weight) in enumerate(lime_pairs_sorted[:3], 1):
            # LIME conditions look like "geo_risk > 0.50" — extract feature name
            feat_name = condition.split(" ")[0].strip()
            label = FEATURE_LABELS.get(feat_name,
                                        feat_name.replace("_", " ").title())
            record[f"lime_driver_{i}_label"]  = label
            record[f"lime_driver_{i}_weight"] = round(float(weight), 4)

        # LIME narrative — describe top driver in plain English
        if lime_pairs_sorted:
            top_cond, top_w = lime_pairs_sorted[0]
            direction = "increases" if top_w > 0 else "reduces"
            feat_raw  = top_cond.split(" ")[0].strip()
            top_label = FEATURE_LABELS.get(feat_raw,
                                            feat_raw.replace("_"," ").title())
            record["lime_narrative"] = (
                f"Locally, {top_label} {direction} the {predicted_tier} risk "
                f"probability most (weight: {top_w:+.3f}). "
                f"LIME analysed the neighbourhood around this vendor's feature "
                f"values to identify which factors are locally decisive."
            )

        # methods_agree: do SHAP and LIME agree on the top driver?
        return record

    except Exception as e:
        log.debug(f"  LIME explanation failed for instance: {e}")
        return {}


def write_explanations_to_postgres(records: list[dict], db: DBClient):
    log.info(f"Writing {len(records):,} explanations to Postgres...")

    expl_df = pd.DataFrame(records)

    # Ensure LIME columns exist in Supabase (safe add if missing)
    for col, dtype in [
        ("lime_driver_1_label",  "VARCHAR(200)"),
        ("lime_driver_1_weight", "FLOAT"),
        ("lime_driver_2_label",  "VARCHAR(200)"),
        ("lime_driver_2_weight", "FLOAT"),
        ("lime_driver_3_label",  "VARCHAR(200)"),
        ("lime_driver_3_weight", "FLOAT"),
        ("lime_narrative",       "TEXT"),
        ("methods_agree",        "VARCHAR(5)"),
    ]:
        db.add_column_if_missing("explanations", col, dtype)
    db.conn.commit()

    # Keep only columns that exist in schema
    schema_cols = [
        "vendor_id","supplier_name","run_date",
        "driver_1_label","driver_1_value","driver_1_shap",
        "driver_2_label","driver_2_value","driver_2_shap",
        "driver_3_label","driver_3_value","driver_3_shap",
        "mitigator_label","mitigator_value","mitigator_shap",
        "lime_driver_1_label","lime_driver_1_weight",
        "lime_driver_2_label","lime_driver_2_weight",
        "lime_driver_3_label","lime_driver_3_weight",
        "lime_narrative","methods_agree",
        "narrative","predicted_risk_tier",
    ]
    expl_df = expl_df[[c for c in schema_cols if c in expl_df.columns]]

    # CRITICAL: Clean NaN values before insert
    # Missing dict keys become NaN in pandas, which becomes 'NaN' in Postgres
    # We convert: float NaN -> None (stored as NULL), string NaN -> empty string
    import numpy as np
    float_cols  = [c for c in expl_df.columns if c.endswith(("_shap","_weight"))]
    text_cols   = [c for c in expl_df.columns
                   if c.endswith(("_label","_value","_narrative","narrative",
                                   "methods_agree","predicted_risk_tier"))]

    # Replace NaN floats with None (so psycopg2 writes SQL NULL, not 'NaN')
    for c in float_cols:
        expl_df[c] = expl_df[c].astype(object).where(expl_df[c].notna(), None)

    # Replace NaN strings with empty string or None
    for c in text_cols:
        if c in expl_df.columns:
            expl_df[c] = expl_df[c].astype(object).where(expl_df[c].notna(), None)
            # Also catch string "nan" that comes from str(NaN)
            expl_df[c] = expl_df[c].replace(["nan","NaN","None"], None)

    log.info(f"  Cleaned DataFrame: {len(float_cols)} float cols, "
             f"{len([c for c in text_cols if c in expl_df.columns])} text cols")

    # Clear previous run before inserting
    db.execute("DELETE FROM explanations")
    db.conn.commit()
    log.info("  Cleared previous explanations")

    rows = db.bulk_insert_df(expl_df, "explanations", if_exists="append")
    log.info(f"  Inserted {rows:,} explanations")

    # Sanity checks
    null_check = db.scalar(
        "SELECT COUNT(*) FROM explanations WHERE driver_1_label IS NULL"
    )
    lime_check = db.scalar(
        "SELECT COUNT(*) FROM explanations WHERE lime_driver_1_label IS NOT NULL"
    )
    if null_check and null_check > 0:
        log.warning(f"  WARNING  {null_check} rows still have NULL driver_1_label")
    else:
        log.info("  OK All SHAP driver labels populated")
    log.info(f"  OK LIME explanations populated: {lime_check or 0:,} vendors")

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

    # Build LIME explainer (uses full training matrix for context)
    class_names  = ["Low", "Medium", "High"]
    lime_explainer = build_lime_explainer(model, X, feat_cols, class_names)
    lime_count   = 0

    # Build explanation per vendor
    records = []

    for i, (_, row) in enumerate(df.iterrows()):
        tier = tiers[i] if i < len(tiers) else "Low"

        # SHAP row for this vendor
        shap_row = None
        if shap_vals is not None and i < n_sample:
            shap_row = shap_vals[i] if not isinstance(shap_vals, list) \
                       else [sv[i] for sv in shap_vals]

        # Build SHAP-based explanation
        record = build_explanation_row(row, shap_row, feat_cols,
                                       class_names, str(tier))

        # Add LIME explanation for this vendor
        lime_result = compute_lime_explanation(
            lime_explainer, model, X[i], str(tier), class_names, feat_cols
        )
        if lime_result:
            record.update(lime_result)
            lime_count += 1

            # methods_agree: do SHAP and LIME agree on top driver?
            shap_top = record.get("driver_1_label", "")
            lime_top = lime_result.get("lime_driver_1_label", "")
            record["methods_agree"] = "YES" if (
                shap_top and lime_top and shap_top == lime_top
            ) else "NO"

        records.append(record)

    log.info(f"  LIME explanations generated: {lime_count:,}/{len(records):,}")

    with DBClient() as db:
        write_explanations_to_postgres(records, db)

    log.info("\n" + "=" * 60)
    log.info("EXPLAINABILITY COMPLETE")
    log.info("=" * 60)
    log.info(f"  Explanations generated: {len(records):,}")
    log.info(f"  SHAP available : {SHAP_AVAILABLE}")
    log.info(f"  LIME available : {LIME_AVAILABLE}")
    log.info("\n  Next: python run_pipeline.py or streamlit run app/dashboard.py")


if __name__ == "__main__":
    main()
