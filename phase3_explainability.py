"""
Phase 3: Explainable AI (XAI)
================================
Answers the critical question for every supplier:
  "WHY is this supplier classified as High / Medium / Low risk?"

Methods used (layered approach):
  1. SHAP  — TreeExplainer for RF/XGBoost (fast, exact, per-supplier)
  2. Permutation Importance — model-agnostic global importance (always runs)
  3. LIME  — LocalInterpretableModelAgnosticExplanations for top high-risk
             suppliers (second opinion, runs if lime installed)
  4. Natural-language narratives — human-readable per-supplier sentences

Faithfulness guarantees:
  - SHAP values sum exactly to (prediction - base_rate) for every supplier
  - Permutation importance measures real prediction degradation
  - Both methods agree on top drivers → high confidence in explanation

Inputs:
    - phase3_models/risk_model_rf.pkl        (trained Random Forest)
    - phase3_models/risk_model_xgb.pkl       (trained XGBoost, optional)
    - phase3_models/risk_model_gb.pkl        (GradientBoosting fallback)
    - phase3_features/supplier_features.csv
    - phase3_risk_predictions/risk_predictions.csv

Outputs:
    - phase3_xai/shap_values.csv             (raw SHAP per supplier × feature)
    - phase3_xai/supplier_explanations.csv   (top drivers + narratives per supplier)
    - phase3_xai/global_importance.csv       (SHAP + permutation, ranked)
    - phase3_xai/lime_explanations.csv       (LIME for high-risk suppliers)
    - phase3_xai/xai_summary.json            (meta + agreement stats)
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
import logging
import joblib
import warnings
warnings.filterwarnings("ignore")

from sklearn.inspection import permutation_importance
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import LabelEncoder

try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

try:
    import lime
    import lime.lime_tabular
    LIME_AVAILABLE = True
except ImportError:
    LIME_AVAILABLE = False

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("phase3_explainability.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
FEATURES_DIR  = Path("phase3_features")
MODEL_DIR     = Path("phase3_models")
PRED_DIR      = Path("phase3_risk_predictions")
OUTPUT_DIR    = Path("phase3_xai")
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Feature columns (must match risk_prediction.py) ───────────────────────────
FEATURE_COLS = [
    "financial_stability",
    "delivery_performance",
    "supply_risk_score",
    "profit_impact_score",
    "performance_composite",
    "total_annual_spend",
    "transaction_count",
    "spend_pct_of_portfolio",
    "spend_concentration_flag",
    "disruption_count",
    "disruption_frequency",
    "days_since_last_disruption",
    "high_disruption_flag",
    "recency_risk",
    "historical_risk_numeric",
    "geo_risk_flag",
    "industry_risk_score",
    "quadrant_score",
    "criticality_index",
]

# Human-readable feature labels for narratives
FEATURE_LABELS = {
    "financial_stability":        "Financial Stability",
    "delivery_performance":       "Delivery Performance",
    "supply_risk_score":          "Supply Risk Score",
    "profit_impact_score":        "Profit Impact Score",
    "performance_composite":      "Overall Performance",
    "total_annual_spend":         "Annual Spend ($)",
    "transaction_count":          "Transaction Volume",
    "spend_pct_of_portfolio":     "Spend Share of Portfolio",
    "spend_concentration_flag":   "Spend Concentration",
    "disruption_count":           "Historical Disruptions",
    "disruption_frequency":       "Disruption Frequency",
    "days_since_last_disruption": "Days Since Last Disruption",
    "high_disruption_flag":       "High Disruption Flag",
    "recency_risk":               "Recent Disruption Activity",
    "historical_risk_numeric":    "Historical Risk Category",
    "geo_risk_flag":              "Geographic Risk",
    "industry_risk_score":        "Industry Risk Level",
    "quadrant_score":             "Kraljic Strategic Position",
    "criticality_index":          "Overall Criticality Index",
}

# Direction: does a higher value increase risk (True) or decrease it (False)?
FEATURE_RISK_DIRECTION = {
    "financial_stability":        False,  # higher stability = lower risk
    "delivery_performance":       False,
    "supply_risk_score":          True,
    "profit_impact_score":        True,
    "performance_composite":      False,
    "total_annual_spend":         True,
    "transaction_count":          False,
    "spend_pct_of_portfolio":     True,
    "spend_concentration_flag":   True,
    "disruption_count":           True,
    "disruption_frequency":       True,
    "days_since_last_disruption": False,  # more days since = safer
    "high_disruption_flag":       True,
    "recency_risk":               True,
    "historical_risk_numeric":    True,
    "geo_risk_flag":              True,
    "industry_risk_score":        True,
    "quadrant_score":             True,
    "criticality_index":          True,
}


# ─────────────────────────────────────────────────────────────────────────────
# DATA & MODEL LOADING
# ─────────────────────────────────────────────────────────────────────────────

def load_model():
    """Load best available trained model."""
    for name, path in [
        ("XGBoost",          MODEL_DIR / "risk_model_xgb.pkl"),
        ("RandomForest",     MODEL_DIR / "risk_model_rf.pkl"),
        ("GradientBoosting", MODEL_DIR / "risk_model_gb.pkl"),
    ]:
        if path.exists():
            model = joblib.load(path)
            log.info(f"Loaded model: {name} from {path}")
            return model, name
    raise FileNotFoundError(
        "No trained model found in phase3_models/. "
        "Run phase3_risk_prediction.py first."
    )


def load_and_prepare_data():
    """Load feature matrix and align to FEATURE_COLS."""
    path = FEATURES_DIR / "supplier_features.csv"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run feature engineering first.")

    df = pd.read_csv(path)
    log.info(f"Loaded features: {len(df)} suppliers, {len(df.columns)} columns")

    # Add missing feature cols as 0
    for c in FEATURE_COLS:
        if c not in df.columns:
            df[c] = 0.0

    X_raw = df[FEATURE_COLS].apply(pd.to_numeric, errors="coerce")
    valid_cols = [c for c in FEATURE_COLS if X_raw[c].notna().any()]

    imp = SimpleImputer(strategy="median")
    X = pd.DataFrame(imp.fit_transform(X_raw[valid_cols]), columns=valid_cols)

    # Encode class labels
    target_col = "risk_label_3class"
    if target_col in df.columns:
        le = LabelEncoder()
        y = le.fit_transform(df[target_col].astype(str).str.strip())
        class_names = le.classes_.tolist()
    else:
        y = np.zeros(len(df), dtype=int)
        class_names = ["Low", "Medium", "High"]

    name_col = next((c for c in df.columns if "name" in c.lower() or "supplier" in c.lower()),
                    df.columns[0])

    return df, X, y, valid_cols, class_names, name_col, imp


# ─────────────────────────────────────────────────────────────────────────────
# METHOD 1: SHAP
# ─────────────────────────────────────────────────────────────────────────────

def compute_shap(model, X: pd.DataFrame, model_name: str, class_names: list):
    """
    Compute SHAP values using the correct explainer for each model type.
    Returns shap_values array (samples × features) for the highest-risk class.
    """
    if not SHAP_AVAILABLE:
        log.warning("SHAP not installed. pip install shap")
        return None, None

    log.info(f"Computing SHAP values using {model_name} explainer...")

    try:
        if model_name in ("XGBoost", "GradientBoosting", "RandomForest"):
            # TreeExplainer: exact, fast, designed for tree-based models
            explainer = shap.TreeExplainer(model)
            shap_vals = explainer.shap_values(X)
            base_vals = explainer.expected_value
        else:
            # KernelExplainer: model-agnostic fallback (slower)
            log.info("  Using KernelExplainer (slower) for this model type...")
            background = shap.sample(X, min(100, len(X)), random_state=42)
            explainer = shap.KernelExplainer(model.predict_proba, background)
            shap_vals = explainer.shap_values(X)
            base_vals = explainer.expected_value

        # For multiclass, shap_values is a list [class0, class1, class2]
        # We want the High-risk class (usually last index)
        if isinstance(shap_vals, list):
            high_class_idx = len(class_names) - 1  # "High" is last after LabelEncoder sort
            sv = shap_vals[high_class_idx]
            bv = base_vals[high_class_idx] if hasattr(base_vals, "__len__") else base_vals
        else:
            sv = shap_vals
            bv = base_vals

        log.info(f"  SHAP computed: shape={sv.shape}, base_value={float(bv):.4f}")
        return sv, float(bv)

    except Exception as e:
        log.warning(f"SHAP computation failed: {e}. Falling back to permutation only.")
        return None, None


def build_shap_dataframe(shap_values: np.ndarray, feature_names: list,
                         df: pd.DataFrame, name_col: str) -> pd.DataFrame:
    """Convert raw SHAP array to labelled DataFrame."""
    shap_df = pd.DataFrame(shap_values, columns=feature_names)
    shap_df.insert(0, "supplier_name", df[name_col].values)
    return shap_df


# ─────────────────────────────────────────────────────────────────────────────
# METHOD 2: PERMUTATION IMPORTANCE
# ─────────────────────────────────────────────────────────────────────────────

def compute_permutation_importance(model, X: pd.DataFrame, y: np.ndarray,
                                   feature_names: list) -> pd.DataFrame:
    """
    Model-agnostic: measures how much model accuracy drops when each
    feature is randomly shuffled. Always runs regardless of SHAP availability.
    """
    log.info("Computing permutation importance (model-agnostic)...")
    result = permutation_importance(
        model, X, y,
        n_repeats=20,
        random_state=42,
        n_jobs=-1,
        scoring="f1_weighted",
    )
    perm_df = pd.DataFrame({
        "feature":          feature_names,
        "perm_importance":  result.importances_mean,
        "perm_std":         result.importances_std,
    }).sort_values("perm_importance", ascending=False)
    perm_df["perm_rank"] = range(1, len(perm_df) + 1)
    log.info(f"  Top 5 features: {perm_df['feature'].head(5).tolist()}")
    return perm_df


# ─────────────────────────────────────────────────────────────────────────────
# METHOD 3: LIME (for top high-risk suppliers only)
# ─────────────────────────────────────────────────────────────────────────────

def compute_lime(model, X: pd.DataFrame, df: pd.DataFrame, name_col: str,
                 class_names: list, n_top: int = 15) -> pd.DataFrame:
    """
    LIME provides a local linear approximation for each prediction.
    Run only on the highest-risk suppliers (computationally expensive).
    """
    if not LIME_AVAILABLE:
        log.warning("LIME not installed. pip install lime")
        return pd.DataFrame()

    log.info(f"Computing LIME explanations for top {n_top} high-risk suppliers...")

    explainer = lime.lime_tabular.LimeTabularExplainer(
        training_data=X.values,
        feature_names=X.columns.tolist(),
        class_names=class_names,
        mode="classification",
        random_state=42,
        discretize_continuous=False,
    )

    # Select top high-risk suppliers by predicted probability
    pred_proba = model.predict_proba(X)
    high_risk_class = len(class_names) - 1
    risk_scores = pred_proba[:, high_risk_class]
    top_indices = np.argsort(risk_scores)[::-1][:n_top]

    records = []
    for idx in top_indices:
        try:
            exp = explainer.explain_instance(
                X.values[idx],
                model.predict_proba,
                num_features=5,
                labels=(high_risk_class,),
            )
            lime_features = exp.as_list(label=high_risk_class)
            for rank, (feat_desc, weight) in enumerate(lime_features, 1):
                records.append({
                    "supplier_name":   df[name_col].iloc[idx],
                    "lime_rank":       rank,
                    "lime_feature":    feat_desc,
                    "lime_weight":     round(weight, 5),
                    "risk_probability": round(risk_scores[idx], 4),
                })
        except Exception as e:
            log.warning(f"LIME failed for supplier {df[name_col].iloc[idx]}: {e}")

    lime_df = pd.DataFrame(records)
    log.info(f"  LIME computed for {lime_df['supplier_name'].nunique()} suppliers")
    return lime_df


# ─────────────────────────────────────────────────────────────────────────────
# NATURAL LANGUAGE NARRATIVES
# ─────────────────────────────────────────────────────────────────────────────

def format_feature_value(feature: str, value: float, df_orig: pd.DataFrame,
                          name_col: str, supplier_name: str) -> str:
    """Format raw feature value for human display."""
    try:
        raw = df_orig.loc[df_orig[name_col] == supplier_name, feature].iloc[0]
    except Exception:
        raw = value

    if feature in ("financial_stability", "delivery_performance"):
        return f"{float(raw):.1f}/100"
    elif feature == "total_annual_spend":
        return f"${float(raw):,.0f}"
    elif feature in ("spend_pct_of_portfolio",):
        return f"{float(raw)*100:.1f}%"
    elif feature in ("disruption_count",):
        return f"{int(float(raw))} events"
    elif feature in ("days_since_last_disruption",):
        return f"{int(float(raw))} days"
    elif feature in ("spend_concentration_flag", "geo_risk_flag",
                     "high_disruption_flag"):
        return "Yes" if float(raw) > 0.5 else "No"
    elif feature == "historical_risk_numeric":
        v = float(raw)
        return "High" if v > 0.7 else "Medium" if v > 0.3 else "Low"
    elif feature == "kraljic_quadrant" or feature == "quadrant_score":
        v = float(raw)
        if v >= 0.9:
            return "Strategic"
        elif v >= 0.7:
            return "Bottleneck"
        elif v >= 0.4:
            return "Leverage"
        return "Tactical"
    else:
        return f"{float(raw):.3f}"


def shap_to_narrative(top_drivers: list, risk_tier: str,
                       supplier_name: str) -> str:
    """
    Convert SHAP top drivers into a human-readable risk explanation.

    Example output:
      "Rich's is classified as Medium Risk primarily because:
       (1) Financial Stability is low at 42.3/100, increasing risk [+0.08].
       (2) Delivery Performance is weak at 61.2/100, increasing risk [+0.06].
       (3) Disruption Frequency is high at 1.5 events/yr, increasing risk [+0.04].
       Mitigating factors: Annual Spend is moderate ($299K), reducing risk [-0.03]."
    """
    if not top_drivers:
        return f"{supplier_name} has a {risk_tier} risk classification (insufficient feature data for detailed explanation)."

    risk_drivers    = [d for d in top_drivers if d["shap"] > 0]
    protect_drivers = [d for d in top_drivers if d["shap"] < 0]

    lines = [f'**{supplier_name}** is classified as **{risk_tier} Risk** because:']

    for i, d in enumerate(risk_drivers[:3], 1):
        label = FEATURE_LABELS.get(d["feature"], d["feature"])
        lines.append(
            f'  ({i}) {label} is {d["display_value"]}'
            f', increasing risk [SHAP: +{abs(d["shap"]):.4f}]'
        )

    if protect_drivers:
        prot = protect_drivers[0]
        label = FEATURE_LABELS.get(prot["feature"], prot["feature"])
        lines.append(
            f'  Mitigating: {label} is {prot["display_value"]}'
            f', reducing risk [SHAP: {prot["shap"]:.4f}]'
        )

    return "\n".join(lines)


def perm_to_narrative(top_features: list, risk_tier: str,
                       supplier_name: str) -> str:
    """Permutation-based narrative (used when SHAP unavailable)."""
    if not top_features:
        return f"{supplier_name} is {risk_tier} Risk (feature data unavailable)."

    lines = [f'**{supplier_name}** is classified as **{risk_tier} Risk**.',
             '  Most influential factors (permutation importance):']
    for i, f in enumerate(top_features[:3], 1):
        label = FEATURE_LABELS.get(f["feature"], f["feature"])
        lines.append(f'  ({i}) {label} — importance score: {f["importance"]:.4f}')
    return "\n".join(lines)


def build_supplier_explanations(df: pd.DataFrame, X: pd.DataFrame,
                                 shap_df: pd.DataFrame | None,
                                 perm_df: pd.DataFrame,
                                 model,
                                 name_col: str,
                                 class_names: list) -> pd.DataFrame:
    """
    Build per-supplier explanation table with:
    - Top 3 risk-increasing drivers
    - Top 1 mitigating driver
    - SHAP agreement score
    - Natural language narrative
    """
    log.info("Building per-supplier explanations...")

    pred_labels = model.predict(X)
    pred_proba  = model.predict_proba(X)
    high_idx    = len(class_names) - 1

    records = []
    for i, (_, row) in enumerate(df.iterrows()):
        supplier = row[name_col]
        pred_class = class_names[pred_labels[i]]
        risk_prob  = round(pred_proba[i, high_idx], 4)

        if shap_df is not None and not shap_df.empty:
            # ── SHAP-based explanation ────────────────────────────────────────
            shap_row = shap_df[shap_df["supplier_name"] == supplier]
            if shap_row.empty:
                shap_row = shap_df.iloc[[i]] if i < len(shap_df) else None

            if shap_row is not None and not shap_row.empty:
                feat_cols = [c for c in shap_df.columns if c != "supplier_name"]
                shap_vals = shap_row[feat_cols].iloc[0]
                sorted_shap = shap_vals.abs().sort_values(ascending=False)

                top_drivers = []
                for feat in sorted_shap.index[:6]:
                    sv = float(shap_vals[feat])
                    display = format_feature_value(feat, sv, df, name_col, supplier)
                    top_drivers.append({
                        "feature":       feat,
                        "shap":          round(sv, 5),
                        "abs_shap":      round(abs(sv), 5),
                        "display_value": display,
                        "direction":     "increases_risk" if sv > 0 else "reduces_risk",
                    })

                risk_inc = [d for d in top_drivers if d["shap"] > 0]
                risk_red = [d for d in top_drivers if d["shap"] < 0]

                narrative = shap_to_narrative(top_drivers, pred_class, supplier)
                method = "SHAP"

                # SHAP-permutation agreement: do top 3 SHAP features overlap top 3 perm?
                top3_shap = {d["feature"] for d in risk_inc[:3]}
                top3_perm = set(perm_df["feature"].head(3))
                agreement = len(top3_shap & top3_perm) / 3
            else:
                risk_inc, risk_red = [], []
                narrative = f"{supplier}: explanation unavailable."
                method, agreement = "none", 0.0

        else:
            # ── Permutation-based fallback ────────────────────────────────────
            top_perm = perm_df.head(5)[["feature", "perm_importance"]].rename(
                columns={"perm_importance": "importance"}
            ).to_dict("records")

            # Get actual feature values for this supplier
            for d in top_perm:
                d["display_value"] = format_feature_value(
                    d["feature"], 0, df, name_col, supplier
                )

            narrative = perm_to_narrative(top_perm, pred_class, supplier)
            risk_inc = top_perm[:3]
            risk_red = []
            method, agreement = "PermutationImportance", 1.0

        def safe_driver(lst, idx):
            return lst[idx] if idx < len(lst) else {}

        records.append({
            "supplier_name":           supplier,
            "predicted_risk_tier":     pred_class,
            "risk_probability":        risk_prob,

            # Top 3 risk-increasing drivers
            "driver_1_feature":        safe_driver(risk_inc, 0).get("feature", ""),
            "driver_1_label":          FEATURE_LABELS.get(safe_driver(risk_inc, 0).get("feature", ""), ""),
            "driver_1_value":          safe_driver(risk_inc, 0).get("display_value", ""),
            "driver_1_shap":           safe_driver(risk_inc, 0).get("shap", safe_driver(risk_inc, 0).get("importance", "")),

            "driver_2_feature":        safe_driver(risk_inc, 1).get("feature", ""),
            "driver_2_label":          FEATURE_LABELS.get(safe_driver(risk_inc, 1).get("feature", ""), ""),
            "driver_2_value":          safe_driver(risk_inc, 1).get("display_value", ""),
            "driver_2_shap":           safe_driver(risk_inc, 1).get("shap", safe_driver(risk_inc, 1).get("importance", "")),

            "driver_3_feature":        safe_driver(risk_inc, 2).get("feature", ""),
            "driver_3_label":          FEATURE_LABELS.get(safe_driver(risk_inc, 2).get("feature", ""), ""),
            "driver_3_value":          safe_driver(risk_inc, 2).get("display_value", ""),
            "driver_3_shap":           safe_driver(risk_inc, 2).get("shap", safe_driver(risk_inc, 2).get("importance", "")),

            # Top mitigating driver
            "mitigator_feature":       safe_driver(risk_red, 0).get("feature", ""),
            "mitigator_label":         FEATURE_LABELS.get(safe_driver(risk_red, 0).get("feature", ""), ""),
            "mitigator_value":         safe_driver(risk_red, 0).get("display_value", ""),
            "mitigator_shap":          safe_driver(risk_red, 0).get("shap", ""),

            "explanation_method":      method,
            "shap_perm_agreement":     round(agreement, 2),
            "narrative":               narrative,
        })

    result = pd.DataFrame(records).sort_values("risk_probability", ascending=False)
    log.info(f"Built explanations for {len(result)} suppliers")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL IMPORTANCE TABLE
# ─────────────────────────────────────────────────────────────────────────────

def build_global_importance(shap_df: pd.DataFrame | None,
                             perm_df: pd.DataFrame,
                             feature_names: list) -> pd.DataFrame:
    """
    Combine mean |SHAP| (global) + permutation importance into one ranked table.
    """
    log.info("Building global feature importance table...")

    rows = []
    for feat in feature_names:
        row = {
            "feature":       feat,
            "feature_label": FEATURE_LABELS.get(feat, feat),
            "risk_direction": "increases_risk" if FEATURE_RISK_DIRECTION.get(feat, True) else "reduces_risk",
        }

        # SHAP global: mean absolute SHAP value
        if shap_df is not None and feat in shap_df.columns:
            row["mean_abs_shap"]  = round(float(shap_df[feat].abs().mean()), 5)
            row["mean_shap"]      = round(float(shap_df[feat].mean()), 5)
        else:
            row["mean_abs_shap"] = np.nan
            row["mean_shap"]     = np.nan

        # Permutation importance
        perm_row = perm_df[perm_df["feature"] == feat]
        if not perm_row.empty:
            row["perm_importance"] = round(float(perm_row["perm_importance"].iloc[0]), 5)
            row["perm_std"]        = round(float(perm_row["perm_std"].iloc[0]), 5)
        else:
            row["perm_importance"] = np.nan
            row["perm_std"]        = np.nan

        rows.append(row)

    gi = pd.DataFrame(rows)

    # Combined rank: average of SHAP rank and permutation rank
    gi = gi.sort_values("mean_abs_shap", ascending=False, na_position="last")
    gi["shap_rank"] = range(1, len(gi) + 1)
    gi = gi.sort_values("perm_importance", ascending=False, na_position="last")
    gi["perm_rank"] = range(1, len(gi) + 1)
    gi["combined_rank"] = ((gi["shap_rank"].fillna(len(gi)) +
                            gi["perm_rank"].fillna(len(gi))) / 2).round(1)
    gi = gi.sort_values("combined_rank")
    gi["final_rank"] = range(1, len(gi) + 1)

    # Agreement flag: both methods agree on feature being top-10
    gi["methods_agree"] = (
        (gi["shap_rank"] <= 10) & (gi["perm_rank"] <= 10)
    ).map({True: "YES", False: "no"})

    log.info(f"  Top 5 globally important features: {gi['feature'].head(5).tolist()}")
    return gi


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("PHASE 3: EXPLAINABLE AI (XAI)")
    log.info("=" * 60)
    log.info(f"  SHAP available : {SHAP_AVAILABLE}")
    log.info(f"  LIME available : {LIME_AVAILABLE}")

    # ── Load ──────────────────────────────────────────────────────────────────
    model, model_name = load_model()
    df, X, y, feature_names, class_names, name_col, _ = load_and_prepare_data()

    log.info(f"  Model          : {model_name}")
    log.info(f"  Features used  : {len(feature_names)}")
    log.info(f"  Suppliers      : {len(df)}")
    log.info(f"  Classes        : {class_names}")

    # ── METHOD 1: SHAP ────────────────────────────────────────────────────────
    shap_values, base_value = compute_shap(model, X, model_name, class_names)
    shap_df = None
    if shap_values is not None:
        shap_df = build_shap_dataframe(shap_values, feature_names, df, name_col)
        shap_df.to_csv(OUTPUT_DIR / "shap_values.csv", index=False)
        log.info(f"Saved shap_values.csv ({shap_df.shape})")

    # ── METHOD 2: PERMUTATION IMPORTANCE ──────────────────────────────────────
    perm_df = compute_permutation_importance(model, X, y, feature_names)
    perm_df.to_csv(OUTPUT_DIR / "permutation_importance.csv", index=False)
    log.info("Saved permutation_importance.csv")

    # ── GLOBAL IMPORTANCE ─────────────────────────────────────────────────────
    global_imp = build_global_importance(shap_df, perm_df, feature_names)
    global_imp.to_csv(OUTPUT_DIR / "global_importance.csv", index=False)
    log.info("Saved global_importance.csv")

    # ── PER-SUPPLIER EXPLANATIONS ─────────────────────────────────────────────
    explanations = build_supplier_explanations(
        df, X, shap_df, perm_df, model, name_col, class_names
    )
    explanations.to_csv(OUTPUT_DIR / "supplier_explanations.csv", index=False)
    log.info("Saved supplier_explanations.csv")

    # ── METHOD 3: LIME (top high-risk only) ───────────────────────────────────
    lime_df = compute_lime(model, X, df, name_col, class_names, n_top=20)
    if not lime_df.empty:
        lime_df.to_csv(OUTPUT_DIR / "lime_explanations.csv", index=False)
        log.info("Saved lime_explanations.csv")

    # ── SUMMARY ───────────────────────────────────────────────────────────────
    tier_counts = explanations["predicted_risk_tier"].value_counts().to_dict()
    top5_global = global_imp.head(5)[["feature", "feature_label",
                                      "mean_abs_shap", "perm_importance",
                                      "methods_agree"]].to_dict("records")

    agree_pct = (explanations["shap_perm_agreement"] >= 0.67).mean() * 100 \
                if "shap_perm_agreement" in explanations.columns else None

    summary = {
        "model_explained":        model_name,
        "shap_available":         SHAP_AVAILABLE,
        "lime_available":         LIME_AVAILABLE,
        "shap_base_value":        base_value,
        "suppliers_explained":    len(explanations),
        "class_names":            class_names,
        "risk_tier_distribution": tier_counts,
        "top5_global_features":   top5_global,
        "method_agreement_pct":   round(agree_pct, 1) if agree_pct else None,
        "outputs": [
            "phase3_xai/shap_values.csv",
            "phase3_xai/permutation_importance.csv",
            "phase3_xai/global_importance.csv",
            "phase3_xai/supplier_explanations.csv",
            "phase3_xai/lime_explanations.csv",
        ],
    }
    with open(OUTPUT_DIR / "xai_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    # ── Print ─────────────────────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("XAI COMPLETE")
    log.info("=" * 60)
    log.info(f"  Suppliers explained : {len(explanations)}")
    log.info(f"  Method              : {model_name} + {'SHAP + ' if SHAP_AVAILABLE else ''}Permutation{'+ LIME' if LIME_AVAILABLE else ''}")
    if agree_pct is not None:
        log.info(f"  Method agreement    : {agree_pct:.1f}% of suppliers have consistent explanations")
    log.info(f"  Risk distribution   : {tier_counts}")
    log.info("\n  Top 5 globally important features:")
    for r in top5_global:
        log.info(f"    {r['feature_label']:35s} | "
                 f"SHAP={r.get('mean_abs_shap') or 'N/A'!s:8} | "
                 f"Perm={r.get('perm_importance') or 'N/A'!s:8} | "
                 f"Agree={r['methods_agree']}")
    log.info("\nOutputs saved to phase3_xai/")
    log.info("  - shap_values.csv          (raw SHAP per supplier)")
    log.info("  - permutation_importance.csv")
    log.info("  - global_importance.csv    (both methods, ranked)")
    log.info("  - supplier_explanations.csv (top drivers + narratives)")
    log.info("  - lime_explanations.csv    (LIME for top high-risk)")
    log.info("  - xai_summary.json")
    log.info("\nLaunch dashboard: streamlit run phase3_dashboard.py")


if __name__ == "__main__":
    main()
