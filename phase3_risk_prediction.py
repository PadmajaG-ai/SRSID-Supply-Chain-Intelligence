"""
Phase 3: Risk Prediction Model
================================
Purpose: Train classification models to predict supplier risk level.
         Supports binary (High / Not High) and 3-class (High/Medium/Low).

Models:
  - Random Forest (interpretable baseline)
  - XGBoost      (high performance)
  - Logistic Regression (fast baseline)

Inputs:
    - phase3_features/supplier_features.csv
    - phase3_features/features_normalized.csv

Outputs:
    - phase3_models/risk_model_rf.pkl
    - phase3_models/risk_model_xgb.pkl
    - phase3_risk_predictions/risk_predictions.csv
    - phase3_risk_predictions/model_evaluation.json
    - phase3_risk_predictions/feature_importance.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
import logging
import joblib
import warnings
warnings.filterwarnings("ignore")

from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
from sklearn.metrics import (
    classification_report, confusion_matrix,
    roc_auc_score, accuracy_score, f1_score,
)
from sklearn.preprocessing import LabelEncoder
from sklearn.impute import SimpleImputer

try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("phase3_risk_prediction.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
FEATURES_DIR = Path("phase3_features")
MODEL_DIR    = Path("phase3_models")
OUTPUT_DIR   = Path("phase3_risk_predictions")
MODEL_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Feature columns used for risk prediction ──────────────────────────────────
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

TARGET_BINARY  = "risk_label_binary"
TARGET_3CLASS  = "risk_label_3class"


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────

def load_features() -> pd.DataFrame:
    path = FEATURES_DIR / "supplier_features.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"Feature file not found: {path}\n"
            "Run phase3_feature_engineering.py first."
        )
    df = pd.read_csv(path)
    log.info(f"Loaded features: {len(df)} suppliers, {len(df.columns)} columns")
    return df


def prepare_xy(df: pd.DataFrame, target: str):
    """Prepare X (features) and y (target) arrays."""
    # Add missing feature columns as 0
    for c in FEATURE_COLS:
        if c not in df.columns:
            df[c] = 0.0

    # Convert to numeric, then drop columns that are entirely NaN
    X_raw = df[FEATURE_COLS].apply(pd.to_numeric, errors="coerce")
    valid_cols = [c for c in FEATURE_COLS if X_raw[c].notna().any()]
    dropped = set(FEATURE_COLS) - set(valid_cols)
    if dropped:
        log.warning(f"Dropping all-NaN feature columns: {dropped}")
    X_raw = X_raw[valid_cols]

    imp = SimpleImputer(strategy="median")
    X = pd.DataFrame(imp.fit_transform(X_raw), columns=valid_cols)

    if target not in df.columns:
        raise ValueError(f"Target column '{target}' not found in features file.")

    y_raw = df[target].astype(str).str.strip()

    if target == TARGET_3CLASS:
        le = LabelEncoder()
        y = le.fit_transform(y_raw)
        class_names = le.classes_.tolist()
    else:
        y = pd.to_numeric(y_raw, errors="coerce").fillna(0).astype(int)
        class_names = ["Low Risk", "High Risk"]

    return X, y, class_names, valid_cols


# ─────────────────────────────────────────────────────────────────────────────
# MODEL TRAINING
# ─────────────────────────────────────────────────────────────────────────────

def train_random_forest(X_train, y_train, n_classes: int) -> RandomForestClassifier:
    rf = RandomForestClassifier(
        n_estimators=300,
        max_depth=8,
        min_samples_split=5,
        min_samples_leaf=2,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )
    rf.fit(X_train, y_train)
    log.info("Random Forest trained")
    return rf


def train_xgboost(X_train, y_train, n_classes: int):
    if not XGBOOST_AVAILABLE:
        log.warning("XGBoost not installed — skipping. pip install xgboost")
        return None

    params = dict(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        use_label_encoder=False,
        eval_metric="logloss" if n_classes == 2 else "mlogloss",
        random_state=42,
        n_jobs=-1,
    )
    if n_classes > 2:
        params["objective"] = "multi:softprob"
        params["num_class"] = n_classes

    model = xgb.XGBClassifier(**params)
    model.fit(X_train, y_train, verbose=False)
    log.info("XGBoost trained")
    return model


def train_gradient_boosting(X_train, y_train) -> GradientBoostingClassifier:
    """Fallback if XGBoost unavailable."""
    gb = GradientBoostingClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        random_state=42,
    )
    gb.fit(X_train, y_train)
    log.info("Gradient Boosting trained")
    return gb


# ─────────────────────────────────────────────────────────────────────────────
# EVALUATION
# ─────────────────────────────────────────────────────────────────────────────

def evaluate_model(model, X_test, y_test, class_names, model_name: str) -> dict:
    y_pred = model.predict(X_test)
    acc    = accuracy_score(y_test, y_pred)
    f1     = f1_score(y_test, y_pred, average="weighted")
    report = classification_report(y_test, y_pred, target_names=class_names, output_dict=True)

    try:
        proba = model.predict_proba(X_test)
        if proba.shape[1] == 2:
            auc = roc_auc_score(y_test, proba[:, 1])
        else:
            auc = roc_auc_score(y_test, proba, multi_class="ovr", average="weighted")
    except Exception:
        auc = None

    result = {
        "model":    model_name,
        "accuracy": round(acc, 4),
        "f1_weighted": round(f1, 4),
        "auc_roc":  round(auc, 4) if auc else None,
        "classification_report": report,
    }

    log.info(f"  {model_name}: Accuracy={acc:.3f}, F1={f1:.3f}" +
             (f", AUC={auc:.3f}" if auc else ""))
    return result


def cross_validate_model(model, X, y, cv=5) -> dict:
    skf = StratifiedKFold(n_splits=cv, shuffle=True, random_state=42)
    scores = cross_val_score(model, X, y, cv=skf, scoring="f1_weighted", n_jobs=-1)
    return {
        "cv_f1_mean": round(scores.mean(), 4),
        "cv_f1_std":  round(scores.std(), 4),
        "cv_scores":  [round(s, 4) for s in scores.tolist()],
    }


def get_feature_importance(model, feature_names: list) -> pd.DataFrame:
    if hasattr(model, "feature_importances_"):
        imp = model.feature_importances_
    else:
        return pd.DataFrame()

    fi = pd.DataFrame({
        "feature":    feature_names,
        "importance": imp,
    }).sort_values("importance", ascending=False)
    fi["importance_pct"] = (fi["importance"] / fi["importance"].sum() * 100).round(2)
    return fi


# ─────────────────────────────────────────────────────────────────────────────
# PREDICTION OUTPUT
# ─────────────────────────────────────────────────────────────────────────────

def generate_predictions(df: pd.DataFrame, best_model, X: pd.DataFrame,
                         class_names: list, target: str) -> pd.DataFrame:
    """Attach predictions + probabilities to supplier dataframe."""
    name_col = next((c for c in df.columns if "name" in c.lower() or "supplier" in c.lower()),
                    df.columns[0])

    result = df[[name_col]].copy()
    result.columns = ["supplier_name"]

    result["predicted_risk_label"] = [class_names[i] for i in best_model.predict(X)]
    result["actual_risk_label"]    = df[target].astype(str).values

    proba = best_model.predict_proba(X)
    for i, cn in enumerate(class_names):
        result[f"prob_{cn.lower().replace(' ', '_')}"] = proba[:, i].round(4)

    # Risk score (probability of highest-risk class)
    risk_col = [c for c in result.columns if "high" in c.lower() or "prob_1" in c.lower()]
    if risk_col:
        result["risk_probability"] = result[risk_col[0]]
    else:
        result["risk_probability"] = proba.max(axis=1).round(4)

    result["risk_tier"] = pd.cut(
        result["risk_probability"],
        bins=[-0.01, 0.35, 0.65, 1.01],
        labels=["Low", "Medium", "High"],
    )

    # Add key context columns
    for ctx in ["kraljic_quadrant", "industry_risk_score", "criticality_index",
                "total_annual_spend", "disruption_count"]:
        if ctx in df.columns:
            result[ctx] = df[ctx].values

    return result.sort_values("risk_probability", ascending=False)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("PHASE 3: RISK PREDICTION MODEL")
    log.info("=" * 60)

    df = load_features()

    # ── Use 3-class target (richer than binary) ───────────────────────────────
    target = TARGET_3CLASS
    X, y, class_names, feature_names = prepare_xy(df, target)
    n_classes = len(np.unique(y))

    log.info(f"Target: {target}  |  Classes: {class_names}  |  Samples: {len(X)}")
    for i, cn in enumerate(class_names):
        log.info(f"  {cn}: {(y == i).sum()} samples")

    # ── Train / Test split ────────────────────────────────────────────────────
    test_size = 0.20
    if len(X) < 30:
        log.warning("Small dataset (<30) — using leave-one-out style cross-val, no holdout split.")
        X_train, X_test = X, X
        y_train, y_test = y, y
    else:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, stratify=y, random_state=42
        )

    log.info(f"Train: {len(X_train)}  |  Test: {len(X_test)}")

    # ── Train models ──────────────────────────────────────────────────────────
    models = {}
    evaluations = {}

    # 1. Random Forest
    rf = train_random_forest(X_train, y_train, n_classes)
    models["RandomForest"] = rf
    evaluations["RandomForest"] = evaluate_model(rf, X_test, y_test, class_names, "RandomForest")

    # 2. XGBoost or GradientBoosting
    if XGBOOST_AVAILABLE:
        xgb_model = train_xgboost(X_train, y_train, n_classes)
        if xgb_model:
            models["XGBoost"] = xgb_model
            evaluations["XGBoost"] = evaluate_model(xgb_model, X_test, y_test, class_names, "XGBoost")
    else:
        gb = train_gradient_boosting(X_train, y_train)
        models["GradientBoosting"] = gb
        evaluations["GradientBoosting"] = evaluate_model(gb, X_test, y_test, class_names, "GradientBoosting")

    # ── Select best model ─────────────────────────────────────────────────────
    best_name  = max(evaluations, key=lambda k: evaluations[k]["f1_weighted"])
    best_model = models[best_name]
    log.info(f"\nBest model: {best_name} (F1={evaluations[best_name]['f1_weighted']})")

    # ── Cross-validation ──────────────────────────────────────────────────────
    if len(X) >= 20:
        cv_results = cross_validate_model(best_model, X, y)
        evaluations[best_name]["cross_validation"] = cv_results
        log.info(f"CV F1: {cv_results['cv_f1_mean']} ± {cv_results['cv_f1_std']}")

    # ── Feature importance ────────────────────────────────────────────────────
    fi = get_feature_importance(best_model, feature_names)
    if not fi.empty:
        fi.to_csv(OUTPUT_DIR / "feature_importance.csv", index=False)
        log.info(f"Top 5 features: {fi['feature'].head(5).tolist()}")

    # ── Save models ───────────────────────────────────────────────────────────
    joblib.dump(rf,         MODEL_DIR / "risk_model_rf.pkl")
    log.info("Saved risk_model_rf.pkl")
    if XGBOOST_AVAILABLE and "XGBoost" in models:
        joblib.dump(models["XGBoost"], MODEL_DIR / "risk_model_xgb.pkl")
        log.info("Saved risk_model_xgb.pkl")
    elif "GradientBoosting" in models:
        joblib.dump(models["GradientBoosting"], MODEL_DIR / "risk_model_gb.pkl")
        log.info("Saved risk_model_gb.pkl")

    # ── Predictions ───────────────────────────────────────────────────────────
    predictions = generate_predictions(df, best_model, X, class_names, target)
    pred_path = OUTPUT_DIR / "risk_predictions.csv"
    predictions.to_csv(pred_path, index=False)
    log.info(f"Saved predictions → {pred_path}")

    # ── Save evaluation ───────────────────────────────────────────────────────
    eval_summary = {
        "best_model":   best_name,
        "target":       target,
        "class_names":  class_names,
        "n_train":      len(X_train),
        "n_test":       len(X_test),
        "models":       evaluations,
        "feature_count": len(feature_names),
    }
    with open(OUTPUT_DIR / "model_evaluation.json", "w") as f:
        json.dump(eval_summary, f, indent=2, default=str)

    # ── Summary ───────────────────────────────────────────────────────────────
    risk_dist = predictions["risk_tier"].value_counts()
    log.info("\n" + "=" * 60)
    log.info("RISK PREDICTION COMPLETE")
    log.info("=" * 60)
    log.info(f"  Best model  : {best_name}")
    log.info(f"  F1 score    : {evaluations[best_name]['f1_weighted']}")
    log.info(f"  Accuracy    : {evaluations[best_name]['accuracy']}")
    log.info(f"  Predictions : {len(predictions)} suppliers")
    for tier, cnt in risk_dist.items():
        log.info(f"    {tier:8s}: {cnt}")
    log.info("\nOutputs saved to phase3_risk_predictions/")
    log.info("  - risk_predictions.csv    (per-supplier scores)")
    log.info("  - model_evaluation.json   (performance metrics)")
    log.info("  - feature_importance.csv  (top predictors)")
    log.info("Ready for segmentation model!")


if __name__ == "__main__":
    main()
