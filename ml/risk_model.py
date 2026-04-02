"""
SRSID  ml/risk_model.py
========================
Risk prediction model adapted for Postgres.

Changes from phase3_risk_prediction.py:
  - Reads feature matrix from reports/supplier_features.csv
    (written by ml/features.py)
  - Writes predictions to Postgres risk_scores table
  - All model training logic kept identical

Run:
    python ml/risk_model.py
"""

import sys, json, logging, warnings
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import joblib

warnings.filterwarnings("ignore")

from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.metrics import (classification_report, f1_score,
                              accuracy_score, roc_auc_score)
from sklearn.preprocessing import LabelEncoder
from sklearn.impute import SimpleImputer

try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import ML_CONFIG, PATHS
from db.db_client import DBClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/risk_model.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

MODEL_DIR  = PATHS["models"]
REPORT_DIR = PATHS["reports"]

FEATURE_COLS = [
    # Core performance
    "financial_stability", "delivery_performance",
    "supply_risk_score", "profit_impact_score",
    # Spend
    "total_annual_spend", "transaction_count",
    "spend_pct_of_portfolio",
    # Geo + Industry risk
    "geo_risk_numeric", "industry_risk_numeric",
    # News signals
    "news_sentiment_30d", "disruption_count_30d",
    # Delivery metrics (SAP-computed)
    "otif_rate", "avg_delay_days",
    "ottr_rate",              # On-Time To Request — added
    "lead_time_variability",  # STDDEV of delay days — added
    "order_accuracy_rate",    # actual/promised qty — added
    "avg_price_variance_pct", # PPV — added
    # Composite signals
    "performance_composite",
    "composite_risk_score",
    "delivery_risk_numeric",
    "spend_concentration_flag",
    "news_risk_flag",
]

TARGET_COL = "risk_label_3class"
RUN_ID     = datetime.now().strftime("%Y%m%d_%H%M%S")


# ─────────────────────────────────────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────────────────────────────────────

def load_features() -> pd.DataFrame:
    """Load feature matrix CSV written by ml/features.py."""
    path = REPORT_DIR / "supplier_features.csv"
    if not path.exists():
        log.error(f"Feature matrix not found at {path}")
        log.error("Run: python ml/features.py first")
        sys.exit(1)
    df = pd.read_csv(path)
    log.info(f"Loaded feature matrix: {len(df):,} rows × {len(df.columns)} cols")
    return df


def prepare_xy(df: pd.DataFrame) -> tuple:
    """Extract X, y and feature names — identical logic to original script."""
    available = [c for c in FEATURE_COLS if c in df.columns]
    log.info(f"Using {len(available)} features: {available}")

    X_raw = df[available].copy()
    # Drop columns that are all NaN
    X_raw = X_raw.dropna(axis=1, how="all")
    feature_names = list(X_raw.columns)

    # Impute + convert
    imputer = SimpleImputer(strategy="median")
    X = imputer.fit_transform(X_raw)

    # Target
    if TARGET_COL not in df.columns:
        log.error(f"Target column '{TARGET_COL}' not in features. Run ml/features.py first.")
        sys.exit(1)

    le = LabelEncoder()
    y  = le.fit_transform(df[TARGET_COL].fillna("Low"))
    class_names = list(le.classes_)

    log.info(f"Target distribution: {dict(zip(*np.unique(y, return_counts=True)))}")
    log.info(f"Class names: {class_names}")
    return X, y, feature_names, class_names, le, df


# ─────────────────────────────────────────────────────────────────────────────
# MODELS  (identical to phase3_risk_prediction.py)
# ─────────────────────────────────────────────────────────────────────────────

def train_random_forest(X_train, y_train) -> RandomForestClassifier:
    rf = RandomForestClassifier(
        n_estimators=ML_CONFIG["rf_n_estimators"],
        max_depth=None, min_samples_split=5,
        class_weight="balanced", random_state=ML_CONFIG["random_state"],
        n_jobs=-1,
    )
    rf.fit(X_train, y_train)
    return rf


def train_xgboost(X_train, y_train):
    model = xgb.XGBClassifier(
        n_estimators=ML_CONFIG["xgb_n_estimators"],
        learning_rate=ML_CONFIG["xgb_learning_rate"],
        max_depth=6, use_label_encoder=False,
        eval_metric="mlogloss", random_state=ML_CONFIG["random_state"],
    )
    model.fit(X_train, y_train)
    return model


def train_gradient_boosting(X_train, y_train) -> GradientBoostingClassifier:
    model = GradientBoostingClassifier(
        n_estimators=100, learning_rate=0.1,
        max_depth=4, random_state=ML_CONFIG["random_state"],
    )
    model.fit(X_train, y_train)
    return model


def evaluate_model(model, X_test, y_test, class_names) -> dict:
    y_pred = model.predict(X_test)
    report = classification_report(y_test, y_pred,
                                   target_names=class_names, output_dict=True)
    return {
        "accuracy":    round(accuracy_score(y_test, y_pred), 4),
        "f1_weighted": round(f1_score(y_test, y_pred, average="weighted"), 4),
        "report":      report,
    }


def get_feature_importance(model, feature_names: list) -> pd.DataFrame:
    if hasattr(model, "feature_importances_"):
        fi = pd.DataFrame({
            "feature":    feature_names,
            "importance": model.feature_importances_,
        }).sort_values("importance", ascending=False)
        fi["feature_label"] = fi["feature"].str.replace("_", " ").str.title()
        return fi
    return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# WRITE TO POSTGRES
# ─────────────────────────────────────────────────────────────────────────────

def write_predictions_to_postgres(df: pd.DataFrame,
                                   predictions: pd.DataFrame,
                                   best_name: str,
                                   best_eval: dict,
                                   db: DBClient):
    """Insert predictions into risk_scores table."""
    log.info("Writing predictions to Postgres risk_scores table...")

    records = []
    for _, row in predictions.iterrows():
        records.append({
            "vendor_id":           row.get("vendor_id", ""),
            "supplier_name":       row.get("supplier_name", ""),
            "run_id":              RUN_ID,
            "risk_probability":    float(row.get("risk_probability", 0.5)),
            "risk_label":          str(row.get("risk_tier", "Low")),
            "risk_label_3class":   str(row.get("risk_label_3class", "Low")),
            "predicted_tier":      str(row.get("risk_tier", "Low")),
            "composite_risk_score":float(row.get("composite_risk_score", 0.0)),
            "financial_stability": float(row.get("financial_stability", 60.0)),
            "delivery_performance":float(row.get("delivery_performance", 75.0)),
            "supply_risk_score":   float(row.get("supply_risk_score", 0.5)),
            "news_sentiment_30d":  float(row.get("news_sentiment_30d", 0.0)),
            "disruption_count_30d":int(row.get("disruption_count_30d", 0)),
            "model_type":          best_name,
            "model_version":       RUN_ID,
            "confidence":          float(row.get("risk_probability", 0.5)),
        })

    pred_df = pd.DataFrame(records)
    # Remove empty vendor_ids
    pred_df = pred_df[pred_df["vendor_id"].str.strip() != ""]

    rows = db.bulk_insert_df(pred_df, "risk_scores", if_exists="append")
    log.info(f"  Inserted {rows:,} rows into risk_scores")

    # Also update vendors.risk_label
    db.execute("""
        UPDATE vendors v
        SET risk_label = rs.risk_label
        FROM (
            SELECT DISTINCT ON (vendor_id) vendor_id, risk_label
            FROM risk_scores
            ORDER BY vendor_id, run_date DESC
        ) rs
        WHERE v.vendor_id = rs.vendor_id
    """)
    log.info("  Updated vendors.risk_label from latest predictions")

    # Save evaluation JSON
    eval_path = REPORT_DIR / "model_evaluation.json"
    with open(eval_path, "w") as f:
        json.dump({
            "run_id": RUN_ID, "best_model": best_name,
            "evaluation": best_eval,
        }, f, indent=2, default=str)
    log.info(f"  Saved model evaluation → {eval_path}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("SRSID Risk Prediction Model (Postgres)")
    log.info("=" * 60)

    df                                              = load_features()
    X, y, feature_names, class_names, le, df_full  = prepare_xy(df)

    # Check class distribution — stratify only works if every class has ≥2 members
    unique, counts = np.unique(y, return_counts=True)
    min_class_count = counts.min()
    use_stratify    = min_class_count >= 2

    if not use_stratify:
        small_classes = [class_names[i] for i, c in zip(unique, counts) if c < 2]
        log.warning(f"Classes with only 1 member: {small_classes}")
        log.warning("Disabling stratify — risk distribution is very skewed.")
        log.warning("Tip: run spend_analytics.py + re-run features.py to improve risk spread.")

    # Train / test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=ML_CONFIG["test_size"],
        random_state=ML_CONFIG["random_state"],
        stratify=y if use_stratify else None,
    )

    # Train models
    models, evaluations = {}, {}

    log.info("Training Random Forest...")
    rf = train_random_forest(X_train, y_train)
    models["RandomForest"]      = rf
    evaluations["RandomForest"] = evaluate_model(rf, X_test, y_test, class_names)

    if XGBOOST_AVAILABLE:
        log.info("Training XGBoost...")
        xgb_m = train_xgboost(X_train, y_train)
        models["XGBoost"]      = xgb_m
        evaluations["XGBoost"] = evaluate_model(xgb_m, X_test, y_test, class_names)
    else:
        log.info("Training Gradient Boosting (XGBoost not available)...")
        gb = train_gradient_boosting(X_train, y_train)
        models["GradientBoosting"]      = gb
        evaluations["GradientBoosting"] = evaluate_model(gb, X_test, y_test, class_names)

    # Select best
    best_name  = max(evaluations, key=lambda k: evaluations[k]["f1_weighted"])
    best_model = models[best_name]
    best_eval  = evaluations[best_name]
    log.info(f"Best model: {best_name}  F1={best_eval['f1_weighted']}")

    # Feature importance
    fi = get_feature_importance(best_model, feature_names)
    if not fi.empty:
        fi.to_csv(REPORT_DIR / "feature_importance.csv", index=False)
        log.info(f"Top 5 features: {fi['feature'].head(5).tolist()}")

    # Save models to disk
    MODEL_DIR.mkdir(exist_ok=True)
    joblib.dump(rf, MODEL_DIR / "risk_model_rf.pkl")
    if XGBOOST_AVAILABLE and "XGBoost" in models:
        joblib.dump(models["XGBoost"], MODEL_DIR / "risk_model_xgb.pkl")

    # Generate predictions for all vendors
    imputer = SimpleImputer(strategy="median")
    X_all   = imputer.fit_transform(
        df_full[[c for c in feature_names if c in df_full.columns]]
    )
    y_pred_all  = best_model.predict(X_all)
    y_proba_all = best_model.predict_proba(X_all)

    predictions = df_full[["vendor_id","supplier_name"]].copy()
    predictions["risk_tier"]          = le.inverse_transform(y_pred_all)
    predictions["risk_label_3class"]  = predictions["risk_tier"]
    predictions["risk_probability"]   = y_proba_all.max(axis=1).round(4)

    # Carry forward key feature values
    for col in ["composite_risk_score","financial_stability",
                "delivery_performance","supply_risk_score",
                "news_sentiment_30d","disruption_count_30d"]:
        if col in df_full.columns:
            predictions[col] = df_full[col].values

    # Write to Postgres
    with DBClient() as db:
        write_predictions_to_postgres(df_full, predictions,
                                       best_name, best_eval, db)

    log.info("\n" + "=" * 60)
    log.info("RISK MODEL COMPLETE")
    log.info("=" * 60)
    log.info(f"  Model       : {best_name}")
    log.info(f"  F1 score    : {best_eval['f1_weighted']}")
    log.info(f"  Predictions : {len(predictions):,} vendors")
    log.info(f"  Distribution: {predictions['risk_tier'].value_counts().to_dict()}")
    log.info("\n  Next: python ml/segmentation.py")


if __name__ == "__main__":
    main()
