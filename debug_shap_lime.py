"""
Deep diagnostic — find out exactly why SHAP/LIME are showing NaN.
Run: python debug_shap_lime.py
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import joblib

sys.path.insert(0, str(Path(__file__).parent))
from db.db_client import DBClient
from config import PATHS

MODEL_DIR  = PATHS["models"]
REPORT_DIR = PATHS["reports"]

print("\n" + "=" * 60)
print("DEEP DIAGNOSTIC — Why SHAP/LIME values are NaN")
print("=" * 60)

# 1. Check SHAP and LIME installations
print("\n1. Package availability:")
try:
    import shap
    print(f"   SHAP version: {shap.__version__}")
except ImportError as e:
    print(f"   ERROR: shap not importable - {e}")

try:
    from lime.lime_tabular import LimeTabularExplainer
    import lime
    version = getattr(lime, "__version__", "installed (version unknown)")
    print(f"   LIME: {version}")
except ImportError as e:
    print(f"   ERROR: lime not importable - {e}")

# 2. Check model pkl file
print("\n2. Saved model file:")
for f in ["risk_model_xgb.pkl", "risk_model_rf.pkl", "risk_model_gb.pkl"]:
    p = MODEL_DIR / f
    if p.exists():
        model = joblib.load(p)
        n_features = getattr(model, "n_features_in_", "unknown")
        print(f"   {f}: loaded, n_features_in_ = {n_features}")
        print(f"   model classes: {getattr(model, 'classes_', 'unknown')}")
        if hasattr(model, "feature_importances_"):
            print(f"   has feature_importances_: YES "
                  f"(length {len(model.feature_importances_)})")
        break
else:
    print("   NO MODEL FILE FOUND")
    sys.exit(1)

# 3. Check feature matrix
print("\n3. Feature matrix (supplier_features.csv):")
feat_path = REPORT_DIR / "supplier_features.csv"
if feat_path.exists():
    df = pd.read_csv(feat_path)
    print(f"   Rows: {len(df)}")
    print(f"   Columns: {len(df.columns)}")
    # How many numeric columns?
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    print(f"   Numeric columns: {len(numeric_cols)}")
else:
    print("   NOT FOUND — run features.py first")
    sys.exit(1)

# 4. Check for the CRITICAL mismatch
print("\n4. CRITICAL CHECK — Model vs Feature Matrix:")
if n_features != "unknown":
    if n_features == len(numeric_cols):
        print(f"   OK: model expects {n_features}, CSV has {len(numeric_cols)} numeric cols")
    else:
        print(f"   ❌ MISMATCH: model expects {n_features} features, "
              f"CSV has {len(numeric_cols)} numeric cols")
        print(f"   This is the root cause — SHAP can't run!")
        print(f"\n   FIX: Retrain the model with current features:")
        print(f"       python ml/risk_model.py")

# 5. Check what's actually in the explanations table
print("\n5. Actual contents of explanations table:")
with DBClient() as db:
    row = db.fetch_one("SELECT * FROM explanations LIMIT 1")
    if row:
        print(f"   First row sample (key fields):")
        for k in ["vendor_id", "predicted_risk_tier",
                  "driver_1_label", "driver_1_shap", "driver_1_value",
                  "lime_driver_1_label", "lime_driver_1_weight",
                  "narrative"]:
            val = row.get(k)
            print(f"     {k:25}: {val!r}")
    else:
        print("   Empty!")

    # Count rows where SHAP ran successfully
    counts = db.fetch_one("""
        SELECT
            COUNT(*) AS total,
            COUNT(driver_1_label) AS has_shap_label,
            COUNT(CASE WHEN driver_1_shap IS NOT NULL AND driver_1_shap != 'NaN'
                       AND driver_1_shap <> 0 THEN 1 END) AS has_real_shap,
            COUNT(lime_driver_1_label) AS has_lime_label,
            COUNT(CASE WHEN lime_driver_1_weight IS NOT NULL
                       AND lime_driver_1_weight <> 0 THEN 1 END) AS has_real_lime
        FROM explanations
    """)
    print(f"\n6. Row counts:")
    print(f"   Total rows          : {counts['total']}")
    print(f"   Has SHAP label      : {counts['has_shap_label']}")
    print(f"   Has real SHAP value : {counts['has_real_shap']}")
    print(f"   Has LIME label      : {counts['has_lime_label']}")
    print(f"   Has real LIME value : {counts['has_real_lime']}")

print("\n" + "=" * 60)
print("DIAGNOSIS COMPLETE")
print("=" * 60)
print()
