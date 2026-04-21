"""
Full end-to-end diagnostic.
Shows exactly what's happening at every step of the SHAP/LIME pipeline.
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import joblib

sys.path.insert(0, str(Path(__file__).parent))
from config import PATHS

MODEL_DIR  = PATHS["models"]
REPORT_DIR = PATHS["reports"]

print("=" * 70)
print("FULL DIAGNOSTIC — What's actually happening with SHAP/LIME")
print("=" * 70)

# ── 1. What does the MODEL say it expects? ──────────────────────────────────
print("\n[1] THE TRAINED MODEL (.pkl file)")
print("-" * 70)

model_path = MODEL_DIR / "risk_model_xgb.pkl"
if not model_path.exists():
    model_path = MODEL_DIR / "risk_model_rf.pkl"
model = joblib.load(model_path)
print(f"File: {model_path.name}")
print(f"Type: {type(model).__name__}")

n_feat = getattr(model, "n_features_in_", None)
print(f"model.n_features_in_ = {n_feat}")

# Get the feature names the model was trained with
feat_names_model = getattr(model, "feature_names_in_", None)
if feat_names_model is None:
    # XGBoost stores them differently
    feat_names_model = getattr(model, "feature_names", None)
if feat_names_model is None and hasattr(model, "get_booster"):
    feat_names_model = model.get_booster().feature_names

if feat_names_model is not None:
    print(f"Model was trained on these features:")
    for i, f in enumerate(feat_names_model, 1):
        print(f"  {i:2}. {f}")
else:
    print("Model does NOT store feature names — will match by column order.")

# ── 2. What's in the CSV? ────────────────────────────────────────────────────
print("\n[2] THE FEATURE CSV (supplier_features.csv)")
print("-" * 70)

df = pd.read_csv(REPORT_DIR / "supplier_features.csv")
print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
print(f"Numeric columns ({len(numeric_cols)}):")
for i, c in enumerate(numeric_cols, 1):
    print(f"  {i:2}. {c}")

# ── 3. What does explainability.py say it uses? ──────────────────────────────
print("\n[3] EXPLAINABILITY.PY's FEATURE_COLS")
print("-" * 70)

# Import the list as the script would
try:
    sys.path.insert(0, str(Path(__file__).parent / "ml"))
    # We cannot just import it cleanly because it runs main, so extract via regex
    import re
    src = open(Path(__file__).parent / "ml" / "explainability.py",
               encoding="utf-8").read()
    match = re.search(r'FEATURE_COLS\s*=\s*\[(.*?)\]', src, re.DOTALL)
    if match:
        feats_expl = re.findall(r'"(\w+)"', match.group(1))
        print(f"Count: {len(feats_expl)}")
        for i, f in enumerate(feats_expl, 1):
            in_csv = "OK" if f in numeric_cols else "FAIL MISSING FROM CSV"
            print(f"  {i:2}. {f:<30} {in_csv}")
except Exception as e:
    print(f"Could not read FEATURE_COLS: {e}")

# ── 4. Simulate exactly what explainability.py does ──────────────────────────
print("\n[4] SIMULATING THE PREDICT CALL")
print("-" * 70)

feat_cols = [c for c in feats_expl if c in df.columns]
print(f"Step 1: filter FEATURE_COLS to those in CSV → {len(feat_cols)} features")

from sklearn.impute import SimpleImputer
X_raw = df[feat_cols].copy()
imputer = SimpleImputer(strategy="median")
X = imputer.fit_transform(X_raw)
print(f"Step 2: X.shape after imputation = {X.shape}")
print(f"Step 3: model expects n_features = {n_feat}")

if n_feat == X.shape[1]:
    print("OK Shapes MATCH — SHAP should work")
    try:
        pred = model.predict(X[:5])
        print(f"OK model.predict worked: sample preds = {pred}")
    except Exception as e:
        print(f"FAIL model.predict FAILED: {e}")
else:
    print(f"FAIL MISMATCH: model={n_feat}, X={X.shape[1]}")
    print()
    print("FEATURES THE MODEL WAS TRAINED ON BUT EXPLAINABILITY SKIPS:")
    if feat_names_model is not None:
        missing = set(feat_names_model) - set(feat_cols)
        for f in missing:
            in_csv = "(is in CSV)" if f in df.columns else "(NOT in CSV either)"
            print(f"  - {f} {in_csv}")
    print()
    print("FEATURES EXPLAINABILITY USES BUT MODEL DOESN'T KNOW:")
    if feat_names_model is not None:
        extra = set(feat_cols) - set(feat_names_model)
        for f in extra:
            print(f"  - {f}")

print("\n" + "=" * 70)
print("DIAGNOSIS")
print("=" * 70)

if n_feat == X.shape[1]:
    print("Everything aligned. If SHAP still fails, there's another issue.")
else:
    print(f"Model trained on {n_feat} features.")
    print(f"Explainability sends {X.shape[1]} features.")
    print()
    print("THE FIX depends on which features are missing:")
    print("  - If model features aren't in CSV → run: python ml/features.py")
    print("  - If explainability's FEATURE_COLS is out of sync → need to align")
    print("  - Easiest universal fix: rebuild CSV then retrain:")
    print("      python ml/features.py")
    print("      python ml/risk_model.py")
    print("      python ml/explainability.py")
