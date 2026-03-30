# SRSID Phase 3 — Feature Engineering & ML Models

## 📁 Script Overview

```
phase3_scripts/
├── run_phase3.py                     ← Master runner (run this first!)
│
├── phase3_feature_engineering.py     ← Step 1: Build ML feature matrix
├── phase3_risk_prediction.py         ← Step 2: Classification (RF + XGBoost)
├── phase3_supplier_segmentation.py   ← Step 3: Clustering + Kraljic + ABC
├── phase3_disruption_forecasting.py  ← Step 4: Time series (Poisson + Prophet)
├── phase3_recommendation_anomaly.py  ← Step 5: Alternatives + Isolation Forest
└── phase3_dashboard.py               ← Step 6: Streamlit interactive dashboard
```

---

## ✅ Prerequisites

### Install required libraries
```bash
pip install pandas numpy scikit-learn joblib scipy streamlit plotly

# Optional (for better models):
pip install xgboost
pip install prophet
```

### Required input files (from Phase 1 & 2)
```
phase2_outputs/unified_supplier_master.csv    ← Primary input
phase1_tables/raw_supply_chain_transactions.csv
phase1_tables/disruptions_combined.csv        ← Or phase2_outputs/disruptions_mapped.csv
```

---

## 🚀 Quick Start

### Option A: Run everything at once
```bash
cd your_project_folder
python phase3_scripts/run_phase3.py
```

### Option B: Run individual steps
```bash
python phase3_scripts/run_phase3.py --step feature_engineering
python phase3_scripts/run_phase3.py --step risk_prediction
python phase3_scripts/run_phase3.py --step segmentation
python phase3_scripts/run_phase3.py --step forecasting
python phase3_scripts/run_phase3.py --step recommendation_anomaly
```

### Option C: Run scripts directly
```bash
python phase3_scripts/phase3_feature_engineering.py
python phase3_scripts/phase3_risk_prediction.py
# ... etc
```

### Launch dashboard
```bash
streamlit run phase3_scripts/phase3_dashboard.py
```

---

## 📊 Outputs

### Step 1: Feature Engineering → `phase3_features/`
| File | Description |
|------|-------------|
| `supplier_features.csv` | Full 25+ feature matrix for all 120 suppliers |
| `features_normalized.csv` | StandardScaler-normalized (ML-ready) |
| `correlation_matrix.csv` | Feature correlation for EDA |
| `feature_summary.json` | Statistics + risk distribution |

### Step 2: Risk Prediction → `phase3_risk_predictions/`
| File | Description |
|------|-------------|
| `risk_predictions.csv` | Per-supplier risk tier + probability score |
| `model_evaluation.json` | Accuracy, F1, AUC-ROC per model |
| `feature_importance.csv` | Top predictors of risk |
| `phase3_models/risk_model_rf.pkl` | Saved Random Forest model |
| `phase3_models/risk_model_xgb.pkl` | Saved XGBoost model |

### Step 3: Segmentation → `phase3_segmentation/`
| File | Description |
|------|-------------|
| `supplier_segments.csv` | All quadrant/cluster/ABC labels per supplier |
| `cluster_profiles.csv` | Cluster summary statistics |
| `abc_analysis.csv` | Pareto spend ranking |
| `segmentation_summary.json` | Silhouette scores + distribution |

### Step 4: Forecasting → `phase3_forecasting/`
| File | Description |
|------|-------------|
| `disruption_forecast.csv` | 30/60/90 day disruption probabilities |
| `portfolio_forecast.csv` | Monthly portfolio-level time series |
| `early_warning_alerts.csv` | Suppliers with >50% prob in 30 days |
| `forecast_summary.json` | Aggregate forecast metrics |

### Step 5: Recommendations + Anomalies
| File | Description |
|------|-------------|
| `phase3_recommendations/alternative_suppliers.csv` | Top 3 alternatives per at-risk supplier |
| `phase3_anomalies/anomaly_report.csv` | All anomaly detection flags |

---

## 🧠 ML Models Used

| Model | Type | Purpose |
|-------|------|---------|
| Random Forest | Classification | Risk tier prediction (High/Medium/Low) |
| XGBoost / GradientBoosting | Classification | Risk prediction (higher accuracy) |
| K-Means | Clustering | Supplier segmentation |
| Agglomerative | Clustering | Hierarchical supplier grouping |
| PCA | Dimensionality reduction | 2D cluster visualization |
| Poisson Rate Model | Statistical | Disruption probability per horizon |
| Prophet / LinearTrend | Time Series | Portfolio disruption forecast |
| Cosine Similarity | Recommendation | Alternative supplier matching |
| Isolation Forest | Anomaly Detection | Unusual supplier patterns |
| Z-Score | Anomaly Detection | Statistical outlier detection |

---

## ⚠️ Notes

- **Small dataset (<30 suppliers):** Cross-validation used instead of holdout split.
- **Missing columns:** All scripts auto-fill missing features with sensible defaults — you won't get crashes.
- **XGBoost optional:** Falls back to sklearn GradientBoosting if not installed.
- **Prophet optional:** Falls back to linear trend if not installed.
- **Additional transactional data:** Drop `raw_transactions.parquet` or `transactions.csv` in `phase1_tables/` and re-run feature engineering — spend features will auto-populate.

---

## 📈 Expected Performance

| Metric | Expected Range |
|--------|---------------|
| Risk Model Accuracy | 75–92% |
| F1 (weighted) | 0.72–0.90 |
| Silhouette Score | 0.25–0.60 |
| Early Warning Precision | ~70% |

*Improves significantly with additional transactional data.*

---

**Phase 3 → ML Complete | Ready for Phase 4: Dashboard Polish & Deployment** 🚀
