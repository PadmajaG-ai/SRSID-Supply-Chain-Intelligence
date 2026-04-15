"""
SRSID Configuration
====================
Single source of truth for all settings across the entire project.
Import this in every script instead of hardcoding values.

Usage:
    from config import DB_URL, SAP_PATH, RISK_THRESHOLDS
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file if present (for local development)
load_dotenv()


def _get_db_config() -> dict:
    """
    Read DB credentials in priority order:
    1. Streamlit secrets (when running on Streamlit Cloud)
    2. Environment variables / .env file (local development)
    3. Hardcoded defaults (local development fallback)
    """
    try:
        import streamlit as st
        if hasattr(st, "secrets") and "database" in st.secrets:
            s = st.secrets["database"]
            return {
                "host":     s.get("DB_HOST",     "localhost"),
                "port":     int(s.get("DB_PORT", "5432")),
                "database": s.get("DB_NAME",     "srsid_db"),
                "user":     s.get("DB_USER",     "srsid_user"),
                "password": s.get("DB_PASSWORD", "srsid_pass123"),
            }
    except Exception:
        pass  # Not running in Streamlit context — use env vars

    return {
        "host":     os.getenv("DB_HOST",     "localhost"),
        "port":     int(os.getenv("DB_PORT", "5432")),
        "database": os.getenv("DB_NAME",     "srsid_db"),
        "user":     os.getenv("DB_USER",     "srsid_user"),
        "password": os.getenv("DB_PASSWORD", "srsid_pass123"),
    }


DB_CONFIG = _get_db_config()

# Full connection URL (used by psycopg2)
DB_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# ─────────────────────────────────────────────────────────────────────────────
# PROJECT PATHS
# ─────────────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent

PATHS = {
    "reports":      PROJECT_ROOT / "reports",
    "models":       PROJECT_ROOT / "models",
    "logs":         PROJECT_ROOT / "logs",
    "phase3_xai":   PROJECT_ROOT / "phase3_xai",
}

for p in PATHS.values():
    p.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# SAP DATASET
# ─────────────────────────────────────────────────────────────────────────────

SAP_PATH = Path(
    r"C:\Users\Reethu\.cache\kagglehub\datasets"
    r"\mustafakeser4\sap-dataset-bigquery-dataset\versions\1"
)

KAGGLE_DATASET = "mustafakeser4/sap-dataset-bigquery-dataset"

# ─────────────────────────────────────────────────────────────────────────────
# NEWS API KEYS
# ─────────────────────────────────────────────────────────────────────────────

NEWS_APIS = {
    "newsapi_key":  os.getenv("NEWSAPI_KEY",  ""),
    "guardian_key": os.getenv("GUARDIAN_KEY", ""),
    # GDELT is free — no key needed
}

NEWS_LOOKBACK_DAYS    = 30    # How many days back to fetch news
MAX_ARTICLES_PER_VENDOR = 5   # Per source per vendor

# ─────────────────────────────────────────────────────────────────────────────
# RISK SCORING THRESHOLDS
# ─────────────────────────────────────────────────────────────────────────────

RISK_THRESHOLDS = {
    # Supply risk score bands (0-1)
    # Calibrated for this SAP dataset where most vendors are US/DE
    # (low geo risk) and OTIF data is partial — scores cluster 0.20–0.45
    "high":   0.45,    # >= 0.45 → High Risk   (was 0.65 — too strict)
    "medium": 0.25,    # 0.25–0.45 → Medium Risk (was 0.35 — too strict)
    # < 0.25 → Low Risk

    # Composite risk score weights
    "weights": {
        "geo_risk":           0.30,
        "industry_risk":      0.25,
        "delivery_risk":      0.25,
        "concentration_risk": 0.20,
    },
}

# Industry risk levels (SAP BRSCH codes → risk score 0-1)
INDUSTRY_RISK = {
    "Electronics & Semiconductors": 0.85,
    "Chemicals":                    0.75,
    "Automotive":                   0.70,
    "Pharma & Life Sciences":       0.65,
    "Energy & Utilities":           0.60,
    "Metals":                       0.60,
    "Food & Beverage":              0.55,
    "Logistics & Transport":        0.55,
    "IT Services":                  0.40,
    "Consulting":                   0.30,
    "Wholesale & Retail":           0.35,
    "Financial Services":           0.35,
    "Manufacturing":                0.55,
    "Agriculture":                  0.50,
}

# ── Sanctions list (legal compliance — binary, not a risk score) ─────────────
# Based on OFAC SDN list + EU restrictive measures as of 2024.
# These countries have active trade/procurement restrictions.
# This is a LEGAL FACT, not a geopolitical judgment.
SANCTIONED_COUNTRIES = {
    "RU",   # Russia  — OFAC + EU sanctions (2022–)
    "IR",   # Iran    — OFAC comprehensive sanctions
    "KP",   # North Korea — OFAC comprehensive sanctions
    "BY",   # Belarus — OFAC + EU sanctions (2020–)
    "CU",   # Cuba    — OFAC embargo
    "SY",   # Syria   — OFAC + EU sanctions
    "SD",   # Sudan   — OFAC sanctions
    "MM",   # Myanmar — OFAC targeted sanctions (2021–)
}

# ── Country supply chain risk (0–1, evidence-based) ──────────────────────────
# Sources:
#   World Bank Logistics Performance Index (LPI) 2023
#   EM-DAT International Disaster Database (disaster frequency)
#   Euler Hermes Country Risk Ratings 2023
#   WEF Global Competitiveness Report (infrastructure scores)
#
# Score meaning:
#   0.0–0.25  → Very reliable supply chains, strong infrastructure
#   0.25–0.45 → Good logistics, manageable risks
#   0.45–0.65 → Moderate disruption risk, some infrastructure gaps
#   0.65–0.85 → High disruption risk, weak infrastructure or instability
#   0.85–1.0  → Very high risk, conflict/disaster prone
#
# NOTE: Taiwan (TW) scores 0.30 — strong semiconductor logistics,
#   geopolitical tension is a separate political risk not a supply chain metric.
# NOTE: India (IN) scores 0.35 — large stable supplier base, improving LPI.
# NOTE: Germany (DE) scores 0.20 — despite 2022 energy crisis, logistics
#   infrastructure remains world-class.

COUNTRY_SUPPLY_RISK = {
    # ── Tier 1: Very reliable (0.10–0.25) ─────────────────────────────────
    "DE": 0.12,   # World-class logistics, strong contracts  (LPI rank 3)
    "NL": 0.12,   # Rotterdam hub, excellent port + customs  (LPI rank 6)
    "SG": 0.12,   # Best-in-class Asian logistics hub        (LPI rank 1)
    "CH": 0.15,   # Stable, strong IP + contract enforcement
    "AT": 0.15,   # Central Europe hub, reliable
    "SE": 0.15,   # Strong LPI, low disaster risk
    "DK": 0.15,   # Strong LPI, low disaster risk
    "FI": 0.15,   # Strong LPI
    "NO": 0.15,   # Stable, reliable
    "BE": 0.15,   # Antwerp port, strong logistics
    "JP": 0.20,   # Excellent logistics (LPI rank 2), moderate seismic risk
    "CA": 0.20,   # Strong logistics, some weather risk
    "AU": 0.20,   # Strong logistics, remote geography adds cost not risk
    "GB": 0.22,   # Post-Brexit customs friction, otherwise strong
    "FR": 0.22,   # Strong LPI, periodic strike disruptions
    "US": 0.25,   # Good LPI, hurricane/weather risk, labour disputes

    # ── Tier 2: Good logistics, manageable risks (0.25–0.45) ──────────────
    "KR": 0.25,   # Strong manufacturing + logistics         (LPI rank 21)
    "TW": 0.30,   # Critical tech supplier, good logistics. Geopolitical
                  # tension is a political risk — NOT supply chain quality
    "PL": 0.28,   # Strong EU logistics corridor
    "CZ": 0.28,   # Reliable Central Europe manufacturing
    "HU": 0.30,   # Good EU logistics access
    "ES": 0.28,   # Strong port infrastructure
    "IT": 0.32,   # Good logistics, some bureaucratic friction
    "PT": 0.30,   # Improving LPI
    "CN": 0.35,   # Scale + efficiency offset by zero-Covid legacy disruptions,
                  # concentration risk if single-source
    "MY": 0.32,   # Strong Southeast Asian logistics hub
    "IL": 0.30,   # Strong logistics, conflict exposure flagged separately
    "IN": 0.38,   # Large stable base, improving LPI (rank 38), some
                  # infrastructure gaps in tier-2/3 cities
    "TH": 0.38,   # Regional hub, flood risk (2011 disruption)
    "MX": 0.40,   # USMCA access, port congestion risk, security variance
    "RO": 0.35,   # Improving EU-standard logistics
    "TR": 0.42,   # Strategic hub, currency + political volatility
    "ZA": 0.45,   # Best African logistics, power outage disruptions

    # ── Tier 3: Moderate risk (0.45–0.65) ─────────────────────────────────
    "BR": 0.48,   # Large supplier base, infrastructure gaps, port delays
    "VN": 0.42,   # Rapidly improving, strong manufacturing growth
    "ID": 0.50,   # Archipelago logistics complexity, disaster frequency
    "EG": 0.52,   # Suez dependency, currency instability
    "PH": 0.55,   # Typhoon risk, island logistics complexity
    "NG": 0.60,   # Infrastructure gaps, security variance
    "BD": 0.55,   # Flood risk, port congestion, improving logistics
    "KZ": 0.50,   # Landlocked, improving corridor links
    "GH": 0.50,   # Relatively stable West Africa logistics

    # ── Tier 4: High disruption risk (0.65–0.85) ──────────────────────────
    "PK": 0.68,   # Flood risk (2022 catastrophic), political instability
    "UA": 0.80,   # Active conflict (2022–), major disruption
    "ET": 0.65,   # Improving but infrastructure gaps + regional conflict
    "KE": 0.58,   # Hub for East Africa, improving
    "TZ": 0.60,   # Improving logistics, infrastructure gaps
    "AR": 0.65,   # Currency controls, logistics complexity
    "VE": 0.85,   # Severe economic collapse, near-zero logistics reliability

    # ── Sanctioned (handled separately — treat as max risk for procurement) ─
    "RU": 0.95,   # Sanctioned + active conflict
    "IR": 0.95,   # Sanctioned
    "KP": 0.99,   # Sanctioned + isolated
    "BY": 0.90,   # Sanctioned
    "SY": 0.95,   # Sanctioned + conflict
}

# Default score for any country not in the map
COUNTRY_SUPPLY_RISK_DEFAULT = 0.45

def get_country_risk(country_code: str) -> float:
    """
    Return supply chain risk score (0-1) for a country.
    Sanctioned countries automatically get 0.95+.
    Unknown countries default to 0.45 (moderate).
    """
    if not country_code or str(country_code).strip() in ("", "nan", "None"):
        return COUNTRY_SUPPLY_RISK_DEFAULT
    code = str(country_code).strip().upper()
    if code in SANCTIONED_COUNTRIES:
        return max(COUNTRY_SUPPLY_RISK.get(code, 0.95), 0.90)
    return COUNTRY_SUPPLY_RISK.get(code, COUNTRY_SUPPLY_RISK_DEFAULT)


def get_country_risk_tier(country_code: str) -> str:
    """Return High / Medium / Low tier from the score."""
    score = get_country_risk(country_code)
    if score >= 0.65: return "High"
    if score >= 0.40: return "Medium"
    return "Low"


# Keep GEO_RISK + GEO_RISK_SCORES for backward compatibility with any
# scripts that still reference them — they now derive from COUNTRY_SUPPLY_RISK
GEO_RISK = {
    "High":   [c for c, s in COUNTRY_SUPPLY_RISK.items() if s >= 0.65],
    "Medium": [c for c, s in COUNTRY_SUPPLY_RISK.items() if 0.40 <= s < 0.65],
    "Low":    [c for c, s in COUNTRY_SUPPLY_RISK.items() if s < 0.40],
}

GEO_RISK_SCORES = {"High": 0.75, "Medium": 0.45, "Low": 0.18}

# ─────────────────────────────────────────────────────────────────────────────
# SPEND ANALYTICS THRESHOLDS
# (Saved from spend_analytics_config.py before deletion)
# ─────────────────────────────────────────────────────────────────────────────

SPEND_CONFIG = {
    # Spend Under Management target
    "sum_target":            0.80,   # 80% spend should be under contract

    # Maverick spend
    "maverick_target":       0.10,   # <10% maverick = healthy portfolio
    "maverick_baseline_rate":0.15,   # Expected ~15% maverick in raw data

    # Concentration risk
    "hhi_healthy":           1500,   # Herfindahl index < 1500 = healthy
    "hhi_moderate":          2500,   # 1500–2500 = moderate
    "top1_supplier_max":     0.10,   # Single supplier should not exceed 10%
    "top5_suppliers_max":    0.40,   # Top 5 should not exceed 40%
    "top10_suppliers_max":   0.65,   # Top 10 should not exceed 65%

    # Spend tiers for ABC analysis
    "abc_a_cumulative":      0.70,   # Class A = top 70% of spend
    "abc_b_cumulative":      0.90,   # Class B = next 20%
    # Class C = remaining 10%
}

# ─────────────────────────────────────────────────────────────────────────────
# ML MODEL SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

ML_CONFIG = {
    "test_size":        0.20,
    "random_state":     42,
    "cv_folds":         5,

    # Risk model
    "rf_n_estimators":  200,
    "xgb_n_estimators": 200,
    "xgb_learning_rate":0.05,

    # Segmentation
    "kmeans_k":         4,      # Number of clusters
    "kmeans_max_iter":  300,

    # SHAP
    "shap_sample_size": 100,    # Max rows for SHAP explanation
}

# ─────────────────────────────────────────────────────────────────────────────
# DISRUPTION FORECAST SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

FORECAST_CONFIG = {
    "horizon_days":     [30, 60, 90],
    "alert_threshold":  0.50,   # Flag if disruption prob > 50%
    "lookback_days":    365,    # Historical window for forecasting
}

# ─────────────────────────────────────────────────────────────────────────────
# OTIF / DELIVERY SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

DELIVERY_CONFIG = {
    "otif_tolerance_pct": 0.98,  # 98% of ordered qty = "in full"
    "delay_threshold_days": 5,   # > 5 days late = significant delay
}

# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD & CHATBOT SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

APP_CONFIG = {
    "page_title":       "SRSID — Supplier Risk & Spend Intelligence",
    "page_icon":        "🤝",
    "layout":           "wide",
    "top_n_suppliers":  10,     # Default top N in spend reports
    "news_days":        30,     # Days of news to show in chatbot
}

# ─────────────────────────────────────────────────────────────────────────────
# QUICK VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("SRSID Configuration")
    print("=" * 60)
    print(f"  Project root : {PROJECT_ROOT}")
    print(f"  SAP path     : {SAP_PATH}")
    print(f"  SAP exists   : {SAP_PATH.exists()}")
    print(f"  DB URL       : {DB_URL}")
    print()
    print("  NewsAPI key  :", "✅ set" if NEWS_APIS["newsapi_key"]  else "❌ not set")
    print("  Guardian key :", "✅ set" if NEWS_APIS["guardian_key"] else "❌ not set")
    print()

    # Test DB connection
    try:
        import psycopg2
        conn = psycopg2.connect(DB_URL)
        conn.close()
        print("  DB connection: ✅ connected to srsid_db")
    except Exception as e:
        print(f"  DB connection: ❌ {e}")

    print("=" * 60)
