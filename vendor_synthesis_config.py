"""
Vendor Synthesis Configuration
Configuration and helper functions for vendor synthesis
Vendors are now loaded from vendors_list_simplified.csv
"""

import os
from datetime import datetime, timedelta

# ============================================================
# PART 1: VENDOR SOURCE
# ============================================================
# NOTE: Vendors are now loaded from vendors_list_simplified.csv
# by the synthesize_vendors.py script.
# The CSV format is simple:
#   Vendor Name,Industry
#   Apple,Electronics & Semiconductors
#   Tesla,Electronics & Vehicles
#   ...
#
# REAL_VENDORS is no longer hardcoded here for flexibility
# and to keep the configuration file focused on parameters,
# not vendor data.

# ============================================================
# PART 2: SAP VENDOR COLUMNS MAPPING
# ============================================================

SAP_COLUMNS = {
    # SAP Standard Fields
    "LIFNR": "Vendor ID (SAP)",
    "NAME1": "Vendor Name (First 35 chars)",
    "NAME2": "Vendor Name Supplement",
    "ORT01": "City (Postal Address)",
    "Land1": "Country (ISO Code)",
    "LAND1": "Country Code",
    "REGIO": "Region",
    "PSTLZ": "Postal Code",
    "TELF1": "Telephone",
    "SMTP_ADDR": "Email Address",
    "KTOKK": "Vendor Account Group",
    "ZTERM": "Payment Terms",
    "INCO1": "Incoterms",
    
    # Custom Risk Fields
    "RISC": "Risk Category (High/Medium/Low)",
    "PERF_SCORE": "Performance Score (0-100)",
    "FIN_STAB": "Financial Stability (0-100)",
    "DELIV_PERF": "Delivery Performance (0-100)",
    "CREA_DATE": "Creation Date",
    "CHANGE_DATE": "Last Change Date",
    "NACE": "Industry Code",
    "NACE_DESC": "Industry Description",
}

# ============================================================
# PART 3: VARIANT GENERATION RULES
# ============================================================

VARIANT_TEMPLATES = [
    # Regional variations
    "{name} USA",
    "{name} EMEA",
    "{name} Europe",
    "{name} Asia Pacific",
    "{name} India",
    
    # Legal entity variations
    "{name} Inc",
    "{name} Ltd",
    "{name} GmbH",
    "{name} S.A.",
    "{name} Corporation",
    
    # Subsidiary variations
    "{name} Business Services",
    "{name} Solutions",
    "{name} Global",
    "{name} International",
    
    # Abbreviated variations
    "{short_name}",
    "{short_name} Inc",
    "{short_name} Ltd",
]

# ============================================================
# PART 4: SYNTHETIC DATA GENERATION RULES
# ============================================================

SYNTHETIC_DATA_CONFIG = {
    # Industry-based risk profiles
    "industry_risk_profiles": {
        "Pharmaceuticals": {"risk": "Low", "perf_min": 85, "perf_max": 98},
        "Electronics": {"risk": "Medium", "perf_min": 75, "perf_max": 95},
        "Electronics & Semiconductors": {"risk": "Medium", "perf_min": 75, "perf_max": 95},
        "Electronics & Vehicles": {"risk": "Medium", "perf_min": 75, "perf_max": 92},
        "Manufacturing": {"risk": "Medium", "perf_min": 70, "perf_max": 92},
        "Chemicals": {"risk": "High", "perf_min": 65, "perf_max": 88},
        "IT Distribution": {"risk": "Low", "perf_min": 80, "perf_max": 96},
        "IT Services": {"risk": "Low", "perf_min": 82, "perf_max": 96},
        "Laboratory Equipment": {"risk": "Low", "perf_min": 82, "perf_max": 97},
        "Distribution": {"risk": "Medium", "perf_min": 70, "perf_max": 90},
        "Retail/Distribution": {"risk": "Medium", "perf_min": 70, "perf_max": 90},
        "Industrial Manufacturing": {"risk": "Low", "perf_min": 80, "perf_max": 95},
        "Robotics & Automation": {"risk": "Low", "perf_min": 82, "perf_max": 96},
        "Networking": {"risk": "Low", "perf_min": 85, "perf_max": 98},
        "Networking & IT": {"risk": "Low", "perf_min": 85, "perf_max": 98},
        "Office Supplies": {"risk": "Medium", "perf_min": 72, "perf_max": 88},
        "Retail": {"risk": "Medium", "perf_min": 75, "perf_max": 92},
        "Entertainment": {"risk": "High", "perf_min": 65, "perf_max": 85},
        "Electrical Distribution": {"risk": "Medium", "perf_min": 72, "perf_max": 90},
        "Electronics Distribution": {"risk": "Medium", "perf_min": 72, "perf_max": 90},
        "Life Sciences": {"risk": "Low", "perf_min": 83, "perf_max": 96},
        "Food & Beverage": {"risk": "Medium", "perf_min": 72, "perf_max": 90},
        "Energy & Oil": {"risk": "High", "perf_min": 60, "perf_max": 85},
        "Building Materials": {"risk": "Medium", "perf_min": 70, "perf_max": 88},
        "Engineering & Construction": {"risk": "High", "perf_min": 65, "perf_max": 85},
        "Logistics": {"risk": "High", "perf_min": 65, "perf_max": 88},
        "Shipping & Maritime": {"risk": "High", "perf_min": 60, "perf_max": 85},
        "Consumer Goods": {"risk": "Medium", "perf_min": 72, "perf_max": 90},
        "Financial Services": {"risk": "Low", "perf_min": 80, "perf_max": 96},
        "Default": {"risk": "Medium", "perf_min": 70, "perf_max": 90},
    },
    
    # Country-based spend profiles
    "country_spend_profiles": {
        "US": {"min_spend": 100000, "max_spend": 5000000},
        "DE": {"min_spend": 80000, "max_spend": 3000000},
        "GB": {"min_spend": 60000, "max_spend": 2000000},
        "FR": {"min_spend": 70000, "max_spend": 2500000},
        "CH": {"min_spend": 90000, "max_spend": 2000000},
        "CA": {"min_spend": 50000, "max_spend": 1500000},
        "IN": {"min_spend": 30000, "max_spend": 800000},
        "Default": {"min_spend": 50000, "max_spend": 1000000},
    },
    
    # Variance for variants (vs parent)
    "variant_variance": {
        "perf_score_variance": 3,  # ±3 points variance
        "fin_stab_variance": 2,    # ±2 points variance
        "spend_variance": 0.1,     # ±10% variance
    },
}

# ============================================================
# PART 5: GUARDIAN API CONFIGURATION
# ============================================================

GUARDIAN_API = {
    "base_url": "https://open-platform.theguardian.com/search",
    "api_key": os.getenv("GUARDIAN_API_KEY", "YOUR_GUARDIAN_API_KEY_HERE"),
    
    # Search configuration
    "search_config": {
        "q_template": "{vendor_name}",  # Search query
        "format": "json",
        "show-fields": "headline,byline,publication,trailText,lastModified",
        "page-size": 50,
        "order-by": "newest",
    },
    
    # News categories relevant to supply chain
    "relevant_keywords": [
        "supply chain",
        "disruption",
        "shortage",
        "recall",
        "strike",
        "layoff",
        "bankruptcy",
        "acquisition",
        "merger",
        "expansion",
        "facility",
        "production",
        "manufacturing",
        "export",
        "tariff",
        "sanctions",
        "earnings",
        "profit warning",
        "lawsuit",
        "investigation",
    ],
    
    # Disruption type mapping from news content
    "disruption_type_mapping": {
        "strike": ["strike", "labor dispute", "worker action"],
        "weather": ["weather", "storm", "flood", "hurricane", "earthquake", "disaster"],
        "shortage": ["shortage", "supply chain", "chip shortage", "supply constraint"],
        "logistics_delay": ["delay", "supply chain", "port", "shipping", "customs"],
        "recall": ["recall", "product recall", "defect"],
        "lawsuit": ["lawsuit", "legal", "court", "settlement"],
        "bankruptcy": ["bankruptcy", "bankrupt", "insolvency"],
        "sanction": ["sanction", "tariff", "trade war", "export ban"],
    },
    
    # Date range for news
    "lookback_days": 365,  # Fetch news from past 1 year
}

# ============================================================
# PART 6: OUTPUT FILE CONFIGURATION
# ============================================================

OUTPUT_FILES = {
    "vendor_master": "vendors_sap_compliant.csv",  # Main vendor file
    "vendor_variants": "vendors_variants.csv",      # Variant vendors
    "vendor_with_spend": "vendors_with_spend.csv",  # With spend data
    "vendor_with_news": "vendors_with_news.csv",    # With news/disruptions
    "news_raw": "vendor_news_raw.csv",              # Raw news data
    "disruption_events": "disruption_events.csv",   # Extracted disruptions
}

# ============================================================
# PART 7: DATABASE TABLE MAPPING
# ============================================================

# Map to SRSID Phase 1 tables
TABLE_MAPPING = {
    "raw_supplier_risk_assessment": {
        "supplier_name": "NAME1",
        "financial_stability_score": "FIN_STAB",  # Normalized to 0-1
        "delivery_performance": "DELIV_PERF",     # Normalized to 0-1
        "historical_risk_category": "RISC",
        "ariba_supplier_id": "LIFNR",
        "supplier_country": "Land1",
    },
    
    "raw_supply_chain_transactions": {
        "supplier": "NAME1",
        "amount": "AMOUNT",
        "transaction_date": "TRANS_DATE",
        "disruption_type": "DISRUPTION",
        "procurement_category": "NACE_DESC",
        "po_number": "PO_NUM",
    },
    
    "raw_kraljic_matrix": {
        "supplier_name": "NAME1",
        "supply_risk_score": "SUPPLY_RISK",
        "profit_impact_score": "PROFIT_IMPACT",
        "kraljic_quadrant": "KVADRANT",
    },
}

# ============================================================
# PART 8: COUNTRY CODE MAPPING (SAP)
# ============================================================

COUNTRY_CODES = {
    "US": "United States",
    "DE": "Germany",
    "GB": "United Kingdom",
    "FR": "France",
    "CH": "Switzerland",
    "CA": "Canada",
    "IN": "India",
    "JP": "Japan",
    "CN": "China",
    "AU": "Australia",
}

REVERSE_COUNTRY_CODES = {v: k for k, v in COUNTRY_CODES.items()}

# ============================================================
# PART 9: VALIDATION & HELPER FUNCTIONS
# ============================================================

def validate_guardian_api_key():
    """Check if Guardian API key is configured."""
    api_key = GUARDIAN_API["api_key"]
    if api_key == "YOUR_GUARDIAN_API_KEY_HERE":
        print("⚠️  WARNING: Guardian API key not configured!")
        print("   Set environment variable: export GUARDIAN_API_KEY='your_key'")
        print("   Get free API key from: https://open-platform.theguardian.com/")
        return False
    return True

def get_industry_risk_profile(industry: str) -> dict:
    """Get risk profile for industry."""
    profiles = SYNTHETIC_DATA_CONFIG["industry_risk_profiles"]
    return profiles.get(industry, profiles["Default"])

def get_country_spend_profile(country: str) -> dict:
    """Get spend profile for country."""
    profiles = SYNTHETIC_DATA_CONFIG["country_spend_profiles"]
    return profiles.get(country, profiles["Default"])

def normalize_score(score: int, max_val: int = 100) -> float:
    """Normalize score to 0-1 range."""
    return round(max(0, min(score / max_val, 1)), 2)

def get_short_name(vendor_name: str) -> str:
    """Generate short name from vendor name."""
    parts = vendor_name.split()
    if len(parts) > 1:
        return parts[0]
    return vendor_name[:3].upper()

# ============================================================
# PART 10: VALIDATION
# ============================================================

if __name__ == "__main__":
    print("Vendor Synthesis Configuration Loaded")
    print(f"✓ Variant templates: {len(VARIANT_TEMPLATES)}")
    print(f"✓ SAP columns mapped: {len(SAP_COLUMNS)}")
    print(f"✓ Industry risk profiles: {len(SYNTHETIC_DATA_CONFIG['industry_risk_profiles'])}")
    print(f"✓ Country codes: {len(COUNTRY_CODES)}")
    print(f"✓ Guardian API configured: {validate_guardian_api_key()}")
    print("\n✅ Configuration ready!")
    print("\n📝 NOTE: Vendors are loaded from vendors_list_simplified.csv")
    print("   by the synthesize_vendors.py script at runtime.")
