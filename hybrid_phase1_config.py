"""
Hybrid Phase 1 Configuration
GDELT + NewsAPI disruptions + Kaggle patterns + 56 SAP vendors
"""

import os
from datetime import datetime, timedelta

# ============================================================
# PART 1: PROJECT PATHS
# ============================================================

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

PATHS = {
    "data_raw": os.path.join(PROJECT_ROOT, "data", "raw"),
    "data_processed": os.path.join(PROJECT_ROOT, "data", "processed"),
    "logs": os.path.join(PROJECT_ROOT, "logs"),
    "outputs": os.path.join(PROJECT_ROOT, "outputs"),
    "phase1_tables": os.path.join(PROJECT_ROOT, "phase1_tables"),
}

# Create directories if they don't exist
for path in PATHS.values():
    os.makedirs(path, exist_ok=True)

# ============================================================
# PART 2: KAGGLE FILES LOCATION
# ============================================================

KAGGLE_FILES = {
    "supply_chain_risk": os.path.join(PATHS["data_raw"], "us_supply_chain_risk.csv"),
    "risk_assessment": os.path.join(PATHS["data_raw"], "supplier_risk_assessment.csv"),
    "kraljic": os.path.join(PATHS["data_raw"], "realistic_kraljic_dataset.csv"),
}

# ============================================================
# PART 3: 56 SAP VENDORS (Existing 25 + New 31)
# ============================================================

SAP_VENDORS = [
    # EXISTING 25 ARIBA VENDORS
    {"name": "Rich's", "country": "US", "city": "Los Angeles", "industry": None},
    {"name": "Indo Autotech Limited", "country": "IN", "city": "Bangalore", "industry": None},
    {"name": "Cirque du Soleil", "country": "CA", "city": "Montreal", "industry": None},
    {"name": "Zentis GmbH & Co. KG", "country": "DE", "city": "Aachen", "industry": None},
    {"name": "Flint Group", "country": "DE", "city": "Wilnsdorf", "industry": None},
    {"name": "Amazon Business Services US", "country": "US", "city": "Seattle", "industry": None},
    {"name": "Fisher Scientific", "country": "US", "city": "Waltham", "industry": None},
    {"name": "Merck Life Sciences", "country": "DE", "city": "Darmstadt", "industry": None},
    {"name": "Office Depot", "country": "US", "city": "Boca Raton", "industry": None},
    {"name": "RS Components", "country": "GB", "city": "Corby", "industry": None},
    {"name": "Eppendorf", "country": "DE", "city": "Hamburg", "industry": None},
    {"name": "Grainger", "country": "US", "city": "Lake Forest", "industry": None},
    {"name": "Qiagen", "country": "DE", "city": "Hilden", "industry": None},
    {"name": "CDW", "country": "US", "city": "Vernon Hills", "industry": None},
    {"name": "Manutan", "country": "FR", "city": "Gonesse", "industry": None},
    {"name": "Siemens", "country": "DE", "city": "Munich", "industry": None},
    {"name": "ABB", "country": "CH", "city": "Zurich", "industry": None},
    {"name": "Continuum", "country": "US", "city": "Cambridge", "industry": None},
    {"name": "Cisco", "country": "US", "city": "San Jose", "industry": None},
    {"name": "Apple", "country": "US", "city": "Cupertino", "industry": None},
    {"name": "Graybar", "country": "US", "city": "Clayton", "industry": None},
    {"name": "Walmart", "country": "US", "city": "Bentonville", "industry": None},
    {"name": "Air Liquide", "country": "FR", "city": "Paris", "industry": None},
    {"name": "Binder", "country": "DE", "city": "Tuttlingen", "industry": None},
    {"name": "AstraZeneca", "country": "GB", "city": "Cambridge", "industry": None},
    {"name": "Saint-Gobain", "country": "FR", "city": "Courbevoie", "industry": None},
    
    # NEW 31 VENDORS
    {"name": "Tesla", "country": "US", "city": "Austin", "industry": None},
    {"name": "Intel", "country": "US", "city": "Santa Clara", "industry": None},
    {"name": "BASF", "country": "DE", "city": "Ludwigshafen", "industry": None},
    {"name": "Dow Chemical", "country": "US", "city": "Midland", "industry": None},
    {"name": "SABIC", "country": "SA", "city": "Riyadh", "industry": None},
    {"name": "LyondellBasell", "country": "NL", "city": "Rotterdam", "industry": None},
    {"name": "ExxonMobil", "country": "US", "city": "Houston", "industry": None},
    {"name": "Unilever", "country": "GB", "city": "London", "industry": None},
    {"name": "Nestlé", "country": "CH", "city": "Vevey", "industry": None},
    {"name": "Aldo Group", "country": "CA", "city": "Montreal", "industry": None},
    {"name": "The Coca-Cola Company", "country": "US", "city": "Atlanta", "industry": None},
    {"name": "Lidl (Schwarz Group)", "country": "DE", "city": "Ludwigsburg", "industry": None},
    {"name": "Shell", "country": "NL", "city": "The Hague", "industry": None},
    {"name": "Chevron", "country": "US", "city": "San Ramon", "industry": None},
    {"name": "Novartis", "country": "CH", "city": "Basel", "industry": None},
    {"name": "Johnson and Johnson", "country": "US", "city": "New Brunswick", "industry": None},
    {"name": "Reckitt Benkiser", "country": "GB", "city": "Slough", "industry": None},
    {"name": "DHL Supply Chain", "country": "DE", "city": "Bonn", "industry": None},
    {"name": "FedEx", "country": "US", "city": "Memphis", "industry": None},
    {"name": "Nippon Express", "country": "JP", "city": "Tokyo", "industry": None},
    {"name": "Maersk Line", "country": "DK", "city": "Copenhagen", "industry": None},
    {"name": "Kellogg Company", "country": "US", "city": "Battle Creek", "industry": None},
    {"name": "PepsiCo Inc", "country": "US", "city": "Purchase", "industry": None},
    {"name": "Monte Carlo Fashions", "country": "IN", "city": "Mumbai", "industry": None},
    {"name": "Fluor Corporation", "country": "US", "city": "Irving", "industry": None},
    {"name": "Dow Inc", "country": "US", "city": "Midland", "industry": None},
    {"name": "DuPont de Nemours Inc", "country": "US", "city": "Wilmington", "industry": None},
    {"name": "Eastman Chemical Company", "country": "US", "city": "Kingsport", "industry": None},
    {"name": "JPMorgan Chase", "country": "US", "city": "New York", "industry": None},
    {"name": "Sanofi S.A", "country": "FR", "city": "Paris", "industry": None},
    {"name": "Valero Energy Corporation", "country": "US", "city": "San Antonio", "industry": None},
]

# ============================================================
# PART 4: INDUSTRY AUTO-ASSIGNMENT RULES
# ============================================================

INDUSTRY_ASSIGNMENT = {
    # Electronics & Technology
    "Intel": "Electronics & Semiconductors",
    "Tesla": "Electronics & Vehicles",
    "Cisco": "Networking & IT",
    "Apple": "Electronics & Computing",
    "CDW": "IT Distribution",
    "Continuum": "IT Services",
    
    # Chemicals & Energy
    "BASF": "Chemicals",
    "Dow Chemical": "Chemicals",
    "Dow Inc": "Chemicals",
    "SABIC": "Chemicals",
    "LyondellBasell": "Chemicals",
    "ExxonMobil": "Energy & Oil",
    "Shell": "Energy & Oil",
    "Chevron": "Energy & Oil",
    "Valero Energy Corporation": "Energy & Oil",
    
    # Pharmaceuticals & Life Sciences
    "Merck Life Sciences": "Pharmaceuticals",
    "Novartis": "Pharmaceuticals",
    "Johnson and Johnson": "Pharmaceuticals",
    "AstraZeneca": "Pharmaceuticals",
    "Sanofi S.A": "Pharmaceuticals",
    "Qiagen": "Life Sciences",
    "Fisher Scientific": "Laboratory Equipment",
    "Eppendorf": "Laboratory Equipment",
    "Binder": "Laboratory Equipment",
    
    # Food & Beverage
    "Nestlé": "Food & Beverage",
    "The Coca-Cola Company": "Food & Beverage",
    "PepsiCo Inc": "Food & Beverage",
    "Kellogg Company": "Food & Beverage",
    "Unilever": "Consumer Goods",
    "Reckitt Benkiser": "Consumer Goods",
    
    # Retail & Distribution
    "Walmart": "Retail",
    "Lidl (Schwarz Group)": "Retail",
    "Aldo Group": "Retail",
    "Grainger": "Distribution",
    "Amazon Business Services US": "Retail/Distribution",
    "Office Depot": "Office Supplies",
    "Manutan": "Office Supplies",
    "RS Components": "Electronics Distribution",
    "Graybar": "Electrical Distribution",
    
    # Manufacturing & Industrial
    "Siemens": "Industrial Manufacturing",
    "ABB": "Robotics & Automation",
    "Zentis GmbH & Co. KG": "Manufacturing",
    "Indo Autotech Limited": "Manufacturing",
    "Flint Group": "Manufacturing",
    "Saint-Gobain": "Building Materials",
    "Eastman Chemical Company": "Chemicals",
    "Fluor Corporation": "Engineering & Construction",
    
    # Logistics & Transportation
    "DHL Supply Chain": "Logistics",
    "FedEx": "Logistics",
    "Nippon Express": "Logistics",
    "Maersk Line": "Shipping & Maritime",
    
    # Entertainment & Services
    "Cirque du Soleil": "Entertainment",
    "Rich's": "Distribution",
    "JPMorgan Chase": "Financial Services",
}

# ============================================================
# PART 5: DISRUPTION TYPES & KEYWORDS
# ============================================================

DISRUPTION_TYPES = {
    "supply_chain": [
        "supply chain",
        "supply disruption",
        "supply constraint",
        "supply problem",
        "supply shortage",
    ],
    "bankruptcy": [
        "bankruptcy",
        "bankrupt",
        "insolvency",
        "financial crisis",
        "default",
    ],
    "labor_strike": [
        "strike",
        "labor dispute",
        "worker protest",
        "union action",
        "labor action",
    ],
    "geopolitical": [
        "sanction",
        "tariff",
        "trade war",
        "export ban",
        "import ban",
        "geopolitical",
        "political risk",
        "tension",
    ],
    "natural_disaster": [
        "earthquake",
        "flood",
        "hurricane",
        "storm",
        "weather disaster",
        "natural disaster",
        "typhoon",
        "cyclone",
    ],
    "recall": [
        "recall",
        "product recall",
        "defect",
        "safety recall",
    ],
    "shortage": [
        "shortage",
        "chip shortage",
        "semiconductor shortage",
        "material shortage",
    ],
    "logistics_delay": [
        "delay",
        "logistics delay",
        "port delay",
        "shipping delay",
        "customs delay",
    ],
}

# ============================================================
# PART 6: GDELT & NewsAPI CONFIGURATION
# ============================================================

GDELT_CONFIG = {
    "base_url": "https://api.gdeltproject.org/api/v2/",
    "endpoint": "timeline/query",
    "lookback_days": 730,  # 2 years
    "max_records": 1000,
    "timeout": 30,
}

NEWSAPI_CONFIG = {
    "base_url": "https://newsapi.org/v2/",
    "endpoint": "everything",
    "api_key": os.getenv("NEWSAPI_KEY", "YOUR_NEWSAPI_KEY_HERE"),
    "lookback_days": 30,  # NewsAPI free tier limitation
    "requests_per_day": 100,
    "timeout": 30,
}

# ============================================================
# PART 7: VARIANT TEMPLATES
# ============================================================

VARIANT_TEMPLATES = [
    "{name}",
    "{name} Inc",
    "{name} Ltd",
    "{name} Corporation",
    "{name} Global",
    "{name} International",
    "{short_name}",
]

# ============================================================
# PART 8: OUTPUT FILES
# ============================================================

OUTPUT_FILES = {
    "gdelt_disruptions": os.path.join(PATHS["data_processed"], "gdelt_disruptions_raw.csv"),
    "newsapi_disruptions": os.path.join(PATHS["data_processed"], "newsapi_disruptions_raw.csv"),
    "combined_disruptions": os.path.join(PATHS["data_processed"], "combined_disruptions.csv"),
    "kaggle_patterns": os.path.join(PATHS["data_processed"], "kaggle_patterns.json"),
    "vendors_with_industries": os.path.join(PATHS["data_processed"], "vendors_with_industries.csv"),
    "vendor_disruption_mapping": os.path.join(PATHS["data_processed"], "vendor_disruption_mapping.csv"),
    "vendor_risk_metrics": os.path.join(PATHS["data_processed"], "vendor_risk_metrics_realistic.csv"),
    
    # PHASE 1 FINAL TABLES
    "phase1_risk_assessment": os.path.join(PATHS["phase1_tables"], "raw_supplier_risk_assessment.csv"),
    "phase1_transactions": os.path.join(PATHS["phase1_tables"], "raw_supply_chain_transactions.csv"),
    "phase1_kraljic": os.path.join(PATHS["phase1_tables"], "raw_kraljic_matrix.csv"),
    "phase1_quality_report": os.path.join(PATHS["phase1_tables"], "phase1_quality_report.json"),
}

# ============================================================
# PART 9: LOGGING CONFIGURATION
# ============================================================

LOGGING = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "log_file": os.path.join(PATHS["logs"], "phase1_hybrid.log"),
}

# ============================================================
# PART 10: VALIDATION & HELPER FUNCTIONS
# ============================================================

def validate_kaggle_files():
    """Check if Kaggle files exist."""
    missing = []
    for key, path in KAGGLE_FILES.items():
        if not os.path.exists(path):
            missing.append(f"{key}: {path}")
    return missing

def validate_newsapi_key():
    """Check if NewsAPI key is configured."""
    key = NEWSAPI_CONFIG["api_key"]
    if key == "YOUR_NEWSAPI_KEY_HERE":
        print("⚠️  WARNING: NewsAPI key not configured!")
        print("   Get free key from: https://newsapi.org/")
        print("   Set environment: export NEWSAPI_KEY='your_key'")
        return False
    return True

def get_short_name(vendor_name: str) -> str:
    """Generate short name from vendor name."""
    parts = vendor_name.split()
    if len(parts) > 1:
        return parts[0]
    return vendor_name[:3].upper()

# ============================================================
# PART 11: VALIDATION ON IMPORT
# ============================================================

if __name__ == "__main__":
    print("=" * 70)
    print("HYBRID PHASE 1 CONFIGURATION")
    print("=" * 70)
    
    print(f"\n✓ SAP Vendors: {len(SAP_VENDORS)}")
    print(f"✓ Disruption Types: {len(DISRUPTION_TYPES)}")
    print(f"✓ Output Files: {len(OUTPUT_FILES)}")
    
    print("\nValidating Kaggle Files:")
    missing = validate_kaggle_files()
    if missing:
        print("  ⚠️  Missing files:")
        for m in missing:
            print(f"    - {m}")
    else:
        print("  ✓ All Kaggle files found")
    
    print("\nValidating NewsAPI Key:")
    if validate_newsapi_key():
        print("  ✓ NewsAPI key configured")
    
    print("\n" + "=" * 70)
    print("Configuration loaded successfully!")
    print("=" * 70)
