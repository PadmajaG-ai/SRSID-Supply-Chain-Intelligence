"""
Hybrid Phase 1 Configuration (Simplified CSV-Based Vendors)
Loads vendors from vendors_list_simplified.csv
Just: Vendor Name + Industry (industry optional)
"""

import os
import json
import logging
import pandas as pd
from datetime import datetime, timedelta

# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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
# PART 3: LOAD VENDORS FROM SIMPLIFIED CSV
# ============================================================

VENDORS_CSV = os.path.join(PROJECT_ROOT, "vendors_list_simplified.csv")

def load_vendors_from_csv(csv_path: str = VENDORS_CSV) -> list:
    """
    Load vendors from simplified CSV file.
    
    CSV should have columns:
    - Vendor Name (required)
    - Industry (optional - can be blank)
    
    Args:
        csv_path: Path to vendors_list_simplified.csv
    
    Returns:
        List of vendor dictionaries
    """
    
    vendors = []
    
    try:
        if not os.path.exists(csv_path):
            logger.error(f"❌ vendors_list_simplified.csv not found at: {csv_path}")
            logger.error("   Please create vendors_list_simplified.csv in your project root")
            return []
        
        df = pd.read_csv(csv_path)
        logger.info(f"\n{'='*70}")
        logger.info(f"Loading vendors from CSV: {csv_path}")
        logger.info(f"{'='*70}")
        
        # Check required columns
        if "Vendor Name" not in df.columns:
            logger.error(f"❌ Missing required column: 'Vendor Name'")
            return []
        
        seen_vendors = set()
        duplicates = []
        
        for idx, row in df.iterrows():
            row_num = idx + 2  # +2 for header row + 1-based indexing
            
            vendor_name = str(row["Vendor Name"]).strip()
            
            # Optional industry
            industry = str(row.get("Industry", "")).strip()
            if pd.isna(row.get("Industry")) or industry == "nan" or industry == "":
                industry = None
            
            # Validation: Check for empty vendor name
            if not vendor_name or vendor_name == "nan":
                logger.warning(f"⚠️  Row {row_num}: Empty vendor name - skipping")
                continue
            
            # Validation: Check for duplicates
            if vendor_name.lower() in seen_vendors:
                msg = f"⚠️  Row {row_num}: Duplicate vendor '{vendor_name}' - skipping"
                logger.warning(msg)
                duplicates.append(msg)
                continue
            
            seen_vendors.add(vendor_name.lower())
            
            vendor = {
                "name": vendor_name,
                "industry": industry,  # Can be None
                "row": row_num,
            }
            
            vendors.append(vendor)
        
        logger.info(f"\n✓ Loaded {len(vendors)} vendors from CSV")
        
        if duplicates:
            logger.info(f"\nDuplicates removed ({len(duplicates)}):")
            for dup in duplicates:
                logger.info(f"  {dup}")
        
        logger.info(f"{'='*70}\n")
        
        return vendors
    
    except Exception as e:
        logger.error(f"❌ Error loading vendors from CSV: {e}")
        return []

# Load vendors at startup
SAP_VENDORS = load_vendors_from_csv()

# ============================================================
# PART 4: INDUSTRY AUTO-ASSIGNMENT (Fallback)
# ============================================================

INDUSTRY_ASSIGNMENT = {
    # Electronics & Technology
    "Intel": "Electronics & Semiconductors",
    "Tesla": "Electronics & Vehicles",
    "Cisco": "Networking & IT",
    "Apple": "Electronics & Semiconductors",
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

def get_industry_for_vendor(vendor_name: str, csv_industry: str = None) -> str:
    """
    Get industry for a vendor.
    
    Priority:
    1. Use CSV value if provided (non-null)
    2. Use lookup from INDUSTRY_ASSIGNMENT
    3. Default to "Other"
    
    Args:
        vendor_name: Vendor name
        csv_industry: Industry from CSV (can be None)
    
    Returns:
        Industry string
    """
    
    # Priority 1: Use CSV value if provided
    if csv_industry and csv_industry.strip():
        return csv_industry.strip()
    
    # Priority 2: Lookup in assignment dict
    if vendor_name in INDUSTRY_ASSIGNMENT:
        return INDUSTRY_ASSIGNMENT[vendor_name]
    
    # Priority 3: Default
    return "Other"

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
    "api_key": os.getenv("NEWSAPI_KEY", "c2ce506d781e4c7f87f3305da07d3430"),
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
# PART 10: HELPER FUNCTIONS
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
    print("HYBRID PHASE 1 CONFIGURATION (SIMPLIFIED)")
    print("=" * 70)
    
    print(f"\n✓ Vendors loaded from CSV: {len(SAP_VENDORS)}")
    print(f"✓ Disruption Types: {len(DISRUPTION_TYPES)}")
    print(f"✓ Output Files: {len(OUTPUT_FILES)}")
    
    if SAP_VENDORS:
        print(f"\nVendor Summary:")
        print(f"  Total: {len(SAP_VENDORS)}")
        
        # Show industry distribution
        industries = {}
        for v in SAP_VENDORS:
            industry = v.get("industry") or "Not specified (will auto-assign)"
            industries[industry] = industries.get(industry, 0) + 1
        
        print(f"  Industries specified: {len([i for i in industries.keys() if i != 'Not specified (will auto-assign)'])}")
        print(f"  Industries to auto-assign: {industries.get('Not specified (will auto-assign)', 0)}")
    
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
