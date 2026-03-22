"""
Spend Analytics Configuration
Thresholds, rules, and parameters for spend analytics calculations
"""

import os

# ============================================================
# SPEND ANALYTICS FEATURE FLAGS
# ============================================================

ENABLE_SPEND_ANALYTICS = True  # Master switch

# Individual components
CALCULATE_SUM = True  # Spend Under Management
IDENTIFY_MAVERICK = True  # Maverick Spend Detection
ANALYZE_CONCENTRATION = True  # Concentration Risk

# ============================================================
# PART 1: SPEND UNDER MANAGEMENT (SUM) CONFIGURATION
# ============================================================

SUM_CONFIG = {
    # Overall SUM target
    "enterprise_sum_target": 0.80,  # 80% of spend should be under contract
    
    # Thresholds for SUM %
    "thresholds": {
        "excellent": 0.85,  # >85%
        "good": 0.80,  # 80-85%
        "fair": 0.75,  # 75-80%
        "poor": 0.70,  # <70%
    },
    
    # Contract classification rules
    "contract_classification": {
        # Default assumption: what % of baseline spend is under contract
        "default_contract_rate": 0.75,  # 75% assumed contract
        
        # During disruptions: what % is under contract
        "disruption_contract_rate": 0.0,  # 0% during disruptions
        
        # Baseline spending (regular, predictable)
        "baseline_contract_rate": 0.80,  # 80% likely contract
        
        # Emergency/unusual spending
        "emergency_contract_rate": 0.0,  # 0% likely contract
    },
    
    # Spend variance thresholds
    "spend_variance": {
        "baseline_max_multiplier": 1.5,  # Spending >1.5x avg = emergency
        "emergency_threshold": 3.0,  # Spending >3x avg = definitely emergency
    },
}

# ============================================================
# PART 2: MAVERICK SPEND CONFIGURATION
# ============================================================

MAVERICK_CONFIG = {
    # Overall maverick spend target (for enterprise)
    "enterprise_maverick_target": 0.10,  # <10% maverick = healthy
    
    # Maverick rates for Phase 1 (realistic estimates)
    "maverick_baseline_rate": 0.15,  # 15% of transactions marked as maverick
    
    # Distribution of maverick types (must sum to 100%)
    "maverick_type_distribution": {
        "off_contract": 0.54,  # 54% of maverick spend
        "unauthorized_vendor": 0.31,  # 31% of maverick spend
        "emergency_high_cost": 0.12,  # 12% of maverick spend
        "non_compliant": 0.03,  # 3% of maverick spend
    },
    
    # Thresholds for marking as maverick
    "classification_rules": {
        # If disruption occurred, mark as non-contract/maverick
        "disruption_is_maverick": True,
        
        # If spending >X% above average, mark emergency
        "emergency_threshold_multiplier": 2.0,  # 2x average spend = emergency
        
        # Random selection rate for non-compliant
        "random_non_compliant_rate": 0.05,  # 5% random non-compliant
    },
    
    # Estimated savings opportunity
    "savings_estimation": {
        # Estimated savings as % of maverick spend
        "off_contract_savings_rate": 0.15,  # 15% savings potential
        "unauthorized_savings_rate": 0.20,  # 20% savings potential
        "emergency_savings_rate": 0.25,  # 25% savings potential
        "non_compliant_savings_rate": 0.30,  # 30% savings potential
    },
}

# ============================================================
# PART 3: CONCENTRATION RISK CONFIGURATION
# ============================================================

CONCENTRATION_CONFIG = {
    # Risk level thresholds (% of total enterprise spend)
    "enterprise_thresholds": {
        "high_risk": 0.15,  # >15% = HIGH RISK
        "medium_risk": 0.10,  # 10-15% = MEDIUM RISK
        "low_risk": 0.10,  # <10% = LOW RISK
    },
    
    # Risk level thresholds (% of category spend)
    "category_thresholds": {
        "high_risk": 0.20,  # >20% = HIGH RISK
        "medium_risk": 0.10,  # 10-20% = MEDIUM RISK
        "low_risk": 0.10,  # <10% = LOW RISK
    },
    
    # HHI Index thresholds
    "hhi_thresholds": {
        "healthy": 1500,  # <1500 = Healthy
        "moderate": 2500,  # 1500-2500 = Moderate
        "high": 2500,  # >2500 = High
    },
    
    # Concentration targets
    "concentration_targets": {
        "top_1_supplier_max": 0.10,  # Top supplier <10%
        "top_5_suppliers_max": 0.40,  # Top 5 suppliers <40%
        "top_10_suppliers_max": 0.65,  # Top 10 suppliers <65%
        "hhi_target": 1500,  # Target HHI <1500
    },
    
    # Diversification needs
    "diversification_rules": {
        # When to flag diversification needed
        "flag_if_above_high_risk": True,  # Flag if concentration >high_risk threshold
        "flag_if_hhi_above_target": True,  # Flag if HHI >target
        
        # Target concentration after diversification
        "target_concentration": 0.10,  # Reduce to <10%
        "target_reduction_percentage": 0.33,  # Reduce by 33% if >15%
    },
}

# ============================================================
# PART 4: CATEGORY DEFINITIONS
# ============================================================

# Map industries/categories to spend categories
SPEND_CATEGORIES = {
    "Electronics & Semiconductors": "Technology & Electronics",
    "Electronics & Vehicles": "Automotive & Technology",
    "Networking & IT": "Technology & IT",
    "IT Distribution": "Technology & IT",
    "IT Services": "Technology & IT",
    "Chemicals": "Raw Materials & Chemicals",
    "Energy & Oil": "Energy & Commodities",
    "Pharmaceuticals": "Pharma & Healthcare",
    "Life Sciences": "Pharma & Healthcare",
    "Laboratory Equipment": "Equipment & Supplies",
    "Food & Beverage": "Food & Beverage",
    "Consumer Goods": "Packaged Goods",
    "Retail": "Distribution & Retail",
    "Office Supplies": "Office & Supplies",
    "Electronics Distribution": "Distribution",
    "Electrical Distribution": "Distribution",
    "Logistics": "Transportation & Logistics",
    "Shipping & Maritime": "Transportation & Logistics",
    "Manufacturing": "Manufacturing",
    "Building Materials": "Construction Materials",
    "Engineering & Construction": "Construction & Engineering",
    "Robotics & Automation": "Industrial Equipment",
    "Industrial Manufacturing": "Manufacturing",
    "Financial Services": "Services",
    "Entertainment": "Services",
    "Distribution": "Distribution",
    "Retail/Distribution": "Distribution & Retail",
    "Other": "Other",
}

# ============================================================
# PART 5: OUTPUT CONFIGURATION
# ============================================================

OUTPUT_CONFIG = {
    # Column names for enhanced tables
    "risk_assessment_new_columns": [
        "total_spend",
        "spend_under_contract",
        "sum_percentage",
        "maverick_spend",
        "maverick_percentage",
        "spend_concentration_percentage",
        "concentration_risk_level",
        "diversification_priority",
        "savings_opportunity",
    ],
    
    "transactions_new_columns": [
        "is_under_contract",
        "is_maverick",
        "maverick_type",
        "estimated_savings",
        "spend_category",
    ],
    
    # New output tables
    "new_output_tables": [
        "raw_spend_analytics.csv",
        "raw_concentration_analysis.csv",
        "raw_maverick_spend_summary.csv",
    ],
    
    # Reports
    "new_reports": [
        "phase1_spend_intelligence_report.json",
    ],
}

# ============================================================
# PART 6: SPEND PATTERNS (For Spend Synthesis)
# ============================================================

# Realistic spend amounts by country (for generating baseline transactions)
COUNTRY_SPEND_PATTERNS = {
    "US": (150000, 1000000),  # $150K - $1M per vendor per year
    "DE": (100000, 800000),   # $100K - $800K
    "GB": (80000, 600000),    # $80K - $600K
    "FR": (80000, 700000),    # $80K - $700K
    "CH": (100000, 600000),   # $100K - $600K
    "CA": (60000, 400000),    # $60K - $400K
    "IN": (40000, 200000),    # $40K - $200K
    "JP": (120000, 900000),   # $120K - $900K
    "NL": (90000, 700000),    # $90K - $700K
    "SA": (200000, 1500000),  # $200K - $1.5M
    "DK": (100000, 600000),   # $100K - $600K
}

# Spend multipliers by industry (relative to baseline)
INDUSTRY_SPEND_MULTIPLIERS = {
    "Electronics & Semiconductors": 2.0,  # High volume
    "Electronics & Vehicles": 2.5,  # Very high
    "Chemicals": 1.8,  # High
    "Energy & Oil": 3.0,  # Very high
    "Pharmaceuticals": 1.5,  # Medium-high
    "Manufacturing": 1.6,  # Medium-high
    "Logistics": 1.7,  # Medium-high
    "Retail": 2.0,  # High
    "Food & Beverage": 1.5,  # Medium
    "Other": 1.0,  # Baseline
}

# ============================================================
# PART 7: VALIDATION & HELPER FUNCTIONS
# ============================================================

def validate_spend_config():
    """Validate spend analytics configuration."""
    errors = []
    
    # Check SUM thresholds are in order
    thresholds = SUM_CONFIG["thresholds"]
    if not (thresholds["poor"] < thresholds["fair"] < 
            thresholds["good"] < thresholds["excellent"]):
        errors.append("SUM thresholds not in ascending order")
    
    # Check maverick distribution sums to 1.0
    maverick_dist = MAVERICK_CONFIG["maverick_type_distribution"]
    total = sum(maverick_dist.values())
    if abs(total - 1.0) > 0.01:
        errors.append(f"Maverick type distribution sum={total}, must be 1.0")
    
    # Check concentration thresholds
    conc = CONCENTRATION_CONFIG["enterprise_thresholds"]
    if not (conc["low_risk"] <= conc["medium_risk"] <= conc["high_risk"]):
        errors.append("Concentration thresholds not in order")
    
    return errors

def get_spend_category(industry: str) -> str:
    """Get spend category for an industry."""
    return SPEND_CATEGORIES.get(industry, "Other")

def get_country_spend_range(country: str) -> tuple:
    """Get expected spend range for a country."""
    return COUNTRY_SPEND_PATTERNS.get(country, (50000, 500000))

def get_industry_multiplier(industry: str) -> float:
    """Get spend multiplier for an industry."""
    return INDUSTRY_SPEND_MULTIPLIERS.get(industry, 1.0)

# ============================================================
# PART 8: VALIDATION ON IMPORT
# ============================================================

if __name__ == "__main__":
    print("=" * 70)
    print("SPEND ANALYTICS CONFIGURATION")
    print("=" * 70)
    
    print(f"\n✓ Spend Analytics Enabled: {ENABLE_SPEND_ANALYTICS}")
    print(f"✓ SUM Calculation: {CALCULATE_SUM}")
    print(f"✓ Maverick Detection: {IDENTIFY_MAVERICK}")
    print(f"✓ Concentration Analysis: {ANALYZE_CONCENTRATION}")
    
    print(f"\nThresholds:")
    print(f"  SUM Target: >{SUM_CONFIG['enterprise_sum_target']*100:.0f}%")
    print(f"  Maverick Target: <{MAVERICK_CONFIG['enterprise_maverick_target']*100:.0f}%")
    print(f"  HHI Target: <{CONCENTRATION_CONFIG['hhi_thresholds']['healthy']}")
    
    print(f"\nValidation:")
    errors = validate_spend_config()
    if errors:
        print("  ❌ Errors found:")
        for error in errors:
            print(f"    - {error}")
    else:
        print("  ✓ Configuration valid")
    
    print("\n" + "=" * 70)
    print("Configuration loaded successfully!")
    print("=" * 70)
