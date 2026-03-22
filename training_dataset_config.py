"""
Training Dataset Configuration
Combines all sources: Synthetic + Real Vendors + Kaggle + Guardian News
For maximum training data while keeping separate for Phase 2 consolidation
"""

import os

# ============================================================
# DATA SOURCES CONFIGURATION
# ============================================================

DATA_SOURCES = {
    # Original Phase 1 Synthetic Data (1,800 suppliers)
    "phase1_synthetic": {
        "enabled": True,
        "files": {
            "risk_assessment": "phase1_supplier_risk_assessment_original.csv",
            "transactions": "phase1_supply_chain_transactions_original.csv",
            "kraljic": "phase1_kraljic_matrix_original.csv",
        },
        "description": "Original synthetic data from Phase 1",
        "estimated_rows": {
            "risk_assessment": 1800,
            "transactions": 1000,
            "kraljic": 16,
        },
    },
    
    # Real Vendors (150: 25 real + variants)
    "real_vendors": {
        "enabled": True,
        "files": {
            "risk_assessment": "phase1_supplier_risk_assessment.csv",
            "transactions": "phase1_supply_chain_transactions.csv",
            "kraljic": "phase1_kraljic_matrix.csv",
        },
        "description": "Synthesized from 25 Ariba vendors + variants",
        "estimated_rows": {
            "risk_assessment": 150,
            "transactions": 425,
            "kraljic": 150,
        },
    },
    
    # Kaggle Data (3 files)
    "kaggle": {
        "enabled": True,
        "files": {
            "transactions": "us_supply_chain_risk.csv",
            "risk_assessment": "supplier_risk_assessment.csv",
            "kraljic": "realistic_kraljic_dataset.csv",
        },
        "description": "Kaggle supply chain datasets",
        "estimated_rows": {
            "transactions": 500,  # Estimate
            "risk_assessment": 100,  # Estimate
            "kraljic": 75,  # Estimate
        },
    },
    
    # Guardian News (Disruptions)
    "guardian_news": {
        "enabled": True,
        "files": {
            "disruptions": "disruption_events.csv",
        },
        "description": "Real news disruptions from Guardian API",
        "estimated_rows": {
            "disruptions": 32,
        },
    },
}

# ============================================================
# COLUMN MAPPINGS - KAGGLE TO SRSID
# ============================================================

KAGGLE_COLUMN_MAPPING = {
    # US Supply Chain Risk → raw_supply_chain_transactions
    "us_supply_chain_risk": {
        "Supplier": "supplier",
        "Amount": "amount",
        "Date": "transaction_date",
        "Disruption Type": "disruption_type",
        "Disruption Details": "disruption_details",
        "Procurement Category": "procurement_category",
        "Invoice Number": "invoice_number",
        "PO Number": "po_number",
    },
    
    # Supplier Risk Assessment → raw_supplier_risk_assessment
    "supplier_risk_assessment": {
        "Supplier Name": "supplier_name",
        "Financial Stability": "financial_stability_score",
        "Delivery Performance": "delivery_performance",
        "Historical Risk Category": "historical_risk_category",
    },
    
    # Realistic Kraljic Dataset → raw_kraljic_matrix
    "realistic_kraljic_dataset": {
        "Product_ID": "supplier_id",
        "Product_Name": "supplier_name",
        "Supplier_Region": "region",
        "Lead_Time_Days": "lead_time_days",
        "Order_Volume_Units": "order_volume",
        "Cost_per_Unit": "cost_per_unit",
        "Supply_Risk_Score": "supply_risk_score",
        "Profit_Impact_Score": "profit_impact_score",
        "Environmental_Impact": "environmental_impact",
        "Single_Source_Risk": "single_source_risk",
        "Kraljic_Category": "kraljic_quadrant",
    },
}

# ============================================================
# TRAINING DATASET OUTPUT CONFIGURATION
# ============================================================

TRAINING_OUTPUT = {
    "output_dir": "training_datasets",
    "files": {
        "risk_assessment": "training_supplier_risk_assessment.csv",
        "transactions": "training_supply_chain_transactions.csv",
        "kraljic": "training_kraljic_matrix.csv",
        "manifest": "training_dataset_manifest.json",
        "summary": "training_dataset_summary.txt",
    },
}

# ============================================================
# DATA QUALITY RULES
# ============================================================

DATA_QUALITY = {
    # Duplicate handling
    "handle_duplicates": "keep_all",  # Keep for Phase 2 fuzzy matching
    
    # Missing values
    "missing_value_strategy": {
        "supplier_name": "drop_row",  # Critical field
        "financial_stability": "fill_with_mean",
        "delivery_performance": "fill_with_mean",
        "amount": "fill_with_0",
        "disruption_type": "fill_with_none",
    },
    
    # Data validation
    "validation_rules": {
        "financial_stability": {"min": 0, "max": 1},
        "delivery_performance": {"min": 0, "max": 1},
        "supply_risk_score": {"min": 0, "max": 1},
        "profit_impact_score": {"min": 0, "max": 1},
        "amount": {"min": 0, "max": 1000000000},  # 1 billion
    },
    
    # Source tracking
    "add_source_column": True,  # Track which source data came from
}

# ============================================================
# MERGE STRATEGY
# ============================================================

MERGE_STRATEGY = {
    "approach": "keep_separate",
    "description": "Keep all sources separate for Phase 2 fuzzy matching consolidation",
    
    "add_metadata_columns": {
        "source": "Which source this record came from",
        "load_timestamp": "When this record was loaded",
        "record_id": "Unique identifier for tracking",
    },
    
    # Don't pre-merge, let Phase 2 handle it
    "pre_merge_duplicates": False,
    
    # But do track source for Phase 2
    "track_variant_source": True,
}

# ============================================================
# PHASE 2 CONSOLIDATION HINTS
# ============================================================

PHASE2_CONSOLIDATION_HINTS = {
    "expected_duplicates": {
        "apple": ["Apple", "Apple Inc", "Apple USA"],
        "cisco": ["Cisco", "Cisco Systems", "Cisco Inc"],
        "dell": ["Dell", "Dell Technologies", "Dell USA"],
        "samsung": ["Samsung", "Samsung Electronics", "Samsung Group"],
        "intel": ["Intel", "Intel Corporation", "Intel Corp"],
    },
    
    "data_sources_by_quadrant": {
        "Strategic": ["Kaggle (lead time, volume)", "Real vendors", "Synthetic"],
        "Leverage": ["Kaggle", "Real vendors", "Synthetic"],
        "Bottleneck": ["Kaggle (single source risk)", "Real vendors"],
        "Tactical": ["Synthetic", "Real vendors"],
    },
}

# ============================================================
# SUMMARY OF COMBINED DATASET
# ============================================================

EXPECTED_DATASET_SIZE = {
    "total_suppliers": "~2,000+",
    "total_transactions": "~3,000+",
    "total_disruption_events": "~32+",
    
    "by_source": {
        "Phase1 Synthetic": {
            "suppliers": 1800,
            "transactions": 1000,
            "kvadrant": 16,
        },
        "Real Vendors": {
            "suppliers": 150,
            "transactions": 425,
            "kvadrant": 150,
        },
        "Kaggle": {
            "suppliers": "100+",
            "transactions": "500+",
            "kvadrant": "75+",
        },
        "Guardian News": {
            "disruptions": "32+",
        },
    },
    
    "total_features": 30,
    "countries_represented": 20,
    "industries_represented": 25,
}

# ============================================================
# LOGGING & REPORTING
# ============================================================

LOGGING = {
    "log_file": "logs/training_dataset_builder.log",
    "log_level": "INFO",
    "report_dir": "reports/training_dataset",
    
    "reports_to_generate": [
        "dataset_composition",
        "source_distribution",
        "data_quality_report",
        "consolidation_hints",
        "phase2_readiness",
    ],
}

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_total_expected_rows(table_type: str) -> int:
    """Calculate expected rows for a table type."""
    total = 0
    
    for source_name, source_config in DATA_SOURCES.items():
        if source_config.get("enabled", False):
            estimated = source_config.get("estimated_rows", {})
            if table_type in estimated:
                total += estimated[table_type]
    
    return total

def get_all_enabled_sources() -> list:
    """Get list of enabled data sources."""
    return [
        name for name, config in DATA_SOURCES.items()
        if config.get("enabled", False)
    ]

def print_dataset_summary():
    """Print summary of expected training dataset."""
    print("\n" + "="*70)
    print("TRAINING DATASET COMPOSITION")
    print("="*70)
    
    print("\nData Sources (Enabled):")
    for source_name in get_all_enabled_sources():
        source = DATA_SOURCES[source_name]
        print(f"  ✓ {source_name.upper()}")
        print(f"    Description: {source['description']}")
    
    print("\nExpected Dataset Size:")
    print(f"  Total Suppliers: {EXPECTED_DATASET_SIZE['total_suppliers']}")
    print(f"  Total Transactions: {EXPECTED_DATASET_SIZE['total_transactions']}")
    print(f"  Disruption Events: {EXPECTED_DATASET_SIZE['total_disruption_events']}")
    
    print("\nBreakdown by Source:")
    for source, sizes in EXPECTED_DATASET_SIZE["by_source"].items():
        print(f"  {source}:")
        for table, count in sizes.items():
            if count:
                print(f"    - {table}: {count}")
    
    print("\nMerge Strategy:")
    print(f"  {MERGE_STRATEGY['description']}")
    print(f"  Keep Separate: {not MERGE_STRATEGY['pre_merge_duplicates']}")
    print(f"  Track Source: {MERGE_STRATEGY['track_variant_source']}")
    
    print("\nPhase 2 Consolidation:")
    print(f"  Expected duplicates across sources")
    print(f"  Fuzzy matching will consolidate variants")
    print(f"  Final unified suppliers: ~1,500")
    
    print("\n" + "="*70)

# ============================================================
# VALIDATION
# ============================================================

if __name__ == "__main__":
    print_dataset_summary()
    print("\nTraining dataset configuration loaded successfully!")
