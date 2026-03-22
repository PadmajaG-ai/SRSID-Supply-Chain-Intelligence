"""
Phase 1 Configuration File
Data Ingestion & Normalization Configuration
"""

import yaml
from datetime import datetime, timedelta

# ========================================
# DATABASE CONFIGURATION
# ========================================
DATABASE_CONFIG = {
    "host": "localhost",  # Change to your PostgreSQL host
    "port": 5432,
    "database": "supplier_risk_db",
    "user": "postgres",  # Change to your username
    "password": "MyAIDB",  # Change to your password
}

# ========================================
# SOURCE FILES CONFIGURATION
# ========================================
SOURCE_FILES = {
    "risk_assessment": {
        "file_path": "data/raw/supplier_risk_assessment.csv",
        "table_name": "raw_supplier_risk_assessment",
        "expected_rows": 1800,
        "has_header": True,
        "encoding": "utf-8",
    },
    "transactions": {
        "file_path": "data/raw/us_supply_chain_risk.csv",
        "table_name": "raw_supply_chain_transactions",
        "expected_rows": 1000,
        "has_header": True,
        "encoding": "utf-8",
    },
    "kraljic": {
        "file_path": "data/raw/kraljic_matrix.csv",
        "table_name": "raw_kraljic_matrix",
        "expected_rows": 1800,
        "has_header": True,
        "encoding": "utf-8",
    },
}

# ========================================
# COLUMN MAPPINGS
# ========================================
# Maps input CSV column names (can be multiple variations) to standardized output names
COLUMN_MAPPINGS = {
    "risk_assessment": {
        "supplier_name": {
            "input_names": ["Supplier Name", "supplier_name", "SUPPLIER_NAME", "Supplier", "supplier"],
            "output_name": "supplier_name",
            "data_type": "string",
            "required": True,
            "normalize": True,  # Trim whitespace, capitalize
        },
        "financial_stability_score": {
            "input_names": ["Financial Stability", "financial_stability_score", "FinancialStability", "fs_score"],
            "output_name": "financial_stability_score",
            "data_type": "float",
            "required": False,
        },
        "delivery_performance_score": {
            "input_names": ["Delivery Performance", "delivery_performance_score", "DeliveryPerformance", "dp_score"],
            "output_name": "delivery_performance_score",
            "data_type": "float",
            "required": False,
        },
        "historical_risk_category": {
            "input_names": ["Historical Risk Category", "historical_risk_category", "Risk Category", "risk_category"],
            "output_name": "historical_risk_category",
            "data_type": "string",
            "required": False,
        },
    },
    "transactions": {
        "supplier_name": {
            "input_names": ["Supplier", "Supplier Name", "supplier_name", "SUPPLIER"],
            "output_name": "supplier_name",
            "data_type": "string",
            "required": True,
            "normalize": True,
        },
        "transaction_amount": {
            "input_names": ["Amount", "transaction_amount", "Transaction Amount", "total_amount"],
            "output_name": "transaction_amount",
            "data_type": "float",
            "required": True,
        },
        "transaction_date": {
            "input_names": ["Date", "transaction_date", "Transaction Date", "date"],
            "output_name": "transaction_date",
            "data_type": "date",
            "required": True,
            "date_formats": ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y"],
        },
        "disruption_type": {
            "input_names": ["Disruption Type", "disruption_type", "Event Type", "event_type"],
            "output_name": "disruption_type",
            "data_type": "string",
            "required": False,
        },
        "disruption_details": {
            "input_names": ["Disruption Details", "disruption_details", "Description", "details"],
            "output_name": "disruption_details",
            "data_type": "string",
            "required": False,
        },
        "procurement_category": {
            "input_names": ["Procurement Category", "procurement_category", "Category", "category"],
            "output_name": "procurement_category",
            "data_type": "string",
            "required": False,
        },
        "invoice_number": {
            "input_names": ["Invoice Number", "invoice_number", "Invoice", "invoice_no"],
            "output_name": "invoice_number",
            "data_type": "string",
            "required": False,
        },
        "purchase_order_number": {
            "input_names": ["PO Number", "purchase_order_number", "PO", "po_number"],
            "output_name": "purchase_order_number",
            "data_type": "string",
            "required": False,
        },
    },
    "kraljic": {
        "supplier_name": {
            "input_names": ["Product_Name", "Supplier Name", "supplier_name", "Supplier", "supplier"],
            "output_name": "supplier_name",
            "data_type": "string",
            "required": True,
            "normalize": True,
        },
        "supply_risk_score": {
            "input_names": ["Supply_Risk_Score", "Supply Risk Score", "supply_risk_score", "SupplyRisk", "supply_risk"],
            "output_name": "supply_risk_score",
            "data_type": "float",
            "required": False,
        },
        "profit_impact_score": {
            "input_names": ["Profit_Impact_Score", "Profit Impact Score", "profit_impact_score", "ProfitImpact", "profit_impact"],
            "output_name": "profit_impact_score",
            "data_type": "float",
            "required": False,
        },
        "kraljic_quadrant": {
            "input_names": ["Kraljic_Category", "Kraljic Quadrant", "kraljic_quadrant", "Quadrant", "quadrant"],
            "output_name": "kraljic_quadrant",
            "data_type": "string",
            "required": False,
        },
        "category": {
            "input_names": ["Category", "category", "Product Category", "Product_Type"],
            "output_name": "category",
            "data_type": "string",
            "required": False,
        },
        "segment": {
            "input_names": ["Segment", "segment", "Supplier_Region", "Region"],
            "output_name": "segment",
            "data_type": "string",
            "required": False,
        },
    },
}

# ========================================
# VALIDATION RULES
# ========================================
VALIDATION_RULES = {
    "supplier_name": {
        "allow_null": False,
        "min_length": 2,
        "max_length": 500,
        "allow_special_chars": True,
        "trim_whitespace": True,
    },
    "financial_stability_score": {
        "allow_null": True,
        "min_value": 0.0,
        "max_value": 1.0,
        "outlier_threshold": 1.0,  # Values > 1.0 are flagged
    },
    "delivery_performance_score": {
        "allow_null": True,
        "min_value": 0.0,
        "max_value": 1.0,
        "outlier_threshold": 1.0,
    },
    "supply_risk_score": {
        "allow_null": True,
        "min_value": 0.0,
        "max_value": 1.0,
        "outlier_threshold": 1.0,
    },
    "profit_impact_score": {
        "allow_null": True,
        "min_value": 0.0,
        "max_value": 1.0,
        "outlier_threshold": 1.0,
    },
    "historical_risk_category": {
        "allow_null": True,
        "allowed_values": ["High", "Medium", "Low", "high", "medium", "low"],
    },
    "transaction_amount": {
        "allow_null": False,
        "min_value": 0.0,
        "max_value": 1000000000.0,  # $1B cap
        "allow_negative": False,
        "outlier_threshold_pct": 95,  # Flag amounts in top 5%
    },
    "transaction_date": {
        "allow_null": False,
        "min_date": datetime(2020, 1, 1),
        "max_date": datetime.now(),
        "allow_future": False,
    },
    "disruption_type": {
        "allow_null": True,
        "allowed_values": [
            "Strike",
            "Weather",
            "Shortage",
            "Recall",
            "Logistics Delay",
            "Factory Fire",
            "Pandemic",
            "Bankruptcy",
            "Lawsuit",
            "Other",
        ],
    },
    "kvadrant": {
        "allow_null": True,
        "allowed_values": ["Strategic", "Tactical", "Bottleneck", "Leverage"],
    },
}

# ========================================
# ERROR TOLERANCE THRESHOLDS
# ========================================
ERROR_TOLERANCE = {
    "max_null_pct": 5.0,  # If >5% of rows have nulls in required fields, warn
    "max_duplicate_pct": 2.0,  # If >2% duplicates, warn
    "max_error_pct": 3.0,  # If >3% rows have errors, fail
    "fail_on_missing_file": True,  # If any source file is missing, fail
    "fail_on_critical_column": True,  # If supplier_name is missing, fail
    "warn_on_data_drift": True,  # Warn if distribution changes significantly
}

# ========================================
# DATA QUALITY CHECKS
# ========================================
DATA_QUALITY_CHECKS = {
    "risk_assessment": {
        "min_rows": 1700,  # At least 90% of expected rows
        "unique_supplier_pct": 95,  # At least 95% unique suppliers
        "financial_stability_completion_pct": 80,  # At least 80% filled
    },
    "transactions": {
        "min_rows": 950,
        "date_range_check": True,  # Ensure dates are in reasonable range
        "amount_positive_pct": 99.0,  # At least 99% positive amounts
    },
    "kraljic": {
        "min_rows": 1750,
        "unique_supplier_pct": 95,
        "quadrant_distribution": {
            "Strategic": {"min_pct": 10, "max_pct": 40},
            "Tactical": {"min_pct": 10, "max_pct": 40},
            "Bottleneck": {"min_pct": 5, "max_pct": 30},
            "Leverage": {"min_pct": 10, "max_pct": 40},
        },
    },
}

# ========================================
# GUARDIAN API KEYWORDS (Phase 4)
# ========================================
GUARDIAN_API_KEYWORDS = {
    "operational": [
        "supply chain disruption",
        "logistics delay",
        "shortage",
        "backlog",
        "production delay",
        "factory fire",
        "quality issue",
        "delivery failure",
        "recall",
    ],
    "financial": [
        "bankruptcy",
        "insolvency",
        "financial loss",
        "restructuring",
        "acquisition",
        "price spike",
        "cost increase",
    ],
    "labor": [
        "strike",
        "labor dispute",
        "labor cost",
        "labor violation",
        "union",
        "worker strike",
    ],
    "geopolitical": [
        "tariff",
        "export restriction",
        "trade tension",
        "geopolitical risk",
        "sanctions",
        "trade war",
    ],
    "regulatory": [
        "regulatory fine",
        "compliance issue",
        "certification revoked",
        "environmental violation",
        "lawsuit",
    ],
    "security": [
        "cybersecurity breach",
        "data breach",
        "security incident",
    ],
}

# ========================================
# PHASE 4: GUARDIAN API CONFIGURATION
# ========================================
GUARDIAN_API_CONFIG = {
    "api_key": "YOUR_GUARDIAN_API_KEY_HERE",  # Set as environment variable
    "base_url": "https://content.guardianapis.com/search",
    "historical_lookback_days": 90,
    "top_suppliers_by_spend": 100,
    "page_size": 50,
    "show_fields": "headline,bodyText,webPublicationDate,byline,thumbnail,trailText",
    "rate_limit": {
        "calls_per_day": 5000,
        "calls_per_hour": 250,
    },
}

# ========================================
# SCHEDULER CONFIGURATION
# ========================================
SCHEDULER_CONFIG = {
    "trigger": "cron",
    "day_of_week": "mon-sun",  # Run every day
    "hour": 2,  # 2:00 AM
    "minute": 0,
    "timezone": "UTC",
    "max_instances": 1,  # Only one instance at a time
    "retry_config": {
        "max_retries": 3,
        "retry_delay_minutes": 5,
    },
    "notifications": {
        "on_success": {
            "email_to": ["procurement-head@company.com", "supply-chain-risk@company.com"],
            "include_report": True,
        },
        "on_failure": {
            "email_to": ["data-team@company.com"],
            "include_logs": True,
        },
    },
}

# ========================================
# LOGGING CONFIGURATION
# ========================================
LOGGING_CONFIG = {
    "log_dir": "logs/",
    "log_level": "INFO",
    "max_log_size_mb": 50,
    "backup_count": 10,
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
}

# ========================================
# HELPER FUNCTIONS
# ========================================
def get_guardian_search_query(supplier_name: str) -> str:
    """
    Constructs a Guardian API search query for a supplier.
    Includes multiple risk keywords to catch relevant articles.
    """
    all_keywords = []
    for category, keywords in GUARDIAN_API_KEYWORDS.items():
        all_keywords.extend(keywords)
    
    # Build OR query: supplier_name AND (keyword1 OR keyword2 OR ...)
    keyword_query = " OR ".join(all_keywords)
    return f'"{supplier_name}" AND ({keyword_query})'


def get_date_range():
    """Returns tuple of (from_date, to_date) for historical lookback."""
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=GUARDIAN_API_CONFIG["historical_lookback_days"])
    return from_date.isoformat(), to_date.isoformat()
