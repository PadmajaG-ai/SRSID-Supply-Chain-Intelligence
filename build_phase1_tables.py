"""
Build Phase 1 Tables
Create final raw_supplier_risk_assessment, raw_supply_chain_transactions, raw_kraljic_matrix
"""

import pandas as pd
import numpy as np
import logging
import sys
import os
import json
import random
from datetime import datetime, timedelta
from typing import Tuple

from hybrid_phase1_config import (
    OUTPUT_FILES,
    PATHS,
    LOGGING,
)

# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=LOGGING["format"],
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGGING["log_file"], encoding="utf-8"),
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# PHASE 1 TABLE BUILDER
# ============================================================

class Phase1TableBuilder:
    """Build final Phase 1 SRSID tables."""
    
    def __init__(self):
        self.vendors_df = None
        self.risk_metrics_df = None
        self.disruptions_df = None
        self.disruption_mapping_df = None
    
    def load_data(self) -> bool:
        """Load all required data."""
        try:
            logger.info("Loading data...")
            
            self.vendors_df = pd.read_csv(OUTPUT_FILES["vendors_with_industries"])
            logger.info(f"  Vendors: {len(self.vendors_df)}")
            
            self.risk_metrics_df = pd.read_csv(OUTPUT_FILES["vendor_risk_metrics"])
            logger.info(f"  Risk metrics: {len(self.risk_metrics_df)}")
            
            self.disruptions_df = pd.read_csv(OUTPUT_FILES["combined_disruptions"])
            logger.info(f"  Disruptions: {len(self.disruptions_df)}")
            
            self.disruption_mapping_df = pd.read_csv(OUTPUT_FILES["vendor_disruption_mapping"])
            logger.info(f"  Disruption mapping: {len(self.disruption_mapping_df)}")
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            return False
    
    def build_risk_assessment(self) -> pd.DataFrame:
        """Build raw_supplier_risk_assessment table."""
        
        logger.info("\nBuilding risk assessment table...")
        
        records = []
        
        for idx, vendor in self.vendors_df.iterrows():
            lifnr = vendor.get("LIFNR")
            
            # Get metrics for this vendor
            metrics = self.risk_metrics_df[self.risk_metrics_df["LIFNR"] == lifnr]
            
            if len(metrics) > 0:
                m = metrics.iloc[0]
            else:
                continue
            
            record = {
                "supplier_name": vendor.get("NAME1"),
                "financial_stability_score": float(m.get("financial_stability_score", 0.75)),
                "delivery_performance": float(m.get("delivery_performance", 0.75)),
                "historical_risk_category": m.get("historical_risk_category", "Medium"),
                "source_file_name": "hybrid_phase1_vendors.csv",
                "source_file_hash": "hybrid_phase1",
                "ingestion_timestamp": datetime.now().isoformat(),
                "ariba_supplier_id": lifnr,
                "supplier_country": vendor.get("Land1"),
                "supplier_city": vendor.get("ORT01"),
                "industry": vendor.get("Industry"),
                "is_variant": vendor.get("IsVariant", False),
            }
            
            records.append(record)
        
        df = pd.DataFrame(records)
        logger.info(f"Created {len(df)} risk assessment records")
        
        return df
    
    def build_transactions(self) -> pd.DataFrame:
        """Build raw_supply_chain_transactions table."""
        
        logger.info("\nBuilding transactions table...")
        
        records = []
        transaction_id = 1000
        
        # Create baseline transactions for all vendors
        country_spend = {
            "US": (150000, 1000000),
            "DE": (100000, 800000),
            "GB": (80000, 600000),
            "FR": (80000, 700000),
            "CH": (100000, 600000),
            "CA": (60000, 400000),
            "IN": (40000, 200000),
            "JP": (120000, 900000),
            "NL": (90000, 700000),
            "SA": (200000, 1500000),
            "DK": (100000, 600000),
        }
        
        # Baseline spending
        for idx, vendor in self.vendors_df.iterrows():
            lifnr = vendor.get("LIFNR")
            name = vendor.get("NAME1")
            country = vendor.get("Land1")
            industry = vendor.get("Industry")
            
            spend_range = country_spend.get(country, (50000, 500000))
            spend = random.randint(spend_range[0], spend_range[1])
            
            # Create 1-3 transactions per vendor
            num_trans = random.randint(1, 3)
            
            for i in range(num_trans):
                days_ago = random.randint(0, 365)
                trans_date = (datetime.now() - timedelta(days=days_ago)).date()
                
                record = {
                    "supplier": name,
                    "amount": spend / num_trans,
                    "transaction_date": trans_date,
                    "disruption_type": "None",
                    "procurement_category": industry,
                    "invoice_number": f"INV-{transaction_id:06d}",
                    "po_number": f"PO-{transaction_id:06d}",
                    "source_file_name": "hybrid_phase1_baseline.csv",
                    "ingestion_timestamp": datetime.now().isoformat(),
                    "ariba_supplier_id": lifnr,
                }
                
                records.append(record)
                transaction_id += 1
        
        # Add disruption events as transactions
        if len(self.disruptions_df) > 0:
            for idx, disruption in self.disruptions_df.iterrows():
                # Try to match vendor in disruption mapping
                actors = disruption.get("actors", "")
                
                if actors and pd.notna(actors):
                    for vendor_name in actors.split(";"):
                        vendor_name = vendor_name.strip()
                        
                        # Find matching vendor
                        matching = self.vendors_df[
                            self.vendors_df["NAME1"].str.contains(vendor_name.split()[0], case=False, na=False)
                        ]
                        
                        if not matching.empty:
                            m = matching.iloc[0]
                            
                            record = {
                                "supplier": vendor_name,
                                "amount": 0,  # Disruptions don't have spend
                                "transaction_date": disruption.get("event_date"),
                                "disruption_type": disruption.get("disruption_type", "None"),
                                "procurement_category": m.get("Industry"),
                                "invoice_number": f"DISP-{transaction_id:06d}",
                                "po_number": f"DISP-{transaction_id:06d}",
                                "source_file_name": "disruption_events_hybrid.csv",
                                "ingestion_timestamp": datetime.now().isoformat(),
                                "ariba_supplier_id": m.get("LIFNR"),
                            }
                            
                            records.append(record)
                            transaction_id += 1
        
        df = pd.DataFrame(records)
        logger.info(f"Created {len(df)} transaction records")
        
        return df
    
    def build_kraljic(self) -> pd.DataFrame:
        """Build raw_kraljic_matrix table."""
        
        logger.info("\nBuilding Kraljic table...")
        
        records = []
        
        for idx, vendor in self.vendors_df.iterrows():
            lifnr = vendor.get("LIFNR")
            
            # Get metrics
            metrics = self.risk_metrics_df[self.risk_metrics_df["LIFNR"] == lifnr]
            
            if len(metrics) > 0:
                m = metrics.iloc[0]
            else:
                continue
            
            record = {
                "supplier_name": vendor.get("NAME1"),
                "supply_risk_score": float(m.get("supply_risk_score", 0.5)),
                "profit_impact_score": float(m.get("profit_impact_score", 0.5)),
                "kraljic_quadrant": m.get("kraljic_quadrant", "Tactical"),
                "source_file_name": "hybrid_phase1_vendors.csv",
                "source_file_hash": "hybrid_phase1",
                "ingestion_timestamp": datetime.now().isoformat(),
                "ariba_supplier_id": lifnr,
                "lead_time_days": m.get("lead_time_days", 30),
                "order_volume": m.get("order_volume", 10000),
            }
            
            records.append(record)
        
        df = pd.DataFrame(records)
        logger.info(f"Created {len(df)} Kraljic records")
        
        return df
    
    def save_tables(self, risk_df: pd.DataFrame, trans_df: pd.DataFrame, kraljic_df: pd.DataFrame) -> bool:
        """Save Phase 1 tables to CSV."""
        
        logger.info("\nSaving Phase 1 tables...")
        
        try:
            os.makedirs(os.path.dirname(OUTPUT_FILES["phase1_risk_assessment"]), exist_ok=True)
            
            risk_df.to_csv(OUTPUT_FILES["phase1_risk_assessment"], index=False)
            logger.info(f"Saved risk assessment to {OUTPUT_FILES['phase1_risk_assessment']}")
            
            trans_df.to_csv(OUTPUT_FILES["phase1_transactions"], index=False)
            logger.info(f"Saved transactions to {OUTPUT_FILES['phase1_transactions']}")
            
            kraljic_df.to_csv(OUTPUT_FILES["phase1_kraljic"], index=False)
            logger.info(f"Saved Kraljic to {OUTPUT_FILES['phase1_kraljic']}")
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to save tables: {e}")
            return False
    
    def generate_quality_report(self, risk_df: pd.DataFrame, trans_df: pd.DataFrame, kraljic_df: pd.DataFrame) -> bool:
        """Generate Phase 1 quality report."""
        
        logger.info("\nGenerating quality report...")
        
        try:
            # Calculate metrics
            vendors_with_disruptions = len(trans_df[trans_df["disruption_type"] != "None"])
            total_vendors = len(risk_df)
            
            report = {
                "timestamp": datetime.now().isoformat(),
                "phase1_tables": {
                    "risk_assessment": {
                        "rows": len(risk_df),
                        "columns": list(risk_df.columns),
                    },
                    "transactions": {
                        "rows": len(trans_df),
                        "columns": list(trans_df.columns),
                        "disruption_events": vendors_with_disruptions,
                    },
                    "kraljic": {
                        "rows": len(kraljic_df),
                        "columns": list(kraljic_df.columns),
                    },
                },
                "data_quality": {
                    "vendors_with_real_disruptions": vendors_with_disruptions,
                    "vendors_total": total_vendors,
                    "coverage_percentage": round(vendors_with_disruptions / total_vendors * 100, 1),
                    "transactions_with_disruptions": len(trans_df[trans_df["disruption_type"] != "None"]),
                    "transactions_baseline": len(trans_df[trans_df["disruption_type"] == "None"]),
                },
                "sources": {
                    "real_disruptions": "GDELT + NewsAPI (hybrid)",
                    "risk_metrics": "Kaggle patterns",
                    "vendors": "56 SAP suppliers + variants",
                },
            }
            
            os.makedirs(os.path.dirname(OUTPUT_FILES["phase1_quality_report"]), exist_ok=True)
            
            with open(OUTPUT_FILES["phase1_quality_report"], "w") as f:
                json.dump(report, f, indent=2)
            
            logger.info(f"Saved report to {OUTPUT_FILES['phase1_quality_report']}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to generate report: {e}")
            return False
    
    def print_summary(self, risk_df: pd.DataFrame, trans_df: pd.DataFrame, kraljic_df: pd.DataFrame):
        """Print summary statistics."""
        
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 1 TABLES SUMMARY")
        logger.info("=" * 70)
        
        logger.info(f"\nraw_supplier_risk_assessment:")
        logger.info(f"  Records: {len(risk_df)}")
        logger.info(f"  Risk distribution:")
        for risk, count in risk_df["historical_risk_category"].value_counts().items():
            logger.info(f"    {risk}: {count}")
        
        logger.info(f"\nraw_supply_chain_transactions:")
        logger.info(f"  Records: {len(trans_df)}")
        logger.info(f"  Baseline transactions: {len(trans_df[trans_df['disruption_type'] == 'None'])}")
        logger.info(f"  Disruption events: {len(trans_df[trans_df['disruption_type'] != 'None'])}")
        
        logger.info(f"\nraw_kraljic_matrix:")
        logger.info(f"  Records: {len(kraljic_df)}")
        logger.info(f"  Quadrant distribution:")
        for quad, count in kraljic_df["kraljic_quadrant"].value_counts().items():
            logger.info(f"    {quad}: {count}")

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main execution."""
    try:
        logger.info("=" * 70)
        logger.info("PHASE 1 TABLE BUILDER")
        logger.info("=" * 70)
        
        # Create builder
        builder = Phase1TableBuilder()
        
        # Load data
        logger.info("\n[STEP 1] Loading data...")
        if not builder.load_data():
            return 1
        
        # Build tables
        logger.info("\n[STEP 2] Building tables...")
        risk_df = builder.build_risk_assessment()
        trans_df = builder.build_transactions()
        kraljic_df = builder.build_kraljic()
        
        # Save
        logger.info("\n[STEP 3] Saving tables...")
        if not builder.save_tables(risk_df, trans_df, kraljic_df):
            return 1
        
        # Generate report
        logger.info("\n[STEP 4] Generating quality report...")
        if not builder.generate_quality_report(risk_df, trans_df, kraljic_df):
            return 1
        
        # Summary
        builder.print_summary(risk_df, trans_df, kraljic_df)
        
        logger.info("\n" + "=" * 70)
        logger.info("✓ PHASE 1 TABLE BUILDING COMPLETE")
        logger.info("=" * 70)
        logger.info("\nPhase 1 tables are ready for Phase 2 consolidation!")
        
        return 0
    
    except Exception as e:
        logger.error(f"Phase 1 building failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
