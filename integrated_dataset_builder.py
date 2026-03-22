"""
Integrated Dataset Builder
Combine synthesized vendors with Guardian news data for SRSID Phase 1 import
"""

import pandas as pd
import numpy as np
import logging
import sys
from datetime import datetime, timedelta
from typing import Tuple
import random

from vendor_synthesis_config import (
    OUTPUT_FILES,
    TABLE_MAPPING,
    normalize_score,
)

# ============================================================
# LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/dataset_builder.log', encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# INTEGRATED DATASET BUILDER
# ============================================================

class IntegratedDatasetBuilder:
    """Build final SRSID Phase 1 ready datasets."""
    
    def __init__(self):
        self.vendor_df = None
        self.disruption_df = None
    
    def load_data(self) -> bool:
        """Load synthesized vendor and disruption data."""
        try:
            logger.info("Loading vendor data...")
            self.vendor_df = pd.read_csv(OUTPUT_FILES["vendor_variants"])
            
            logger.info("Loading disruption data...")
            self.disruption_df = pd.read_csv(OUTPUT_FILES["disruption_events"])
            
            return True
        
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")
            logger.info("Run synthesize_vendors.py and guardian_news_integration.py first")
            return False
    
    def build_risk_assessment_table(self) -> pd.DataFrame:
        """
        Build raw_supplier_risk_assessment table.
        Maps to SRSID columns.
        """
        logger.info("\nBuilding raw_supplier_risk_assessment...")
        
        records = []
        
        for idx, vendor in self.vendor_df.iterrows():
            record = {
                "supplier_name": vendor["NAME1"],
                "financial_stability_score": vendor["FIN_STAB_NORM"],
                "delivery_performance": vendor["PERF_NORM"],
                "historical_risk_category": vendor["RISC"],
                "source_file_name": "vendor_synthesis_sap_compliant.csv",
                "source_file_hash": "synthetic_vendor_data",
                "ingestion_timestamp": datetime.now(),
                "ariba_supplier_id": vendor["LIFNR"],
                "supplier_country": vendor["Land1"],
                "supplier_city": vendor["ORT01"],
                "industry": vendor["NACE_DESC"],
            }
            records.append(record)
        
        df = pd.DataFrame(records)
        logger.info(f"Created {len(df)} risk assessment records")
        
        return df
    
    def build_transaction_table(self) -> pd.DataFrame:
        """
        Build raw_supply_chain_transactions table.
        Creates transactions from disruption events and baseline spend.
        """
        logger.info("\nBuilding raw_supply_chain_transactions...")
        
        records = []
        transaction_id = 1000
        
        # First, create baseline transactions (current spend)
        for idx, vendor in self.vendor_df.iterrows():
            # Generate baseline spend transactions
            country = vendor["Land1"]
            
            # Determine spend based on country
            spend_mapping = {
                "US": (150000, 1000000),
                "DE": (100000, 800000),
                "GB": (80000, 600000),
                "FR": (80000, 700000),
                "CH": (100000, 600000),
                "CA": (60000, 400000),
                "IN": (40000, 200000),
            }
            
            min_spend, max_spend = spend_mapping.get(country, (50000, 500000))
            spend = random.randint(min_spend, max_spend)
            
            # Create 1-3 transactions per vendor (simulate purchase history)
            num_transactions = random.randint(1, 3)
            
            for i in range(num_transactions):
                # Spread transactions over past year
                days_ago = random.randint(0, 365)
                transaction_date = (datetime.now() - timedelta(days=days_ago)).date()
                
                record = {
                    "supplier": vendor["NAME1"],
                    "amount": spend / num_transactions,
                    "transaction_date": transaction_date,
                    "disruption_type": "None",
                    "procurement_category": vendor["NACE_DESC"],
                    "invoice_number": f"INV-{transaction_id:06d}",
                    "po_number": f"PO-{transaction_id:06d}",
                    "source_file_name": "vendor_synthesis_sap_compliant.csv",
                    "ingestion_timestamp": datetime.now(),
                    "ariba_supplier_id": vendor["LIFNR"],
                }
                records.append(record)
                transaction_id += 1
        
        # Add disruption events as transactions
        if self.disruption_df is not None and len(self.disruption_df) > 0:
            for idx, disruption in self.disruption_df.iterrows():
                # Find matching vendor
                vendor_name = disruption.get("Vendor_Name", "")
                matching = self.vendor_df[
                    self.vendor_df["NAME1"].str.contains(
                        vendor_name.split()[0],
                        case=False,
                        na=False
                    )
                ]
                
                if not matching.empty:
                    record = {
                        "supplier": vendor_name,
                        "amount": 0,  # Disruption doesn't have amount
                        "transaction_date": pd.to_datetime(disruption.get("Event_Date")).date() 
                                          if pd.notna(disruption.get("Event_Date")) else datetime.now().date(),
                        "disruption_type": disruption.get("Disruption_Type", "None"),
                        "procurement_category": matching.iloc[0]["NACE_DESC"],
                        "invoice_number": f"DISRUPTION-{transaction_id:06d}",
                        "po_number": f"DISP-{transaction_id:06d}",
                        "source_file_name": "disruption_events_guardian_api.csv",
                        "ingestion_timestamp": datetime.now(),
                        "ariba_supplier_id": matching.iloc[0]["LIFNR"],
                    }
                    records.append(record)
                    transaction_id += 1
        
        df = pd.DataFrame(records)
        logger.info(f"Created {len(df)} transaction records")
        
        return df
    
    def build_kraljic_table(self) -> pd.DataFrame:
        """
        Build raw_kraljic_matrix table.
        Strategic positioning for each supplier.
        """
        logger.info("\nBuilding raw_kraljic_matrix...")
        
        records = []
        
        for idx, vendor in self.vendor_df.iterrows():
            record = {
                "supplier_name": vendor["NAME1"],
                "supply_risk_score": vendor["SupplyRiskScore"],
                "profit_impact_score": vendor["ProfitImpactScore"],
                "kraljic_quadrant": vendor["KVADRANT"],
                "source_file_name": "vendor_synthesis_sap_compliant.csv",
                "source_file_hash": "synthetic_vendor_data",
                "ingestion_timestamp": datetime.now(),
                "ariba_supplier_id": vendor["LIFNR"],
            }
            records.append(record)
        
        df = pd.DataFrame(records)
        logger.info(f"Created {len(df)} Kraljic matrix records")
        
        return df
    
    def build_all_tables(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Build all three Phase 1 tables.
        
        Returns:
            Tuple of (risk_assessment, transactions, kraljic)
        """
        risk_assessment = self.build_risk_assessment_table()
        transactions = self.build_transaction_table()
        kraljic = self.build_kraljic_table()
        
        return risk_assessment, transactions, kraljic
    
    def save_phase1_tables(self):
        """Save Phase 1 tables to CSV."""
        logger.info("\nSaving Phase 1 tables...")
        
        # Load data if not already loaded
        if self.vendor_df is None:
            if not self.load_data():
                return False
        
        # Build tables
        risk_assessment, transactions, kraljic = self.build_all_tables()
        
        # Save to CSV
        risk_file = "phase1_supplier_risk_assessment.csv"
        trans_file = "phase1_supply_chain_transactions.csv"
        kraljic_file = "phase1_kraljic_matrix.csv"
        
        risk_assessment.to_csv(risk_file, index=False)
        logger.info(f"Saved to {risk_file}")
        
        transactions.to_csv(trans_file, index=False)
        logger.info(f"Saved to {trans_file}")
        
        kraljic.to_csv(kraljic_file, index=False)
        logger.info(f"Saved to {kraljic_file}")
        
        return True
    
    def print_summary(self):
        """Print dataset summary."""
        if self.vendor_df is None:
            return
        
        logger.info("\n" + "=" * 70)
        logger.info("INTEGRATED DATASET SUMMARY")
        logger.info("=" * 70)
        logger.info(f"\nVendor Data:")
        logger.info(f"  Master vendors: {len(self.vendor_df[~self.vendor_df.get('IsVariant', False)])}")
        logger.info(f"  Total vendors (with variants): {len(self.vendor_df)}")
        logger.info(f"  Countries: {self.vendor_df['Land1'].nunique()}")
        logger.info(f"  Industries: {self.vendor_df['NACE_DESC'].nunique()}")
        
        logger.info(f"\nRisk Distribution:")
        risk_counts = self.vendor_df["RISC"].value_counts()
        for risk, count in risk_counts.items():
            logger.info(f"  {risk}: {count} vendors")
        
        logger.info(f"\nKraljic Distribution:")
        kraljic_counts = self.vendor_df["KVADRANT"].value_counts()
        for quad, count in kraljic_counts.items():
            logger.info(f"  {quad}: {count} vendors")
        
        if self.disruption_df is not None and len(self.disruption_df) > 0:
            logger.info(f"\nDisruption Events:")
            logger.info(f"  Total events: {len(self.disruption_df)}")
            
            disruption_counts = self.disruption_df["Disruption_Type"].value_counts()
            for disruption, count in disruption_counts.items():
                logger.info(f"  {disruption}: {count} events")

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main execution."""
    try:
        logger.info("=" * 70)
        logger.info("INTEGRATED DATASET BUILDER")
        logger.info("=" * 70)
        
        # Create builder
        builder = IntegratedDatasetBuilder()
        
        # Load data
        logger.info("\n[STEP 1] Loading data...")
        if not builder.load_data():
            return 1
        
        # Save Phase 1 tables
        logger.info("\n[STEP 2] Building Phase 1 tables...")
        if not builder.save_phase1_tables():
            return 1
        
        # Print summary
        logger.info("\n[STEP 3] Generating summary...")
        builder.print_summary()
        
        logger.info("\n" + "=" * 70)
        logger.info("✓ DATASET INTEGRATION COMPLETE")
        logger.info("=" * 70)
        logger.info("\nReady for Phase 1 import!")
        logger.info("Files created:")
        logger.info("  - phase1_supplier_risk_assessment.csv")
        logger.info("  - phase1_supply_chain_transactions.csv")
        logger.info("  - phase1_kraljic_matrix.csv")
        
        return 0
    
    except Exception as e:
        logger.error(f"Dataset building failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    import os
    os.makedirs("logs", exist_ok=True)
    sys.exit(main())
