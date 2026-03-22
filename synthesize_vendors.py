"""
Vendor Synthesis Engine (FIXED)
Generate synthetic vendor data with variants and SAP-compliant columns
Loads vendors from vendors_list_simplified.csv instead of hardcoded config
"""

import pandas as pd
import numpy as np
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import random

from vendor_synthesis_config import (
    VARIANT_TEMPLATES,
    SYNTHETIC_DATA_CONFIG,
    SAP_COLUMNS,
    COUNTRY_CODES,
    OUTPUT_FILES,
    get_industry_risk_profile,
    get_country_spend_profile,
    normalize_score,
    get_short_name,
)

# ============================================================
# LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/vendor_synthesis.log', encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# LOAD VENDORS FROM CSV
# ============================================================

def load_vendors_from_csv(csv_path: str = "vendors_list_simplified.csv") -> List[Dict]:
    """
    Load vendors from CSV file (vendors_list_simplified.csv)
    
    CSV format:
    Vendor Name,Industry
    Apple,Electronics & Semiconductors
    Tesla,Electronics & Vehicles
    ...
    
    Args:
        csv_path: Path to CSV file with vendors
    
    Returns:
        List of vendor dictionaries with keys: name, country, city, industry
    """
    
    vendors = []
    
    try:
        # Check if file exists
        if not os.path.exists(csv_path):
            logger.error(f"\n❌ ERROR: {csv_path} not found!")
            logger.error(f"   Looking in: {os.path.abspath(csv_path)}")
            logger.error("   Please create vendors_list_simplified.csv with columns: 'Vendor Name', 'Industry'")
            return []
        
        logger.info(f"\n📂 Loading vendors from: {csv_path}")
        
        # Load CSV
        df = pd.read_csv(csv_path)
        
        # Check required columns
        if "Vendor Name" not in df.columns:
            logger.error(f"❌ ERROR: Missing required column 'Vendor Name'")
            logger.error(f"   Available columns: {list(df.columns)}")
            return []
        
        logger.info(f"✓ CSV columns: {list(df.columns)}")
        
        # Process each row
        skipped = 0
        for idx, row in df.iterrows():
            vendor_name = str(row["Vendor Name"]).strip()
            
            # Skip empty rows
            if not vendor_name or vendor_name == "nan" or pd.isna(row["Vendor Name"]):
                skipped += 1
                continue
            
            # Get industry (optional)
            industry = str(row.get("Industry", "")).strip() if "Industry" in df.columns else ""
            if pd.isna(row.get("Industry")) or industry == "nan" or industry == "":
                industry = None
            
            vendor = {
                "name": vendor_name,
                "country": "US",  # Default (not in simplified CSV)
                "city": "Default",  # Default (not in simplified CSV)
                "industry": industry,
            }
            
            vendors.append(vendor)
        
        logger.info(f"✓ Loaded {len(vendors)} vendors from CSV")
        if skipped > 0:
            logger.warning(f"⚠ Skipped {skipped} empty rows")
        
        return vendors
    
    except Exception as e:
        logger.error(f"❌ ERROR loading vendors from CSV: {e}", exc_info=True)
        return []

# ============================================================
# VENDOR SYNTHESIS ENGINE
# ============================================================

class VendorSynthesizer:
    """Generate synthetic vendor data with variants and realistic metrics."""
    
    def __init__(self, vendors: List[Dict]):
        self.vendors = vendors  # Vendors loaded from CSV
        self.vendors_master = []
        self.vendors_with_variants = []
        self.vendor_counter = 1000
        self.logger = logging.getLogger(__name__)
    
    def synthesize_all_vendors(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Synthesize master vendors and their variants.
        
        Returns:
            Tuple[master_df, complete_df]
        """
        self.logger.info("=" * 70)
        self.logger.info("VENDOR SYNTHESIS ENGINE")
        self.logger.info("=" * 70)
        
        # Step 1: Create master vendors
        self.logger.info("\n[STEP 1] Creating master vendor records...")
        master_df = self._create_master_vendors()
        self.logger.info(f"Created {len(master_df)} master vendors")
        
        # Step 2: Generate variants
        self.logger.info("\n[STEP 2] Generating vendor variants...")
        variants_df = self._generate_variants(master_df)
        self.logger.info(f"Generated {len(variants_df)} variant vendors")
        
        # Step 3: Combine for complete vendor list
        self.logger.info("\n[STEP 3] Combining master + variants...")
        complete_df = pd.concat([master_df, variants_df], ignore_index=True)
        self.logger.info(f"Total unique vendors: {len(complete_df)}")
        
        # Step 4: Add synthetic metrics
        self.logger.info("\n[STEP 4] Adding synthetic metrics...")
        complete_df = self._add_synthetic_metrics(complete_df)
        
        # Step 5: Add SAP columns
        self.logger.info("\n[STEP 5] Adding SAP-compliant columns...")
        complete_df = self._add_sap_columns(complete_df)
        
        # Step 6: Extract masters from complete_df (after SAP columns added)
        self.logger.info("\n[STEP 6] Extracting master vendors with SAP columns...")
        master_df = complete_df[complete_df["IsVariant"] == False].copy()
        self.logger.info(f"Master vendors (with SAP columns): {len(master_df)}")
        
        return master_df, complete_df
    
    def _create_master_vendors(self) -> pd.DataFrame:
        """Create master vendor records from CSV vendors."""
        records = []
        
        for vendor in self.vendors:
            lifnr = f"{self.vendor_counter:06d}"
            self.vendor_counter += 1
            
            record = {
                "LIFNR": lifnr,
                "Name": vendor["name"],
                "ORT01": vendor.get("city", "Default"),
                "Land1": vendor.get("country", "US"),
                "Industry": vendor.get("industry"),  # Can be None
                "IsVariant": False,
                "ParentLIFNR": lifnr,
                "CreationDate": datetime.now().date(),
            }
            
            records.append(record)
        
        return pd.DataFrame(records)
    
    def _generate_variants(self, master_df: pd.DataFrame) -> pd.DataFrame:
        """Generate variants for each master vendor."""
        variant_records = []
        
        for idx, master in master_df.iterrows():
            # Generate 3-5 variants per vendor
            num_variants = random.randint(3, 5)
            
            for _ in range(num_variants):
                # Select random template
                template = random.choice(VARIANT_TEMPLATES)
                
                # Generate variant name
                short_name = get_short_name(master["Name"])
                variant_name = template.format(
                    name=master["Name"],
                    short_name=short_name
                )
                
                # Create variant record
                lifnr = f"{self.vendor_counter:06d}"
                self.vendor_counter += 1
                
                record = {
                    "LIFNR": lifnr,
                    "Name": variant_name,
                    "ORT01": master["ORT01"],  # Same city as parent
                    "Land1": master["Land1"],   # Same country as parent
                    "Industry": master["Industry"],
                    "IsVariant": True,
                    "ParentLIFNR": master["LIFNR"],
                    "ParentName": master["Name"],
                    "CreationDate": datetime.now().date(),
                }
                
                variant_records.append(record)
        
        return pd.DataFrame(variant_records)
    
    def _add_synthetic_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add synthetic performance and risk metrics."""
        
        # Get risk profile based on industry
        def get_metrics(row):
            industry = row.get("Industry", "Default")
            
            # Handle None/empty industry
            if pd.isna(industry) or industry is None or industry == "":
                industry = "Default"
            
            profile = get_industry_risk_profile(str(industry))
            
            # Base performance score
            base_perf = random.randint(
                profile["perf_min"],
                profile["perf_max"]
            )
            
            # Variants have slightly lower scores
            if row.get("IsVariant", False):
                variance = SYNTHETIC_DATA_CONFIG["variant_variance"]["perf_score_variance"]
                perf_score = max(0, base_perf - random.randint(0, variance))
            else:
                perf_score = base_perf
            
            # Financial stability (slightly lower than perf score)
            fin_stab = max(0, perf_score - random.randint(0, 5))
            
            # Risk category based on performance
            if perf_score >= 85:
                risk = "Low"
            elif perf_score >= 70:
                risk = "Medium"
            else:
                risk = "High"
            
            # Supply risk score (for Kraljic)
            supply_risk = 1.0 - (perf_score / 100)  # Inverse relationship
            
            # Profit impact (random, independent of performance)
            profit_impact = random.uniform(0.3, 0.9)
            
            return pd.Series({
                "PERF_SCORE": perf_score,
                "FIN_STAB": fin_stab,
                "RISC": risk,
                "SupplyRiskScore": round(supply_risk, 2),
                "ProfitImpactScore": round(profit_impact, 2),
            })
        
        metrics_df = df.apply(get_metrics, axis=1)
        df = pd.concat([df, metrics_df], axis=1)
        
        # Add normalized scores (0-1)
        df["PERF_NORM"] = df["PERF_SCORE"].apply(lambda x: normalize_score(x))
        df["FIN_STAB_NORM"] = df["FIN_STAB"].apply(lambda x: normalize_score(x))
        
        return df
    
    def _add_sap_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add SAP-compliant columns."""
        
        # Truncate Name to SAP limit (35 chars for NAME1)
        df["NAME1"] = df["Name"].str[:35]
        df["NAME2"] = df["Name"].str[35:]
        
        # Add SAP fields
        df["KTOKK"] = "Z001"  # Vendor account group (Z001 = standard)
        df["ZTERM"] = "NET30"  # Payment terms: Net 30
        df["INCO1"] = "FOB"    # Incoterms: FOB
        df["REGIO"] = df["Land1"]  # Region = Country
        df["PSTLZ"] = "00000"  # Postal code (placeholder)
        df["TELF1"] = ""       # Telephone (empty)
        df["SMTP_ADDR"] = ""   # Email (empty)
        
        # Industry mapping to SAP NACE codes
        nace_mapping = {
            "Pharmaceuticals": "2100",
            "Electronics": "2620",
            "Electronics & Semiconductors": "2620",
            "Electronics & Vehicles": "2910",
            "Manufacturing": "2500",
            "Chemicals": "2000",
            "IT Distribution": "4759",
            "IT Services": "6209",
            "Laboratory Equipment": "2841",
            "Distribution": "4610",
            "Retail/Distribution": "4610",
            "Industrial Manufacturing": "2900",
            "Robotics & Automation": "2851",
            "Networking": "2620",
            "Networking & IT": "2620",
            "Office Supplies": "4762",
            "Retail": "4711",
            "Entertainment": "9001",
            "Electrical Distribution": "4755",
            "Electronics Distribution": "4751",
            "Life Sciences": "2170",
            "Food & Beverage": "1000",
            "Energy & Oil": "0600",
            "Building Materials": "2370",
            "Engineering & Construction": "4120",
            "Logistics": "4939",
            "Shipping & Maritime": "5012",
            "Consumer Goods": "1520",
            "Financial Services": "6411",
        }
        
        df["NACE"] = df["Industry"].map(nace_mapping).fillna("9999")
        df["NACE_DESC"] = df["Industry"]
        
        # Timestamps
        df["CREA_DATE"] = df["CreationDate"]
        df["CHANGE_DATE"] = datetime.now().date()
        
        # Add category (for Kraljic)
        def get_kvadrant(supply_risk, profit_impact):
            if supply_risk > 0.5 and profit_impact > 0.5:
                return "Strategic"
            elif supply_risk <= 0.5 and profit_impact > 0.5:
                return "Leverage"
            elif supply_risk > 0.5 and profit_impact <= 0.5:
                return "Bottleneck"
            else:
                return "Tactical"
        
        df["KVADRANT"] = df.apply(
            lambda x: get_kvadrant(x["SupplyRiskScore"], x["ProfitImpactScore"]),
            axis=1
        )
        
        return df
    
    def save_to_csv(self, master_df: pd.DataFrame, complete_df: pd.DataFrame):
        """Save vendor data to CSV files."""
        
        # Define columns for output
        sap_output_cols = [
            "LIFNR", "NAME1", "NAME2", "ORT01", "Land1", "REGIO",
            "PSTLZ", "TELF1", "SMTP_ADDR", "KTOKK", "ZTERM", "INCO1",
            "NACE", "NACE_DESC", "PERF_SCORE", "FIN_STAB", "RISC",
            "SupplyRiskScore", "ProfitImpactScore", "KVADRANT",
            "CREA_DATE", "CHANGE_DATE",
        ]
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(OUTPUT_FILES["vendor_master"]) or ".", exist_ok=True)
        
        # Save master vendors
        master_output = master_df[sap_output_cols].copy()
        master_output.to_csv(OUTPUT_FILES["vendor_master"], index=False)
        self.logger.info(f"Saved master vendors to {OUTPUT_FILES['vendor_master']}")
        
        # Save all vendors (master + variants)
        complete_output = complete_df[sap_output_cols].copy()
        complete_output.to_csv(OUTPUT_FILES["vendor_variants"], index=False)
        self.logger.info(f"Saved all vendors to {OUTPUT_FILES['vendor_variants']}")
        
        return master_output, complete_output

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main execution."""
    try:
        logger.info("\n" + "=" * 70)
        logger.info("VENDOR SYNTHESIS ENGINE - Loading from vendors_list_simplified.csv")
        logger.info("=" * 70)
        
        # Step 0: Load vendors from CSV
        logger.info("\n[STEP 0] Loading vendors from CSV...")
        vendors = load_vendors_from_csv("vendors_list_simplified.csv")
        
        if not vendors:
            logger.error("\n❌ CRITICAL: No vendors loaded from CSV!")
            logger.error("   Cannot continue without vendors.")
            return 1
        
        logger.info(f"\n✅ Successfully loaded {len(vendors)} vendors")
        
        # Step 1: Create synthesizer with loaded vendors
        logger.info(f"\n[STEP 1] Creating VendorSynthesizer with {len(vendors)} vendors...")
        synthesizer = VendorSynthesizer(vendors)
        
        # Step 2: Synthesize vendors
        logger.info("\n[STEP 2] Synthesizing vendors (masters + variants)...")
        master_df, complete_df = synthesizer.synthesize_all_vendors()
        
        # Step 3: Save to CSV
        logger.info("\n[STEP 3] Saving to CSV files...")
        master_csv, complete_csv = synthesizer.save_to_csv(master_df, complete_df)
        
        # Print summary
        logger.info("\n" + "=" * 70)
        logger.info("VENDOR SYNTHESIS COMPLETE ✅")
        logger.info("=" * 70)
        logger.info(f"Master vendors: {len(master_df)}")
        logger.info(f"Total vendors (with variants): {len(complete_df)}")
        logger.info(f"Synthesis ratio: {len(complete_df) / len(master_df):.1f}x expansion")
        logger.info(f"\nOutput files:")
        logger.info(f"  ✓ {OUTPUT_FILES['vendor_master']}")
        logger.info(f"  ✓ {OUTPUT_FILES['vendor_variants']}")
        
        # Show sample data
        logger.info("\nSample vendor data (first 10):")
        sample = complete_df[["LIFNR", "NAME1", "Land1", "PERF_SCORE", "RISC", "KVADRANT"]].head(10)
        logger.info("\n" + sample.to_string())
        
        logger.info("\n✅ Synthesis complete! Ready for Phase 1 processing.")
        
        return 0
    
    except Exception as e:
        logger.error(f"\n❌ Synthesis failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    sys.exit(main())
