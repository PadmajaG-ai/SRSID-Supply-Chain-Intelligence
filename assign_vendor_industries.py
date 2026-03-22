"""
Assign Vendor Industries and Create Variants
Auto-classify industries and generate 3-5 name variants per vendor
"""

import pandas as pd
import logging
import sys
import os
import random
from datetime import datetime
from typing import List, Dict

from hybrid_phase1_config import (
    SAP_VENDORS,
    INDUSTRY_ASSIGNMENT,
    VARIANT_TEMPLATES,
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
# VENDOR INDUSTRY ASSIGNER
# ============================================================

class VendorIndustryAssigner:
    """Assign industries to vendors and create variants."""
    
    def __init__(self):
        self.vendors = []
        self.vendor_counter = 1000
    
    def assign_industries(self) -> pd.DataFrame:
        """Assign industries to vendors based on config."""
        
        logger.info("Assigning industries to vendors...")
        
        records = []
        
        for vendor in SAP_VENDORS:
            vendor_name = vendor["name"]
            
            # Look up industry
            industry = INDUSTRY_ASSIGNMENT.get(vendor_name, "Other")
            
            record = {
                "LIFNR": f"{self.vendor_counter:06d}",
                "Name": vendor_name,
                "ORT01": vendor["city"],
                "Land1": vendor["country"],
                "Industry": industry,
                "IsVariant": False,
                "ParentLIFNR": f"{self.vendor_counter:06d}",
                "ParentName": vendor_name,
                "CreationDate": datetime.now().date(),
            }
            
            records.append(record)
            self.vendor_counter += 1
        
        df = pd.DataFrame(records)
        logger.info(f"Assigned industries to {len(df)} master vendors")
        
        return df
    
    def generate_variants(self, master_df: pd.DataFrame) -> pd.DataFrame:
        """Generate 3-5 variants per vendor."""
        
        logger.info("\nGenerating vendor variants...")
        
        variant_records = []
        
        for idx, vendor in master_df.iterrows():
            vendor_name = vendor["Name"]
            num_variants = random.randint(3, 5)
            
            for _ in range(num_variants):
                # Select random template
                template = random.choice(VARIANT_TEMPLATES)
                
                # Generate short name
                parts = vendor_name.split()
                if len(parts) > 1:
                    short_name = parts[0]
                else:
                    short_name = vendor_name[:3].upper()
                
                # Create variant name
                variant_name = template.format(
                    name=vendor_name,
                    short_name=short_name
                )
                
                # Skip if same as parent
                if variant_name == vendor_name:
                    continue
                
                lifnr = f"{self.vendor_counter:06d}"
                self.vendor_counter += 1
                
                record = {
                    "LIFNR": lifnr,
                    "Name": variant_name,
                    "ORT01": vendor["ORT01"],
                    "Land1": vendor["Land1"],
                    "Industry": vendor["Industry"],
                    "IsVariant": True,
                    "ParentLIFNR": vendor["LIFNR"],
                    "ParentName": vendor_name,
                    "CreationDate": datetime.now().date(),
                }
                
                variant_records.append(record)
        
        df = pd.DataFrame(variant_records)
        logger.info(f"Generated {len(df)} variant vendors")
        
        return df
    
    def combine_vendors(self, master_df: pd.DataFrame, variants_df: pd.DataFrame) -> pd.DataFrame:
        """Combine master and variant vendors."""
        
        logger.info("\nCombining master and variant vendors...")
        
        complete_df = pd.concat([master_df, variants_df], ignore_index=True)
        
        logger.info(f"Total vendors (master + variants): {len(complete_df)}")
        
        return complete_df
    
    def add_sap_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add SAP-specific columns."""
        
        logger.info("\nAdding SAP columns...")
        
        # Truncate name to SAP limits
        df["NAME1"] = df["Name"].str[:35]
        df["NAME2"] = df["Name"].str[35:]
        
        # Add SAP fields
        df["KTOKK"] = "Z001"  # Vendor account group
        df["ZTERM"] = "NET30"  # Payment terms
        df["INCO1"] = "FOB"    # Incoterms
        df["REGIO"] = df["Land1"]  # Region
        df["PSTLZ"] = "00000"  # Postal code
        df["TELF1"] = ""       # Telephone
        df["SMTP_ADDR"] = ""   # Email
        
        # Industry to NACE code mapping
        nace_mapping = {
            "Electronics & Semiconductors": "2620",
            "Electronics & Vehicles": "2910",
            "Networking & IT": "2620",
            "IT Distribution": "4759",
            "IT Services": "6209",
            "Chemicals": "2000",
            "Energy & Oil": "0610",
            "Pharmaceuticals": "2100",
            "Life Sciences": "2120",
            "Laboratory Equipment": "2841",
            "Food & Beverage": "1000",
            "Consumer Goods": "1520",
            "Retail": "4711",
            "Office Supplies": "4762",
            "Electronics Distribution": "4651",
            "Electrical Distribution": "4755",
            "Logistics": "4939",
            "Shipping & Maritime": "5012",
            "Manufacturing": "2500",
            "Building Materials": "2370",
            "Engineering & Construction": "4399",
            "Robotics & Automation": "2851",
            "Industrial Manufacturing": "2900",
            "Financial Services": "6419",
            "Entertainment": "9001",
            "Distribution": "4610",
            "Retail/Distribution": "4730",
            "Other": "9999",
        }
        
        df["NACE"] = df["Industry"].map(nace_mapping).fillna("9999")
        df["NACE_DESC"] = df["Industry"]
        
        # Timestamps
        df["CREA_DATE"] = df["CreationDate"]
        df["CHANGE_DATE"] = datetime.now().date()
        
        return df
    
    def save_to_csv(self, df: pd.DataFrame) -> bool:
        """Save vendor data to CSV."""
        
        logger.info("\nSaving vendor data...")
        
        try:
            os.makedirs(os.path.dirname(OUTPUT_FILES["vendors_with_industries"]), exist_ok=True)
            
            # Select output columns
            output_cols = [
                "LIFNR", "NAME1", "NAME2", "ORT01", "Land1", "REGIO",
                "PSTLZ", "TELF1", "SMTP_ADDR", "KTOKK", "ZTERM", "INCO1",
                "NACE", "NACE_DESC", "Industry", "IsVariant", "ParentLIFNR",
                "ParentName", "CREA_DATE", "CHANGE_DATE",
            ]
            
            output_df = df[output_cols].copy()
            output_df.to_csv(OUTPUT_FILES["vendors_with_industries"], index=False)
            
            logger.info(f"Saved to {OUTPUT_FILES['vendors_with_industries']}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to save: {e}")
            return False
    
    def generate_summary(self, df: pd.DataFrame):
        """Generate summary statistics."""
        
        logger.info("\n" + "=" * 70)
        logger.info("VENDOR INDUSTRY ASSIGNMENT SUMMARY")
        logger.info("=" * 70)
        
        logger.info(f"\nTotal Vendors: {len(df)}")
        
        master_count = len(df[df["IsVariant"] == False])
        variant_count = len(df[df["IsVariant"] == True])
        
        logger.info(f"  Master vendors: {master_count}")
        logger.info(f"  Variant vendors: {variant_count}")
        
        logger.info(f"\nIndustries:")
        for industry, count in df["Industry"].value_counts().items():
            master = len(df[(df["Industry"] == industry) & (df["IsVariant"] == False)])
            variants = count - master
            logger.info(f"  {industry}: {count} (master: {master}, variants: {variants})")
        
        logger.info(f"\nCountries:")
        for country, count in df["Land1"].value_counts().items():
            logger.info(f"  {country}: {count}")

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main execution."""
    try:
        logger.info("=" * 70)
        logger.info("VENDOR INDUSTRY ASSIGNER")
        logger.info("=" * 70)
        
        # Create assigner
        assigner = VendorIndustryAssigner()
        
        # Assign industries
        logger.info("\n[STEP 1] Assigning industries...")
        master_df = assigner.assign_industries()
        
        # Generate variants
        logger.info("\n[STEP 2] Generating variants...")
        variants_df = assigner.generate_variants(master_df)
        
        # Combine
        logger.info("\n[STEP 3] Combining vendors...")
        complete_df = assigner.combine_vendors(master_df, variants_df)
        
        # Add SAP columns
        logger.info("\n[STEP 4] Adding SAP columns...")
        complete_df = assigner.add_sap_columns(complete_df)
        
        # Save
        logger.info("\n[STEP 5] Saving vendor data...")
        if not assigner.save_to_csv(complete_df):
            return 1
        
        # Summary
        assigner.generate_summary(complete_df)
        
        logger.info("\n" + "=" * 70)
        logger.info("✓ VENDOR ASSIGNMENT COMPLETE")
        logger.info("=" * 70)
        
        return 0
    
    except Exception as e:
        logger.error(f"Vendor assignment failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
