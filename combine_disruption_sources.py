"""
Combine Disruption Sources
Merge GDELT + NewsAPI disruptions and deduplicate
"""

import pandas as pd
import logging
import sys
import os
from datetime import datetime
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
# DISRUPTION COMBINER
# ============================================================

class DisruptionCombiner:
    """Combine and deduplicate disruptions from multiple sources."""
    
    def __init__(self):
        self.gdelt_df = None
        self.newsapi_df = None
        self.combined_df = None
    
    def load_source_files(self) -> bool:
        """Load GDELT and NewsAPI disruption files."""
        try:
            logger.info("Loading GDELT disruptions...")
            self.gdelt_df = pd.read_csv(OUTPUT_FILES["gdelt_disruptions"])
            logger.info(f"  Loaded {len(self.gdelt_df)} GDELT records")
            
            logger.info("Loading NewsAPI disruptions...")
            self.newsapi_df = pd.read_csv(OUTPUT_FILES["newsapi_disruptions"])
            logger.info(f"  Loaded {len(self.newsapi_df)} NewsAPI records")
            
            return True
        
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")
            logger.error("Run fetch_gdelt_disruptions.py and fetch_newsapi_disruptions.py first")
            return False
    
    def standardize_columns(self):
        """Ensure both dataframes have same column structure."""
        
        # Define standard columns
        standard_cols = [
            "event_id",
            "event_date",
            "disruption_type",
            "title",
            "description",
            "actors",
            "location",
            "source",
            "url",
            "source_file",
            "fetch_timestamp",
        ]
        
        # Add missing columns to GDELT
        for col in standard_cols:
            if col not in self.gdelt_df.columns:
                self.gdelt_df[col] = ""
        
        # Add missing columns to NewsAPI
        for col in standard_cols:
            if col not in self.newsapi_df.columns:
                self.newsapi_df[col] = ""
        
        # Select only standard columns
        self.gdelt_df = self.gdelt_df[standard_cols]
        self.newsapi_df = self.newsapi_df[standard_cols]
    
    def combine_sources(self) -> pd.DataFrame:
        """Combine GDELT and NewsAPI data."""
        
        logger.info("\nCombining sources...")
        
        # Add source indicator
        self.gdelt_df["_source"] = "GDELT"
        self.newsapi_df["_source"] = "NewsAPI"
        
        # Combine
        combined = pd.concat([self.gdelt_df, self.newsapi_df], ignore_index=True)
        
        logger.info(f"Combined: {len(combined)} total records")
        
        return combined
    
    def deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate disruptions from same event."""
        
        logger.info("\nDeduplicating...")
        
        initial_count = len(df)
        
        # Normalize dates
        df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
        
        # Create a dedup key (event_date + disruption_type + title similarity)
        # For now, use date + type as primary dedup
        df = df.sort_values("fetch_timestamp", ascending=False)
        df = df.drop_duplicates(
            subset=["event_date", "disruption_type"],
            keep="first"
        )
        
        removed = initial_count - len(df)
        logger.info(f"Removed {removed} duplicates")
        
        return df
    
    def add_confidence_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add confidence level based on source."""
        
        def get_confidence(row):
            source = row.get("_source", "")
            if source == "GDELT":
                return "High"  # Event-based data
            elif source == "NewsAPI":
                return "Medium"  # Article-based
            else:
                return "Low"
        
        df["confidence"] = df.apply(get_confidence, axis=1)
        
        return df
    
    def standardize_disruption_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize disruption type values."""
        
        # Define standard types
        type_mapping = {
            "supply_chain": "supply_chain",
            "supply chain": "supply_chain",
            "supply disruption": "supply_chain",
            "bankruptcy": "bankruptcy",
            "bankrupt": "bankruptcy",
            "labor_strike": "labor_strike",
            "strike": "labor_strike",
            "labor dispute": "labor_strike",
            "geopolitical": "geopolitical",
            "sanction": "geopolitical",
            "tariff": "geopolitical",
            "natural_disaster": "natural_disaster",
            "natural disaster": "natural_disaster",
            "earthquake": "natural_disaster",
            "flood": "natural_disaster",
            "recall": "recall",
            "product recall": "recall",
            "shortage": "shortage",
            "logistics_delay": "logistics_delay",
            "logistics delay": "logistics_delay",
        }
        
        def map_type(dtype):
            dtype_lower = str(dtype).lower()
            for key, value in type_mapping.items():
                if key in dtype_lower:
                    return value
            return dtype
        
        df["disruption_type"] = df["disruption_type"].apply(map_type)
        
        return df
    
    def save_combined(self, df: pd.DataFrame) -> bool:
        """Save combined disruptions to CSV."""
        
        logger.info("\nSaving combined disruptions...")
        
        try:
            os.makedirs(os.path.dirname(OUTPUT_FILES["combined_disruptions"]), exist_ok=True)
            df.to_csv(OUTPUT_FILES["combined_disruptions"], index=False)
            logger.info(f"Saved to {OUTPUT_FILES['combined_disruptions']}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to save: {e}")
            return False
    
    def generate_summary(self, df: pd.DataFrame):
        """Generate summary statistics."""
        
        logger.info("\n" + "=" * 70)
        logger.info("COMBINED DISRUPTIONS SUMMARY")
        logger.info("=" * 70)
        
        logger.info(f"\nTotal Disruptions: {len(df)}")
        
        logger.info(f"\nBy Source:")
        for source, count in df["_source"].value_counts().items():
            logger.info(f"  {source}: {count}")
        
        logger.info(f"\nBy Disruption Type:")
        for dtype, count in df["disruption_type"].value_counts().items():
            logger.info(f"  {dtype}: {count}")
        
        logger.info(f"\nBy Confidence:")
        for conf, count in df["confidence"].value_counts().items():
            logger.info(f"  {conf}: {count}")
        
        logger.info(f"\nDate Range:")
        logger.info(f"  Earliest: {df['event_date'].min()}")
        logger.info(f"  Latest: {df['event_date'].max()}")
        
        logger.info(f"\nVendors Mentioned (sample):")
        # Extract unique vendors from actors
        all_actors = df[df["actors"].notna()]["actors"].str.split("; ").explode()
        top_vendors = all_actors.value_counts().head(10)
        for vendor, count in top_vendors.items():
            if vendor.strip():
                logger.info(f"  {vendor.strip()}: {count}")

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main execution."""
    try:
        logger.info("=" * 70)
        logger.info("DISRUPTION SOURCE COMBINER")
        logger.info("=" * 70)
        
        # Create combiner
        combiner = DisruptionCombiner()
        
        # Load sources
        logger.info("\n[STEP 1] Loading source files...")
        if not combiner.load_source_files():
            return 1
        
        # Standardize columns
        logger.info("\n[STEP 2] Standardizing columns...")
        combiner.standardize_columns()
        
        # Combine
        logger.info("\n[STEP 3] Combining sources...")
        combined = combiner.combine_sources()
        
        # Standardize disruption types
        logger.info("\n[STEP 4] Standardizing disruption types...")
        combined = combiner.standardize_disruption_types(combined)
        
        # Deduplicate
        logger.info("\n[STEP 5] Deduplicating...")
        combined = combiner.deduplicate(combined)
        
        # Add confidence
        logger.info("\n[STEP 6] Adding confidence scores...")
        combined = combiner.add_confidence_scores(combined)
        
        # Save
        logger.info("\n[STEP 7] Saving combined dataset...")
        if not combiner.save_combined(combined):
            return 1
        
        # Summary
        combiner.generate_summary(combined)
        
        logger.info("\n" + "=" * 70)
        logger.info("✓ DISRUPTION COMBINATION COMPLETE")
        logger.info("=" * 70)
        
        return 0
    
    except Exception as e:
        logger.error(f"Disruption combination failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
