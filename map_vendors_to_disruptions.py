"""
Map Vendors to Disruptions
Fuzzy match vendors against disruption events
"""

import pandas as pd
import logging
import sys
import os
from datetime import datetime
from difflib import SequenceMatcher
from typing import List, Dict, Tuple

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
# FUZZY MATCHER
# ============================================================

class FuzzyMatcher:
    """Fuzzy match vendors to disruptions."""
    
    @staticmethod
    def similarity_score(s1: str, s2: str) -> float:
        """Calculate similarity between two strings (0-1)."""
        s1 = str(s1).lower().strip()
        s2 = str(s2).lower().strip()
        
        return SequenceMatcher(None, s1, s2).ratio()
    
    @staticmethod
    def match_vendors(vendor_name: str, actors_string: str, threshold: float = 0.7) -> Tuple[bool, float]:
        """
        Check if vendor appears in actors string (fuzzy match).
        
        Returns:
            (matched: bool, score: float)
        """
        if not actors_string or pd.isna(actors_string):
            return False, 0.0
        
        actors = str(actors_string).split(";")
        
        best_score = 0.0
        
        for actor in actors:
            actor = actor.strip()
            score = FuzzyMatcher.similarity_score(vendor_name, actor)
            
            if score > best_score:
                best_score = score
        
        matched = best_score >= threshold
        
        return matched, best_score

# ============================================================
# VENDOR DISRUPTION MAPPER
# ============================================================

class VendorDisruptionMapper:
    """Map vendors to disruption events."""
    
    def __init__(self):
        self.vendors_df = None
        self.disruptions_df = None
        self.mappings = []
    
    def load_data(self) -> bool:
        """Load vendor and disruption data."""
        try:
            logger.info("Loading vendor data...")
            self.vendors_df = pd.read_csv(OUTPUT_FILES["vendors_with_industries"])
            logger.info(f"  Loaded {len(self.vendors_df)} vendors")
            
            logger.info("Loading disruption data...")
            self.disruptions_df = pd.read_csv(OUTPUT_FILES["combined_disruptions"])
            logger.info(f"  Loaded {len(self.disruptions_df)} disruptions")
            
            return True
        
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")
            return False
    
    def map_vendors(self, threshold: float = 0.7) -> pd.DataFrame:
        """Map vendors to disruptions using fuzzy matching."""
        
        logger.info(f"\nFuzzy matching vendors to disruptions (threshold: {threshold})...")
        
        mappings = []
        
        # Get master vendors only (not variants for mapping)
        master_vendors = self.vendors_df[self.vendors_df["IsVariant"] == False].copy()
        
        logger.info(f"Matching {len(master_vendors)} master vendors...")
        
        for idx, vendor in master_vendors.iterrows():
            vendor_name = vendor["NAME1"]
            parent_lifnr = vendor["LIFNR"]
            industry = vendor["Industry"]
            
            # Find disruptions mentioning this vendor
            matched_disruptions = []
            
            for dis_idx, disruption in self.disruptions_df.iterrows():
                actors = disruption.get("actors", "")
                matched, score = FuzzyMatcher.match_vendors(vendor_name, actors, threshold)
                
                if matched:
                    matched_disruptions.append({
                        "event_id": disruption.get("event_id"),
                        "event_date": disruption.get("event_date"),
                        "disruption_type": disruption.get("disruption_type"),
                        "title": disruption.get("title"),
                        "match_score": score,
                        "source": disruption.get("_source"),
                    })
            
            # Create mapping record
            mapping = {
                "LIFNR": parent_lifnr,
                "vendor_name": vendor_name,
                "industry": industry,
                "disruptions_found": len(matched_disruptions),
                "disruption_types": "; ".join(set([d["disruption_type"] for d in matched_disruptions])),
                "latest_disruption_date": max([d["event_date"] for d in matched_disruptions]) if matched_disruptions else None,
                "sources_found": "; ".join(set([d["source"] for d in matched_disruptions])) if matched_disruptions else "None",
                "manual_review_required": len(matched_disruptions) == 0,
                "match_confidence": "High" if len(matched_disruptions) > 0 else "Low",
            }
            
            mappings.append(mapping)
            
            # Log if no disruptions found
            if len(matched_disruptions) == 0:
                logger.warning(f"  No disruptions found for: {vendor_name}")
        
        df = pd.DataFrame(mappings)
        
        return df
    
    def save_mapping(self, df: pd.DataFrame) -> bool:
        """Save vendor disruption mapping to CSV."""
        
        logger.info("\nSaving vendor disruption mapping...")
        
        try:
            os.makedirs(os.path.dirname(OUTPUT_FILES["vendor_disruption_mapping"]), exist_ok=True)
            df.to_csv(OUTPUT_FILES["vendor_disruption_mapping"], index=False)
            logger.info(f"Saved to {OUTPUT_FILES['vendor_disruption_mapping']}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to save: {e}")
            return False
    
    def generate_summary(self, df: pd.DataFrame):
        """Generate summary statistics."""
        
        logger.info("\n" + "=" * 70)
        logger.info("VENDOR DISRUPTION MAPPING SUMMARY")
        logger.info("=" * 70)
        
        logger.info(f"\nTotal Vendors: {len(df)}")
        
        with_disruptions = len(df[df["disruptions_found"] > 0])
        without_disruptions = len(df[df["manual_review_required"] == True])
        
        logger.info(f"  With disruptions: {with_disruptions}")
        logger.info(f"  Without disruptions (manual review): {without_disruptions}")
        
        logger.info(f"\nDisruption Coverage: {with_disruptions / len(df) * 100:.1f}%")
        
        logger.info(f"\nDisruption Types Found:")
        for dtype in df[df["disruption_types"].notna()]["disruption_types"].unique():
            count = len(df[df["disruption_types"].str.contains(dtype, na=False)])
            logger.info(f"  {dtype}: {count} vendors")
        
        logger.info(f"\nVendors Needing Manual Review:")
        manual_review = df[df["manual_review_required"] == True]
        for idx, vendor in manual_review.iterrows():
            logger.info(f"  - {vendor['vendor_name']} ({vendor['industry']})")

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main execution."""
    try:
        logger.info("=" * 70)
        logger.info("VENDOR DISRUPTION MAPPER")
        logger.info("=" * 70)
        
        # Create mapper
        mapper = VendorDisruptionMapper()
        
        # Load data
        logger.info("\n[STEP 1] Loading data...")
        if not mapper.load_data():
            return 1
        
        # Map vendors to disruptions
        logger.info("\n[STEP 2] Mapping vendors to disruptions...")
        mapping_df = mapper.map_vendors(threshold=0.7)
        
        # Save
        logger.info("\n[STEP 3] Saving mapping...")
        if not mapper.save_mapping(mapping_df):
            return 1
        
        # Summary
        mapper.generate_summary(mapping_df)
        
        logger.info("\n" + "=" * 70)
        logger.info("✓ VENDOR MAPPING COMPLETE")
        logger.info("=" * 70)
        
        return 0
    
    except Exception as e:
        logger.error(f"Vendor mapping failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
