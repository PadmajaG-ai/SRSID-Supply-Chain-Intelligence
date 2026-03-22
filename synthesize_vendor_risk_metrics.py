"""
Synthesize Vendor Risk Metrics
Apply Kaggle patterns to create realistic risk scores
"""

import pandas as pd
import numpy as np
import logging
import sys
import os
import json
import random
from datetime import datetime
from typing import Dict

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
# RISK METRICS SYNTHESIZER
# ============================================================

class RiskMetricsSynthesizer:
    """Synthesize realistic risk metrics based on Kaggle patterns."""
    
    def __init__(self):
        self.vendors_df = None
        self.patterns = {}
        self.disruption_mapping = None
    
    def load_data(self) -> bool:
        """Load vendor and pattern data."""
        try:
            logger.info("Loading data...")
            
            self.vendors_df = pd.read_csv(OUTPUT_FILES["vendors_with_industries"])
            logger.info(f"  Vendors: {len(self.vendors_df)}")
            
            with open(OUTPUT_FILES["kaggle_patterns"], "r") as f:
                self.patterns = json.load(f)
            logger.info(f"  Patterns loaded")
            
            self.disruption_mapping = pd.read_csv(OUTPUT_FILES["vendor_disruption_mapping"])
            logger.info(f"  Disruption mapping: {len(self.disruption_mapping)}")
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            return False
    
    def get_industry_pattern(self, industry: str) -> Dict:
        """Get pattern for industry."""
        
        patterns = self.patterns.get("industry_patterns", {})
        
        if industry in patterns:
            return patterns[industry]
        
        # Return default pattern if not found
        return {
            "avg_financial_stability": 0.75,
            "avg_delivery_performance": 0.75,
            "avg_supply_risk": 0.50,
            "avg_profit_impact": 0.75,
            "avg_lead_time_days": 30,
            "avg_order_volume": 10000,
        }
    
    def synthesize_metrics(self) -> pd.DataFrame:
        """Synthesize risk metrics for all vendors."""
        
        logger.info("\nSynthesizing risk metrics...")
        
        records = []
        variance = 0.05  # 5% variance
        
        for idx, vendor in self.vendors_df.iterrows():
            industry = vendor.get("Industry", "Other")
            pattern = self.get_industry_pattern(industry)
            
            # Apply variance to patterns
            fin_stab = pattern.get("avg_financial_stability", 0.75)
            fin_stab_var = fin_stab + random.uniform(-variance, variance)
            fin_stab_var = max(0.1, min(0.99, fin_stab_var))  # Clamp to [0.1, 0.99]
            
            deliv_perf = pattern.get("avg_delivery_performance", 0.75)
            deliv_perf_var = deliv_perf + random.uniform(-variance, variance)
            deliv_perf_var = max(0.1, min(0.99, deliv_perf_var))
            
            supply_risk = pattern.get("avg_supply_risk", 0.50)
            supply_risk_var = supply_risk + random.uniform(-variance, variance)
            supply_risk_var = max(0.1, min(0.99, supply_risk_var))
            
            profit_impact = pattern.get("avg_profit_impact", 0.75)
            profit_impact_var = profit_impact + random.uniform(-variance, variance)
            profit_impact_var = max(0.1, min(0.99, profit_impact_var))
            
            lead_time = pattern.get("avg_lead_time_days", 30)
            lead_time_var = int(lead_time + random.uniform(-5, 5))
            
            order_volume = pattern.get("avg_order_volume", 10000)
            order_volume_var = int(order_volume + random.uniform(-order_volume*0.1, order_volume*0.1))
            
            # Determine risk category from performance
            if deliv_perf_var >= 0.85:
                risk_category = "Low"
            elif deliv_perf_var >= 0.70:
                risk_category = "Medium"
            else:
                risk_category = "High"
            
            # Determine Kraljic quadrant
            if supply_risk_var > 0.5 and profit_impact_var > 0.5:
                kvadrant = "Strategic"
            elif supply_risk_var <= 0.5 and profit_impact_var > 0.5:
                kvadrant = "Leverage"
            elif supply_risk_var > 0.5 and profit_impact_var <= 0.5:
                kvadrant = "Bottleneck"
            else:
                kvadrant = "Tactical"
            
            record = {
                "LIFNR": vendor.get("LIFNR"),
                "vendor_name": vendor.get("NAME1"),
                "industry": industry,
                "financial_stability_score": round(fin_stab_var, 2),
                "delivery_performance": round(deliv_perf_var, 2),
                "historical_risk_category": risk_category,
                "supply_risk_score": round(supply_risk_var, 2),
                "profit_impact_score": round(profit_impact_var, 2),
                "lead_time_days": lead_time_var,
                "order_volume": order_volume_var,
                "cost_per_unit": round(random.uniform(10, 1000), 2),
                "kraljic_quadrant": kvadrant,
                "source": "Kaggle Patterns",
            }
            
            records.append(record)
        
        df = pd.DataFrame(records)
        logger.info(f"Synthesized metrics for {len(df)} vendors")
        
        return df
    
    def save_metrics(self, df: pd.DataFrame) -> bool:
        """Save synthesized metrics to CSV."""
        
        logger.info("\nSaving risk metrics...")
        
        try:
            os.makedirs(os.path.dirname(OUTPUT_FILES["vendor_risk_metrics"]), exist_ok=True)
            df.to_csv(OUTPUT_FILES["vendor_risk_metrics"], index=False)
            logger.info(f"Saved to {OUTPUT_FILES['vendor_risk_metrics']}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to save: {e}")
            return False
    
    def generate_summary(self, df: pd.DataFrame):
        """Generate summary statistics."""
        
        logger.info("\n" + "=" * 70)
        logger.info("SYNTHESIZED METRICS SUMMARY")
        logger.info("=" * 70)
        
        logger.info(f"\nTotal vendors: {len(df)}")
        
        logger.info(f"\nRisk Distribution:")
        for risk, count in df["historical_risk_category"].value_counts().items():
            logger.info(f"  {risk}: {count}")
        
        logger.info(f"\nKraljic Distribution:")
        for quad, count in df["kraljic_quadrant"].value_counts().items():
            logger.info(f"  {quad}: {count}")
        
        logger.info(f"\nMetric Ranges:")
        logger.info(f"  Financial Stability: {df['financial_stability_score'].min():.2f} - {df['financial_stability_score'].max():.2f}")
        logger.info(f"  Delivery Performance: {df['delivery_performance'].min():.2f} - {df['delivery_performance'].max():.2f}")
        logger.info(f"  Supply Risk: {df['supply_risk_score'].min():.2f} - {df['supply_risk_score'].max():.2f}")
        logger.info(f"  Profit Impact: {df['profit_impact_score'].min():.2f} - {df['profit_impact_score'].max():.2f}")

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main execution."""
    try:
        logger.info("=" * 70)
        logger.info("VENDOR RISK METRICS SYNTHESIZER")
        logger.info("=" * 70)
        
        # Create synthesizer
        synthesizer = RiskMetricsSynthesizer()
        
        # Load data
        logger.info("\n[STEP 1] Loading data...")
        if not synthesizer.load_data():
            return 1
        
        # Synthesize metrics
        logger.info("\n[STEP 2] Synthesizing metrics...")
        metrics_df = synthesizer.synthesize_metrics()
        
        # Save
        logger.info("\n[STEP 3] Saving metrics...")
        if not synthesizer.save_metrics(metrics_df):
            return 1
        
        # Summary
        synthesizer.generate_summary(metrics_df)
        
        logger.info("\n" + "=" * 70)
        logger.info("✓ METRICS SYNTHESIS COMPLETE")
        logger.info("=" * 70)
        
        return 0
    
    except Exception as e:
        logger.error(f"Metrics synthesis failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
