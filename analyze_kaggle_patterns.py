"""
Analyze Kaggle Patterns
Extract risk metric distributions from Kaggle datasets by industry
"""

import pandas as pd
import numpy as np
import logging
import sys
import os
import json
from typing import Dict

from hybrid_phase1_config import (
    KAGGLE_FILES,
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
# PATTERN ANALYZER
# ============================================================

class KagglePatternAnalyzer:
    """Extract patterns from Kaggle datasets."""
    
    def __init__(self):
        self.risk_assessment_df = None
        self.supply_chain_df = None
        self.kraljic_df = None
        self.patterns = {}
    
    def load_kaggle_files(self) -> bool:
        """Load all Kaggle files."""
        try:
            logger.info("Loading Kaggle files...")
            
            # Load risk assessment
            if os.path.exists(KAGGLE_FILES["risk_assessment"]):
                self.risk_assessment_df = pd.read_csv(KAGGLE_FILES["risk_assessment"])
                logger.info(f"  Risk Assessment: {len(self.risk_assessment_df)} records")
            else:
                logger.warning(f"  Risk Assessment file not found: {KAGGLE_FILES['risk_assessment']}")
            
            # Load supply chain
            if os.path.exists(KAGGLE_FILES["supply_chain_risk"]):
                self.supply_chain_df = pd.read_csv(KAGGLE_FILES["supply_chain_risk"])
                logger.info(f"  Supply Chain Risk: {len(self.supply_chain_df)} records")
            else:
                logger.warning(f"  Supply Chain file not found: {KAGGLE_FILES['supply_chain_risk']}")
            
            # Load Kraljic
            if os.path.exists(KAGGLE_FILES["kraljic"]):
                self.kraljic_df = pd.read_csv(KAGGLE_FILES["kraljic"])
                logger.info(f"  Kraljic: {len(self.kraljic_df)} records")
            else:
                logger.warning(f"  Kraljic file not found: {KAGGLE_FILES['kraljic']}")
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to load Kaggle files: {e}")
            return False
    
    def analyze_risk_assessment(self) -> Dict:
        """Analyze risk assessment patterns."""
        
        if self.risk_assessment_df is None:
            logger.warning("Risk assessment data not available")
            return {}
        
        logger.info("\nAnalyzing risk assessment patterns...")
        
        df = self.risk_assessment_df.copy()
        patterns = {}
        
        # Get unique industries/categories
        if "Supplier Name" in df.columns:
            # Group by potential industry column if exists
            industry_col = None
            for col in df.columns:
                if "industri" in col.lower() or "categor" in col.lower() or "sector" in col.lower():
                    industry_col = col
                    break
            
            if industry_col:
                for industry in df[industry_col].unique():
                    industry_data = df[df[industry_col] == industry]
                    
                    patterns[industry] = {
                        "count": len(industry_data),
                        "avg_financial_stability": float(
                            industry_data["Financial Stability"].mean() if "Financial Stability" in industry_data.columns else 0.5
                        ),
                        "avg_delivery_performance": float(
                            industry_data["Delivery Performance"].mean() if "Delivery Performance" in industry_data.columns else 0.5
                        ),
                    }
            else:
                # No industry column, use global stats
                patterns["Global"] = {
                    "count": len(df),
                    "avg_financial_stability": float(
                        df["Financial Stability"].mean() if "Financial Stability" in df.columns else 0.75
                    ),
                    "avg_delivery_performance": float(
                        df["Delivery Performance"].mean() if "Delivery Performance" in df.columns else 0.75
                    ),
                }
        
        return patterns
    
    def analyze_supply_chain(self) -> Dict:
        """Analyze supply chain patterns."""
        
        if self.supply_chain_df is None:
            logger.warning("Supply chain data not available")
            return {}
        
        logger.info("\nAnalyzing supply chain patterns...")
        
        df = self.supply_chain_df.copy()
        patterns = {}
        
        # Analyze disruption types if available
        if "Disruption Type" in df.columns:
            disruption_dist = df["Disruption Type"].value_counts(normalize=True).to_dict()
            patterns["disruption_distribution"] = {
                str(k): float(v) for k, v in disruption_dist.items()
            }
        
        # Analyze amounts if available
        if "Amount" in df.columns:
            patterns["spend_statistics"] = {
                "avg_amount": float(df["Amount"].mean()),
                "min_amount": float(df["Amount"].min()),
                "max_amount": float(df["Amount"].max()),
                "median_amount": float(df["Amount"].median()),
            }
        
        return patterns
    
    def analyze_kraljic(self) -> Dict:
        """Analyze Kraljic positioning patterns."""
        
        if self.kraljic_df is None:
            logger.warning("Kraljic data not available")
            return {}
        
        logger.info("\nAnalyzing Kraljic patterns...")
        
        df = self.kraljic_df.copy()
        patterns = {}
        
        # Normalize scores if needed
        for col in ["Supply_Risk_Score", "Profit_Impact_Score", "supply_risk_score", "profit_impact_score"]:
            if col in df.columns:
                if df[col].max() > 1:
                    df[col] = df[col] / 100
        
        # Get score distributions by category if available
        category_col = None
        for col in df.columns:
            if "category" in col.lower() or "quadrant" in col.lower():
                category_col = col
                break
        
        if category_col:
            for category in df[category_col].unique():
                cat_data = df[df[category_col] == category]
                
                # Find supply and profit score columns
                supply_col = None
                profit_col = None
                for col in df.columns:
                    if "supply" in col.lower() and "risk" in col.lower():
                        supply_col = col
                    if "profit" in col.lower() and "impact" in col.lower():
                        profit_col = col
                
                pattern = {
                    "count": len(cat_data),
                }
                
                if supply_col:
                    pattern["avg_supply_risk"] = float(cat_data[supply_col].mean())
                
                if profit_col:
                    pattern["avg_profit_impact"] = float(cat_data[profit_col].mean())
                
                patterns[str(category)] = pattern
        else:
            # Global Kraljic stats
            supply_col = None
            profit_col = None
            for col in df.columns:
                if "supply" in col.lower() and "risk" in col.lower():
                    supply_col = col
                if "profit" in col.lower() and "impact" in col.lower():
                    profit_col = col
            
            pattern = {"count": len(df)}
            if supply_col:
                pattern["avg_supply_risk"] = float(df[supply_col].mean())
            if profit_col:
                pattern["avg_profit_impact"] = float(df[profit_col].mean())
            
            patterns["Global"] = pattern
        
        return patterns
    
    def create_default_patterns(self) -> Dict:
        """Create default patterns for all industries."""
        
        logger.info("\nCreating default patterns...")
        
        default_patterns = {
            "Electronics & Semiconductors": {
                "avg_financial_stability": 0.85,
                "avg_delivery_performance": 0.82,
                "avg_supply_risk": 0.40,
                "avg_profit_impact": 0.80,
                "avg_lead_time_days": 30,
                "avg_order_volume": 10000,
            },
            "Chemicals": {
                "avg_financial_stability": 0.72,
                "avg_delivery_performance": 0.68,
                "avg_supply_risk": 0.55,
                "avg_profit_impact": 0.65,
                "avg_lead_time_days": 45,
                "avg_order_volume": 5000,
            },
            "Pharmaceuticals": {
                "avg_financial_stability": 0.88,
                "avg_delivery_performance": 0.85,
                "avg_supply_risk": 0.30,
                "avg_profit_impact": 0.90,
                "avg_lead_time_days": 20,
                "avg_order_volume": 1000,
            },
            "Energy & Oil": {
                "avg_financial_stability": 0.80,
                "avg_delivery_performance": 0.75,
                "avg_supply_risk": 0.60,
                "avg_profit_impact": 0.85,
                "avg_lead_time_days": 60,
                "avg_order_volume": 20000,
            },
            "Food & Beverage": {
                "avg_financial_stability": 0.78,
                "avg_delivery_performance": 0.80,
                "avg_supply_risk": 0.35,
                "avg_profit_impact": 0.70,
                "avg_lead_time_days": 15,
                "avg_order_volume": 50000,
            },
            "Retail": {
                "avg_financial_stability": 0.75,
                "avg_delivery_performance": 0.78,
                "avg_supply_risk": 0.45,
                "avg_profit_impact": 0.75,
                "avg_lead_time_days": 10,
                "avg_order_volume": 100000,
            },
            "Logistics": {
                "avg_financial_stability": 0.82,
                "avg_delivery_performance": 0.85,
                "avg_supply_risk": 0.40,
                "avg_profit_impact": 0.80,
                "avg_lead_time_days": 5,
                "avg_order_volume": 50000,
            },
            "Industrial Manufacturing": {
                "avg_financial_stability": 0.80,
                "avg_delivery_performance": 0.80,
                "avg_supply_risk": 0.45,
                "avg_profit_impact": 0.75,
                "avg_lead_time_days": 40,
                "avg_order_volume": 5000,
            },
            "Consumer Goods": {
                "avg_financial_stability": 0.82,
                "avg_delivery_performance": 0.82,
                "avg_supply_risk": 0.35,
                "avg_profit_impact": 0.70,
                "avg_lead_time_days": 12,
                "avg_order_volume": 50000,
            },
            "Distribution": {
                "avg_financial_stability": 0.78,
                "avg_delivery_performance": 0.75,
                "avg_supply_risk": 0.50,
                "avg_profit_impact": 0.65,
                "avg_lead_time_days": 8,
                "avg_order_volume": 30000,
            },
        }
        
        return default_patterns
    
    def save_patterns(self, patterns: Dict) -> bool:
        """Save patterns to JSON file."""
        
        logger.info("\nSaving patterns...")
        
        try:
            os.makedirs(os.path.dirname(OUTPUT_FILES["kaggle_patterns"]), exist_ok=True)
            
            with open(OUTPUT_FILES["kaggle_patterns"], "w") as f:
                json.dump(patterns, f, indent=2)
            
            logger.info(f"Saved to {OUTPUT_FILES['kaggle_patterns']}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to save patterns: {e}")
            return False

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main execution."""
    try:
        logger.info("=" * 70)
        logger.info("KAGGLE PATTERN ANALYZER")
        logger.info("=" * 70)
        
        # Create analyzer
        analyzer = KagglePatternAnalyzer()
        
        # Load Kaggle files
        logger.info("\n[STEP 1] Loading Kaggle files...")
        if not analyzer.load_kaggle_files():
            logger.warning("Some Kaggle files missing. Using default patterns.")
        
        # Analyze patterns
        logger.info("\n[STEP 2] Analyzing patterns...")
        patterns = {
            "industry_patterns": analyzer.create_default_patterns(),
        }
        
        # If Kaggle data available, try to extract patterns
        if analyzer.risk_assessment_df is not None:
            risk_patterns = analyzer.analyze_risk_assessment()
            if risk_patterns:
                patterns["risk_assessment_patterns"] = risk_patterns
        
        if analyzer.supply_chain_df is not None:
            supply_patterns = analyzer.analyze_supply_chain()
            if supply_patterns:
                patterns["supply_chain_patterns"] = supply_patterns
        
        if analyzer.kraljic_df is not None:
            kraljic_patterns = analyzer.analyze_kraljic()
            if kraljic_patterns:
                patterns["kraljic_patterns"] = kraljic_patterns
        
        # Save
        logger.info("\n[STEP 3] Saving patterns...")
        if not analyzer.save_patterns(patterns):
            return 1
        
        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("KAGGLE PATTERN ANALYSIS COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Industries in patterns: {len(patterns.get('industry_patterns', {}))}")
        
        return 0
    
    except Exception as e:
        logger.error(f"Pattern analysis failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
