"""
Calculate Spend Analytics
Integrate Spend Under Management, Maverick Spend, and Concentration Risk into Phase 1
"""

import pandas as pd
import numpy as np
import logging
import sys
import os
import json
import random
from datetime import datetime
from typing import Dict, Tuple, List

from spend_analytics_config import (
    SUM_CONFIG,
    MAVERICK_CONFIG,
    CONCENTRATION_CONFIG,
    SPEND_CATEGORIES,
    COUNTRY_SPEND_PATTERNS,
    INDUSTRY_SPEND_MULTIPLIERS,
    get_spend_category,
    get_country_spend_range,
    get_industry_multiplier,
    validate_spend_config,
)

from hybrid_phase1_config_simplified import (
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
# SPEND ANALYTICS CALCULATOR
# ============================================================

class SpendAnalyticsCalculator:
    """Calculate spend analytics metrics for Phase 1."""
    
    def __init__(self):
        self.risk_assessment_df = None
        self.transactions_df = None
        self.vendors_df = None
        self.spend_analytics_data = []
        self.concentration_data = []
        self.maverick_summary = []
    
    def load_phase1_data(self) -> bool:
        """Load Phase 1 tables."""
        try:
            logger.info("Loading Phase 1 data...")
            
            self.risk_assessment_df = pd.read_csv(
                OUTPUT_FILES["phase1_risk_assessment"]
            )
            logger.info(f"  Risk Assessment: {len(self.risk_assessment_df)} records")
            
            self.transactions_df = pd.read_csv(
                OUTPUT_FILES["phase1_transactions"]
            )
            logger.info(f"  Transactions: {len(self.transactions_df)} records")
            
            self.vendors_df = pd.read_csv(
                OUTPUT_FILES["vendors_with_industries"]
            )
            logger.info(f"  Vendors: {len(self.vendors_df)} records")
            
            return True
        
        except FileNotFoundError as e:
            logger.error(f"Failed to load Phase 1 data: {e}")
            logger.error("Run Phase 1 scripts first to generate tables")
            return False
    
    def calculate_sum(self) -> pd.DataFrame:
        """Calculate Spend Under Management (SUM)."""
        logger.info("\nCalculating Spend Under Management (SUM)...")
        
        sum_data = []
        
        for supplier in self.risk_assessment_df["supplier_name"].unique():
            # Get transactions for this supplier
            supplier_txns = self.transactions_df[
                self.transactions_df["supplier"] == supplier
            ]
            
            if len(supplier_txns) == 0:
                continue
            
            total_spend = supplier_txns["amount"].sum()
            
            # Classify transactions as contract or non-contract
            contract_spend = 0
            for idx, txn in supplier_txns.iterrows():
                if self._is_under_contract(txn, supplier):
                    contract_spend += txn["amount"]
            
            sum_pct = (contract_spend / total_spend * 100) if total_spend > 0 else 0
            
            sum_data.append({
                "supplier_name": supplier,
                "total_spend": total_spend,
                "spend_under_contract": contract_spend,
                "sum_percentage": round(sum_pct, 2),
                "contract_gaps": total_spend - contract_spend,
            })
        
        logger.info(f"  Calculated SUM for {len(sum_data)} suppliers")
        
        return pd.DataFrame(sum_data)
    
    def identify_maverick_spend(self) -> pd.DataFrame:
        """Identify and mark maverick transactions."""
        logger.info("\nIdentifying Maverick Spend...")
        
        # Add maverick classification to transactions
        self.transactions_df["is_under_contract"] = False
        self.transactions_df["is_maverick"] = False
        self.transactions_df["maverick_type"] = ""
        self.transactions_df["estimated_savings"] = 0.0
        
        maverick_count = 0
        
        for idx, txn in self.transactions_df.iterrows():
            # Classify as contract
            if self._is_under_contract(txn, txn["supplier"]):
                self.transactions_df.at[idx, "is_under_contract"] = True
            
            # Classify as maverick
            is_maverick, maverick_type = self._classify_maverick(txn)
            
            if is_maverick:
                self.transactions_df.at[idx, "is_maverick"] = True
                self.transactions_df.at[idx, "maverick_type"] = maverick_type
                
                # Estimate savings
                savings = self._calculate_savings(txn["amount"], maverick_type)
                self.transactions_df.at[idx, "estimated_savings"] = savings
                
                maverick_count += 1
        
        logger.info(f"  Marked {maverick_count} maverick transactions ({maverick_count/len(self.transactions_df)*100:.1f}%)")
        
        return self.transactions_df
    
    def analyze_concentration(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Analyze spend concentration risk."""
        logger.info("\nAnalyzing Spend Concentration...")
        
        # Calculate per-supplier concentration
        concentration_data = []
        
        total_enterprise_spend = self.transactions_df["amount"].sum()
        
        # Group by supplier
        supplier_groups = self.transactions_df.groupby("supplier")
        
        supplier_spends = []
        for supplier, group in supplier_groups:
            supplier_spend = group["amount"].sum()
            supplier_spends.append((supplier, supplier_spend))
        
        # Sort by spend descending
        supplier_spends.sort(key=lambda x: x[1], reverse=True)
        
        # Rank and calculate concentrations
        for rank, (supplier, spend) in enumerate(supplier_spends, 1):
            pct_enterprise = (spend / total_enterprise_spend * 100) if total_enterprise_spend > 0 else 0
            
            # Get category
            supplier_info = self.risk_assessment_df[
                self.risk_assessment_df["supplier_name"] == supplier
            ]
            if len(supplier_info) > 0:
                industry = supplier_info.iloc[0].get("industry", "Other")
            else:
                industry = "Other"
            
            category = get_spend_category(industry)
            
            # Get category total
            category_txns = self.transactions_df[
                self.transactions_df["procurement_category"] == category
            ]
            category_spend = category_txns["amount"].sum()
            pct_category = (spend / category_spend * 100) if category_spend > 0 else 0
            
            # Determine risk level
            risk_level = self._classify_concentration_risk(pct_enterprise, pct_category)
            
            concentration_data.append({
                "supplier_name": supplier,
                "category": category,
                "spend": spend,
                "pct_enterprise": round(pct_enterprise, 2),
                "pct_category": round(pct_category, 2),
                "rank": rank,
                "concentration_risk_level": risk_level,
            })
        
        logger.info(f"  Analyzed concentration for {len(concentration_data)} suppliers")
        
        return pd.DataFrame(concentration_data), supplier_spends
    
    def generate_spend_analytics_summary(self, sum_df: pd.DataFrame, 
                                       concentration_df: pd.DataFrame) -> pd.DataFrame:
        """Generate comprehensive spend analytics summary."""
        logger.info("\nGenerating Spend Analytics Summary...")
        
        spend_analytics = []
        
        for idx, vendor in self.risk_assessment_df.iterrows():
            supplier_name = vendor["supplier_name"]
            
            # Get SUM data
            sum_data = sum_df[sum_df["supplier_name"] == supplier_name]
            if len(sum_data) == 0:
                continue
            
            sum_info = sum_data.iloc[0]
            
            # Get concentration data
            conc_data = concentration_df[concentration_df["supplier_name"] == supplier_name]
            
            # Calculate maverick
            supplier_txns = self.transactions_df[
                self.transactions_df["supplier"] == supplier_name
            ]
            maverick_spend = supplier_txns[supplier_txns["is_maverick"] == True]["amount"].sum()
            maverick_pct = (maverick_spend / sum_info["total_spend"] * 100) if sum_info["total_spend"] > 0 else 0
            
            # Get concentration info
            if len(conc_data) > 0:
                conc_info = conc_data.iloc[0]
                conc_pct_enterprise = conc_info["pct_enterprise"]
                conc_risk = conc_info["concentration_risk_level"]
            else:
                conc_pct_enterprise = 0
                conc_risk = "LOW"
            
            # Determine diversification priority
            diversification_needed = (
                sum_info["sum_percentage"] < 75 or  # Low SUM
                maverick_pct > 15 or  # High maverick
                conc_pct_enterprise > 15  # High concentration
            )
            
            # Savings opportunity
            total_savings = supplier_txns[supplier_txns["is_maverick"] == True]["estimated_savings"].sum()
            
            spend_analytics.append({
                "supplier_name": supplier_name,
                "industry": vendor.get("Industry", "Other"),
                "total_spend": sum_info["total_spend"],
                "spend_under_contract": sum_info["spend_under_contract"],
                "sum_percentage": sum_info["sum_percentage"],
                "maverick_spend": maverick_spend,
                "maverick_percentage": round(maverick_pct, 2),
                "spend_concentration_percentage": round(conc_pct_enterprise, 2),
                "concentration_risk_level": conc_risk,
                "diversification_needed": diversification_needed,
                "estimated_savings_opportunity": round(total_savings, 0),
            })
        
        logger.info(f"  Generated analytics for {len(spend_analytics)} suppliers")
        
        return pd.DataFrame(spend_analytics)
    
    def save_spend_analytics(self, spend_analytics_df: pd.DataFrame,
                            concentration_df: pd.DataFrame) -> bool:
        """Save spend analytics outputs."""
        logger.info("\nSaving Spend Analytics outputs...")
        
        try:
            os.makedirs(os.path.dirname(OUTPUT_FILES["phase1_risk_assessment"]), exist_ok=True)
            
            # Create spend analytics path
            spend_analytics_path = os.path.join(
                PATHS["phase1_tables"],
                "raw_spend_analytics.csv"
            )
            
            concentration_path = os.path.join(
                PATHS["phase1_tables"],
                "raw_concentration_analysis.csv"
            )
            
            # Save spend analytics
            spend_analytics_df.to_csv(spend_analytics_path, index=False)
            logger.info(f"  Saved spend analytics to {spend_analytics_path}")
            
            # Save concentration analysis
            concentration_df.to_csv(concentration_path, index=False)
            logger.info(f"  Saved concentration analysis to {concentration_path}")
            
            # Save enhanced transactions
            trans_path = OUTPUT_FILES["phase1_transactions"]
            self.transactions_df.to_csv(trans_path, index=False)
            logger.info(f"  Saved enhanced transactions to {trans_path}")
            
            # Save enhanced risk assessment
            risk_path = OUTPUT_FILES["phase1_risk_assessment"]
            
            # Merge spend analytics into risk assessment
            enhanced_risk = self.risk_assessment_df.merge(
                spend_analytics_df[["supplier_name", "total_spend", "spend_under_contract",
                                   "sum_percentage", "maverick_spend", "maverick_percentage",
                                   "spend_concentration_percentage", "concentration_risk_level",
                                   "diversification_needed"]],
                left_on="supplier_name",
                right_on="supplier_name",
                how="left"
            )
            
            enhanced_risk.to_csv(risk_path, index=False)
            logger.info(f"  Saved enhanced risk assessment to {risk_path}")
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to save outputs: {e}")
            return False
    
    def generate_spend_intelligence_report(self, spend_analytics_df: pd.DataFrame) -> bool:
        """Generate spend intelligence report."""
        logger.info("\nGenerating Spend Intelligence Report...")
        
        try:
            total_spend = spend_analytics_df["total_spend"].sum()
            total_contract_spend = spend_analytics_df["spend_under_contract"].sum()
            total_maverick = spend_analytics_df["maverick_spend"].sum()
            total_savings = spend_analytics_df["estimated_savings_opportunity"].sum()
            
            enterprise_sum = (total_contract_spend / total_spend * 100) if total_spend > 0 else 0
            enterprise_maverick = (total_maverick / total_spend * 100) if total_spend > 0 else 0
            
            # Count concentration risks
            high_concentration = len(spend_analytics_df[spend_analytics_df["concentration_risk_level"] == "HIGH"])
            medium_concentration = len(spend_analytics_df[spend_analytics_df["concentration_risk_level"] == "MEDIUM"])
            
            report = {
                "timestamp": datetime.now().isoformat(),
                "spend_intelligence": {
                    "total_enterprise_spend": float(total_spend),
                    "total_contract_spend": float(total_contract_spend),
                    "total_maverick_spend": float(total_maverick),
                    "enterprise_sum_percentage": round(enterprise_sum, 2),
                    "enterprise_maverick_percentage": round(enterprise_maverick, 2),
                },
                "concentration_analysis": {
                    "suppliers_high_risk": high_concentration,
                    "suppliers_medium_risk": medium_concentration,
                    "suppliers_low_risk": len(spend_analytics_df) - high_concentration - medium_concentration,
                    "top_supplier_concentration": round(spend_analytics_df["spend_concentration_percentage"].max(), 2),
                },
                "opportunity_analysis": {
                    "total_savings_opportunity": float(total_savings),
                    "suppliers_needing_diversification": int(spend_analytics_df["diversification_needed"].sum()),
                    "suppliers_below_sum_target": int(spend_analytics_df[spend_analytics_df["sum_percentage"] < 80].shape[0]),
                },
                "targets": {
                    "sum_target": f">{SUM_CONFIG['enterprise_sum_target']*100:.0f}%",
                    "maverick_target": f"<{MAVERICK_CONFIG['enterprise_maverick_target']*100:.0f}%",
                    "current_sum": f"{enterprise_sum:.1f}%",
                    "current_maverick": f"{enterprise_maverick:.1f}%",
                },
            }
            
            report_path = os.path.join(
                PATHS["phase1_tables"],
                "phase1_spend_intelligence_report.json"
            )
            
            with open(report_path, "w") as f:
                json.dump(report, f, indent=2)
            
            logger.info(f"  Saved report to {report_path}")
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to generate report: {e}")
            return False
    
    def _is_under_contract(self, transaction: pd.Series, supplier: str) -> bool:
        """Determine if transaction is under contract."""
        
        # Rule 1: Disruptions are non-contract
        if transaction.get("disruption_type") != "None":
            return False
        
        # Rule 2: Check spend variance
        supplier_txns = self.transactions_df[
            self.transactions_df["supplier"] == supplier
        ]
        avg_spend = supplier_txns["amount"].mean()
        
        if transaction["amount"] > avg_spend * SUM_CONFIG["contract_classification"]["emergency_threshold_multiplier"]:
            return False
        
        # Default: assume under contract with probability
        return random.random() < SUM_CONFIG["contract_classification"]["default_contract_rate"]
    
    def _classify_maverick(self, transaction: pd.Series) -> Tuple[bool, str]:
        """Classify if transaction is maverick spend."""
        
        maverick_prob = MAVERICK_CONFIG["maverick_baseline_rate"]
        
        if random.random() < maverick_prob:
            # Select maverick type based on distribution
            rand = random.random()
            dist = MAVERICK_CONFIG["maverick_type_distribution"]
            
            if rand < dist["off_contract"]:
                return True, "off_contract"
            elif rand < dist["off_contract"] + dist["unauthorized_vendor"]:
                return True, "unauthorized_vendor"
            elif rand < dist["off_contract"] + dist["unauthorized_vendor"] + dist["emergency_high_cost"]:
                return True, "emergency_high_cost"
            else:
                return True, "non_compliant"
        
        return False, ""
    
    def _classify_concentration_risk(self, pct_enterprise: float, pct_category: float) -> str:
        """Classify concentration risk level."""
        
        conc_cfg = CONCENTRATION_CONFIG
        
        if (pct_enterprise > conc_cfg["enterprise_thresholds"]["high_risk"] or
            pct_category > conc_cfg["category_thresholds"]["high_risk"]):
            return "HIGH"
        elif (pct_enterprise > conc_cfg["enterprise_thresholds"]["medium_risk"] or
              pct_category > conc_cfg["category_thresholds"]["medium_risk"]):
            return "MEDIUM"
        else:
            return "LOW"
    
    def _calculate_savings(self, amount: float, maverick_type: str) -> float:
        """Calculate estimated savings for maverick spend."""
        
        savings_rates = MAVERICK_CONFIG["savings_estimation"]
        
        if maverick_type == "off_contract":
            return amount * savings_rates["off_contract_savings_rate"]
        elif maverick_type == "unauthorized_vendor":
            return amount * savings_rates["unauthorized_savings_rate"]
        elif maverick_type == "emergency_high_cost":
            return amount * savings_rates["emergency_savings_rate"]
        elif maverick_type == "non_compliant":
            return amount * savings_rates["non_compliant_savings_rate"]
        else:
            return 0

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main execution."""
    try:
        logger.info("=" * 70)
        logger.info("SPEND ANALYTICS CALCULATOR")
        logger.info("=" * 70)
        
        # Validate config
        logger.info("\n[STEP 1] Validating configuration...")
        errors = validate_spend_config()
        if errors:
            logger.error("Configuration errors found:")
            for error in errors:
                logger.error(f"  - {error}")
            return 1
        logger.info("  ✓ Configuration valid")
        
        # Create calculator
        calculator = SpendAnalyticsCalculator()
        
        # Load Phase 1 data
        logger.info("\n[STEP 2] Loading Phase 1 data...")
        if not calculator.load_phase1_data():
            return 1
        
        # Calculate SUM
        logger.info("\n[STEP 3] Calculating Spend Under Management...")
        sum_df = calculator.calculate_sum()
        
        # Identify maverick
        logger.info("\n[STEP 4] Identifying Maverick Spend...")
        calculator.identify_maverick_spend()
        
        # Analyze concentration
        logger.info("\n[STEP 5] Analyzing Concentration Risk...")
        concentration_df, supplier_spends = calculator.analyze_concentration()
        
        # Generate summary
        logger.info("\n[STEP 6] Generating Spend Analytics Summary...")
        spend_analytics_df = calculator.generate_spend_analytics_summary(sum_df, concentration_df)
        
        # Save outputs
        logger.info("\n[STEP 7] Saving outputs...")
        if not calculator.save_spend_analytics(spend_analytics_df, concentration_df):
            return 1
        
        # Generate report
        logger.info("\n[STEP 8] Generating Spend Intelligence Report...")
        if not calculator.generate_spend_intelligence_report(spend_analytics_df):
            return 1
        
        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("SPEND ANALYTICS CALCULATION COMPLETE")
        logger.info("=" * 70)
        
        total_spend = spend_analytics_df["total_spend"].sum()
        total_maverick = spend_analytics_df["maverick_spend"].sum()
        total_savings = spend_analytics_df["estimated_savings_opportunity"].sum()
        
        logger.info(f"\nResults:")
        logger.info(f"  Total Enterprise Spend: ${total_spend:,.0f}")
        logger.info(f"  Total Maverick Spend: ${total_maverick:,.0f}")
        logger.info(f"  Estimated Savings: ${total_savings:,.0f}")
        logger.info(f"  High Concentration Suppliers: {len(spend_analytics_df[spend_analytics_df['concentration_risk_level'] == 'HIGH'])}")
        
        logger.info("\n✓ Phase 1 enhanced with spend analytics!")
        
        return 0
    
    except Exception as e:
        logger.error(f"Spend analytics calculation failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
