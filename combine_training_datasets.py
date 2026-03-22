"""
Training Dataset Builder
Combines all sources: Phase 1 Synthetic + Real Vendors + Kaggle + Guardian News
Keeps separate for Phase 2 fuzzy matching consolidation
"""

import pandas as pd
import numpy as np
import logging
import sys
import json
import os
from datetime import datetime
from typing import Tuple, Dict, List

from training_dataset_config import (
    DATA_SOURCES,
    KAGGLE_COLUMN_MAPPING,
    TRAINING_OUTPUT,
    DATA_QUALITY,
    MERGE_STRATEGY,
    get_all_enabled_sources,
    get_total_expected_rows,
)

# ============================================================
# LOGGING SETUP
# ============================================================
os.makedirs("logs", exist_ok=True)
os.makedirs(TRAINING_OUTPUT["output_dir"], exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(logging_config := logging.getLogger().handlers[0].baseFilename or "logs/training_dataset_builder.log", encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# TRAINING DATASET BUILDER
# ============================================================

class TrainingDatasetBuilder:
    """Build combined training dataset from all sources."""
    
    def __init__(self):
        self.risk_assessment_dfs = []
        self.transactions_dfs = []
        self.kraljic_dfs = []
        self.manifest = {
            "timestamp": datetime.now().isoformat(),
            "sources": {},
            "statistics": {},
        }
    
    def build_training_dataset(self) -> bool:
        """Main execution pipeline."""
        try:
            logger.info("=" * 70)
            logger.info("TRAINING DATASET BUILDER")
            logger.info("=" * 70)
            
            # Step 1: Load Phase 1 Synthetic
            logger.info("\n[STEP 1] Loading Phase 1 Synthetic Data...")
            if not self._load_phase1_synthetic():
                logger.warning("Phase 1 synthetic data not found (optional)")
            
            # Step 2: Load Real Vendors
            logger.info("\n[STEP 2] Loading Real Vendor Data...")
            if not self._load_real_vendors():
                logger.warning("Real vendor data not found (optional)")
            
            # Step 3: Load Kaggle Data
            logger.info("\n[STEP 3] Loading Kaggle Data...")
            if not self._load_kaggle_data():
                logger.warning("Kaggle data not found (optional)")
            
            # Step 4: Load Guardian News
            logger.info("\n[STEP 4] Loading Guardian News Disruptions...")
            if not self._load_guardian_news():
                logger.warning("Guardian news data not found (optional)")
            
            # Step 5: Combine all data
            logger.info("\n[STEP 5] Combining All Data Sources...")
            risk_df, trans_df, kraljic_df = self._combine_all_data()
            
            # Step 6: Add source tracking
            logger.info("\n[STEP 6] Adding Source Tracking...")
            risk_df = self._add_source_column(risk_df)
            trans_df = self._add_source_column(trans_df)
            kraljic_df = self._add_source_column(kraljic_df)
            
            # Step 7: Validate data quality
            logger.info("\n[STEP 7] Validating Data Quality...")
            risk_df = self._validate_data(risk_df, "risk_assessment")
            trans_df = self._validate_data(trans_df, "transactions")
            kraljic_df = self._validate_data(kraljic_df, "kraljic")
            
            # Step 8: Save to CSV
            logger.info("\n[STEP 8] Saving Training Datasets...")
            self._save_datasets(risk_df, trans_df, kraljic_df)
            
            # Step 9: Generate reports
            logger.info("\n[STEP 9] Generating Reports...")
            self._generate_reports(risk_df, trans_df, kraljic_df)
            
            # Summary
            logger.info("\n" + "=" * 70)
            logger.info("✓ TRAINING DATASET CREATION COMPLETE")
            logger.info("=" * 70)
            self._print_summary(risk_df, trans_df, kraljic_df)
            
            return True
        
        except Exception as e:
            logger.error(f"Dataset building failed: {e}", exc_info=True)
            return False
    
    def _load_phase1_synthetic(self) -> bool:
        """Load original Phase 1 synthetic data."""
        source_config = DATA_SOURCES.get("phase1_synthetic")
        if not source_config.get("enabled"):
            return False
        
        try:
            files = source_config.get("files", {})
            
            # Load risk assessment
            if os.path.exists(files.get("risk_assessment", "")):
                df = pd.read_csv(files["risk_assessment"])
                df["_source"] = "phase1_synthetic"
                self.risk_assessment_dfs.append(df)
                logger.info(f"  Loaded {len(df)} risk assessment records")
            
            # Load transactions
            if os.path.exists(files.get("transactions", "")):
                df = pd.read_csv(files["transactions"])
                df["_source"] = "phase1_synthetic"
                self.transactions_dfs.append(df)
                logger.info(f"  Loaded {len(df)} transaction records")
            
            # Load Kraljic
            if os.path.exists(files.get("kraljic", "")):
                df = pd.read_csv(files["kraljic"])
                df["_source"] = "phase1_synthetic"
                self.kraljic_dfs.append(df)
                logger.info(f"  Loaded {len(df)} Kraljic records")
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to load Phase 1 synthetic: {e}")
            return False
    
    def _load_real_vendors(self) -> bool:
        """Load synthesized real vendor data."""
        source_config = DATA_SOURCES.get("real_vendors")
        if not source_config.get("enabled"):
            return False
        
        try:
            files = source_config.get("files", {})
            
            # Load risk assessment
            if os.path.exists(files.get("risk_assessment", "")):
                df = pd.read_csv(files["risk_assessment"])
                df["_source"] = "real_vendors"
                self.risk_assessment_dfs.append(df)
                logger.info(f"  Loaded {len(df)} real vendor risk records")
            
            # Load transactions
            if os.path.exists(files.get("transactions", "")):
                df = pd.read_csv(files["transactions"])
                df["_source"] = "real_vendors"
                self.transactions_dfs.append(df)
                logger.info(f"  Loaded {len(df)} real vendor transaction records")
            
            # Load Kraljic
            if os.path.exists(files.get("kraljic", "")):
                df = pd.read_csv(files["kraljic"])
                df["_source"] = "real_vendors"
                self.kraljic_dfs.append(df)
                logger.info(f"  Loaded {len(df)} real vendor Kraljic records")
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to load real vendors: {e}")
            return False
    
    def _load_kaggle_data(self) -> bool:
        """Load Kaggle datasets and map columns."""
        source_config = DATA_SOURCES.get("kaggle")
        if not source_config.get("enabled"):
            return False
        
        try:
            files = source_config.get("files", {})
            
            # Load transactions (us_supply_chain_risk)
            trans_file = files.get("transactions")
            if trans_file and os.path.exists(trans_file):
                df = pd.read_csv(trans_file)
                # Map columns
                mapping = KAGGLE_COLUMN_MAPPING.get("us_supply_chain_risk", {})
                df = df.rename(columns=mapping)
                df["_source"] = "kaggle"
                self.transactions_dfs.append(df)
                logger.info(f"  Loaded {len(df)} Kaggle transactions")
            
            # Load risk assessment
            risk_file = files.get("risk_assessment")
            if risk_file and os.path.exists(risk_file):
                df = pd.read_csv(risk_file)
                # Map columns
                mapping = KAGGLE_COLUMN_MAPPING.get("supplier_risk_assessment", {})
                df = df.rename(columns=mapping)
                # Normalize scores to 0-1 if needed
                if "financial_stability_score" in df.columns:
                    df["financial_stability_score"] = df["financial_stability_score"].apply(
                        lambda x: x / 100 if x > 1 else x
                    )
                if "delivery_performance" in df.columns:
                    df["delivery_performance"] = df["delivery_performance"].apply(
                        lambda x: x / 100 if x > 1 else x
                    )
                df["_source"] = "kaggle"
                self.risk_assessment_dfs.append(df)
                logger.info(f"  Loaded {len(df)} Kaggle risk assessment records")
            
            # Load Kraljic
            kraljic_file = files.get("kraljic")
            if kraljic_file and os.path.exists(kraljic_file):
                df = pd.read_csv(kraljic_file)
                # Map columns
                mapping = KAGGLE_COLUMN_MAPPING.get("realistic_kraljic_dataset", {})
                df = df.rename(columns=mapping)
                # Normalize scores
                for col in ["supply_risk_score", "profit_impact_score", "environmental_impact", "single_source_risk"]:
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: x / 100 if pd.notna(x) and x > 1 else x)
                df["_source"] = "kaggle"
                self.kraljic_dfs.append(df)
                logger.info(f"  Loaded {len(df)} Kaggle Kraljic records")
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to load Kaggle data: {e}")
            return False
    
    def _load_guardian_news(self) -> bool:
        """Load Guardian API disruption data."""
        source_config = DATA_SOURCES.get("guardian_news")
        if not source_config.get("enabled"):
            return False
        
        try:
            files = source_config.get("files", {})
            disruptions_file = files.get("disruptions")
            
            if disruptions_file and os.path.exists(disruptions_file):
                df = pd.read_csv(disruptions_file)
                
                # Convert to transaction format
                trans_records = []
                for idx, row in df.iterrows():
                    record = {
                        "supplier": row.get("Vendor_Name"),
                        "amount": 0,  # Disruptions don't have amount
                        "transaction_date": row.get("Event_Date"),
                        "disruption_type": row.get("Disruption_Type", "None"),
                        "procurement_category": "News Event",
                        "invoice_number": f"NEWS-{idx:06d}",
                        "po_number": f"NEWS-{idx:06d}",
                        "source_file_name": "guardian_news.csv",
                        "_source": "guardian_news",
                    }
                    trans_records.append(record)
                
                if trans_records:
                    trans_df = pd.DataFrame(trans_records)
                    self.transactions_dfs.append(trans_df)
                    logger.info(f"  Loaded {len(trans_df)} Guardian news disruptions")
                
                return True
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to load Guardian news: {e}")
            return False
    
    def _combine_all_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Combine all loaded data sources."""
        
        # Combine risk assessment
        if self.risk_assessment_dfs:
            risk_df = pd.concat(self.risk_assessment_dfs, ignore_index=True)
            logger.info(f"Combined {len(self.risk_assessment_dfs)} risk sources → {len(risk_df)} rows")
        else:
            risk_df = pd.DataFrame()
            logger.warning("No risk assessment data loaded")
        
        # Combine transactions
        if self.transactions_dfs:
            trans_df = pd.concat(self.transactions_dfs, ignore_index=True)
            logger.info(f"Combined {len(self.transactions_dfs)} transaction sources → {len(trans_df)} rows")
        else:
            trans_df = pd.DataFrame()
            logger.warning("No transaction data loaded")
        
        # Combine Kraljic
        if self.kraljic_dfs:
            kraljic_df = pd.concat(self.kraljic_dfs, ignore_index=True)
            logger.info(f"Combined {len(self.kraljic_dfs)} Kraljic sources → {len(kraljic_df)} rows")
        else:
            kraljic_df = pd.DataFrame()
            logger.warning("No Kraljic data loaded")
        
        return risk_df, trans_df, kraljic_df
    
    def _add_source_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure source column exists and add load timestamp."""
        if "_source" not in df.columns:
            df["_source"] = "unknown"
        
        df["_load_timestamp"] = datetime.now()
        
        return df
    
    def _validate_data(self, df: pd.DataFrame, table_type: str) -> pd.DataFrame:
        """Validate and clean data."""
        initial_count = len(df)
        
        # Handle missing critical fields
        if "supplier_name" in df.columns or "supplier" in df.columns:
            supplier_col = "supplier_name" if "supplier_name" in df.columns else "supplier"
            df = df.dropna(subset=[supplier_col])
            dropped = initial_count - len(df)
            if dropped > 0:
                logger.warning(f"Dropped {dropped} rows with missing supplier_name")
        
        # Fill missing numerical values
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df[col].isnull().sum() > 0:
                fill_strategy = DATA_QUALITY["missing_value_strategy"].get(col, "keep")
                if fill_strategy == "fill_with_mean":
                    df[col] = df[col].fillna(df[col].mean())
                elif fill_strategy == "fill_with_0":
                    df[col] = df[col].fillna(0)
        
        logger.info(f"Validated {table_type}: {len(df)} rows")
        
        return df
    
    def _save_datasets(self, risk_df: pd.DataFrame, trans_df: pd.DataFrame, kraljic_df: pd.DataFrame):
        """Save datasets to CSV."""
        output_dir = TRAINING_OUTPUT["output_dir"]
        
        # Save risk assessment
        risk_file = os.path.join(output_dir, TRAINING_OUTPUT["files"]["risk_assessment"])
        risk_df.to_csv(risk_file, index=False)
        logger.info(f"Saved {len(risk_df)} risk records to {risk_file}")
        
        # Save transactions
        trans_file = os.path.join(output_dir, TRAINING_OUTPUT["files"]["transactions"])
        trans_df.to_csv(trans_file, index=False)
        logger.info(f"Saved {len(trans_df)} transaction records to {trans_file}")
        
        # Save Kraljic
        kraljic_file = os.path.join(output_dir, TRAINING_OUTPUT["files"]["kraljic"])
        kraljic_df.to_csv(kraljic_file, index=False)
        logger.info(f"Saved {len(kraljic_df)} Kraljic records to {kraljic_file}")
    
    def _generate_reports(self, risk_df: pd.DataFrame, trans_df: pd.DataFrame, kraljic_df: pd.DataFrame):
        """Generate training dataset reports."""
        output_dir = TRAINING_OUTPUT["output_dir"]
        
        # Generate manifest
        manifest = {
            "timestamp": datetime.now().isoformat(),
            "sources": get_all_enabled_sources(),
            "statistics": {
                "risk_assessment": {
                    "rows": len(risk_df),
                    "columns": list(risk_df.columns),
                    "source_distribution": risk_df.get("_source", "").value_counts().to_dict() if "_source" in risk_df.columns else {},
                },
                "transactions": {
                    "rows": len(trans_df),
                    "columns": list(trans_df.columns),
                    "source_distribution": trans_df.get("_source", "").value_counts().to_dict() if "_source" in trans_df.columns else {},
                },
                "kraljic": {
                    "rows": len(kraljic_df),
                    "columns": list(kraljic_df.columns),
                    "source_distribution": kraljic_df.get("_source", "").value_counts().to_dict() if "_source" in kraljic_df.columns else {},
                },
            },
        }
        
        manifest_file = os.path.join(output_dir, TRAINING_OUTPUT["files"]["manifest"])
        with open(manifest_file, 'w') as f:
            json.dump(manifest, f, indent=2)
        logger.info(f"Saved manifest to {manifest_file}")
    
    def _print_summary(self, risk_df: pd.DataFrame, trans_df: pd.DataFrame, kraljic_df: pd.DataFrame):
        """Print summary statistics."""
        logger.info("\n" + "=" * 70)
        logger.info("TRAINING DATASET SUMMARY")
        logger.info("=" * 70)
        
        logger.info(f"\nRisk Assessment:")
        logger.info(f"  Total records: {len(risk_df)}")
        if "_source" in risk_df.columns:
            for source, count in risk_df["_source"].value_counts().items():
                logger.info(f"    {source}: {count}")
        
        logger.info(f"\nSupply Chain Transactions:")
        logger.info(f"  Total records: {len(trans_df)}")
        if "_source" in trans_df.columns:
            for source, count in trans_df["_source"].value_counts().items():
                logger.info(f"    {source}: {count}")
        
        logger.info(f"\nKraljic Matrix:")
        logger.info(f"  Total records: {len(kraljic_df)}")
        if "_source" in kraljic_df.columns:
            for source, count in kraljic_df["_source"].value_counts().items():
                logger.info(f"    {source}: {count}")
        
        logger.info(f"\nDistribution:")
        logger.info(f"  Total unique suppliers: {len(set(risk_df.get('supplier_name', [])) | set(trans_df.get('supplier', [])))}")
        
        logger.info(f"\nDisruption Events:")
        if "disruption_type" in trans_df.columns:
            disruptions = trans_df[trans_df["disruption_type"] != "None"]["disruption_type"].value_counts()
            for disruption_type, count in disruptions.items():
                logger.info(f"  {disruption_type}: {count}")
        
        logger.info(f"\nReady for Phase 2 Consolidation!")
        logger.info(f"  Keep Separate: {not MERGE_STRATEGY['pre_merge_duplicates']}")
        logger.info(f"  Track Source: {MERGE_STRATEGY['track_variant_source']}")

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main execution."""
    try:
        builder = TrainingDatasetBuilder()
        success = builder.build_training_dataset()
        
        return 0 if success else 1
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
