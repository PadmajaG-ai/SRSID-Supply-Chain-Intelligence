"""
SRSID Unified Data Pipeline
Combines Synthetic Data Generation + Kaggle Downloads + Data Integration
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os
import subprocess
import zipfile
from pathlib import Path
import sys

# Set random seed
np.random.seed(42)
random.seed(42)

# ========================================
# CONFIGURATION
# ========================================
OUTPUT_DIR = "data/raw"
NUM_SUPPLIERS = 1800
NUM_TRANSACTIONS = 1000

# Kaggle datasets
KAGGLE_DATASETS = {
    "us_supply_chain": {
        "dataset_id": "yuanchunhong/us-supply-chain-risk-analysis-dataset",
        "expected_file": "Supply_chain_data.csv",
        "description": "US Supply Chain Risk Analysis"
    },
    "kraljic": {
        "dataset_id": "shahriarkabir/procurement-strategy-dataset-for-kraljic-matrix",
        "expected_file": "supplier_data.csv",
        "description": "Procurement Strategy Dataset (Kraljic Matrix)"
    }
}

# Real suppliers for synthetic generation
REAL_SUPPLIERS = [
    "Apple Inc", "Dell Technologies", "Samsung Electronics", "Intel Corporation",
    "TSMC", "Nvidia", "AMD", "Qualcomm", "Broadcom", "Micron Technology",
    "SK Hynix", "Kioxia", "Western Digital", "Seagate Technology", "Corsair",
    "Kingston", "Crucial", "HyperX", "ADATA", "Patriot", "G.Skill", "EVGA",
    "MSI", "Asus", "Gigabyte", "Zotac", "INNO3D", "PNY", "Gainward", "Palit",
    "Foxconn", "Pegatron", "Compal", "Wistron", "Quanta", "Flex", "Sanmina",
    "Jaco Electronics", "Benchmark", "Celestica", "Plexus", "Solectron",
    "Artesyn", "Emerson", "Delta", "Acbel", "FSP", "Seasonic",
    "Lian Li", "Meshify", "SilentPC", "Akasa", "Nanoxia", "Phanteks",
    "EK Water Blocks", "XSPC", "Swiftech", "Alphacool",
]

RISK_CATEGORIES = ["High", "Medium", "Low"]
CATEGORIES = ["Electronics", "Memory", "Storage", "Peripherals", "Power Supplies", 
              "Cooling", "Cases", "Cables", "Accessories"]

# ========================================
# STEP 1: GENERATE SYNTHETIC SUPPLIER RISK DATA
# ========================================
def generate_synthetic_supplier_risk(num_suppliers: int) -> pd.DataFrame:
    """Generate synthetic supplier risk assessment data."""
    
    print("\n" + "=" * 70)
    print("[STEP 1] GENERATING SYNTHETIC SUPPLIER RISK DATA")
    print("=" * 70)
    print(f"Generating {num_suppliers} supplier risk records...")
    
    # Use real suppliers + generate additional ones
    suppliers = REAL_SUPPLIERS.copy()
    
    while len(suppliers) < num_suppliers:
        new_supplier = f"{random.choice(['Tech', 'Manufacturing', 'Component', 'Industrial', 'Supply'])}_" \
                      f"{random.choice(['Solutions', 'Systems', 'Corp', 'Industries', 'Group'])}_" \
                      f"{random.randint(100, 9999)}"
        if new_supplier not in suppliers:
            suppliers.append(new_supplier)
    
    suppliers = suppliers[:num_suppliers]
    
    data = {
        "Supplier Name": suppliers,
        "Financial Stability": np.random.uniform(0.6, 1.0, num_suppliers),
        "Delivery Performance": np.random.uniform(0.5, 0.99, num_suppliers),
        "Historical Risk Category": np.random.choice(RISK_CATEGORIES, num_suppliers),
    }
    
    df = pd.DataFrame(data)
    df["Financial Stability"] = df["Financial Stability"].round(2)
    df["Delivery Performance"] = df["Delivery Performance"].round(2)
    
    print(f"✓ Generated {len(df)} records")
    print(f"  Sample suppliers: {', '.join(df['Supplier Name'].head(3).tolist())}")
    
    return df


# ========================================
# STEP 2: CHECK KAGGLE SETUP
# ========================================
def check_kaggle_setup() -> bool:
    """Check if Kaggle API is available."""
    
    print("\n" + "=" * 70)
    print("[STEP 2] CHECKING KAGGLE API")
    print("=" * 70)
    
    try:
        result = subprocess.run(["kaggle", "--version"], capture_output=True, check=False)
        if result.returncode != 0:
            print("⚠ Kaggle CLI not installed")
            return False
        
        kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
        if not kaggle_json.exists():
            print("⚠ Kaggle API credentials not found")
            return False
        
        print("✓ Kaggle API configured")
        return True
    
    except Exception as e:
        print(f"⚠ Kaggle check failed: {e}")
        return False


# ========================================
# STEP 3: DOWNLOAD KAGGLE DATASETS
# ========================================
def download_kaggle_datasets() -> dict:
    """Download datasets from Kaggle."""
    
    print("\n" + "=" * 70)
    print("[STEP 3] DOWNLOADING KAGGLE DATASETS")
    print("=" * 70)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    downloads = {}
    
    for key, config in KAGGLE_DATASETS.items():
        print(f"\nDownloading: {config['description']}")
        print(f"  Dataset: {config['dataset_id']}")
        
        try:
            cmd = ["kaggle", "datasets", "download", "-d", config['dataset_id'], "-p", OUTPUT_DIR, "--quiet"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"  ✓ Downloaded successfully")
                downloads[key] = True
            else:
                print(f"  ✗ Download failed")
                downloads[key] = False
        
        except Exception as e:
            print(f"  ✗ Error: {e}")
            downloads[key] = False
    
    return downloads


# ========================================
# STEP 4: EXTRACT & PROCESS KAGGLE FILES
# ========================================
def extract_and_process_kaggle_data() -> dict:
    """Extract zip files and load CSV data."""
    
    print("\n" + "=" * 70)
    print("[STEP 4] EXTRACTING & PROCESSING KAGGLE DATA")
    print("=" * 70)
    
    # Extract all zip files
    for file in Path(OUTPUT_DIR).glob("*.zip"):
        print(f"\nExtracting: {file.name}")
        try:
            with zipfile.ZipFile(file, 'r') as zip_ref:
                zip_ref.extractall(OUTPUT_DIR)
            print(f"  ✓ Extracted successfully")
            file.unlink()  # Remove zip
        except Exception as e:
            print(f"  ✗ Extraction failed: {e}")
    
    # Find and load CSV files
    csv_files = {}
    
    print("\nFound CSV files:")
    for csv_file in Path(OUTPUT_DIR).glob("*.csv"):
        print(f"  - {csv_file.name}")
        try:
            df = pd.read_csv(csv_file)
            csv_files[csv_file.name] = df
            print(f"    Rows: {len(df)}, Columns: {len(df.columns)}")
        except Exception as e:
            print(f"    ✗ Error reading: {e}")
    
    return csv_files


# ========================================
# STEP 5: PROCESS & STANDARDIZE DATA
# ========================================
def standardize_kaggle_data(csv_files: dict) -> dict:
    """Standardize Kaggle data to our schema."""
    
    print("\n" + "=" * 70)
    print("[STEP 5] STANDARDIZING KAGGLE DATA")
    print("=" * 70)
    
    processed = {}
    
    # Try to find and standardize US Supply Chain data
    print("\nProcessing Supply Chain data...")
    for filename, df in csv_files.items():
        if 'supply' in filename.lower() or 'chain' in filename.lower() or 'transaction' in filename.lower():
            print(f"  Found: {filename}")
            print(f"  Columns: {list(df.columns)}")
            
            # Try to standardize to our schema
            try:
                # Common column name variations
                col_mapping = {
                    'supplier': 'Supplier',
                    'supplier_name': 'Supplier',
                    'amount': 'Amount',
                    'date': 'Date',
                    'disruption': 'Disruption Type',
                }
                
                df_std = df.copy()
                
                # Rename columns if they exist
                for old_col in df_std.columns:
                    for key, val in col_mapping.items():
                        if key in old_col.lower():
                            df_std.rename(columns={old_col: val}, inplace=True)
                            break
                
                # Ensure required columns
                if 'Supplier' in df_std.columns or 'supplier' in df_std.columns:
                    processed['us_supply_chain'] = df_std
                    print(f"  ✓ Standardized as US Supply Chain data")
            
            except Exception as e:
                print(f"  ✗ Standardization failed: {e}")
    
    # Try to find and standardize Kraljic data
    print("\nProcessing Kraljic/Supplier Strategy data...")
    for filename, df in csv_files.items():
        if 'kraljic' in filename.lower() or 'strategy' in filename.lower() or 'supplier' in filename.lower():
            if filename not in [k for k, v in processed.items()]:  # Skip if already processed
                print(f"  Found: {filename}")
                print(f"  Columns: {list(df.columns)}")
                
                try:
                    processed['kraljic'] = df
                    print(f"  ✓ Processed as Kraljic/Strategy data")
                    break
                
                except Exception as e:
                    print(f"  ✗ Processing failed: {e}")
    
    return processed


# ========================================
# STEP 6: SAVE FINAL DATA
# ========================================
def save_final_datasets(synthetic_risk: pd.DataFrame, processed_kaggle: dict):
    """Save all data in final format."""
    
    print("\n" + "=" * 70)
    print("[STEP 6] SAVING FINAL DATASETS")
    print("=" * 70)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. Save synthetic supplier risk
    output_file_1 = os.path.join(OUTPUT_DIR, "supplier_risk_assessment.csv")
    synthetic_risk.to_csv(output_file_1, index=False)
    print(f"\n✓ Saved: supplier_risk_assessment.csv")
    print(f"  Rows: {len(synthetic_risk)}")
    print(f"  Columns: {list(synthetic_risk.columns)}")
    
    # 2. Save Kaggle US Supply Chain data (if downloaded)
    if 'us_supply_chain' in processed_kaggle:
        output_file_2 = os.path.join(OUTPUT_DIR, "us_supply_chain_risk.csv")
        processed_kaggle['us_supply_chain'].to_csv(output_file_2, index=False)
        print(f"\n✓ Saved: us_supply_chain_risk.csv (from Kaggle)")
        print(f"  Rows: {len(processed_kaggle['us_supply_chain'])}")
        print(f"  Columns: {list(processed_kaggle['us_supply_chain'].columns)}")
    else:
        print(f"\n⚠ US Supply Chain data not found in Kaggle downloads")
        print(f"  Will generate synthetic version...")
        # Generate synthetic transactions
        suppliers = synthetic_risk["Supplier Name"].tolist()
        end_date = datetime.now()
        start_date = end_date - timedelta(days=730)
        
        dates = [start_date + timedelta(days=random.randint(0, 730)) for _ in range(NUM_TRANSACTIONS)]
        
        data = {
            "Supplier": np.random.choice(suppliers, NUM_TRANSACTIONS),
            "Amount": np.random.uniform(10000, 1000000, NUM_TRANSACTIONS),
            "Date": dates,
            "Disruption Type": np.random.choice(["None", "Strike", "Weather", "Shortage"], NUM_TRANSACTIONS),
            "Disruption Details": [""] * NUM_TRANSACTIONS,
            "Procurement Category": np.random.choice(CATEGORIES, NUM_TRANSACTIONS),
            "Invoice Number": [f"INV-{str(i).zfill(6)}" for i in range(1, NUM_TRANSACTIONS + 1)],
            "PO Number": [f"PO-{str(i).zfill(6)}" for i in range(1, NUM_TRANSACTIONS + 1)],
        }
        
        df_transactions = pd.DataFrame(data)
        df_transactions["Amount"] = df_transactions["Amount"].round(2)
        df_transactions["Date"] = df_transactions["Date"].dt.strftime('%Y-%m-%d')
        
        output_file_2 = os.path.join(OUTPUT_DIR, "us_supply_chain_risk.csv")
        df_transactions.to_csv(output_file_2, index=False)
        print(f"\n✓ Saved: us_supply_chain_risk.csv (synthetic)")
        print(f"  Rows: {len(df_transactions)}")
        print(f"  Columns: {list(df_transactions.columns)}")
    
    # 3. Save Kaggle Kraljic data (if downloaded)
    if 'kraljic' in processed_kaggle:
        output_file_3 = os.path.join(OUTPUT_DIR, "kraljic_matrix.csv")
        processed_kaggle['kraljic'].to_csv(output_file_3, index=False)
        print(f"\n✓ Saved: kraljic_matrix.csv (from Kaggle)")
        print(f"  Rows: {len(processed_kaggle['kraljic'])}")
        print(f"  Columns: {list(processed_kaggle['kraljic'].columns)}")
    else:
        print(f"\n⚠ Kraljic data not found in Kaggle downloads")
        print(f"  Will generate synthetic version...")
        # Generate synthetic Kraljic data
        suppliers = synthetic_risk["Supplier Name"].tolist()
        supply_risk = np.random.uniform(0.1, 0.95, len(suppliers))
        profit_impact = np.random.uniform(0.1, 0.95, len(suppliers))
        
        kvadrants = []
        for sr, pi in zip(supply_risk, profit_impact):
            if sr > 0.5 and pi > 0.5:
                kvadrants.append("Strategic")
            elif sr <= 0.5 and pi > 0.5:
                kvadrants.append("Leverage")
            elif sr > 0.5 and pi <= 0.5:
                kvadrants.append("Bottleneck")
            else:
                kvadrants.append("Tactical")
        
        data = {
            "Supplier Name": suppliers,
            "Supply Risk Score": supply_risk.round(2),
            "Profit Impact Score": profit_impact.round(2),
            "Kraljic Quadrant": kvadrants,
            "Category": np.random.choice(CATEGORIES, len(suppliers)),
            "Segment": np.random.choice(["Critical", "Standard"], len(suppliers)),
        }
        
        df_kvadrant = pd.DataFrame(data)
        
        output_file_3 = os.path.join(OUTPUT_DIR, "kraljic_matrix.csv")
        df_kvadrant.to_csv(output_file_3, index=False)
        print(f"\n✓ Saved: kraljic_matrix.csv (synthetic)")
        print(f"  Rows: {len(df_kvadrant)}")
        print(f"  Columns: {list(df_kvadrant.columns)}")


# ========================================
# MAIN EXECUTION
# ========================================
def main():
    """Main pipeline."""
    
    print("=" * 70)
    print("SRSID UNIFIED DATA PIPELINE")
    print("Combining Synthetic + Kaggle Datasets")
    print("=" * 70)
    
    try:
        # Step 1: Generate synthetic supplier risk
        df_synthetic_risk = generate_synthetic_supplier_risk(NUM_SUPPLIERS)
        
        # Step 2: Check Kaggle setup
        kaggle_available = check_kaggle_setup()
        
        if kaggle_available:
            # Step 3: Download Kaggle datasets
            downloads = download_kaggle_datasets()
            
            # Step 4: Extract and process
            csv_files = extract_and_process_kaggle_data()
            
            # Step 5: Standardize
            processed_kaggle = standardize_kaggle_data(csv_files)
        else:
            print("\n⚠ Kaggle API not configured. Will use synthetic data for all datasets.")
            processed_kaggle = {}
        
        # Step 6: Save final datasets
        save_final_datasets(df_synthetic_risk, processed_kaggle)
        
        # Summary
        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"✓ All datasets ready in: {os.path.abspath(OUTPUT_DIR)}")
        print(f"\nFiles created:")
        
        for csv_file in Path(OUTPUT_DIR).glob("*.csv"):
            size_mb = csv_file.stat().st_size / (1024 * 1024)
            print(f"  ✓ {csv_file.name} ({size_mb:.2f} MB)")
        
        print("\n✓ Ready for Phase 1 ingestion!")
        print("\nNext: python phase1_ingestion.py")
        print("=" * 70)
    
    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
