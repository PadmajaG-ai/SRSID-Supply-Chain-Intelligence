"""
Kaggle Dataset Downloader for SRSID
Downloads real supply chain datasets from Kaggle
"""

import os
import subprocess
import zipfile
from pathlib import Path

# ========================================
# CONFIGURATION
# ========================================
OUTPUT_DIR = "data/raw"

KAGGLE_DATASETS = {
    "us_supply_chain": {
        "dataset_id": "yuanchunhong/us-supply-chain-risk-analysis-dataset",
        "output_file": "us_supply_chain_risk.csv",
        "description": "US Supply Chain Risk Analysis Dataset"
    },
    "kraljic": {
        "dataset_id": "shahriarkabir/procurement-strategy-dataset-for-kraljic-matrix",
        "output_file": "kraljic_matrix.csv",
        "description": "Procurement Strategy Dataset (Kraljic Matrix)"
    }
}

# ========================================
# HELPER FUNCTIONS
# ========================================
def check_kaggle_installed() -> bool:
    """Check if Kaggle CLI is installed."""
    try:
        subprocess.run(["kaggle", "--version"], capture_output=True, check=True)
        return True
    except Exception:
        return False


def check_kaggle_credentials() -> bool:
    """Check if Kaggle API credentials exist."""
    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    return kaggle_json.exists()


def setup_kaggle() -> bool:
    """Guide user through Kaggle setup."""
    print("\n" + "=" * 70)
    print("KAGGLE SETUP REQUIRED")
    print("=" * 70)
    
    if not check_kaggle_installed():
        print("\n[1] Install Kaggle CLI:")
        print("    pip install kaggle")
        return False
    
    if not check_kaggle_credentials():
        print("\n[2] Get Kaggle API Token:")
        print("    1. Go to: https://www.kaggle.com/settings/account")
        print("    2. Click 'Create New API Token'")
        print("    3. This downloads kaggle.json")
        print("    4. Move it to: C:\\Users\\YourName\\.kaggle\\kaggle.json")
        print("\n    Then run this script again.")
        return False
    
    return True


def download_dataset(dataset_id: str, output_dir: str) -> bool:
    """Download dataset using Kaggle CLI."""
    try:
        print(f"\nDownloading: {dataset_id}")
        cmd = ["kaggle", "datasets", "download", "-d", dataset_id, "-p", output_dir]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✓ Downloaded successfully")
            return True
        else:
            print(f"✗ Download failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def extract_zip_files(directory: str):
    """Extract all zip files in directory."""
    for file in Path(directory).glob("*.zip"):
        print(f"\nExtracting: {file.name}")
        try:
            with zipfile.ZipFile(file, 'r') as zip_ref:
                zip_ref.extractall(directory)
            print(f"✓ Extracted successfully")
            # Remove zip file
            file.unlink()
        except Exception as e:
            print(f"✗ Extraction failed: {e}")


def main():
    """Main execution."""
    
    print("=" * 70)
    print("SRSID KAGGLE DATASET DOWNLOADER")
    print("=" * 70)
    
    # Step 1: Check Kaggle setup
    if not setup_kaggle():
        print("\n⚠ Please complete the setup steps above, then run this script again.")
        return
    
    print("\n✓ Kaggle API is configured")
    
    # Step 2: Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"✓ Output directory: {os.path.abspath(OUTPUT_DIR)}")
    
    # Step 3: Download datasets
    print("\n" + "=" * 70)
    print("DOWNLOADING DATASETS")
    print("=" * 70)
    
    success_count = 0
    for key, config in KAGGLE_DATASETS.items():
        print(f"\n[{key.upper()}] {config['description']}")
        if download_dataset(config['dataset_id'], OUTPUT_DIR):
            success_count += 1
    
    # Step 4: Extract zip files
    print("\n" + "=" * 70)
    print("EXTRACTING FILES")
    print("=" * 70)
    extract_zip_files(OUTPUT_DIR)
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Downloaded: {success_count}/{len(KAGGLE_DATASETS)} datasets")
    
    # List downloaded files
    print("\nFiles in data/raw/:")
    for file in Path(OUTPUT_DIR).glob("*.csv"):
        size_mb = file.stat().st_size / (1024 * 1024)
        print(f"  ✓ {file.name} ({size_mb:.2f} MB)")
    
    print("\n✓ Ready for Phase 1 ingestion!")


if __name__ == "__main__":
    main()
