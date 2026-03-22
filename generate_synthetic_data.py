"""
Synthetic Data Generator for SRSID Phase 1
Generates realistic supplier risk, transaction, and Kraljic matrix datasets
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# ========================================
# CONFIGURATION
# ========================================
OUTPUT_DIR = "data/raw"
NUM_SUPPLIERS = 1800  # Supplier Risk Assessment
NUM_TRANSACTIONS = 1000  # US Supply Chain Risk
NUM_KVADRANT = 1800  # Kraljic Matrix

# Create output directory if not exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========================================
# REAL SUPPLIER NAMES (Tech & Manufacturing)
# ========================================
REAL_SUPPLIERS = [
    "Apple Inc", "Dell Technologies", "Samsung Electronics", "Intel Corporation",
    "TSMC", "Nvidia", "AMD", "Qualcomm", "Broadcom", "Micron Technology",
    "SK Hynix", "Kioxia", "Western Digital", "Seagate Technology", "Corsair",
    "Kingston", "Crucial", "HyperX", "ADATA", "Patriot", "G.Skill", "EVGA",
    "MSI", "Asus", "Gigabyte", "Zotac", "INNO3D", "PNY", "Gainward", "Palit",
    "NVIDIA", "AMD", "Intel", "Qualcomm", "Broadcom", "Marvell", "Xilinx",
    "Actel", "Altera", "Atmel", "Infineon", "Maxim Integrated", "NXP",
    "Renesas", "STMicroelectronics", "Texas Instruments", "Microchip", "Cypress",
    "Lattice", "Cadence", "Synopsys", "Mentor Graphics", "Keysight", "Rohde",
    "Analog Devices", "ON Semiconductor", "Power Integrations", "Vishay",
    "TDK", "Murata", "AVX", "Yageo", "KEMET", "Panasonic", "Sony", "Samsung",
    "LG", "BOE", "AU Optronics", "Sharp", "Innolux", "CSOT", "Huangstone",
    "Foxconn", "Pegatron", "Compal", "Wistron", "Quanta", "Flex", "Sanmina",
    "Jaco Electronics", "Benchmark", "Celestica", "Plexus", "Solectron",
    "Hon Hai Precision", "Flextronics", "SolEctron", "Nortech", "CUI Global",
    "Artesyn", "Emerson", "Delta", "Acbel", "GREAT", "FSP", "Seasonic",
    "Corsair", "EVGA", "Super Flower", "Gigabyte", "Thermaltake", "Antec",
    "Cooler Master", "NZXT", "Fractal Design", "Be Quiet", "ThermalTake",
    "Lian Li", "Meshify", "SilentPC", "Akasa", "Nanoxia", "Phanteks",
    "EK Water Blocks", "XSPC", "Swiftech", "Aqua Computer", "Alphacool",
]

DISRUPTION_TYPES = ["None", "Strike", "Weather", "Shortage", "Logistics Delay", 
                     "Recall", "Lawsuit", "Bankruptcy", "Pandemic", "Factory Fire"]

RISK_CATEGORIES = ["High", "Medium", "Low"]

KVADRANT_TYPES = ["Strategic", "Tactical", "Bottleneck", "Leverage"]

CATEGORIES = ["Electronics", "Memory", "Storage", "Peripherals", "Power Supplies", 
              "Cooling", "Cases", "Cables", "Accessories"]

# ========================================
# 1. GENERATE SUPPLIER RISK ASSESSMENT DATA
# ========================================
def generate_supplier_risk_assessment(num_suppliers: int) -> pd.DataFrame:
    """Generate synthetic supplier risk assessment data."""
    
    print(f"Generating {num_suppliers} supplier risk assessment records...")
    
    # Use real suppliers + generate additional ones
    suppliers = REAL_SUPPLIERS.copy()
    
    # Add more supplier names
    while len(suppliers) < num_suppliers:
        new_supplier = f"{random.choice(['Tech', 'Manufacturing', 'Component', 'Industrial'])}_" \
                      f"{random.choice(['Solutions', 'Systems', 'Corp', 'Industries'])}_" \
                      f"{random.randint(100, 999)}"
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
    
    return df


# ========================================
# 2. GENERATE SUPPLY CHAIN TRANSACTIONS DATA
# ========================================
def generate_supply_chain_transactions(num_transactions: int, suppliers: list) -> pd.DataFrame:
    """Generate synthetic supply chain transaction data."""
    
    print(f"Generating {num_transactions} transaction records...")
    
    # Create date range (last 2 years)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)
    
    dates = [start_date + timedelta(days=random.randint(0, 730)) for _ in range(num_transactions)]
    
    data = {
        "Supplier": np.random.choice(suppliers, num_transactions),
        "Amount": np.random.uniform(10000, 1000000, num_transactions),
        "Date": dates,
        "Disruption Type": np.random.choice(DISRUPTION_TYPES, num_transactions, p=[0.7, 0.08, 0.05, 0.05, 0.05, 0.02, 0.02, 0.02, 0.01, 0.00]),
        "Disruption Details": [f"Detail {i}" if i % 3 == 0 else "" for i in range(num_transactions)],
        "Procurement Category": np.random.choice(CATEGORIES, num_transactions),
        "Invoice Number": [f"INV-{str(i).zfill(6)}" for i in range(1, num_transactions + 1)],
        "PO Number": [f"PO-{str(i).zfill(6)}" for i in range(1, num_transactions + 1)],
    }
    
    df = pd.DataFrame(data)
    df["Amount"] = df["Amount"].round(2)
    df["Date"] = df["Date"].dt.strftime('%Y-%m-%d')
    
    return df


# ========================================
# 3. GENERATE KRALJIC MATRIX DATA
# ========================================
def generate_kraljic_matrix(num_suppliers: int, suppliers: list) -> pd.DataFrame:
    """Generate synthetic Kraljic matrix data."""
    
    print(f"Generating {num_suppliers} Kraljic matrix records...")
    
    # Use suppliers list, fill remaining with synthetic names
    suppliers_list = suppliers[:num_suppliers] if len(suppliers) >= num_suppliers else suppliers.copy()
    
    while len(suppliers_list) < num_suppliers:
        new_supplier = f"Supplier_{len(suppliers_list)}"
        suppliers_list.append(new_supplier)
    
    suppliers_list = suppliers_list[:num_suppliers]
    
    # Generate scores
    supply_risk = np.random.uniform(0.1, 0.95, num_suppliers)
    profit_impact = np.random.uniform(0.1, 0.95, num_suppliers)
    
    # Assign quadrants based on scores
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
        "Supplier Name": suppliers_list,
        "Supply Risk Score": supply_risk.round(2),
        "Profit Impact Score": profit_impact.round(2),
        "Kraljic Quadrant": kvadrants,
        "Category": np.random.choice(CATEGORIES, num_suppliers),
        "Segment": np.random.choice(["Critical", "Standard", "Optional"], num_suppliers),
    }
    
    df = pd.DataFrame(data)
    
    return df


# ========================================
# MAIN EXECUTION
# ========================================
def main():
    """Generate all datasets."""
    
    print("=" * 70)
    print("SRSID SYNTHETIC DATA GENERATOR")
    print("=" * 70)
    
    # Generate Supplier Risk Assessment
    print("\n[1/3] SUPPLIER RISK ASSESSMENT")
    df_risk = generate_supplier_risk_assessment(NUM_SUPPLIERS)
    output_file_1 = os.path.join(OUTPUT_DIR, "supplier_risk_assessment.csv")
    df_risk.to_csv(output_file_1, index=False)
    print(f"✓ Saved: {output_file_1} ({len(df_risk)} rows)")
    
    # Generate Supply Chain Transactions
    print("\n[2/3] SUPPLY CHAIN TRANSACTIONS")
    suppliers = df_risk["Supplier Name"].tolist()
    df_transactions = generate_supply_chain_transactions(NUM_TRANSACTIONS, suppliers)
    output_file_2 = os.path.join(OUTPUT_DIR, "us_supply_chain_risk.csv")
    df_transactions.to_csv(output_file_2, index=False)
    print(f"✓ Saved: {output_file_2} ({len(df_transactions)} rows)")
    
    # Generate Kraljic Matrix
    print("\n[3/3] KRALJIC MATRIX")
    df_kvadrant = generate_kraljic_matrix(NUM_KVADRANT, suppliers)
    output_file_3 = os.path.join(OUTPUT_DIR, "kraljic_matrix.csv")
    df_kvadrant.to_csv(output_file_3, index=False)
    print(f"✓ Saved: {output_file_3} ({len(df_kvadrant)} rows)")
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Supplier Risk Assessment: {len(df_risk):,} rows")
    print(f"Supply Chain Transactions: {len(df_transactions):,} rows")
    print(f"Kraljic Matrix: {len(df_kvadrant):,} rows")
    print(f"\nOutput directory: {os.path.abspath(OUTPUT_DIR)}")
    print("=" * 70)
    
    # Display sample
    print("\nSAMPLE DATA:")
    print("\nSupplier Risk Assessment (first 5 rows):")
    print(df_risk.head())
    print("\nSupply Chain Transactions (first 5 rows):")
    print(df_transactions.head())
    print("\nKraljic Matrix (first 5 rows):")
    print(df_kvadrant.head())


if __name__ == "__main__":
    main()
