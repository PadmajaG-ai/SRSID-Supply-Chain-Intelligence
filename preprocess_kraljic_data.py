"""
Preprocess Kaggle Kraljic Data
Aggregates product-level data to supplier-level for Phase 1 ingestion
"""

import pandas as pd
import numpy as np

print("=" * 70)
print("KRALJIC DATA PREPROCESSING")
print("=" * 70)

# Load the raw Kaggle Kraljic data
input_file = "data/raw/kraljic_matrix.csv"
output_file = "data/raw/kraljic_matrix_processed.csv"

print(f"\nLoading: {input_file}")
df = pd.read_csv(input_file)

print(f"Initial rows: {len(df)}")
print(f"Columns: {list(df.columns)}")

# Display sample
print(f"\nSample data:")
print(df.head())

# Group by supplier and aggregate
print(f"\nAggregating by supplier...")

# Map column names to standard names
column_mapping = {
    'Product_Name': 'supplier_name',
    'Supply_Risk_Score': 'supply_risk_score',
    'Profit_Impact_Score': 'profit_impact_score',
    'Kraljic_Category': 'kraljic_quadrant',
    'Supplier_Region': 'segment',
}

# Create aggregated dataframe
aggregated_data = []

for supplier_name in df['Product_Name'].unique():
    supplier_df = df[df['Product_Name'] == supplier_name]
    
    # Aggregate: take average of numeric scores
    supply_risk = supplier_df['Supply_Risk_Score'].mean() if 'Supply_Risk_Score' in df.columns else None
    profit_impact = supplier_df['Profit_Impact_Score'].mean() if 'Profit_Impact_Score' in df.columns else None
    
    # Take the most common category
    if 'Kraljic_Category' in df.columns:
        kvadrant = supplier_df['Kraljic_Category'].mode()[0] if len(supplier_df['Kraljic_Category'].mode()) > 0 else None
    else:
        kvadrant = None
    
    # Take first segment
    segment = supplier_df['Supplier_Region'].iloc[0] if 'Supplier_Region' in df.columns else None
    
    aggregated_data.append({
        'supplier_name': supplier_name,
        'supply_risk_score': supply_risk,
        'profit_impact_score': profit_impact,
        'kraljic_quadrant': kvadrant,
        'category': 'Electronics',  # Default category
        'segment': segment,
    })

# Create new dataframe
df_aggregated = pd.DataFrame(aggregated_data)

# Remove duplicates (in case of exact duplicates)
df_aggregated = df_aggregated.drop_duplicates(subset=['supplier_name'])

print(f"\nAggregated rows: {len(df_aggregated)}")
print(f"\nAggregated data sample:")
print(df_aggregated.head(10))

# Save aggregated data
df_aggregated.to_csv(output_file, index=False)
print(f"\nSaved: {output_file}")

# Replace original with aggregated
import shutil
shutil.copy(output_file, input_file)
print(f"\nReplaced: {input_file} with aggregated version")

print("\n" + "=" * 70)
print("PREPROCESSING COMPLETE")
print(f"New row count: {len(df_aggregated)} (unique suppliers)")
print("=" * 70)
