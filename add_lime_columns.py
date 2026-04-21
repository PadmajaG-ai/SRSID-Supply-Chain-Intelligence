"""
Add LIME columns to the explanations table in Supabase.
Run once before running ml/explainability.py with LIME support.

Usage:  python db/migrations/003_lime_columns.sql
        -- or run this Python script directly:
        python add_lime_columns.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from db.db_client import DBClient

LIME_COLUMNS = [
    ("lime_driver_1_label",  "VARCHAR(200)"),
    ("lime_driver_1_weight", "FLOAT"),
    ("lime_driver_2_label",  "VARCHAR(200)"),
    ("lime_driver_2_weight", "FLOAT"),
    ("lime_driver_3_label",  "VARCHAR(200)"),
    ("lime_driver_3_weight", "FLOAT"),
    ("lime_narrative",       "TEXT"),
    ("methods_agree",        "VARCHAR(5)"),
]

print("Adding LIME columns to explanations table in Supabase...")
with DBClient() as db:
    for col, dtype in LIME_COLUMNS:
        db.add_column_if_missing("explanations", col, dtype)
        print(f"  OK  {col} ({dtype})")
    db.conn.commit()

print("\nDone. Now run: python ml/explainability.py")
