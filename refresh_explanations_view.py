"""
Recreates the latest_explanations view to pick up the new LIME columns.
Run this after add_lime_columns.py.

Usage:  python refresh_explanations_view.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from db.db_client import DBClient

with DBClient() as db:
    db.execute("DROP VIEW IF EXISTS latest_explanations")
    db.execute("""
        CREATE VIEW latest_explanations AS
        SELECT DISTINCT ON (vendor_id) *
        FROM explanations
        ORDER BY vendor_id, run_date DESC
    """)
    db.conn.commit()

    # Verify LIME columns are now in the view
    cols = db.fetch_df(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'explanations' ORDER BY column_name"
    )["column_name"].tolist()
    lime_cols = [c for c in cols if "lime" in c]

    print("✅ latest_explanations view recreated")
    print(f"   LIME columns now visible: {lime_cols}")
    print("\nNext: python ml/explainability.py")
