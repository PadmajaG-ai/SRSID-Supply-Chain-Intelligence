from db.db_client import DBClient

with DBClient() as db:
    cols = db.fetch_df(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'explanations' ORDER BY column_name"
    )["column_name"].tolist()

    lime_cols = [c for c in cols if "lime" in c]
    all_cols  = cols

    print(f"Total columns in explanations: {len(all_cols)}")
    print(f"LIME columns found: {lime_cols if lime_cols else 'NONE — run add_lime_columns.py first'}")
    print(f"\nAll columns:")
    for c in all_cols:
        print(f"  {c}")
