"""
Quick diagnostic — run this to check why Risk Drivers are empty.
Usage:  python debug_explanations.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from db.db_client import DBClient

with DBClient() as db:

    # 1. How many rows in explanations table?
    count = db.scalar("SELECT COUNT(*) FROM explanations")
    print(f"\n1. Rows in explanations table : {count}")

    if not count:
        print("   ❌ Table is EMPTY — explainability.py did not write data.")
        print("   Fix: python ml/explainability.py")
        sys.exit(1)

    # 2. Check latest_explanations view exists and has data
    view_count = db.scalar("SELECT COUNT(*) FROM latest_explanations")
    print(f"2. Rows in latest_explanations: {view_count}")

    # 3. Sample a few vendor_ids from explanations
    sample = db.fetch_df(
        "SELECT vendor_id, driver_1_label, driver_1_shap, "
        "predicted_risk_tier FROM explanations LIMIT 5"
    )
    print(f"\n3. Sample vendor_ids in explanations:")
    print(sample[["vendor_id","driver_1_label","driver_1_shap",
                  "predicted_risk_tier"]].to_string(index=False))

    # 4. Sample vendor_ids from vendors table
    vendors_sample = db.fetch_df(
        "SELECT vendor_id, supplier_name FROM vendors LIMIT 5"
    )
    print(f"\n4. Sample vendor_ids in vendors table:")
    print(vendors_sample.to_string(index=False))

    # 5. Check if vendor_ids match (key test)
    expl_ids  = set(db.fetch_df(
        "SELECT DISTINCT vendor_id FROM explanations"
    )["vendor_id"].astype(str))
    vendor_ids = set(db.fetch_df(
        "SELECT DISTINCT vendor_id FROM vendors WHERE is_active=TRUE"
    )["vendor_id"].astype(str))

    matched   = expl_ids & vendor_ids
    unmatched = expl_ids - vendor_ids

    print(f"\n5. Vendor ID overlap check:")
    print(f"   Explanation vendor_ids    : {len(expl_ids)}")
    print(f"   Vendors table vendor_ids  : {len(vendor_ids)}")
    print(f"   Matched (join will work)  : {len(matched)}")
    print(f"   Unmatched in explanations : {len(unmatched)}")

    if unmatched:
        print(f"\n   ❌ MISMATCH FOUND — vendor_ids in explanations don't match vendors.")
        print(f"   Sample unmatched: {list(unmatched)[:5]}")
        print(f"   Sample vendors:   {list(vendor_ids)[:5]}")
        print("\n   Fix: vendor_id format differs. Re-run explainability after checking.")
    else:
        print("   ✅ vendor_ids match perfectly")

    # 6. Try fetching explanation for first vendor that exists in both
    if matched:
        test_vid = list(matched)[0]
        row = db.fetch_one(
            "SELECT * FROM latest_explanations WHERE vendor_id = %s",
            (test_vid,)
        )
        print(f"\n6. Test fetch for vendor_id='{test_vid}':")
        if row:
            print(f"   ✅ Row found: driver_1='{row.get('driver_1_label')}' "
                  f"shap={row.get('driver_1_shap')}")
            print(f"   narrative: {str(row.get('narrative',''))[:80]}")
        else:
            print(f"   ❌ fetch_one returned None — view may have issue")
            # Try direct table query
            direct = db.fetch_one(
                "SELECT * FROM explanations WHERE vendor_id = %s "
                "ORDER BY run_date DESC LIMIT 1",
                (test_vid,)
            )
            print(f"   Direct table query: {'✅ found' if direct else '❌ also empty'}")

    # 7. Check if latest_explanations view definition is correct
    view_def = db.fetch_one("""
        SELECT view_definition FROM information_schema.views
        WHERE table_name = 'latest_explanations'
    """)
    if view_def:
        print(f"\n7. latest_explanations view exists: ✅")
    else:
        print(f"\n7. latest_explanations view: ❌ MISSING")
        print("   Fix: Run the schema migration to create the view.")

print("\n--- Diagnostic complete ---\n")
