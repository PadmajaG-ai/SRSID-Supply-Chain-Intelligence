"""
Vendor Comparison: Old Synthetic vs New SAP
=============================================
Compares the 225 old synthetic vendors against the 3,035 real SAP vendors
using fuzzy name matching. Tells you:

  1. Which old vendors matched a real SAP vendor  (safe to migrate)
  2. Which old vendors have no SAP match          (need manual review)
  3. Which SAP vendors are new / not in old list  (net new coverage)
  4. Overall coverage report + migration map

Outputs:
    reports/vendor_comparison_report.json
    reports/vendor_migration_map.csv        ← old name → SAP LIFNR mapping
    reports/unmatched_old_vendors.csv       ← need manual mapping
    reports/new_sap_vendors.csv             ← vendors only in SAP

Usage:
    python vendor_comparison.py
    python vendor_comparison.py --old vendors_list_simplified.csv
    python vendor_comparison.py --old vendors_list_simplified.csv --threshold 80
"""

import sys
import json
import argparse
import logging
from pathlib import Path

import pandas as pd
import numpy as np

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

REPORTS_DIR = Path("reports")

# ─────────────────────────────────────────────────────────────────────────────
# FUZZY MATCHING (no external library needed)
# ─────────────────────────────────────────────────────────────────────────────

def normalise(name: str) -> str:
    """Lowercase, strip common suffixes, remove punctuation."""
    import re
    name = str(name).lower().strip()
    # Remove common legal suffixes
    for suffix in [" inc", " ltd", " llc", " plc", " gmbh", " co.", " corp",
                   " corporation", " limited", " group", " holding", " holdings",
                   " & co", " ag", " sa", " bv", " nv", " s.a", " s.l",
                   ".", ",", "'", '"']:
        name = name.replace(suffix, "")
    # Collapse whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def token_overlap_score(a: str, b: str) -> float:
    """Score 0-100 based on word overlap between two normalised names."""
    words_a = set(normalise(a).split())
    words_b = set(normalise(b).split())
    if not words_a or not words_b:
        return 0.0
    # Jaccard similarity weighted by longer name
    intersection = words_a & words_b
    union        = words_a | words_b
    jaccard      = len(intersection) / len(union)
    # Bonus if one is a substring of the other
    na, nb = normalise(a), normalise(b)
    bonus = 0.2 if (na in nb or nb in na) else 0
    return min(100.0, round((jaccard + bonus) * 100, 2))


def levenshtein_ratio(a: str, b: str) -> float:
    """Simple edit-distance ratio (0-100)."""
    na, nb = normalise(a), normalise(b)
    if na == nb:
        return 100.0
    la, lb = len(na), len(nb)
    if la == 0 or lb == 0:
        return 0.0
    # Build DP matrix
    dp = list(range(lb + 1))
    for i in range(1, la + 1):
        prev = dp[:]
        dp[0] = i
        for j in range(1, lb + 1):
            cost = 0 if na[i-1] == nb[j-1] else 1
            dp[j] = min(dp[j] + 1, dp[j-1] + 1, prev[j-1] + cost)
    dist = dp[lb]
    return round((1 - dist / max(la, lb)) * 100, 2)


def best_match(old_name: str, sap_names: list, threshold: int) -> dict:
    """
    Find the best SAP vendor match for an old vendor name.
    Returns match info or None if below threshold.
    """
    best_score  = 0
    best_vendor = None

    na = normalise(old_name)

    for sap_name, lifnr in sap_names:
        ns = normalise(sap_name)

        # Fast exact check
        if na == ns:
            return {
                "sap_name":  sap_name,
                "lifnr":     lifnr,
                "score":     100.0,
                "method":    "exact",
            }

        # Combined score
        tok   = token_overlap_score(old_name, sap_name)
        lev   = levenshtein_ratio(old_name, sap_name)
        score = tok * 0.6 + lev * 0.4   # token overlap weighted higher

        if score > best_score:
            best_score  = score
            best_vendor = (sap_name, lifnr)

    if best_score >= threshold and best_vendor:
        return {
            "sap_name": best_vendor[0],
            "lifnr":    best_vendor[1],
            "score":    best_score,
            "method":   "fuzzy",
        }
    return None


# ─────────────────────────────────────────────────────────────────────────────
# LOAD OLD VENDORS
# ─────────────────────────────────────────────────────────────────────────────

OLD_VENDOR_CANDIDATES = [
    "vendors_list_simplified.csv",
    "data/processed/vendors_list_enriched.csv",
    "phase1_tables/raw_supplier_risk_assessment.csv",
]


def load_old_vendors(path: str | None) -> pd.DataFrame:
    """Load the old synthetic vendor list."""
    candidates = [Path(path)] if path else [Path(p) for p in OLD_VENDOR_CANDIDATES]

    for p in candidates:
        if p.exists():
            df = pd.read_csv(p)
            # Find name column
            nc = next((c for c in df.columns
                       if "vendor" in c.lower() or "supplier" in c.lower()
                       or "name" in c.lower()), df.columns[0])
            df = df.rename(columns={nc: "old_name"})
            df["old_name"] = df["old_name"].astype(str).str.strip()
            df = df[df["old_name"].notna() & (df["old_name"] != "nan")]
            log.info(f"Loaded old vendors from {p}: {len(df):,} rows")
            return df

    log.warning("Old vendor file not found. Creating a sample for demonstration.")
    # Sample of what old vendors looked like
    sample = [
        "Apple", "Tesla", "BASF", "Intel", "Samsung", "Toyota", "Siemens",
        "Pfizer", "Johnson and Johnson", "DHL Supply Chain", "FedEx",
        "Kellogg Company", "Chevron", "JPMorgan Chase", "Sanofi",
        "Rich's", "Indo Autotech Limited", "Cirque du Soleil",
        "Zentis GmbH & Co. KG", "Grainger Ltd",
    ]
    return pd.DataFrame({"old_name": sample})


# ─────────────────────────────────────────────────────────────────────────────
# LOAD SAP VENDORS
# ─────────────────────────────────────────────────────────────────────────────

# ── Default SAP dataset path (kagglehub cache on your machine) ────────────────
DEFAULT_SAP_PATH = Path(
    r"C:\Users\Reethu\.cache\kagglehub\datasets"
    r"\mustafakeser4\sap-dataset-bigquery-dataset\versions\1"
)


def load_lfa1(folder: Path) -> pd.DataFrame | None:
    """Load vendor master from LFA1.csv in the given folder."""
    lfa1_path = folder / "lfa1.csv"
    if not lfa1_path.exists():
        log.warning(f"  lfa1.csv not found in: {folder}")
        return None

    for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
        try:
            df = pd.read_csv(lfa1_path, encoding=enc, low_memory=False)
            df.columns = [c.upper().strip() for c in df.columns]

            if "LIFNR" not in df.columns:
                log.warning("  LFA1 has no LIFNR column — skipping")
                return None

            result = pd.DataFrame()
            result["lifnr"]    = df["LIFNR"].astype(str).str.strip().str.lstrip("0")
            result["sap_name"] = df["NAME1"].astype(str).str.strip() \
                                 if "NAME1" in df.columns else result["lifnr"]
            if "BRSCH" in df.columns:
                result["industry_code"] = df["BRSCH"].astype(str).str.strip()
            if "LAND1" in df.columns:
                result["country"] = df["LAND1"].astype(str).str.strip()

            # Remove deleted / blank vendors
            if "LOEVM" in df.columns:
                result = result[df["LOEVM"].astype(str).str.strip() != "X"]
            result = result[
                result["sap_name"].notna() &
                (result["sap_name"] != "nan") &
                (result["sap_name"].str.strip().str.len() > 1)
            ].reset_index(drop=True)

            log.info(f"  Loaded LFA1.csv from {folder}: {len(result):,} vendors")
            return result

        except UnicodeDecodeError:
            continue
        except Exception as e:
            log.warning(f"  Error reading LFA1.csv: {e}")
            return None

    return None


def load_sap_vendors(sap_folder: Path | None) -> pd.DataFrame:
    """
    Load real SAP vendors — priority order:
      1. LFA1.csv from the provided / default SAP path  ← always try first
      2. Postgres vendors table                          ← if DB is set up
      3. phase1_tables CSVs                             ← last resort fallback
    """

    # ── 1. LFA1.csv — try provided path first, then default ──────────────
    candidates = []
    if sap_folder:
        candidates.append(Path(sap_folder))
    candidates.append(DEFAULT_SAP_PATH)

    for folder in candidates:
        if folder.exists():
            result = load_lfa1(folder)
            if result is not None and len(result) > 100:
                return result
        else:
            log.debug(f"  SAP folder not found: {folder}")

    # ── 2. Postgres ───────────────────────────────────────────────────────
    try:
        import psycopg2
        import os
        db_url = os.environ.get(
            "DATABASE_URL",
            "postgresql://srsid_user:srsid_pass@localhost:5432/srsid_db"
        )
        conn = psycopg2.connect(db_url)
        df   = pd.read_sql(
            "SELECT vendor_id, supplier_name, industry, country FROM vendors", conn
        )
        conn.close()
        if len(df) > 100:
            log.info(f"  Loaded SAP vendors from Postgres: {len(df):,} rows")
            return df.rename(columns={"vendor_id": "lifnr",
                                      "supplier_name": "sap_name"})
    except Exception:
        pass

    # ── 3. Fallback CSVs (old phase1 outputs — only if nothing else worked) ─
    for p in [
        Path("reports/vendor_industry_map.csv"),
    ]:
        if p.exists():
            df = pd.read_csv(p)
            nc = next((c for c in df.columns
                       if "name" in c.lower() or "supplier" in c.lower()), df.columns[0])
            id_col = next((c for c in df.columns
                           if "lifnr" in c.lower() or "vendor_id" in c.lower()), None)
            result = pd.DataFrame()
            result["sap_name"] = df[nc].astype(str).str.strip()
            result["lifnr"]    = df[id_col].astype(str) if id_col \
                                 else result.index.astype(str)
            if "industry" in df.columns:
                result["industry"] = df["industry"]
            if "country" in df.columns:
                result["country"] = df["country"]
            if len(result) > 100:
                log.info(f"  Loaded SAP vendors from {p}: {len(result):,} rows")
                return result

    log.error("Could not load real SAP vendor data.")
    log.error(f"Expected LFA1.csv at: {DEFAULT_SAP_PATH}")
    log.error("Check the path exists or run: python sap_download_and_explore.py")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# COMPARISON ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def compare(old_df: pd.DataFrame, sap_df: pd.DataFrame,
            threshold: int) -> dict:
    """
    Run fuzzy matching of every old vendor against all SAP vendors.
    Returns structured results dict.
    """
    log.info(f"\nMatching {len(old_df)} old vendors against "
             f"{len(sap_df)} SAP vendors (threshold={threshold})...")

    # Build lookup list — (sap_name, lifnr)
    sap_list = list(zip(
        sap_df["sap_name"].astype(str).tolist(),
        sap_df["lifnr"].astype(str).tolist(),
    ))

    matched   = []
    unmatched = []

    for _, row in old_df.iterrows():
        old_name = str(row["old_name"])
        result   = best_match(old_name, sap_list, threshold)

        if result:
            matched.append({
                "old_name":       old_name,
                "sap_name":       result["sap_name"],
                "lifnr":          result["lifnr"],
                "match_score":    result["score"],
                "match_method":   result["method"],
                "confidence":     "High"   if result["score"] >= 85 else
                                  "Medium" if result["score"] >= threshold else
                                  "Low",
            })
        else:
            unmatched.append({
                "old_name": old_name,
                "action":   "Manual mapping needed — not found in SAP",
            })

    # SAP vendors not in old list
    matched_sap_names = {normalise(m["sap_name"]) for m in matched}
    new_sap = sap_df[
        ~sap_df["sap_name"].apply(normalise).isin(matched_sap_names)
    ].copy()

    return {
        "matched":    pd.DataFrame(matched),
        "unmatched":  pd.DataFrame(unmatched),
        "new_in_sap": new_sap,
    }


# ─────────────────────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────────────────────

def print_report(results: dict, old_count: int, sap_count: int, threshold: int):
    matched   = results["matched"]
    unmatched = results["unmatched"]
    new_sap   = results["new_in_sap"]

    high_conf   = len(matched[matched["confidence"] == "High"])   if not matched.empty else 0
    medium_conf = len(matched[matched["confidence"] == "Medium"]) if not matched.empty else 0

    print()
    print("=" * 70)
    print("  VENDOR COMPARISON REPORT")
    print("=" * 70)
    print(f"\n  Old vendor list    : {old_count:>6,} vendors (synthetic)")
    print(f"  SAP vendor list    : {sap_count:>6,} vendors (real LFA1)")
    print(f"  Fuzzy threshold    : {threshold}%")
    print()
    print(f"  ✅ Matched         : {len(matched):>6,}  "
          f"({len(matched)/old_count*100:.1f}% of old list)")
    print(f"     High confidence : {high_conf:>6,}  (score ≥ 85%)")
    print(f"     Medium conf.    : {medium_conf:>6,}  (score {threshold}–84%)")
    print(f"  ❌ Unmatched       : {len(unmatched):>6,}  "
          f"(not found in SAP — need manual mapping)")
    print(f"  🆕 New in SAP      : {len(new_sap):>6,}  "
          f"(real vendors not in old list)")
    print()

    if not matched.empty:
        print("  TOP MATCHED VENDORS (highest confidence):")
        top = matched.sort_values("match_score", ascending=False).head(10)
        for _, r in top.iterrows():
            print(f"    {r['old_name']:<35} → {r['sap_name']:<35} "
                  f"({r['match_score']:.0f}%)")
        print()

    if not unmatched.empty:
        print("  UNMATCHED OLD VENDORS (need manual review):")
        for _, r in unmatched.head(15).iterrows():
            print(f"    ✗ {r['old_name']}")
        if len(unmatched) > 15:
            print(f"    ... and {len(unmatched)-15} more (see unmatched_old_vendors.csv)")
        print()

    if not new_sap.empty:
        print(f"  SAMPLE OF NEW SAP VENDORS (first 10 of {len(new_sap):,}):")
        for _, r in new_sap.head(10).iterrows():
            country = r.get("country", "")
            ind     = r.get("industry", r.get("industry_code", ""))
            print(f"    + {r['sap_name']:<40} {country:<5} {ind}")
        print()

    # Coverage insight
    coverage = len(matched) / old_count * 100
    if coverage >= 80:
        verdict = "✅ Excellent — most old vendors found in SAP. Safe to migrate."
    elif coverage >= 50:
        verdict = "🟡 Good — majority found. Review unmatched before proceeding."
    else:
        verdict = "⚠️  Low overlap — old and SAP vendor bases are quite different."
    print(f"  VERDICT: {verdict}")
    print()


def save_outputs(results: dict, output_dir: Path) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {}

    if not results["matched"].empty:
        p = output_dir / "vendor_migration_map.csv"
        results["matched"].to_csv(p, index=False, encoding="utf-8-sig")
        paths["migration_map"] = str(p)
        log.info(f"  💾 Saved migration map   : {p}")

    if not results["unmatched"].empty:
        p = output_dir / "unmatched_old_vendors.csv"
        results["unmatched"].to_csv(p, index=False, encoding="utf-8-sig")
        paths["unmatched"] = str(p)
        log.info(f"  💾 Saved unmatched       : {p}")

    if not results["new_in_sap"].empty:
        p = output_dir / "new_sap_vendors.csv"
        results["new_in_sap"].to_csv(p, index=False, encoding="utf-8-sig")
        paths["new_in_sap"] = str(p)
        log.info(f"  💾 Saved new SAP vendors : {p}")

    return paths


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Compare old synthetic vendors vs real SAP vendors")
    parser.add_argument("--old",       type=str, default=None,
                        help="Path to old vendor CSV (default: auto-detect)")
    parser.add_argument("--sap-path",  type=str, default=None,
                        help=f"SAP dataset folder with lfa1.csv "
                             f"(default: {DEFAULT_SAP_PATH})")
    parser.add_argument("--threshold", type=int, default=75,
                        help="Fuzzy match threshold 0-100 (default: 75)")
    parser.add_argument("--output",    type=str, default="reports",
                        help="Output folder (default: reports/)")
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║   SRSID Vendor Comparison — Old Synthetic vs Real SAP           ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print()
    print(f"  SAP path : {args.sap_path or DEFAULT_SAP_PATH}")
    print(f"  Threshold: {args.threshold}%")
    print()

    old_df = load_old_vendors(args.old)
    sap_df = load_sap_vendors(Path(args.sap_path) if args.sap_path else None)

    # Remove blanks
    old_df = old_df[old_df["old_name"].str.strip().str.len() > 1]
    sap_df = sap_df[sap_df["sap_name"].str.strip().str.len() > 1]

    log.info(f"Old vendors: {len(old_df):,}  |  SAP vendors: {len(sap_df):,}")

    if len(sap_df) < 500:
        log.warning(f"Only {len(sap_df)} SAP vendors loaded — expected ~3,035.")
        log.warning(f"Check that lfa1.csv exists at: {DEFAULT_SAP_PATH}")

    results = compare(old_df, sap_df, args.threshold)

    print_report(results, len(old_df), len(sap_df), args.threshold)

    output_dir = Path(args.output)
    paths = save_outputs(results, output_dir)

    # Save JSON summary
    summary = {
        "old_vendor_count":  len(old_df),
        "sap_vendor_count":  len(sap_df),
        "threshold":         args.threshold,
        "matched_count":     len(results["matched"]),
        "unmatched_count":   len(results["unmatched"]),
        "new_in_sap_count":  len(results["new_in_sap"]),
        "coverage_pct":      round(len(results["matched"]) / len(old_df) * 100, 1),
        "high_confidence":   int(
            (results["matched"]["confidence"] == "High").sum()
        ) if not results["matched"].empty else 0,
        "output_files":      paths,
    }
    rpt = output_dir / "vendor_comparison_report.json"
    with open(rpt, "w") as f:
        json.dump(summary, f, indent=2)
    log.info(f"  📄 Summary report: {rpt}")

    print(f"  All outputs saved to: {output_dir.resolve()}/")
    print()
    print("  NEXT STEPS:")
    print("    1. Review vendor_migration_map.csv — check high-confidence matches")
    print("    2. Review unmatched_old_vendors.csv — these are purely synthetic, discard")
    print("    3. new_sap_vendors.csv — these are your real vendor universe going forward")
    print("    4. Set up Postgres and run sap_phase1_rebuild.py")
    print()


if __name__ == "__main__":
    main()
