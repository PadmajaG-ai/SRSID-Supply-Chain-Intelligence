"""
SAP Dataset Downloader + Explorer for SRSID
=============================================
Single script — downloads the SAP dataset via kagglehub,
then explores every table and maps it to SRSID data gaps.

Usage:
    python sap_download_and_explore.py

Optional flags:
    --no-download   Skip download, use cached version
    --path PATH     Use a local folder instead of kagglehub
    --output DIR    Where to save output files (default: current dir)

Requirements:
    pip install kagglehub pandas numpy

Kaggle credentials needed (~/.kaggle/kaggle.json OR env vars):
    KAGGLE_USERNAME=your_username
    KAGGLE_KEY=your_api_key
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
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("sap_explorer.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — DOWNLOAD VIA KAGGLEHUB
# ─────────────────────────────────────────────────────────────────────────────

KAGGLE_DATASET = "mustafakeser4/sap-dataset-bigquery-dataset"


def download_dataset(skip: bool = False) -> Path:
    """
    Download dataset via kagglehub.
    Returns the local path where files were saved.
    If skip=True, just returns the cached path without re-downloading.
    """
    try:
        import kagglehub
    except ImportError:
        log.error("kagglehub not installed. Run: pip install kagglehub")
        sys.exit(1)

    if skip:
        log.info("Skipping download — using cached version...")
    else:
        log.info(f"Downloading dataset: {KAGGLE_DATASET}")
        log.info("(Cached after first run — subsequent runs are instant)")

    try:
        path = kagglehub.dataset_download(KAGGLE_DATASET)
        dataset_path = Path(path)
        log.info(f"✅ Dataset ready at: {dataset_path}")
        return dataset_path
    except Exception as e:
        error_msg = str(e).lower()
        if "credential" in error_msg or "401" in error_msg or "forbidden" in error_msg:
            log.error("Kaggle authentication failed.")
            log.error("Set up credentials in one of these ways:")
            log.error("  Option 1: Create ~/.kaggle/kaggle.json with your API key")
            log.error("            {\"username\":\"your_user\",\"key\":\"your_key\"}")
            log.error("  Option 2: Set environment variables:")
            log.error("            KAGGLE_USERNAME=your_user  KAGGLE_KEY=your_key")
            log.error("  Get your key from: https://www.kaggle.com/settings → API")
        else:
            log.error(f"Download failed: {e}")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# SAP TABLE KNOWLEDGE BASE
# Maps SAP table names → what they contain → which SRSID gap they fill
# ─────────────────────────────────────────────────────────────────────────────

SAP_TABLE_MAP = {
    # ── Procurement / Purchase Orders ─────────────────────────────────────────
    "ekko": {
        "name":      "Purchase Order Header",
        "key_cols":  ["EBELN", "LIFNR", "BEDAT", "BUKRS", "EKORG", "BSTYP", "KDATB", "KDATE"],
        "srsid_gap": "TRANSACTIONS WITH DATES + CONTRACTS — vendor, PO date, contract dates",
        "priority":  1,
        "unlocks":   "Q31–45 (quarterly spend), Q44 (no-contract), Q76–77 (contract renewals)",
    },
    "ekpo": {
        "name":      "Purchase Order Line Items",
        "key_cols":  ["EBELN", "EBELP", "MATNR", "MENGE", "NETPR", "EINDT", "WERKS", "LOEKZ"],
        "srsid_gap": "SPEND DETAIL + DELIVERY PROMISES — material, quantity, price, promised date",
        "priority":  1,
        "unlocks":   "Q31–45 (exact spend), Q46–58 (OTIF, delivery delays)",
    },
    "ekbe": {
        "name":      "PO History (Goods Receipts + Invoices)",
        "key_cols":  ["EBELN", "EBELP", "VGABE", "BLDAT", "BUDAT", "MENGE", "DMBTR", "BELNR"],
        "srsid_gap": "ACTUAL DELIVERY DATES — when goods were actually received",
        "priority":  1,
        "unlocks":   "Q21 (delays 30d), Q47 (OTIF), Q57 (promised vs actual = delay days)",
    },
    "eket": {
        "name":      "PO Delivery Schedule Lines",
        "key_cols":  ["EBELN", "EBELP", "EINDT", "MENGE", "WEMNG", "GLMNG"],
        "srsid_gap": "DELIVERY SCHEDULE — confirmed delivery dates per PO line",
        "priority":  1,
        "unlocks":   "Q46–47 (OTIF), Q56–57 (delivery variance)",
    },
    "mseg": {
        "name":      "Material Document Segments (Goods Movement)",
        "key_cols":  ["MBLNR", "ZEILE", "EBELN", "EBELP", "MATNR", "MENGE", "WERKS", "BLDAT"],
        "srsid_gap": "ACTUAL GOODS RECEIPT — real arrival date for OTIF calculation",
        "priority":  1,
        "unlocks":   "Q21 (delay 30d), Q46 (OTIF failure), Q55 (stock-out risk)",
    },
    # ── Vendor Master ─────────────────────────────────────────────────────────
    "lfa1": {
        "name":      "Vendor Master — General Data",
        "key_cols":  ["LIFNR", "NAME1", "LAND1", "ORT01", "BRSCH", "KTOKK", "LOEVM"],
        "srsid_gap": "VENDOR COUNTRY + INDUSTRY CODE — fills the industry classification gap",
        "priority":  1,
        "unlocks":   "Q3 (category risk), Q7 (APAC region), Q34 (logistics trend), Q65 (comparison)",
    },
    "lfm1": {
        "name":      "Vendor Master — Purchasing Org Data",
        "key_cols":  ["LIFNR", "EKORG", "MINBW", "KZGRS", "INCO1", "INCO2"],
        "srsid_gap": "SINGLE SOURCE + INCOTERMS — sole-source flag per purchasing org",
        "priority":  2,
        "unlocks":   "Q27 (single/multi source), Q89 (single-source high-risk spend)",
    },
    "lfb1": {
        "name":      "Vendor Master — Company Code Data",
        "key_cols":  ["LIFNR", "BUKRS", "AKONT", "ZTERM", "REPRF", "ZWELS"],
        "srsid_gap": "PAYMENT TERMS — payment conditions and credit terms per vendor",
        "priority":  2,
        "unlocks":   "Q29 (SLA/contract terms), Q35 (over-budget detection)",
    },
    # ── Invoice / Finance ─────────────────────────────────────────────────────
    "rbkp": {
        "name":      "Invoice Document Header",
        "key_cols":  ["BELNR", "BUKRS", "BLDAT", "BUDAT", "LIFNR", "RMWWR", "XBLNR"],
        "srsid_gap": "INVOICE DATES + AMOUNTS — actual invoiced dates for exact spend timeline",
        "priority":  2,
        "unlocks":   "Q31–45 (invoice-based spend), Q42 (maverick spend detection)",
    },
    "rseg": {
        "name":      "Invoice Document Line Items",
        "key_cols":  ["BELNR", "BUZEI", "EBELN", "EBELP", "MATNR", "MENGE", "WRBTR"],
        "srsid_gap": "ACTUAL INVOICE AMOUNTS — invoiced values per PO line",
        "priority":  2,
        "unlocks":   "Q31 (accurate quarterly spend), Q43 (saving from switching)",
    },
    # ── Material Master ───────────────────────────────────────────────────────
    "mara": {
        "name":      "Material Master — General Data",
        "key_cols":  ["MATNR", "MTART", "MATKL", "MEINS", "BRGEW", "TRAGR"],
        "srsid_gap": "MATERIAL CATEGORY — product category each supplier provides",
        "priority":  2,
        "unlocks":   "Q3 (category risk concentration), Q50 (semiconductor suppliers)",
    },
    "makt": {
        "name":      "Material Descriptions",
        "key_cols":  ["MATNR", "SPRAS", "MAKTX"],
        "srsid_gap": "PRODUCT NAMES — human-readable names for chatbot responses",
        "priority":  3,
        "unlocks":   "Q55 (components at risk), Q66 (reliable supplier for part X)",
    },
    # ── Vendor Evaluation ─────────────────────────────────────────────────────
    "elbk": {
        "name":      "Vendor Evaluation — Header",
        "key_cols":  ["LIFNR", "EKORG", "GHPKT", "QPKT", "LPKT", "PPKT", "SPKT"],
        "srsid_gap": "VENDOR SCORES — SAP quality/delivery/price/service evaluation",
        "priority":  2,
        "unlocks":   "Q52 (best quality despite risk), Q75 (most improved), Q53 (improving delivery)",
    },
    # ── Purchase Requisitions ─────────────────────────────────────────────────
    "eban": {
        "name":      "Purchase Requisitions",
        "key_cols":  ["BANFN", "BNFPO", "MATNR", "MENGE", "PREIS", "AFNAM", "BSART"],
        "srsid_gap": "DEMAND ORIGIN — which department/user requested the purchase",
        "priority":  3,
        "unlocks":   "Q41 (departments spending with high-risk vendors)",
    },
    # ── Org Structure ─────────────────────────────────────────────────────────
    "t001w": {
        "name":      "Plant / Business Unit Master",
        "key_cols":  ["WERKS", "NAME1", "LAND1", "REGIO", "VKORG"],
        "srsid_gap": "BUSINESS UNIT MAPPING — which plant ordered from which vendor",
        "priority":  3,
        "unlocks":   "Q10 (business units exposed to high-risk suppliers)",
    },
    "t024e": {
        "name":      "Purchasing Organisations",
        "key_cols":  ["EKORG", "EKNAM"],
        "srsid_gap": "PURCHASING ORG NAMES — decode EKORG codes to readable names",
        "priority":  3,
        "unlocks":   "Context for all spend and contract queries",
    },
}

SRSID_GAPS = {
    1: "Transactions with real dates  → quarterly spend reports",
    2: "Delivery performance / OTIF   → delay tracking, OTIF failure list",
    3: "Vendor industry classification → category-level risk analysis",
    4: "Contract & renewal dates      → renewal calendar, no-contract alerts",
    5: "Vendor evaluation scores      → performance improvement tracking",
    6: "Department / business unit    → department-level risk exposure",
}


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — FILE SCANNING
# ─────────────────────────────────────────────────────────────────────────────

def scan_files(base_path: Path) -> dict:
    """Return {normalised_stem: Path} for every data file found."""
    files = {}
    for pattern in ["*.csv", "*.CSV", "*.json", "*.JSON", "*.parquet", "*.xlsx"]:
        for f in base_path.rglob(pattern):
            key = f.stem.lower().replace("-", "_").replace(" ", "_").strip()
            files[key] = f
    return files


def safe_read(path: Path, nrows: int = 10) -> pd.DataFrame:
    """Read a sample of any supported file format."""
    try:
        suffix = path.suffix.lower()
        if suffix == ".parquet":
            return pd.read_parquet(path).head(nrows)
        elif suffix == ".json":
            return pd.read_json(path).head(nrows)
        elif suffix in (".xlsx", ".xls"):
            return pd.read_excel(path, nrows=nrows)
        else:
            for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
                try:
                    return pd.read_csv(path, nrows=nrows, encoding=enc,
                                       low_memory=False, on_bad_lines="skip")
                except UnicodeDecodeError:
                    continue
    except Exception as e:
        return pd.DataFrame({"_error": [str(e)]})
    return pd.DataFrame()


def count_rows(path: Path) -> int:
    """Count rows without loading the whole file."""
    try:
        suffix = path.suffix.lower()
        if suffix == ".parquet":
            return len(pd.read_parquet(path))
        elif suffix in (".xlsx", ".xls"):
            return len(pd.read_excel(path))
        elif suffix == ".json":
            return len(pd.read_json(path))
        else:
            for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
                try:
                    with open(path, encoding=enc) as f:
                        return max(0, sum(1 for _ in f) - 1)
                except UnicodeDecodeError:
                    continue
    except Exception:
        pass
    return -1


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — COLUMN ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

DATE_HINTS   = {"date", "dat", "datum", "bldat", "budat", "eindt", "kdatb",
                "kdate", "bedat", "erdat", "aedat", "laeda"}
VENDOR_HINTS = {"lifnr", "vendor", "supplier", "name1", "vend", "lief"}
AMOUNT_HINTS = {"wrbtr", "dmbtr", "netpr", "rmwwr", "effwr", "menge", "amount",
                "value", "price", "cost", "spend", "betrag"}


def find_cols(df: pd.DataFrame, hints: set) -> list:
    return [c for c in df.columns if any(h in c.lower() for h in hints)]


def sniff_date_values(df: pd.DataFrame) -> list:
    """Find columns whose values parse as dates even if the name doesn't hint."""
    extra = []
    for col in df.columns:
        if col in find_cols(df, DATE_HINTS):
            continue
        sample = df[col].dropna().astype(str).head(3).tolist()
        for val in sample:
            try:
                pd.to_datetime(val)
                extra.append(col)
                break
            except Exception:
                pass
    return extra


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — TABLE MATCHING
# ─────────────────────────────────────────────────────────────────────────────

def match_table(file_key: str, df: pd.DataFrame) -> dict | None:
    """Match a file to the SAP knowledge base by name or column overlap."""
    # 1. Direct name match
    for sap_key, info in SAP_TABLE_MAP.items():
        if sap_key == file_key or file_key.startswith(sap_key) or file_key.endswith(sap_key):
            return {"sap_key": sap_key, **info}

    # 2. Column overlap match (need ≥2 SAP key columns present)
    cols_upper = {c.upper() for c in df.columns}
    best, best_score = None, 1
    for sap_key, info in SAP_TABLE_MAP.items():
        score = sum(1 for c in info["key_cols"] if c in cols_upper)
        if score > best_score:
            best_score, best = score, {"sap_key": sap_key, **info, "_col_match_score": score}
    return best


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — GAP COVERAGE SCORING
# ─────────────────────────────────────────────────────────────────────────────

GAP_KEYWORDS = {
    1: ["transaction", "date", "bedat", "bldat", "budat", "purchase order"],
    2: ["delivery", "otif", "goods receipt", "eindt", "ekbe", "mseg", "eket"],
    3: ["industry", "brsch", "vendor master", "lfa1", "category", "matkl"],
    4: ["contract", "kdatb", "kdate", "bstyp", "renewal"],
    5: ["evaluation", "score", "elbk", "qpkt", "lpkt", "ghpkt"],
    6: ["plant", "business unit", "werks", "department", "eban", "afnam"],
}


def score_gaps(matched: list) -> set:
    covered = set()
    for r in matched:
        text = (r["sap_info"]["srsid_gap"] + " " +
                r["sap_info"]["name"] + " " +
                r["sap_info"]["sap_key"]).lower()
        for gap_id, keywords in GAP_KEYWORDS.items():
            if any(k in text for k in keywords):
                covered.add(gap_id)
    return covered


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — REPORT PRINTING
# ─────────────────────────────────────────────────────────────────────────────

PRIORITY_ICON = {1: "🔴", 2: "🟡", 3: "🟢"}
DIV = "─" * 70


def print_report(matched: list, unmatched: list, output_dir: Path):
    print()
    print("=" * 70)
    print("  SRSID × SAP DATASET  —  EXPLORATION REPORT")
    print("=" * 70)

    # ── Matched tables ─────────────────────────────────────────────────────
    if matched:
        print(f"\n  ✅  {len(matched)} SAP TABLE(S) IDENTIFIED\n  {DIV}")
        matched_sorted = sorted(matched, key=lambda x: x["sap_info"].get("priority", 9))

        for r in matched_sorted:
            si    = r["sap_info"]
            icon  = PRIORITY_ICON.get(si.get("priority", 3), "⚪")
            extra = f"  (column match score: {si.get('_col_match_score','name')})" \
                    if "_col_match_score" in si else ""

            print(f"\n  {icon}  {r['filename']}{extra}")
            print(f"      SAP table  : {si['name']}  ({si['sap_key'].upper()})")
            print(f"      Size       : {r['rows']:>8,} rows  ×  {r['cols']} columns")
            print(f"      Fills gap  : {si['srsid_gap']}")
            print(f"      Unlocks    : {si['unlocks']}")

            if r["date_cols"]:
                print(f"      📅 Date cols   : {', '.join(r['date_cols'][:6])}")
            if r["vendor_cols"]:
                print(f"      🏭 Vendor cols : {', '.join(r['vendor_cols'][:4])}")
            if r["amt_cols"]:
                print(f"      💰 Amount cols : {', '.join(r['amt_cols'][:5])}")

            all_cols = r["columns"]
            preview  = ", ".join(all_cols[:14]) + ("  ..." if len(all_cols) > 14 else "")
            print(f"      All columns: {preview}")

    # ── Unmatched files ────────────────────────────────────────────────────
    if unmatched:
        print(f"\n  ❓  {len(unmatched)} UNRECOGNISED FILE(S) — review manually\n  {DIV}")
        for r in unmatched:
            print(f"\n  {r['filename']}")
            print(f"    Rows × Cols : {r['rows']:,} × {r['cols']}")
            col_preview = ", ".join(r["columns"][:12]) + \
                          ("  ..." if len(r["columns"]) > 12 else "")
            print(f"    Columns     : {col_preview}")
            if r["vendor_cols"]:
                print(f"    Possible vendor cols : {r['vendor_cols']}")
            if r["date_cols"]:
                print(f"    Possible date cols   : {r['date_cols']}")

    # ── Gap coverage ───────────────────────────────────────────────────────
    covered = score_gaps(matched)
    print(f"\n  📊  SRSID GAP COVERAGE AFTER THIS DATASET\n  {DIV}")
    for gap_id, label in SRSID_GAPS.items():
        tick = "✅" if gap_id in covered else "❌"
        note = "" if gap_id in covered else "  ← still missing"
        print(f"  {tick}  Gap {gap_id}: {label}{note}")

    questions_unlocked = sum(
        1 for r in matched
        for _ in [r["sap_info"].get("unlocks", "")]
        if r["sap_info"].get("unlocks")
    )

    # ── Next steps ─────────────────────────────────────────────────────────
    print(f"\n  🚀  NEXT STEPS\n  {DIV}")
    p1 = [r for r in matched if r["sap_info"].get("priority") == 1]
    if p1:
        print("\n  Priority 1 tables — pass these to the ingestion script:")
        for r in p1:
            print(f"    • {r['filename']:<40} ({r['rows']:,} rows)")
            print(f"      → {r['sap_info']['srsid_gap']}")

    print()
    print("  Run next:")
    print("    python sap_ingestion.py          # builds SRSID-ready CSVs")
    print("    python run_phase3.py             # re-runs ML pipeline")
    print("    streamlit run phase3_chatbot.py  # launch chatbot")
    print()

    # ── Save JSON report ───────────────────────────────────────────────────
    report = {
        "matched_tables": [r["filename"] for r in matched],
        "unmatched_files": [r["filename"] for r in unmatched],
        "gaps_covered": sorted(covered),
        "total_matched": len(matched),
        "total_unmatched": len(unmatched),
        "file_details": {
            r["filename"]: {
                "rows":        r["rows"],
                "cols":        r["cols"],
                "columns":     r["columns"],
                "date_cols":   r["date_cols"],
                "vendor_cols": r["vendor_cols"],
                "amt_cols":    r["amt_cols"],
                "sap_table":   r["sap_info"]["sap_key"] if r["sap_info"] else None,
                "sap_name":    r["sap_info"]["name"]    if r["sap_info"] else None,
                "srsid_gap":   r["sap_info"]["srsid_gap"] if r["sap_info"] else None,
                "priority":    r["sap_info"].get("priority") if r["sap_info"] else None,
                "path":        str(r["path"]),
            }
            for r in matched + unmatched
        },
    }

    out_path = output_dir / "sap_exploration_report.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2)

    log.info(f"Report saved → {out_path}")
    log.info("(sap_ingestion.py will read this automatically)")
    return report


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def explore(dataset_path: Path, output_dir: Path) -> dict:
    """Scan the dataset folder and return the full analysis report."""
    log.info(f"Scanning: {dataset_path}")

    files = scan_files(dataset_path)
    if not files:
        log.error(f"No data files found in: {dataset_path}")
        log.error("Check the path is correct and the download completed.")
        sys.exit(1)

    log.info(f"Found {len(files)} file(s) — analysing...")

    matched, unmatched = [], []

    for file_key, path in sorted(files.items()):
        df = safe_read(path, nrows=15)
        if df.empty or "_error" in df.columns:
            log.warning(f"Could not read: {path.name}")
            continue

        rows        = count_rows(path)
        sap_info    = match_table(file_key, df)
        date_cols   = find_cols(df, DATE_HINTS) + sniff_date_values(df)
        vendor_cols = find_cols(df, VENDOR_HINTS)
        amt_cols    = find_cols(df, AMOUNT_HINTS)

        record = {
            "file_key":    file_key,
            "filename":    path.name,
            "path":        path,
            "rows":        rows,
            "cols":        len(df.columns),
            "columns":     df.columns.tolist(),
            "date_cols":   list(set(date_cols)),
            "vendor_cols": vendor_cols,
            "amt_cols":    amt_cols,
            "sap_info":    sap_info,
            "sample":      df,
        }

        if sap_info:
            matched.append(record)
            log.info(f"  ✅ {path.name} → {sap_info['name']} ({sap_info['sap_key'].upper()})")
        else:
            unmatched.append(record)
            log.info(f"  ❓ {path.name} — not matched to known SAP table")

    output_dir.mkdir(parents=True, exist_ok=True)
    return print_report(matched, unmatched, output_dir)


def main():
    parser = argparse.ArgumentParser(
        description="Download SAP dataset via kagglehub and map to SRSID gaps")
    parser.add_argument(
        "--no-download", action="store_true",
        help="Skip download, use kagglehub cache")
    parser.add_argument(
        "--path", type=str, default=None,
        help="Use a specific local folder instead of kagglehub")
    parser.add_argument(
        "--output", type=str, default=".",
        help="Directory to save report files (default: current dir)")
    args = parser.parse_args()

    output_dir = Path(args.output)

    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║      SRSID SAP Dataset Downloader + Explorer                    ║")
    print("║      Downloads via kagglehub · Maps tables to data gaps         ║")
    print("╚══════════════════════════════════════════════════════════════════╝")

    # ── Download or locate ─────────────────────────────────────────────────
    if args.path:
        dataset_path = Path(args.path)
        log.info(f"Using provided path: {dataset_path}")
        if not dataset_path.exists():
            log.error(f"Path does not exist: {dataset_path}")
            sys.exit(1)
    else:
        dataset_path = download_dataset(skip=args.no_download)

    # ── Find deepest folder with actual data files ─────────────────────────
    # kagglehub sometimes nests files in version subfolders
    actual_path = dataset_path
    for sub in sorted(dataset_path.rglob("*")):
        if sub.is_dir() and any(sub.glob("*.csv")) or any(sub.glob("*.json")):
            actual_path = sub
            break

    log.info(f"Data files found at: {actual_path}")

    # ── Explore ────────────────────────────────────────────────────────────
    explore(actual_path, output_dir)


if __name__ == "__main__":
    main()
