"""
SRSID SAP Loader  —  ingestion/sap_loader.py
=============================================
Reads the SAP BigQuery dataset (downloaded via kagglehub) and loads
it into the Postgres tables defined in db/schema.sql.

Replaces:  sap_phase1_rebuild.py + sap_ingestion.py
Writes to: vendors, transactions, delivery_events, contracts

Run:
    python ingestion/sap_loader.py                  # full load
    python ingestion/sap_loader.py --table vendors  # single table
    python ingestion/sap_loader.py --dry-run        # validate only

Requires:
    pip install psycopg2-binary pandas numpy kagglehub
"""

import sys
import logging
import argparse
from pathlib import Path

import pandas as pd
import numpy as np

# ── project imports ───────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    SAP_PATH, KAGGLE_DATASET,
    GEO_RISK, GEO_RISK_SCORES,
    INDUSTRY_RISK, RISK_THRESHOLDS,
    DELIVERY_CONFIG,
)
from db.db_client import DBClient

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/sap_loader.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# SAP REFERENCE DATA
# ─────────────────────────────────────────────────────────────────────────────

INDUSTRY_MAP = {
    "A": ("Agriculture",               "Agriculture"),
    "B": ("Mining",                    "Mining & Resources"),
    "C": ("Manufacturing",             "Manufacturing"),
    "D": ("Energy & Utilities",        "Energy"),
    "F": ("Construction",              "Construction"),
    "G": ("Wholesale & Retail",        "Wholesale & Retail"),
    "H": ("Logistics & Transport",     "Logistics"),
    "J": ("IT & Telecommunications",  "IT Services"),
    "K": ("Financial Services",        "Financial Services"),
    "M": ("Professional Services",     "Consulting"),
    "Q": ("Healthcare & Pharma",       "Pharma & Life Sciences"),
    "V": ("Automotive",                "Automotive"),
    "W": ("Chemicals",                 "Chemicals"),
    "X": ("Electronics & Semiconductors", "Electronics & Semiconductors"),
    "Y": ("Pharma & Life Sciences",    "Pharma & Life Sciences"),
    "Z": ("Food & Beverage",           "Food & Beverage"),
    "14":("Chemicals",                 "Chemicals"),
    "15":("Food & Beverage",           "Food & Beverage"),
    "24":("Metals & Steel",            "Manufacturing"),
    "25":("Electronics",               "Electronics & Semiconductors"),
    "26":("Semiconductors",            "Electronics & Semiconductors"),
    "29":("Automotive",                "Automotive"),
    "46":("Wholesale",                 "Wholesale & Retail"),
    "51":("Logistics",                 "Logistics"),
    "60":("IT Services",               "IT Services"),
    "72":("Consulting",                "Consulting"),
    "85":("Healthcare",                "Pharma & Life Sciences"),
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def locate_sap_data() -> Path:
    """Find SAP dataset — config path first, then kagglehub download."""
    if SAP_PATH.exists() and list(SAP_PATH.glob("*.csv")):
        log.info(f"SAP data found at: {SAP_PATH}")
        return SAP_PATH

    log.info("SAP path not found — downloading via kagglehub...")
    try:
        import kagglehub
        path = Path(kagglehub.dataset_download(KAGGLE_DATASET))
        for sub in sorted(path.rglob("*"), reverse=True):
            if sub.is_dir() and list(sub.glob("*.csv")):
                log.info(f"Downloaded to: {sub}")
                return sub
        return path
    except Exception as e:
        log.error(f"kagglehub download failed: {e}")
        sys.exit(1)


def read_sap(folder: Path, filename: str,
             cols: list = None) -> pd.DataFrame:
    """Read a SAP CSV with encoding fallback. Returns empty DF if missing."""
    for name in [filename, filename.upper(), filename.lower()]:
        p = folder / name
        if p.exists():
            for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
                try:
                    df = pd.read_csv(p, encoding=enc, low_memory=False,
                                     on_bad_lines="skip")
                    df.columns = [c.upper().strip() for c in df.columns]
                    if cols:
                        df = df[[c for c in cols if c in df.columns]]
                    log.info(f"  Read {name}: {len(df):,} rows × {len(df.columns)} cols")
                    return df
                except UnicodeDecodeError:
                    continue
    log.warning(f"  {filename} not found in {folder}")
    return pd.DataFrame()


def parse_dates(df: pd.DataFrame, *cols) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=False)
    return df


def normalise_lifnr(series: pd.Series) -> pd.Series:
    """Strip leading zeros from SAP vendor IDs."""
    return series.astype(str).str.strip().str.lstrip("0")


def pct_score(series: pd.Series, invert: bool = False) -> pd.Series:
    """Normalise to 0–100 scale."""
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series(50.0, index=series.index)
    norm = (series - mn) / (mx - mn)
    return ((1 - norm if invert else norm) * 100).round(2)


def geo_risk_score(country_code: str) -> float:
    for tier, codes in GEO_RISK.items():
        if country_code in codes:
            return GEO_RISK_SCORES[tier]
    return GEO_RISK_SCORES["Medium"]


# ─────────────────────────────────────────────────────────────────────────────
# TABLE 1 — VENDORS
# Source: LFA1
# ─────────────────────────────────────────────────────────────────────────────

def load_vendors(folder: Path, db: DBClient, dry_run: bool) -> int:
    log.info("\n" + "="*60)
    log.info("LOADING: vendors")
    log.info("="*60)

    lfa1 = read_sap(folder, "lfa1.csv")
    if lfa1.empty:
        log.error("LFA1.csv required — cannot load vendors")
        return 0

    lfa1["LIFNR"] = normalise_lifnr(lfa1["LIFNR"])

    # Remove deleted vendors
    if "LOEVM" in lfa1.columns:
        before = len(lfa1)
        lfa1 = lfa1[lfa1["LOEVM"].astype(str).str.strip() != "X"]
        log.info(f"  Removed {before - len(lfa1)} deleted vendors (LOEVM=X)")

    v = pd.DataFrame()
    v["vendor_id"]       = lfa1["LIFNR"]
    v["supplier_name"]   = lfa1["NAME1"].astype(str).str.strip() \
                           if "NAME1" in lfa1.columns else lfa1["LIFNR"]
    v["country_code"]    = lfa1["LAND1"].astype(str).str.strip() \
                           if "LAND1" in lfa1.columns else None
    v["city"]            = lfa1["ORT01"].astype(str).str.strip() \
                           if "ORT01" in lfa1.columns else None

    # Decode industry
    if "BRSCH" in lfa1.columns:
        v["industry_code"] = lfa1["BRSCH"].astype(str).str.strip()
        v["industry"]      = v["industry_code"].map(
            {k: vv[0] for k, vv in INDUSTRY_MAP.items()}
        ).fillna("Other")
        v["industry_category"] = v["industry_code"].map(
            {k: vv[1] for k, vv in INDUSTRY_MAP.items()}
        ).fillna("Other")
    else:
        v["industry_code"]     = None
        v["industry"]          = "Other"
        v["industry_category"] = "Other"

    # Geo risk
    v["geo_risk"] = v["country_code"].apply(
        lambda c: next(
            (tier for tier, codes in GEO_RISK.items() if str(c) in codes),
            "Medium"
        )
    )

    # Remove blanks
    v = v[
        v["supplier_name"].notna() &
        (v["supplier_name"] != "nan") &
        (v["supplier_name"].str.strip().str.len() > 1)
    ].drop_duplicates("vendor_id").reset_index(drop=True)

    v["is_active"] = True

    log.info(f"  Prepared {len(v):,} vendors")
    log.info(f"  Country distribution: {v['country_code'].value_counts().head(5).to_dict()}")
    log.info(f"  Industry distribution: {v['industry_category'].value_counts().head(5).to_dict()}")

    if dry_run:
        log.info(f"  [DRY RUN] Would insert {len(v):,} vendors")
        return len(v)

    rows = db.upsert_df(v, "vendors", conflict_col="vendor_id")
    log.info(f"  ✅ Loaded {rows:,} vendors into Postgres")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# TABLE 2 — TRANSACTIONS
# Source: EKKO (PO headers) + EKPO (PO line items) + LFA1 (vendor name)
# ─────────────────────────────────────────────────────────────────────────────

def load_transactions(folder: Path, db: DBClient, dry_run: bool) -> int:
    log.info("\n" + "="*60)
    log.info("LOADING: transactions")
    log.info("="*60)

    ekko = read_sap(folder, "ekko.csv")
    ekpo = read_sap(folder, "ekpo.csv")
    lfa1 = read_sap(folder, "lfa1.csv")

    if ekko.empty or ekpo.empty:
        log.warning("EKKO or EKPO missing — skipping transactions")
        return 0

    # Normalise vendor IDs
    ekko["LIFNR"] = normalise_lifnr(ekko["LIFNR"])
    lfa1["LIFNR"] = normalise_lifnr(lfa1["LIFNR"])

    # Parse PO date
    ekko = parse_dates(ekko, "BEDAT")

    # Filter to Purchase Orders only (BSTYP = F)
    if "BSTYP" in ekko.columns:
        ekko_po = ekko[ekko["BSTYP"].astype(str).str.upper() == "F"].copy()
        log.info(f"  PO records (BSTYP=F): {len(ekko_po):,} of {len(ekko):,}")
    else:
        ekko_po = ekko.copy()

    # Calculate line value
    if "NETPR" in ekpo.columns and "MENGE" in ekpo.columns:
        ekpo["NETPR"] = pd.to_numeric(ekpo["NETPR"], errors="coerce").fillna(0)
        ekpo["MENGE"] = pd.to_numeric(ekpo["MENGE"], errors="coerce").fillna(0)
        ekpo["transaction_amount"] = (ekpo["NETPR"] * ekpo["MENGE"]).round(2)
    elif "NETWR" in ekpo.columns:
        ekpo["transaction_amount"] = pd.to_numeric(
            ekpo["NETWR"], errors="coerce"
        ).fillna(0).round(2)
    else:
        ekpo["transaction_amount"] = 0.0

    # Join header → line items
    header_cols = [c for c in ["EBELN","LIFNR","BEDAT","BUKRS","EKORG","WAERS"]
                   if c in ekko_po.columns]
    tx = ekpo.merge(
        ekko_po[header_cols].drop_duplicates("EBELN"),
        on="EBELN", how="left"
    )
    tx["LIFNR"] = normalise_lifnr(tx["LIFNR"])

    # Join vendor name from LFA1
    if "NAME1" in lfa1.columns:
        tx = tx.merge(
            lfa1[["LIFNR","NAME1"]].drop_duplicates("LIFNR"),
            on="LIFNR", how="left"
        )
        tx["supplier_name"] = tx["NAME1"].astype(str).str.strip()
    else:
        tx["supplier_name"] = tx["LIFNR"]

    # Time dimensions
    if "BEDAT" in tx.columns:
        tx["po_date"]      = tx["BEDAT"].dt.date
        tx["year"]         = tx["BEDAT"].dt.year
        tx["quarter"]      = tx["BEDAT"].dt.quarter
        tx["month"]        = tx["BEDAT"].dt.month
        tx["year_quarter"] = (tx["year"].astype(str) + "-Q" +
                              tx["quarter"].astype(str))

    # Rename to schema columns
    col_map = {
        "EBELN": "po_number",
        "EBELP": "po_line",
        "LIFNR": "vendor_id",
        "MATNR": "material_number",
        "MATKL": "material_group",
        "MENGE": "quantity",
        "MEINS": "unit_of_measure",
        "NETPR": "unit_price",
        "WERKS": "plant",
        "EKORG": "purchasing_org",
        "BUKRS": "company_code",
        "WAERS": "currency",
    }
    tx.rename(columns={k: v for k, v in col_map.items()
                        if k in tx.columns}, inplace=True)

    # Select final columns (only those in schema)
    schema_cols = [
        "po_number","po_line","vendor_id","supplier_name",
        "po_date","year","quarter","month","year_quarter",
        "material_number","material_group","quantity","unit_of_measure",
        "unit_price","transaction_amount","currency",
        "plant","purchasing_org","company_code",
    ]
    tx = tx[[c for c in schema_cols if c in tx.columns]]

    # Quality filter
    before = len(tx)
    tx = tx[tx["transaction_amount"] > 0]
    if "po_date" in tx.columns:
        tx = tx[tx["po_date"].notna()]
    log.info(f"  Dropped {before - len(tx):,} zero-value / no-date rows")
    log.info(f"  Final: {len(tx):,} transaction rows")

    total_spend = tx["transaction_amount"].sum()
    log.info(f"  Total spend: {total_spend:,.0f}")

    if dry_run:
        log.info(f"  [DRY RUN] Would insert {len(tx):,} transactions")
        return len(tx)

    rows = db.bulk_insert_df(tx, "transactions", if_exists="replace")
    log.info(f"  ✅ Loaded {rows:,} transactions into Postgres")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# TABLE 3 — DELIVERY EVENTS (OTIF)
# Source: EKET (promised) + EKBE (actual GR) + EKKO (vendor link)
# ─────────────────────────────────────────────────────────────────────────────

def load_delivery_events(folder: Path, db: DBClient, dry_run: bool) -> int:
    log.info("\n" + "="*60)
    log.info("LOADING: delivery_events")
    log.info("="*60)

    eket = read_sap(folder, "eket.csv")
    ekbe = read_sap(folder, "ekbe.csv")
    ekko = read_sap(folder, "ekko.csv")
    lfa1 = read_sap(folder, "lfa1.csv")

    if eket.empty and ekbe.empty:
        log.warning("Both EKET and EKBE missing — skipping delivery events")
        return 0

    # ── Promised dates from EKET ──────────────────────────────────────────
    promised = pd.DataFrame()
    if not eket.empty and "EBELN" in eket.columns:
        date_col = next((c for c in ["EINDT","SLFDT"] if c in eket.columns), None)
        if date_col:
            eket = parse_dates(eket, date_col)
            qty_col = "MENGE" if "MENGE" in eket.columns else None
            agg_dict = {"promised_date": (date_col, "max")}
            if qty_col:
                agg_dict["promised_qty"] = (qty_col, "sum")
            promised = eket.groupby(["EBELN","EBELP"]).agg(**agg_dict).reset_index()
            log.info(f"  Promised delivery records: {len(promised):,}")

    # ── Actual GR dates from EKBE ──────────────────────────────────────────
    actual = pd.DataFrame()
    if not ekbe.empty and "EBELN" in ekbe.columns:
        # Filter to goods receipts only
        if "VGABE" in ekbe.columns:
            gr = ekbe[ekbe["VGABE"].astype(str).isin(["1","E","WE"])]
        elif "BWART" in ekbe.columns:
            gr = ekbe[ekbe["BWART"].astype(str).str[:1] == "1"]
        else:
            gr = ekbe.copy()

        date_col = next((c for c in ["BLDAT","BUDAT"] if c in gr.columns), None)
        if date_col and "EBELP" in gr.columns:
            gr = parse_dates(gr, date_col)
            qty_col = "MENGE" if "MENGE" in gr.columns else None
            agg_dict = {"actual_date": (date_col, "max")}
            if qty_col:
                agg_dict["actual_qty"] = (qty_col, "sum")
            actual = gr.groupby(["EBELN","EBELP"]).agg(**agg_dict).reset_index()
            log.info(f"  Actual delivery records: {len(actual):,}")

    # ── Join promised + actual ────────────────────────────────────────────
    if not promised.empty and not actual.empty:
        ev = promised.merge(actual, on=["EBELN","EBELP"], how="outer")
    elif not promised.empty:
        ev = promised.copy()
        ev["actual_date"] = pd.NaT
        ev["actual_qty"]  = np.nan
    elif not actual.empty:
        ev = actual.copy()
        ev["promised_date"] = pd.NaT
        ev["promised_qty"]  = np.nan
    else:
        log.warning("No delivery data built — skipping")
        return 0

    # ── OTIF metrics ───────────────────────────────────────────────────────
    if "promised_date" in ev.columns and "actual_date" in ev.columns:
        ev["delay_days"] = (
            pd.to_datetime(ev["actual_date"]) -
            pd.to_datetime(ev["promised_date"])
        ).dt.days.fillna(0).astype(int)

        ev["on_time"] = (ev["delay_days"] <= 0).astype(int)

    if "promised_qty" in ev.columns and "actual_qty" in ev.columns:
        tol = DELIVERY_CONFIG["otif_tolerance_pct"]
        ev["in_full"] = (
            pd.to_numeric(ev["actual_qty"],  errors="coerce").fillna(0) >=
            pd.to_numeric(ev["promised_qty"], errors="coerce").fillna(0) * tol
        ).astype(int)

        if "on_time" in ev.columns:
            ev["otif"] = ((ev["on_time"] == 1) & (ev["in_full"] == 1)).astype(int)

    # ── Add vendor info via EKKO ───────────────────────────────────────────
    if not ekko.empty and "LIFNR" in ekko.columns:
        ekko["LIFNR"] = normalise_lifnr(ekko["LIFNR"])
        ev = ev.merge(
            ekko[["EBELN","LIFNR"]].drop_duplicates("EBELN"),
            on="EBELN", how="left"
        )
        ev["LIFNR"] = normalise_lifnr(ev["LIFNR"].fillna(""))

        if not lfa1.empty and "NAME1" in lfa1.columns:
            lfa1["LIFNR"] = normalise_lifnr(lfa1["LIFNR"])
            ev = ev.merge(
                lfa1[["LIFNR","NAME1"]].drop_duplicates("LIFNR"),
                on="LIFNR", how="left"
            )
            ev["supplier_name"] = ev["NAME1"].astype(str).str.strip()

    # ── Rename ────────────────────────────────────────────────────────────
    col_map = {
        "EBELN": "po_number",
        "EBELP": "po_line",
        "LIFNR": "vendor_id",
        "MATNR": "material_number",
        "WERKS": "plant",
    }
    ev.rename(columns={k: v for k, v in col_map.items()
                        if k in ev.columns}, inplace=True)

    # Date to date only (not datetime)
    for dcol in ["promised_date","actual_date"]:
        if dcol in ev.columns:
            ev[dcol] = pd.to_datetime(ev[dcol], errors="coerce").dt.date

    # Select schema columns
    schema_cols = [
        "po_number","po_line","vendor_id","supplier_name",
        "material_number","plant",
        "promised_date","actual_date","promised_qty","actual_qty",
        "delay_days","on_time","in_full","otif",
    ]
    ev = ev[[c for c in schema_cols if c in ev.columns]]

    if "otif" in ev.columns:
        otif_pct = ev["otif"].mean() * 100
        late_pct = (1 - ev["on_time"].mean()) * 100
        log.info(f"  Portfolio OTIF: {otif_pct:.1f}%  |  Late: {late_pct:.1f}%")
    log.info(f"  Final: {len(ev):,} delivery event rows")

    if dry_run:
        log.info(f"  [DRY RUN] Would insert {len(ev):,} delivery events")
        return len(ev)

    rows = db.bulk_insert_df(ev, "delivery_events", if_exists="replace")
    log.info(f"  ✅ Loaded {rows:,} delivery events into Postgres")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# TABLE 4 — CONTRACTS
# Source: EKKO where BSTYP = 'K'
# ─────────────────────────────────────────────────────────────────────────────

def load_contracts(folder: Path, db: DBClient, dry_run: bool) -> int:
    log.info("\n" + "="*60)
    log.info("LOADING: contracts")
    log.info("="*60)

    ekko = read_sap(folder, "ekko.csv")
    lfa1 = read_sap(folder, "lfa1.csv")

    if ekko.empty:
        log.warning("EKKO missing — skipping contracts")
        return 0

    ekko["LIFNR"] = normalise_lifnr(ekko["LIFNR"])

    # Filter to contracts (BSTYP = K)
    if "BSTYP" in ekko.columns:
        contracts = ekko[
            ekko["BSTYP"].astype(str).str.upper().isin(["K","MK","L"])
        ].copy()
        log.info(f"  Contract records: {len(contracts):,} of {len(ekko):,} EKKO rows")
    else:
        log.warning("  BSTYP column missing — cannot identify contracts")
        return 0

    if contracts.empty:
        log.warning("  No contract records found in EKKO (BSTYP=K)")
        return 0

    # Parse dates
    contracts = parse_dates(contracts, "KDATB", "KDATE", "BEDAT")

    # Enrich with vendor name
    if not lfa1.empty and "NAME1" in lfa1.columns:
        lfa1["LIFNR"] = normalise_lifnr(lfa1["LIFNR"])
        contracts = contracts.merge(
            lfa1[["LIFNR","NAME1"]].drop_duplicates("LIFNR"),
            on="LIFNR", how="left"
        )
        contracts["supplier_name"] = contracts["NAME1"].astype(str).str.strip()
    else:
        contracts["supplier_name"] = contracts["LIFNR"]

    # Contract status
    today = pd.Timestamp.today()
    if "KDATE" in contracts.columns:
        contracts["days_to_expiry"] = (
            contracts["KDATE"] - today
        ).dt.days.fillna(-999).astype(int)

        def status(d):
            if d < 0:   return "Expired"
            if d <= 30: return "Expires <30d"
            if d <= 60: return "Expires 30-60d"
            if d <= 90: return "Expires 60-90d"
            return "Active"

        contracts["contract_status"] = contracts["days_to_expiry"].apply(status)

        exp30 = (contracts["days_to_expiry"].between(0, 30)).sum()
        exp60 = (contracts["days_to_expiry"].between(0, 60)).sum()
        expired = (contracts["days_to_expiry"] < 0).sum()
        log.info(f"  Expiring in 30d: {exp30} | 60d: {exp60} | Expired: {expired}")

    # Rename
    col_map = {
        "EBELN": "contract_number",
        "LIFNR": "vendor_id",
        "KDATB": "contract_start",
        "KDATE": "contract_end",
        "EKORG": "purchasing_org",
        "BUKRS": "company_code",
        "WAERS": "currency",
    }
    contracts.rename(columns={k: v for k, v in col_map.items()
                               if k in contracts.columns}, inplace=True)

    # Date columns → date only
    for dcol in ["contract_start","contract_end"]:
        if dcol in contracts.columns:
            contracts[dcol] = pd.to_datetime(
                contracts[dcol], errors="coerce"
            ).dt.date

    schema_cols = [
        "contract_number","vendor_id","supplier_name",
        "contract_start","contract_end","days_to_expiry",
        "contract_status","currency","purchasing_org","company_code",
    ]
    contracts = contracts[[c for c in schema_cols if c in contracts.columns]]

    if "contract_number" in contracts.columns:
        contracts = contracts.drop_duplicates("contract_number")

    log.info(f"  Final: {len(contracts):,} contract rows")

    if dry_run:
        log.info(f"  [DRY RUN] Would insert {len(contracts):,} contracts")
        return len(contracts)

    rows = db.upsert_df(contracts, "contracts", conflict_col="contract_number")
    log.info(f"  ✅ Loaded {rows:,} contracts into Postgres")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# POST-LOAD: UPDATE VENDOR AGGREGATE SCORES
# Calculates spend totals, OTIF, financial stability, supply risk
# directly in Postgres using SQL — faster than doing it in pandas
# ─────────────────────────────────────────────────────────────────────────────

def update_vendor_scores(db: DBClient, dry_run: bool):
    """
    After loading all tables, compute and write aggregate scores
    back to the vendors table using SQL UPDATE.
    """
    log.info("\n" + "="*60)
    log.info("UPDATING: vendor aggregate scores")
    log.info("="*60)

    if dry_run:
        log.info("  [DRY RUN] Would update vendor scores")
        return

    # Clear any failed transaction state from the bulk loads above
    try:
        db.conn.rollback()
        log.debug("  Transaction state cleared before score updates")
    except Exception:
        pass

    steps = [
        ("Spend totals", """
            UPDATE vendors v
            SET
                total_annual_spend = sub.total_spend,
                transaction_count  = sub.tx_count,
                avg_order_value    = sub.avg_val
            FROM (
                SELECT vendor_id,
                       SUM(transaction_amount)  AS total_spend,
                       COUNT(*)                 AS tx_count,
                       AVG(transaction_amount)  AS avg_val
                FROM transactions
                GROUP BY vendor_id
            ) sub
            WHERE v.vendor_id = sub.vendor_id
        """),
        ("Spend concentration", """
            UPDATE vendors
            SET spend_pct_of_portfolio = ROUND(
                total_annual_spend::NUMERIC /
                NULLIF((SELECT SUM(total_annual_spend) FROM vendors), 0) * 100,
                4
            )
            WHERE total_annual_spend IS NOT NULL
        """),
        ("OTIF per vendor", """
            UPDATE vendors v
            SET
                otif_rate            = sub.otif_avg,
                avg_delay_days       = sub.avg_delay,
                total_deliveries     = sub.del_count,
                delivery_performance = ROUND((sub.otif_avg * 100)::NUMERIC, 2)
            FROM (
                SELECT vendor_id,
                       AVG(otif::FLOAT)        AS otif_avg,
                       AVG(delay_days::FLOAT)  AS avg_delay,
                       COUNT(*)                AS del_count
                FROM delivery_events
                WHERE otif IS NOT NULL
                GROUP BY vendor_id
            ) sub
            WHERE v.vendor_id = sub.vendor_id
        """),
        ("Financial stability", """
            UPDATE vendors v
            SET financial_stability = sub.fin_score
            FROM (
                SELECT vendor_id,
                       LEAST(95, GREATEST(20,
                           ROUND((
                               (LN(GREATEST(total_annual_spend, 1)) /
                                NULLIF(LN(MAX(total_annual_spend) OVER ()), 0)) * 50
                               +
                               (transaction_count::FLOAT /
                                NULLIF(MAX(transaction_count) OVER (), 0)) * 30
                               + 20
                           )::NUMERIC, 2)
                       )) AS fin_score
                FROM vendors
                WHERE total_annual_spend IS NOT NULL
            ) sub
            WHERE v.vendor_id = sub.vendor_id
        """),
        ("Supply risk + label", """
            UPDATE vendors v
            SET
                supply_risk_score    = sub.risk,
                composite_risk_score = sub.risk,
                risk_label = CASE
                    WHEN sub.risk >= 0.65 THEN 'High'
                    WHEN sub.risk >= 0.35 THEN 'Medium'
                    ELSE 'Low'
                END
            FROM (
                SELECT vendor_id,
                       ROUND(LEAST(1.0, GREATEST(0.0, (
                           CASE geo_risk
                               WHEN 'High'   THEN 0.80
                               WHEN 'Medium' THEN 0.50
                               ELSE 0.20
                           END * 0.30
                           + (1.0 - COALESCE(otif_rate, 0.75)) * 0.25
                           + CASE WHEN COALESCE(spend_pct_of_portfolio, 0) > 10
                               THEN 0.80 ELSE 0.20 END * 0.20
                           + 0.50 * 0.25
                       )))::NUMERIC, 4) AS risk
                FROM vendors
            ) sub
            WHERE v.vendor_id = sub.vendor_id
        """),
    ]

    for label, sql in steps:
        log.info(f"  Calculating {label}...")
        try:
            db.execute(sql)
            log.info(f"  ✅ {label} updated")
        except Exception as e:
            log.error(f"  ❌ {label} failed: {e}")
            log.error("     Continuing with remaining steps...")

    # Summary
    try:
        summary = db.fetch_df("""
            SELECT risk_label,
                   COUNT(*)                                  AS count,
                   ROUND(AVG(total_annual_spend)::NUMERIC,0) AS avg_spend
            FROM vendors
            WHERE risk_label IS NOT NULL
            GROUP BY risk_label
            ORDER BY risk_label
        """)
        log.info(f"\n  Risk distribution:\n{summary.to_string(index=False)}")
    except Exception as e:
        log.warning(f"  Could not fetch summary: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

LOADERS = {
    "vendors":         load_vendors,
    "transactions":    load_transactions,
    "delivery_events": load_delivery_events,
    "contracts":       load_contracts,
}


def main():
    parser = argparse.ArgumentParser(
        description="Load SAP data into SRSID Postgres database")
    parser.add_argument("--table",   type=str, default=None,
                        choices=list(LOADERS.keys()),
                        help="Load a single table (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate + count rows without writing to DB")
    parser.add_argument("--path",    type=str, default=None,
                        help="Override SAP data path from config")
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║   SRSID SAP Loader — Loading SAP data into Postgres             ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    if args.dry_run:
        print("  ⚠️  DRY RUN — no data will be written to the database")
    print()

    # Locate SAP data
    folder = Path(args.path) if args.path else locate_sap_data()
    csv_files = list(folder.glob("*.csv"))
    log.info(f"SAP folder: {folder}")
    log.info(f"CSV files found: {[f.name for f in csv_files]}")

    results = {}

    with DBClient() as db:
        tables = [args.table] if args.table else list(LOADERS.keys())

        for table in tables:
            try:
                rows = LOADERS[table](folder, db, args.dry_run)
                results[table] = rows
            except Exception as e:
                log.error(f"  ❌ Failed to load {table}: {e}", exc_info=True)
                results[table] = -1

        # Update vendor scores after all data is loaded
        if not args.table and not args.dry_run:
            update_vendor_scores(db, args.dry_run)

    # Summary
    print()
    print("=" * 60)
    print("  SAP LOAD COMPLETE")
    print("=" * 60)
    for table, rows in results.items():
        status = "✅" if rows >= 0 else "❌"
        note   = f"{rows:,} rows" if rows >= 0 else "FAILED"
        print(f"  {status}  {table:<25} {note}")

    if not args.dry_run:
        print()
        print("  NEXT STEPS:")
        print("    python ml/features.py          → build feature matrix")
        print("    python ml/risk_model.py        → train risk model")
        print("    streamlit run app/dashboard.py → launch dashboard")
    print()


if __name__ == "__main__":
    main()
