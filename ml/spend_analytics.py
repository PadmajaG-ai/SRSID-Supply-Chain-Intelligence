"""
SRSID  ml/spend_analytics.py
==============================
Calculates spend analytics using real SAP data from Postgres.

Computes:
  - Spend Under Management (SUM %) — real contract coverage from contracts table
  - Maverick spend — transactions with no active contract
  - HHI concentration index — portfolio dependency risk
  - QoQ spend trend — quarter-over-quarter change
  - Spend at risk — High-risk vendor spend £ value
  - Risk-adjusted spend score — used in composite risk

Updates vendors table + writes spend_analytics report.

Run:
    python ml/spend_analytics.py

Replaces: calculate_spend_analytics.py (old synthetic-era script)
"""

import sys, json, logging
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import SPEND_CONFIG, PATHS
from db.db_client import DBClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/spend_analytics.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)
REPORT_DIR = PATHS["reports"]


# ─────────────────────────────────────────────────────────────────────────────
# 1. SPEND UNDER MANAGEMENT (SUM)
# Real: vendor has active contract in contracts table = under management
# ─────────────────────────────────────────────────────────────────────────────

def calculate_sum(db: DBClient) -> pd.DataFrame:
    """
    Spend Under Management = spend from vendors with an active contract.
    Uses real contracts table — no random guessing.
    """
    log.info("Calculating Spend Under Management (SUM)...")

    df = db.fetch_df("""
        SELECT
            v.vendor_id,
            v.supplier_name,
            v.total_annual_spend,
            -- Has active contract?
            CASE WHEN c.vendor_id IS NOT NULL THEN TRUE ELSE FALSE END
                AS has_active_contract,
            c.contract_end,
            c.days_to_expiry
        FROM vendors v
        LEFT JOIN (
            SELECT DISTINCT ON (vendor_id)
                vendor_id, contract_end, days_to_expiry
            FROM contracts
            WHERE contract_status NOT IN ('Expired')
            ORDER BY vendor_id, contract_end DESC
        ) c ON v.vendor_id = c.vendor_id
        WHERE v.is_active = TRUE
          AND v.total_annual_spend IS NOT NULL
        ORDER BY v.total_annual_spend DESC NULLS LAST
    """)

    if df.empty:
        log.warning("No vendor spend data found")
        return df

    total_spend = df["total_annual_spend"].sum()

    # SUM calculation
    df["spend_under_contract"] = df.apply(
        lambda r: r["total_annual_spend"] if r["has_active_contract"] else 0,
        axis=1
    )
    df["sum_percentage"] = (
        df["spend_under_contract"] / df["total_annual_spend"] * 100
    ).round(2)

    # Contract gap = spend not under management
    df["contract_gap"] = df["total_annual_spend"] - df["spend_under_contract"]

    # Portfolio SUM
    total_contracted = df["spend_under_contract"].sum()
    portfolio_sum    = total_contracted / total_spend * 100 if total_spend else 0
    target           = SPEND_CONFIG["sum_target"] * 100

    log.info(f"  Portfolio SUM: {portfolio_sum:.1f}%  (target: >{target:.0f}%)"
             f"  {'✅' if portfolio_sum >= target else '⚠️  Below target'}")
    log.info(f"  Vendors with contract: "
             f"{df['has_active_contract'].sum():,} / {len(df):,}")
    log.info(f"  Total contract gap: "
             f"${df['contract_gap'].sum():,.0f}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# 2. MAVERICK SPEND
# Real: transaction from vendor with no active contract = maverick
# ─────────────────────────────────────────────────────────────────────────────

def calculate_maverick(db: DBClient) -> pd.DataFrame:
    """
    Maverick spend = transactions from vendors with no active contract.
    This is real detection using the contracts table.
    """
    log.info("Calculating Maverick Spend...")

    df = db.fetch_df("""
        SELECT
            t.vendor_id,
            t.supplier_name,
            SUM(t.transaction_amount)     AS total_spend,
            COUNT(*)                      AS tx_count,
            -- Maverick = no active contract for this vendor
            CASE WHEN c.vendor_id IS NULL THEN TRUE ELSE FALSE END
                AS is_maverick,
            SUM(t.transaction_amount) FILTER (
                WHERE c.vendor_id IS NULL
            ) AS maverick_spend
        FROM transactions t
        LEFT JOIN (
            SELECT DISTINCT vendor_id
            FROM contracts
            WHERE contract_status NOT IN ('Expired')
        ) c ON t.vendor_id = c.vendor_id
        WHERE t.transaction_amount > 0
        GROUP BY t.vendor_id, t.supplier_name,
                 (c.vendor_id IS NULL)
        ORDER BY total_spend DESC
    """)

    if df.empty:
        log.warning("No transaction data — skipping maverick analysis")
        return df

    total_spend  = df["total_spend"].sum()
    mav_spend    = df[df["is_maverick"] == True]["total_spend"].sum()
    mav_pct      = mav_spend / total_spend * 100 if total_spend else 0
    target       = SPEND_CONFIG["maverick_target"] * 100

    log.info(f"  Maverick spend: ${mav_spend:,.0f} ({mav_pct:.1f}%)"
             f"  (target: <{target:.0f}%)"
             f"  {'✅' if mav_pct <= target else '⚠️  Exceeds target'}")

    # Classify maverick type based on spend size
    # (without random — use spend size as proxy)
    def classify_type(row):
        if not row["is_maverick"]:
            return "Contracted"
        if row["total_spend"] > df["total_spend"].quantile(0.75):
            return "Off-Contract (High Value)"
        if row["tx_count"] == 1:
            return "Emergency / One-Off"
        return "Off-Contract (Regular)"

    df["maverick_type"] = df.apply(classify_type, axis=1)

    # Savings opportunity (off-contract vendors typically have 15–25% savings potential)
    savings_rate_map = {
        "Off-Contract (High Value)": 0.20,
        "Off-Contract (Regular)":    0.15,
        "Emergency / One-Off":       0.25,
        "Contracted":                0.00,
    }
    df["savings_opportunity"] = df.apply(
        lambda r: r["total_spend"] * savings_rate_map.get(r["maverick_type"], 0),
        axis=1
    ).round(2)

    total_savings = df["savings_opportunity"].sum()
    log.info(f"  Estimated savings opportunity: ${total_savings:,.0f}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# 3. CONCENTRATION RISK (HHI)
# ─────────────────────────────────────────────────────────────────────────────

def calculate_concentration(db: DBClient) -> dict:
    """
    HHI + per-vendor concentration %.
    Returns portfolio metrics dict and per-vendor DataFrame.
    """
    log.info("Calculating Concentration Risk (HHI)...")

    df = db.fetch_df("""
        SELECT
            vendor_id, supplier_name, industry_category,
            total_annual_spend,
            spend_pct_of_portfolio
        FROM vendors
        WHERE is_active = TRUE
          AND total_annual_spend IS NOT NULL
          AND total_annual_spend > 0
        ORDER BY total_annual_spend DESC
    """)

    if df.empty:
        return {}, pd.DataFrame()

    total = df["total_annual_spend"].sum()

    # HHI = Σ (share_pct)²
    shares = (df["total_annual_spend"] / total * 100)
    hhi    = round(float((shares ** 2).sum()), 2)

    # Per-vendor concentration tier
    top1_max  = SPEND_CONFIG["top1_supplier_max"]  * 100
    top5_max  = SPEND_CONFIG["top5_suppliers_max"] * 100

    df["spend_pct"] = shares.round(4)
    df["spend_rank"] = df["total_annual_spend"].rank(ascending=False).astype(int)

    def concentration_risk(pct):
        if pct >= SPEND_CONFIG["top1_supplier_max"] * 100:
            return "HIGH"
        if pct >= SPEND_CONFIG["top1_supplier_max"] * 100 * 0.67:
            return "MEDIUM"
        return "LOW"

    df["concentration_risk"] = df["spend_pct"].apply(concentration_risk)

    # Diversification needed flag
    df["diversification_needed"] = (
        df["concentration_risk"].isin(["HIGH", "MEDIUM"])
    ).astype(int)

    # Top N metrics
    top5_share  = df.head(5)["total_annual_spend"].sum()  / total * 100
    top10_share = df.head(10)["total_annual_spend"].sum() / total * 100

    hhi_status = (
        "HIGH"     if hhi >= SPEND_CONFIG["hhi_moderate"] else
        "MODERATE" if hhi >= SPEND_CONFIG["hhi_healthy"]  else
        "HEALTHY"
    )

    portfolio = {
        "hhi":                 hhi,
        "hhi_status":          hhi_status,
        "top1_pct":            round(float(shares.max()), 2),
        "top5_pct":            round(top5_share, 2),
        "top10_pct":           round(top10_share, 2),
        "top5_exceeds_limit":  top5_share > SPEND_CONFIG["top5_suppliers_max"] * 100,
        "top10_exceeds_limit": top10_share > SPEND_CONFIG["top10_suppliers_max"] * 100,
        "high_concentration_vendors": int((df["concentration_risk"] == "HIGH").sum()),
        "diversification_needed":     int(df["diversification_needed"].sum()),
    }

    log.info(f"  HHI: {hhi:,.0f} ({hhi_status}) "
             f"(healthy < {SPEND_CONFIG['hhi_healthy']:,})")
    log.info(f"  Top 5 vendors: {top5_share:.1f}%  "
             f"{'⚠️' if portfolio['top5_exceeds_limit'] else '✅'}")
    log.info(f"  Top 10 vendors: {top10_share:.1f}%  "
             f"{'⚠️' if portfolio['top10_exceeds_limit'] else '✅'}")
    log.info(f"  High concentration vendors: "
             f"{portfolio['high_concentration_vendors']}")

    return portfolio, df


# ─────────────────────────────────────────────────────────────────────────────
# 4. QoQ SPEND TREND
# ─────────────────────────────────────────────────────────────────────────────

def calculate_qoq_trend(db: DBClient) -> pd.DataFrame:
    """Quarter-over-quarter spend change per vendor."""
    log.info("Calculating QoQ spend trend...")

    df = db.fetch_df("""
        SELECT
            vendor_id, supplier_name,
            year, quarter,
            year_quarter,
            SUM(transaction_amount) AS quarterly_spend
        FROM transactions
        WHERE transaction_amount > 0
          AND year IS NOT NULL
          AND quarter IS NOT NULL
        GROUP BY vendor_id, supplier_name, year, quarter, year_quarter
        ORDER BY vendor_id, year, quarter
    """)

    if df.empty:
        log.warning("No quarterly transaction data")
        return pd.DataFrame()

    # Compute QoQ change per vendor
    df = df.sort_values(["vendor_id", "year", "quarter"])
    df["prev_spend"] = df.groupby("vendor_id")["quarterly_spend"].shift(1)
    df["qoq_change_pct"] = (
        (df["quarterly_spend"] - df["prev_spend"]) /
        df["prev_spend"].replace(0, np.nan) * 100
    ).round(2)

    df["spend_trend"] = pd.cut(
        df["qoq_change_pct"].fillna(0),
        bins=[-np.inf, -10, 10, np.inf],
        labels=["Decreasing", "Stable", "Increasing"]
    ).astype(str)

    # Latest quarter only per vendor
    latest = (
        df.sort_values(["year","quarter"])
          .groupby("vendor_id")
          .last()
          .reset_index()
    )[["vendor_id","year_quarter","quarterly_spend","qoq_change_pct","spend_trend"]]

    increasing = (latest["spend_trend"] == "Increasing").sum()
    decreasing = (latest["spend_trend"] == "Decreasing").sum()
    log.info(f"  QoQ: {increasing} vendors increasing, {decreasing} decreasing")

    return latest


# ─────────────────────────────────────────────────────────────────────────────
# 5. WRITE BACK TO POSTGRES + SAVE REPORTS
# ─────────────────────────────────────────────────────────────────────────────

def write_spend_analytics(sum_df: pd.DataFrame,
                           maverick_df: pd.DataFrame,
                           concentration_portfolio: dict,
                           concentration_df: pd.DataFrame,
                           qoq_df: pd.DataFrame,
                           db: DBClient):
    """
    Update vendors table with spend analytics scores.
    Save reports to reports/ folder.
    """
    log.info("Writing spend analytics back to Postgres...")

    # Build update DataFrame
    updates = pd.DataFrame()

    if not sum_df.empty:
        updates = sum_df[["vendor_id","sum_percentage",
                           "has_active_contract","contract_gap"]].copy()
        updates.rename(columns={"has_active_contract": "under_contract"}, inplace=True)

    if not maverick_df.empty:
        mav_sub = maverick_df[["vendor_id","is_maverick",
                                "maverick_type","savings_opportunity"]].copy()
        updates = updates.merge(mav_sub, on="vendor_id", how="outer") \
                  if not updates.empty else mav_sub

    if not concentration_df.empty:
        conc_sub = concentration_df[["vendor_id","spend_pct",
                                      "concentration_risk",
                                      "diversification_needed"]].copy()
        updates = updates.merge(conc_sub, on="vendor_id", how="outer") \
                  if not updates.empty else conc_sub

    if not qoq_df.empty:
        qoq_sub = qoq_df[["vendor_id","qoq_change_pct","spend_trend"]].copy()
        updates = updates.merge(qoq_sub, on="vendor_id", how="outer") \
                  if not updates.empty else qoq_sub

    if updates.empty:
        log.warning("No spend analytics data to write")
        return

    # Add missing columns to vendors table dynamically
    col_type_map = {
        "sum_percentage":       "FLOAT",
        "contract_gap":         "FLOAT",
        "under_contract":       "BOOLEAN",
        "is_maverick":          "BOOLEAN",
        "maverick_type":        "VARCHAR(100)",
        "savings_opportunity":  "FLOAT",
        "concentration_risk":   "VARCHAR(20)",
        "diversification_needed": "INT",
        "qoq_change_pct":       "FLOAT",
        "spend_trend":          "VARCHAR(20)",
    }
    for col, dtype in col_type_map.items():
        if col in updates.columns:
            db.add_column_if_missing("vendors", col, dtype)

    # UPDATE only — never insert new rows
    set_cols = [c for c in updates.columns if c != "vendor_id" and c in
                set(db.fetch_df(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'vendors'"
                )["column_name"].tolist())]

    if not set_cols:
        log.warning("No matching columns to update in vendors table")
        return

    set_clause = ", ".join([f"{c} = %s" for c in set_cols])
    sql = f"UPDATE vendors SET {set_clause} WHERE vendor_id = %s"

    updated = 0
    with db.conn.cursor() as cur:
        for _, row in updates.iterrows():
            vals = tuple(
                None if isinstance(v, float) and np.isnan(v) else v
                for v in [row.get(c) for c in set_cols]
            )
            vals = vals + (str(row["vendor_id"]),)
            cur.execute(sql, vals)
            updated += cur.rowcount
    db.conn.commit()
    log.info(f"  Updated {updated:,} vendor records with spend analytics")

    # ── Save CSV reports ───────────────────────────────────────────────────
    if not concentration_df.empty:
        concentration_df.to_csv(
            REPORT_DIR / "concentration_analysis.csv", index=False
        )
    if not sum_df.empty:
        sum_df.to_csv(REPORT_DIR / "sum_analysis.csv", index=False)

    # ── Save spend intelligence JSON report ────────────────────────────────
    total_spend = sum_df["total_annual_spend"].sum() if not sum_df.empty else 0
    total_contract = sum_df["spend_under_contract"].sum() if not sum_df.empty else 0
    mav_spend  = maverick_df[maverick_df["is_maverick"] == True]["total_spend"].sum() \
                 if not maverick_df.empty else 0

    report = {
        "generated_at": datetime.now().isoformat(),
        "spend_intelligence": {
            "total_portfolio_spend":      round(float(total_spend), 2),
            "spend_under_management":     round(float(total_contract), 2),
            "sum_percentage":             round(total_contract / total_spend * 100, 2)
                                          if total_spend else 0,
            "sum_target":                 f">{SPEND_CONFIG['sum_target']*100:.0f}%",
            "maverick_spend":             round(float(mav_spend), 2),
            "maverick_percentage":        round(mav_spend / total_spend * 100, 2)
                                          if total_spend else 0,
            "maverick_target":            f"<{SPEND_CONFIG['maverick_target']*100:.0f}%",
        },
        "concentration": concentration_portfolio,
        "opportunity": {
            "total_savings_opportunity":  round(float(
                maverick_df["savings_opportunity"].sum()
                if not maverick_df.empty else 0
            ), 2),
            "vendors_below_sum_target":   int(
                (sum_df["sum_percentage"] < SPEND_CONFIG["sum_target"] * 100).sum()
                if not sum_df.empty else 0
            ),
            "vendors_needing_contracts":  int(
                (~sum_df["has_active_contract"]).sum()
                if not sum_df.empty else 0
            ),
        },
    }

    report_path = REPORT_DIR / "spend_intelligence_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    log.info(f"  Saved spend intelligence report → {report_path}")

    return report


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("SRSID Spend Analytics (Postgres)")
    log.info("=" * 60)

    with DBClient() as db:

        # Run all 4 analytics modules
        sum_df              = calculate_sum(db)
        maverick_df         = calculate_maverick(db)
        conc_portfolio, \
        concentration_df    = calculate_concentration(db)
        qoq_df              = calculate_qoq_trend(db)

        # Write back + generate reports
        report = write_spend_analytics(
            sum_df, maverick_df,
            conc_portfolio, concentration_df,
            qoq_df, db
        )

    # ── Summary ───────────────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("SPEND ANALYTICS COMPLETE")
    log.info("=" * 60)

    if report:
        si = report["spend_intelligence"]
        op = report["opportunity"]
        co = report.get("concentration", {})

        log.info(f"  Total spend          : ${si['total_portfolio_spend']:>18,.0f}")
        log.info(f"  Under management     : {si['sum_percentage']:>7.1f}%"
                 f"  (target {si['sum_target']})")
        log.info(f"  Maverick spend       : {si['maverick_percentage']:>7.1f}%"
                 f"  (target {si['maverick_target']})")
        log.info(f"  HHI index            : {co.get('hhi',0):>10,.0f}"
                 f"  ({co.get('hhi_status','?')})")
        log.info(f"  Savings opportunity  : ${op['total_savings_opportunity']:>18,.0f}")
        log.info(f"  Vendors need contract: {op['vendors_needing_contracts']:>10,}")
        log.info(f"\n  Report: {REPORT_DIR / 'spend_intelligence_report.json'}")

    log.info("\n  Next: python ml/features.py  (picks up spend analytics signals)")


if __name__ == "__main__":
    main()
