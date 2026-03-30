"""
SRSID Database Client
======================
Single database connection used by every script in the project.
Never import psycopg2 directly anywhere else — always use this.

Usage:
    from db.db_client import DBClient

    # As a context manager (recommended — auto-closes)
    with DBClient() as db:
        vendors = db.fetch_all("SELECT * FROM vendors WHERE risk_label = %s", ("High",))

    # Or keep open across multiple calls
    db = DBClient()
    db.connect()
    db.execute("INSERT INTO vendors (...) VALUES (...)", (...))
    db.close()
"""

import sys
import logging
import psycopg2
import psycopg2.extras
import pandas as pd
from pathlib import Path
from contextlib import contextmanager
from typing import Any

# Add project root to path so we can import config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_URL, DB_CONFIG

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN CLIENT CLASS
# ─────────────────────────────────────────────────────────────────────────────

class DBClient:
    """
    Thin wrapper around psycopg2.
    Handles connection, disconnection, queries, and bulk loads.
    All methods log errors clearly so you know exactly what failed.
    """

    def __init__(self):
        self._conn = None

    # ── Connection lifecycle ──────────────────────────────────────────────

    def connect(self) -> "DBClient":
        """Open a connection to srsid_db."""
        try:
            self._conn = psycopg2.connect(DB_URL)
            self._conn.autocommit = False
            log.debug("Connected to srsid_db")
            return self
        except psycopg2.OperationalError as e:
            log.error(f"Cannot connect to database: {e}")
            log.error(f"  Host    : {DB_CONFIG['host']}:{DB_CONFIG['port']}")
            log.error(f"  Database: {DB_CONFIG['database']}")
            log.error(f"  User    : {DB_CONFIG['user']}")
            log.error("  Fix: check Postgres is running  →  pg_isready")
            raise

    def close(self):
        """Close the connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            log.debug("Connection closed")

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        self.close()
        return False  # don't suppress exceptions

    @property
    def conn(self):
        if not self._conn or self._conn.closed:
            self.connect()
        return self._conn

    # ── Transactions ──────────────────────────────────────────────────────

    def commit(self):
        self._conn.commit()

    def rollback(self):
        if self._conn and not self._conn.closed:
            self._conn.rollback()

    # ── Core query methods ────────────────────────────────────────────────

    def execute(self, sql: str, params: tuple = None) -> int:
        """
        Run INSERT / UPDATE / DELETE.
        Returns number of rows affected.

        Example:
            db.execute(
                "UPDATE vendors SET risk_label = %s WHERE vendor_id = %s",
                ("High", "1001")
            )
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            self.conn.commit()
            return cur.rowcount

    def execute_many(self, sql: str, params_list: list) -> int:
        """
        Bulk INSERT / UPDATE using executemany.
        Faster than looping execute() for large datasets.

        Example:
            db.execute_many(
                "INSERT INTO vendors (vendor_id, supplier_name) VALUES (%s, %s)",
                [("1001", "Siemens"), ("1002", "BASF")]
            )
        """
        with self.conn.cursor() as cur:
            cur.executemany(sql, params_list)
            self.conn.commit()
            return cur.rowcount

    def fetch_one(self, sql: str, params: tuple = None) -> dict | None:
        """
        Fetch a single row as a dict.
        Returns None if no row found.

        Example:
            vendor = db.fetch_one(
                "SELECT * FROM vendors WHERE vendor_id = %s", ("1001",)
            )
            print(vendor["supplier_name"])
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None

    def fetch_all(self, sql: str, params: tuple = None) -> list[dict]:
        """
        Fetch all rows as a list of dicts.

        Example:
            high_risk = db.fetch_all(
                "SELECT * FROM vendors WHERE risk_label = %s ORDER BY total_annual_spend DESC",
                ("High",)
            )
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    def fetch_df(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """
        Fetch query results directly as a pandas DataFrame.
        Most convenient for ML scripts and dashboard.

        Example:
            df = db.fetch_df("SELECT * FROM vendors WHERE is_active = TRUE")
            df["risk_label"].value_counts()
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)

    def scalar(self, sql: str, params: tuple = None) -> Any:
        """
        Fetch a single value (first column of first row).
        Useful for COUNT, SUM, MAX queries.

        Example:
            count = db.scalar("SELECT COUNT(*) FROM vendors WHERE risk_label = 'High'")
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row[0] if row else None

    # ── Bulk load from DataFrame ──────────────────────────────────────────

    def bulk_insert_df(self, df: pd.DataFrame, table: str,
                       if_exists: str = "append",
                       chunksize: int = 1000) -> int:
        """
        Load a pandas DataFrame into a Postgres table efficiently.

        Args:
            df:         DataFrame to load
            table:      Target table name (must exist in schema)
            if_exists:  'append' (default) or 'replace' (truncates first)
            chunksize:  Rows per batch

        Returns:
            Number of rows inserted

        Example:
            rows = db.bulk_insert_df(vendors_df, "vendors", if_exists="replace")
            print(f"Loaded {rows} vendors")
        """
        if df.empty:
            log.warning(f"  bulk_insert_df: empty DataFrame — nothing loaded to {table}")
            return 0

        if if_exists == "replace":
            self.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
            log.info(f"  Truncated {table}")

        # Use psycopg2 copy_expert for speed on large datasets
        if len(df) > 5000:
            return self._copy_from_df(df, table)

        # For smaller datasets use executemany
        cols    = list(df.columns)
        placeholders = ", ".join(["%s"] * len(cols))
        col_list     = ", ".join(cols)
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

        total = 0
        for i in range(0, len(df), chunksize):
            chunk = df.iloc[i:i + chunksize]
            rows  = [tuple(row) for row in chunk.itertuples(index=False)]
            with self.conn.cursor() as cur:
                cur.executemany(sql, rows)
            self.conn.commit()
            total += len(rows)
            log.debug(f"  Inserted chunk {i//chunksize + 1}: {total}/{len(df)} rows → {table}")

        log.info(f"  bulk_insert_df: {total:,} rows → {table}")
        return total

    def _copy_from_df(self, df: pd.DataFrame, table: str) -> int:
        """Use COPY for very large DataFrames (fastest Postgres load method)."""
        import io
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, na_rep="\\N")
        buffer.seek(0)

        cols = ", ".join(df.columns)
        with self.conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                buffer
            )
        self.conn.commit()
        log.info(f"  COPY: {len(df):,} rows → {table}")
        return len(df)

    # ── Convenience helpers ───────────────────────────────────────────────

    def table_exists(self, table: str) -> bool:
        """Check if a table exists."""
        result = self.scalar("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        """, (table,))
        return bool(result)

    def row_count(self, table: str) -> int:
        """Fast row count for any table."""
        return self.scalar(f"SELECT COUNT(*) FROM {table}") or 0

    def column_exists(self, table: str, column: str) -> bool:
        """Check if a column exists in a table."""
        result = self.scalar("""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = %s AND column_name = %s
        """, (table, column))
        return bool(result)

    def add_column_if_missing(self, table: str, column: str, dtype: str):
        """Add a column to a table if it doesn't already exist."""
        if not self.column_exists(table, column):
            self.execute(f"ALTER TABLE {table} ADD COLUMN {column} {dtype}")
            log.info(f"  Added column {column} ({dtype}) to {table}")

    def truncate(self, table: str):
        """Truncate a table (delete all rows, keep structure)."""
        self.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
        log.info(f"  Truncated {table}")

    def upsert_df(self, df: pd.DataFrame, table: str,
                  conflict_col: str, chunksize: int = 1000) -> int:
        """
        Insert rows, updating existing ones on conflict.
        Uses INSERT ... ON CONFLICT (conflict_col) DO UPDATE.

        Example:
            db.upsert_df(vendors_df, "vendors", conflict_col="vendor_id")
        """
        if df.empty:
            return 0

        cols         = list(df.columns)
        col_list     = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        updates      = ", ".join(
            [f"{c} = EXCLUDED.{c}" for c in cols if c != conflict_col]
        )

        sql = f"""
            INSERT INTO {table} ({col_list})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_col}) DO UPDATE SET {updates}
        """

        total = 0
        for i in range(0, len(df), chunksize):
            chunk = df.iloc[i:i + chunksize]
            rows  = [tuple(row) for row in chunk.itertuples(index=False)]
            with self.conn.cursor() as cur:
                cur.executemany(sql, rows)
            self.conn.commit()
            total += len(rows)

        log.info(f"  upsert_df: {total:,} rows → {table} (conflict on {conflict_col})")
        return total

    # ── Pre-built SRSID queries ───────────────────────────────────────────
    # Frequently used queries so scripts don't repeat SQL

    def get_all_vendors(self) -> pd.DataFrame:
        """All active vendors ordered by spend."""
        return self.fetch_df("""
            SELECT vendor_id, supplier_name, industry, industry_category,
                   country_code, risk_label, total_annual_spend,
                   delivery_performance, financial_stability,
                   otif_rate, news_sentiment_30d, disruption_count_30d
            FROM vendors
            WHERE is_active = TRUE
            ORDER BY total_annual_spend DESC NULLS LAST
        """)

    def get_vendors_by_risk(self, tier: str) -> pd.DataFrame:
        """Vendors filtered by risk tier: High / Medium / Low."""
        return self.fetch_df("""
            SELECT vendor_id, supplier_name, industry_category,
                   country_code, risk_label, composite_risk_score,
                   total_annual_spend, delivery_performance
            FROM vendors
            WHERE risk_label = %s AND is_active = TRUE
            ORDER BY composite_risk_score DESC NULLS LAST
        """, (tier,))

    def get_spend_by_quarter(self, year: int = None,
                              quarter: int = None) -> pd.DataFrame:
        """Spend aggregated by supplier for a given quarter."""
        if year and quarter:
            return self.fetch_df("""
                SELECT vendor_id, supplier_name,
                       SUM(transaction_amount) AS total_spend,
                       COUNT(*)                AS transaction_count,
                       AVG(transaction_amount) AS avg_order_value
                FROM transactions
                WHERE year = %s AND quarter = %s
                GROUP BY vendor_id, supplier_name
                ORDER BY total_spend DESC
            """, (year, quarter))
        return self.fetch_df("""
            SELECT year, quarter,
                   SUM(transaction_amount) AS total_spend,
                   COUNT(DISTINCT vendor_id) AS active_vendors,
                   COUNT(*)                  AS transaction_count
            FROM transactions
            GROUP BY year, quarter
            ORDER BY year, quarter
        """)

    def get_otif_summary(self) -> pd.DataFrame:
        """OTIF performance per vendor."""
        return self.fetch_df("""
            SELECT vendor_id, supplier_name,
                   COUNT(*)                              AS total_deliveries,
                   AVG(otif)                            AS otif_rate,
                   AVG(on_time)                         AS on_time_rate,
                   AVG(delay_days) FILTER (WHERE delay_days > 0) AS avg_delay_days,
                   SUM(CASE WHEN delay_days > 5 THEN 1 ELSE 0 END) AS significant_delays
            FROM delivery_events
            GROUP BY vendor_id, supplier_name
            ORDER BY otif_rate ASC NULLS LAST
        """)

    def get_expiring_contracts(self, days: int = 60) -> pd.DataFrame:
        """Contracts expiring within N days."""
        return self.fetch_df("""
            SELECT c.contract_number, c.supplier_name, c.contract_end,
                   c.days_to_expiry, c.contract_status,
                   v.risk_label, v.total_annual_spend
            FROM contracts c
            LEFT JOIN vendors v ON c.vendor_id = v.vendor_id
            WHERE c.days_to_expiry BETWEEN 0 AND %s
            ORDER BY c.days_to_expiry ASC
        """, (days,))

    def get_recent_news(self, days: int = 7,
                         disruption_only: bool = False) -> pd.DataFrame:
        """Recent news articles, optionally filtered to disruptions."""
        sql = """
            SELECT n.supplier_name, n.title, n.source_name,
                   n.published_at, n.sentiment_score,
                   n.disruption_type, n.disruption_flag, n.url,
                   v.risk_label, v.industry_category
            FROM vendor_news n
            LEFT JOIN vendors v ON n.vendor_id = v.vendor_id
            WHERE n.published_at >= NOW() - INTERVAL '%s days'
        """
        if disruption_only:
            sql += " AND n.disruption_flag = TRUE"
        sql += " ORDER BY n.published_at DESC LIMIT 100"
        return self.fetch_df(sql, (days,))

    def get_portfolio_summary(self) -> dict:
        """Single-row KPI summary for sidebar / chatbot."""
        row = self.fetch_one("SELECT * FROM portfolio_summary")
        return row or {}

    def get_vendor_profile(self, vendor_id: str) -> dict:
        """Full profile for one vendor — joins all tables."""
        vendor = self.fetch_one(
            "SELECT * FROM vendors WHERE vendor_id = %s", (vendor_id,)
        )
        if not vendor:
            return {}

        risk = self.fetch_one("""
            SELECT * FROM latest_risk_scores WHERE vendor_id = %s
        """, (vendor_id,))

        segment = self.fetch_one("""
            SELECT * FROM latest_segments WHERE vendor_id = %s
        """, (vendor_id,))

        explanation = self.fetch_one("""
            SELECT * FROM latest_explanations WHERE vendor_id = %s
        """, (vendor_id,))

        contracts = self.fetch_all("""
            SELECT * FROM contracts WHERE vendor_id = %s
            ORDER BY contract_end ASC
        """, (vendor_id,))

        news = self.fetch_all("""
            SELECT title, source_name, published_at,
                   sentiment_score, disruption_type, url
            FROM vendor_news
            WHERE vendor_id = %s
            ORDER BY published_at DESC LIMIT 5
        """, (vendor_id,))

        return {
            "vendor":      vendor,
            "risk":        risk,
            "segment":     segment,
            "explanation": explanation,
            "contracts":   contracts,
            "news":        news,
        }


# ─────────────────────────────────────────────────────────────────────────────
# MODULE-LEVEL CONVENIENCE FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

@contextmanager
def get_db():
    """
    Context manager for one-off queries.

    Usage:
        from db.db_client import get_db

        with get_db() as db:
            df = db.fetch_df("SELECT * FROM vendors")
    """
    db = DBClient()
    db.connect()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# SELF-TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    print()
    print("=" * 60)
    print("  SRSID DB Client — Connection Test")
    print("=" * 60)

    try:
        with DBClient() as db:

            # 1. Basic connection
            version = db.scalar("SELECT version()")
            print(f"\n  ✅ Connected to: {version[:60]}...")

            # 2. Check all tables exist
            print("\n  Tables:")
            expected = ["vendors","transactions","delivery_events",
                        "contracts","risk_scores","segments",
                        "explanations","vendor_news"]
            for t in expected:
                count = db.row_count(t)
                print(f"    {'✅' if db.table_exists(t) else '❌'} "
                      f"{t:<25} {count:>8,} rows")

            # 3. Check views
            print("\n  Views:")
            for v in ["portfolio_summary","latest_risk_scores",
                      "latest_segments","latest_explanations"]:
                exists = db.scalar("""
                    SELECT COUNT(*) FROM information_schema.views
                    WHERE table_name = %s
                """, (v,))
                print(f"    {'✅' if exists else '❌'} {v}")

            # 4. Portfolio summary
            summary = db.get_portfolio_summary()
            print(f"\n  Portfolio summary:")
            if summary:
                for k, v in summary.items():
                    print(f"    {k:<30} {v}")
            else:
                print("    (empty — no data loaded yet)")

        print()
        print("  ✅ All checks passed — DB client is ready")
        print()
        print("  Next: run ingestion/sap_loader.py to load SAP data")
        print("=" * 60)

    except Exception as e:
        print(f"\n  ❌ Connection failed: {e}")
        print("  Check: Is Postgres running?  →  pg_isready")
        print("  Check: Did you run db/schema.sql?")
        print("=" * 60)
