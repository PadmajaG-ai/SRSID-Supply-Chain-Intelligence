"""
SRSID  —  run_pipeline.py
===========================
Master orchestrator. Runs every step of the SRSID pipeline in order,
with options to skip, resume from, or run only specific steps.

Usage:
    python run_pipeline.py                        # full pipeline
    python run_pipeline.py --from features        # resume from features step
    python run_pipeline.py --only risk_model      # single step
    python run_pipeline.py --skip news            # skip news ingestion
    python run_pipeline.py --dry-run              # validate only, no writes

Steps (in order):
    1  schema          Create / migrate database tables
    2  sap             Load SAP data → Postgres
    3  spend           Spend analytics (SUM%, maverick, HHI)
    4  features        Feature engineering
    5  risk_model      Train risk prediction model
    6  segmentation    Kraljic + ABC + K-Means
    7  explainability  SHAP explanations + feature importance
    8  recommendations Alternative suppliers + anomaly detection
    9  news            News ingestion (NewsAPI / Guardian / GDELT)
"""

import sys
import argparse
import subprocess
import time
import logging
from pathlib import Path
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# STEP DEFINITIONS
# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA RUNNER  — pure Python, no psql CLI needed (works on Windows)
# ─────────────────────────────────────────────────────────────────────────────

def run_schema(dry_run: bool) -> bool:
    """Execute schema.sql and schema_ext.sql via psycopg2 — no psql CLI needed."""
    if dry_run:
        log.info("  [DRY RUN] Would apply db/schema.sql + db/schema_ext.sql")
        return True

    sys.path.insert(0, str(Path(__file__).parent))
    try:
        from db.db_client import DBClient
    except ImportError:
        log.error("  Cannot import DBClient — ensure db/db_client.py exists.")
        return False

    # Check if tables already exist (skip-safe on re-runs)
    try:
        with DBClient() as db:
            exists = db.table_exists("vendors")
            if exists:
                log.info("  Tables already exist — skipping schema (use --from schema to force re-apply)")
                return True
    except Exception as e:
        log.error(f"  Cannot connect to database: {e}")
        log.error("  For Supabase: check DB_HOST/DB_USER/DB_PASSWORD in .env or Streamlit secrets")
        return False

    sql_files = [Path("db/schema.sql"), Path("db/schema_ext.sql")]
    for sql_file in sql_files:
        if not sql_file.exists():
            log.warning(f"  {sql_file} not found — skipping")
            continue
        log.info(f"  Applying {sql_file}...")
        try:
            with DBClient() as db:
                sql = sql_file.read_text(encoding="utf-8")
                statements = [s.strip() for s in sql.split(";")
                              if s.strip() and not s.strip().startswith("--")]
                ok, skip = 0, 0
                for stmt in statements:
                    if not stmt:
                        continue
                    try:
                        db.execute(stmt)
                        db.conn.commit()
                        ok += 1
                    except Exception as e:
                        db.conn.rollback()
                        if any(kw in str(e).lower() for kw in
                               ["already exists", "duplicate"]):
                            skip += 1
                        else:
                            log.warning(f"    Statement warning: {e}")
                            skip += 1
            log.info(f"  ✅ {sql_file.name}: {ok} statements applied, {skip} skipped")
        except Exception as e:
            log.error(f"  ❌ Failed to apply {sql_file}: {e}")
            return False
    return True


STEPS = [
    {
        "name":     "schema",
        "label":    "Database schema",
        "fn":       run_schema,
        "timeout":  120,
        "critical": True,    # ← Critical: if tables don't exist, nothing else works
    },
    {
        "name":    "sap",
        "label":   "SAP data loader",
        "cmd":     [sys.executable, "ingestion/sap_loader.py"],
        "timeout": 600,
        "critical": True,
    },
    {
        "name":    "spend",
        "label":   "Spend analytics",
        "cmd":     [sys.executable, "ml/spend_analytics.py"],
        "timeout": 120,
        "critical": False,
    },
    {
        "name":    "features",
        "label":   "Feature engineering",
        "cmd":     [sys.executable, "ml/features.py"],
        "timeout": 180,
        "critical": True,
    },
    {
        "name":    "risk_model",
        "label":   "Risk prediction model",
        "cmd":     [sys.executable, "ml/risk_model.py"],
        "timeout": 300,
        "critical": True,
    },
    {
        "name":    "segmentation",
        "label":   "Supplier segmentation",
        "cmd":     [sys.executable, "ml/segmentation.py"],
        "timeout": 120,
        "critical": False,
    },
    {
        "name":    "explainability",
        "label":   "SHAP explainability",
        "cmd":     [sys.executable, "ml/explainability.py"],
        "timeout": 300,
        "critical": False,
    },
    {
        "name":    "recommendations",
        "label":   "Alternatives + anomaly detection",
        "cmd":     [sys.executable, "ml/recommendations.py"],
        "timeout": 300,
        "critical": False,
    },
    {
        "name":    "news",
        "label":   "News ingestion (sample: 200 vendors)",
        # Pipeline runs a 200-vendor sample (~20 min).
        # For full 2,541-vendor run: python news_ingestion.py --source gdelt --days 30
        "cmd":     [sys.executable, "news_ingestion.py",
                    "--source", "gdelt", "--days", "30", "--limit", "200"],
        "timeout": 1800,      # 30 min for 200 vendors
        "critical": False,
    },
]

STEP_NAMES = [s["name"] for s in STEPS]


# ─────────────────────────────────────────────────────────────────────────────
# RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def run_step(step: dict, dry_run: bool) -> bool:
    """Run one pipeline step. Returns True if successful."""
    name    = step["name"]
    label   = step["label"]
    timeout = step.get("timeout", 300)

    log.info(f"\n{'='*60}")
    log.info(f"  STEP: {label.upper()}")
    log.info(f"{'='*60}")

    # ── Python function step (no subprocess) ──────────────────────────────────
    if "fn" in step:
        t0 = time.time()
        try:
            ok = step["fn"](dry_run)
            elapsed = time.time() - t0
            if ok:
                log.info(f"  ✅ {label} completed in {elapsed:.1f}s")
            else:
                log.error(f"  ❌ {label} failed")
            return ok
        except Exception as e:
            log.error(f"  ❌ {label} error: {e}")
            return False

    # ── Subprocess step ───────────────────────────────────────────────────────
    if dry_run:
        log.info(f"  [DRY RUN] Would run: {' '.join(step['cmd'])}")
        if step.get("then"):
            log.info(f"  [DRY RUN] Then run: {' '.join(step['then'])}")
        return True

    cmds = [step["cmd"]]
    if step.get("then"):
        cmds.append(step["then"])

    for cmd in cmds:
        log.info(f"  Running: {' '.join(cmd)}")
        t0 = time.time()
        try:
            result = subprocess.run(cmd, timeout=timeout, text=True)
            elapsed = time.time() - t0
            if result.returncode == 0:
                log.info(f"  ✅ {label} completed in {elapsed:.1f}s")
            else:
                log.error(f"  ❌ {label} failed (exit {result.returncode})")
                return False
        except subprocess.TimeoutExpired:
            log.error(f"  ❌ {label} timed out after {timeout}s")
            return False
        except FileNotFoundError:
            log.error(f"  ❌ Command not found: {cmd[0]}")
            return False
        except Exception as e:
            log.error(f"  ❌ {label} error: {e}")
            return False

    return True


def main():
    parser = argparse.ArgumentParser(
        description="SRSID pipeline orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Available steps: {', '.join(STEP_NAMES)}"
    )
    parser.add_argument("--from",  dest="from_step", metavar="STEP",
                        help="Resume from this step (inclusive)")
    parser.add_argument("--only",  metavar="STEP",
                        help="Run only this single step")
    parser.add_argument("--skip",  metavar="STEP", action="append", default=[],
                        help="Skip this step (repeatable)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print commands without executing")
    args = parser.parse_args()

    # Validate step names
    all_named = [args.from_step, args.only] + args.skip
    for name in all_named:
        if name and name not in STEP_NAMES:
            log.error(f"Unknown step '{name}'. Valid: {STEP_NAMES}")
            sys.exit(1)

    # Determine which steps to run
    steps_to_run = list(STEPS)
    if args.only:
        steps_to_run = [s for s in STEPS if s["name"] == args.only]
    elif args.from_step:
        idx = STEP_NAMES.index(args.from_step)
        steps_to_run = STEPS[idx:]
    if args.skip:
        steps_to_run = [s for s in steps_to_run if s["name"] not in args.skip]

    # ── Banner ─────────────────────────────────────────────────────────────
    log.info("")
    log.info("=" * 60)
    log.info("  SRSID Pipeline" + (" [DRY RUN]" if args.dry_run else ""))
    log.info(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  Steps to run: {', '.join(s['name'] for s in steps_to_run)}")
    log.info("=" * 60)

    # ── Run ────────────────────────────────────────────────────────────────
    results = {}
    t_start = time.time()

    for step in steps_to_run:
        ok = run_step(step, dry_run=args.dry_run)
        results[step["name"]] = ok

        if not ok and step["critical"]:
            log.error(f"\n❌ Critical step '{step['name']}' failed — aborting pipeline.")
            log.error("Fix the issue above and re-run with:")
            log.error(f"  python run_pipeline.py --from {step['name']}")
            sys.exit(1)
        elif not ok:
            log.warning(f"  ⚠️  Non-critical step '{step['name']}' failed — continuing.")

    # ── Summary ────────────────────────────────────────────────────────────
    elapsed = time.time() - t_start
    passed  = sum(1 for ok in results.values() if ok)
    failed  = sum(1 for ok in results.values() if not ok)

    log.info("\n" + "=" * 60)
    log.info("  PIPELINE COMPLETE")
    log.info("=" * 60)
    log.info(f"  Total time : {elapsed/60:.1f} min")
    log.info(f"  Passed     : {passed}/{len(results)}")
    if failed:
        log.info(f"  Failed     : {failed}")
        for name, ok in results.items():
            if not ok:
                log.info(f"    - {name}")

    if not failed:
        log.info("\n  Next steps:")
        log.info("    streamlit run app/dashboard.py")
        log.info("    streamlit run app/chatbot.py")


if __name__ == "__main__":
    main()
