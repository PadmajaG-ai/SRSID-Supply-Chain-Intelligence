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
STEPS = [
    {
        "name":    "schema",
        "label":   "Database schema",
        "cmd":     ["./psql", "-U", "srsid_user", "-d", "srsid_db",
                    "-h", "localhost", "-f", "db/schema.sql"],
        "then":    ["./psql", "-U", "srsid_user", "-d", "srsid_db",
                    "-h", "localhost", "-f", "db/schema_ext.sql"],
        "timeout": 60,
        "critical": True,
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
        "label":   "News ingestion",
        "cmd":     [sys.executable, "news_ingestion.py",
                    "--source", "gdelt", "--days", "30"],
        "timeout": 600,
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
    timeout = step["timeout"]

    log.info(f"\n{'='*60}")
    log.info(f"  STEP: {label.upper()}")
    log.info(f"{'='*60}")

    if dry_run:
        log.info(f"  [DRY RUN] Would run: {' '.join(step['cmd'])}")
        if step.get("then"):
            log.info(f"  [DRY RUN] Then run: {' '.join(step['then'])}")
        return True

    # Primary command
    cmds = [step["cmd"]]
    if step.get("then"):
        cmds.append(step["then"])

    for cmd in cmds:
        log.info(f"  Running: {' '.join(cmd)}")
        t0 = time.time()
        try:
            result = subprocess.run(
                cmd,
                timeout=timeout,
                capture_output=False,   # show output live
                text=True,
            )
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
