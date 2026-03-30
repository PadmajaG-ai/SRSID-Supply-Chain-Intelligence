"""
Phase 3 Pipeline Runner
========================
Run all Phase 3 scripts in sequence with a single command.

Usage:
    python run_phase3.py

Or run individual steps:
    python run_phase3.py --step feature_engineering
    python run_phase3.py --step risk_prediction
    python run_phase3.py --step segmentation
    python run_phase3.py --step forecasting
    python run_phase3.py --step recommendation_anomaly
"""

import subprocess
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

SCRIPTS = {
    "feature_engineering":    "phase3_feature_engineering.py",
    "risk_prediction":        "phase3_risk_prediction.py",
    "segmentation":           "phase3_supplier_segmentation.py",
    "forecasting":            "phase3_disruption_forecasting.py",
    "recommendation_anomaly": "phase3_recommendation_anomaly.py",
    "explainability":         "phase3_explainability.py",
}

SCRIPT_DIR = Path(__file__).parent


def run_script(name: str, script: str) -> bool:
    path = SCRIPT_DIR / script
    if not path.exists():
        log.error(f"Script not found: {path}")
        return False

    log.info(f"\n{'='*60}")
    log.info(f"RUNNING: {name.upper()}")
    log.info(f"{'='*60}")

    result = subprocess.run(
        [sys.executable, str(path)],
        capture_output=False,
        text=True,
    )

    if result.returncode == 0:
        log.info(f"✅ {name} completed successfully")
        return True
    else:
        log.error(f"❌ {name} failed (exit code {result.returncode})")
        return False


def main():
    parser = argparse.ArgumentParser(description="Phase 3 Pipeline Runner")
    parser.add_argument(
        "--step",
        choices=list(SCRIPTS.keys()) + ["all"],
        default="all",
        help="Which step to run (default: all)",
    )
    args = parser.parse_args()

    start = datetime.now()
    log.info(f"Phase 3 Pipeline started at {start.strftime('%Y-%m-%d %H:%M:%S')}")

    if args.step == "all":
        steps = list(SCRIPTS.items())
    else:
        steps = [(args.step, SCRIPTS[args.step])]

    results = {}
    for name, script in steps:
        results[name] = run_script(name, script)
        if not results[name] and args.step == "all":
            log.warning(f"Step '{name}' failed — continuing with remaining steps...")

    elapsed = (datetime.now() - start).seconds

    log.info(f"\n{'='*60}")
    log.info("PHASE 3 PIPELINE SUMMARY")
    log.info(f"{'='*60}")
    for name, success in results.items():
        status = "✅" if success else "❌"
        log.info(f"  {status} {name}")

    passed = sum(results.values())
    log.info(f"\n{passed}/{len(results)} steps completed successfully")
    log.info(f"Total time: {elapsed}s")

    if passed == len(results):
        log.info("\n🎉 All Phase 3 steps complete!")
        log.info("Next: streamlit run phase3_scripts/phase3_dashboard.py")
    else:
        log.warning("\nSome steps failed. Check individual logs.")

    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
