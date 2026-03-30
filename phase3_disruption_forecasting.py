"""
Phase 3: Disruption Forecasting
=================================
Purpose: Forecast supplier disruption probability over next 30/60/90 days.

Approach:
  - Per-supplier: Poisson-rate model from historical disruption frequency
  - Portfolio-level: Rolling time series + trend extrapolation
  - If Prophet available: Prophet model for time-series seasonality
  - Output: probability scores per supplier × horizon

Inputs:
    - phase3_features/supplier_features.csv
    - disruptions data (from phase1_tables or phase2_outputs)

Outputs:
    - phase3_forecasting/disruption_forecast.csv
    - phase3_forecasting/portfolio_forecast.csv
    - phase3_forecasting/early_warning_alerts.csv
    - phase3_forecasting/forecast_summary.json
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
import logging
import warnings
from datetime import datetime, timedelta
from scipy import stats
warnings.filterwarnings("ignore")

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("phase3_forecasting.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
FEATURES_DIR = Path("phase3_features")
PHASE1_DIR   = Path("phase1_tables")
PHASE2_DIR   = Path("phase2_outputs")
OUTPUT_DIR   = Path("phase3_forecasting")
OUTPUT_DIR.mkdir(exist_ok=True)

HORIZONS_DAYS = [30, 60, 90]

# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────

def load_features() -> pd.DataFrame:
    path = FEATURES_DIR / "supplier_features.csv"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run phase3_feature_engineering.py first.")
    return pd.read_csv(path)


def load_disruptions() -> pd.DataFrame:
    candidates = [
        PHASE2_DIR / "disruptions_mapped.csv",
        PHASE1_DIR / "disruptions_combined.csv",
        Path(".")   / "disruptions.csv",
    ]
    for p in candidates:
        if p.exists():
            df = pd.read_csv(p)
            log.info(f"Loaded disruptions from {p}: {len(df)} rows")
            return df
    log.warning("No disruption file found — will use simulated time series.")
    return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# POISSON RATE MODEL (per supplier)
# ─────────────────────────────────────────────────────────────────────────────

def poisson_disruption_probability(rate_per_day: float, days: int) -> float:
    """
    P(at least 1 disruption in `days`) given Poisson rate λ per day.
    P = 1 - e^(-λ * days)
    """
    lam = rate_per_day * days
    return 1 - np.exp(-lam)


def build_supplier_forecast(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each supplier, estimate disruption probability over 30/60/90 days
    using a Poisson model seeded from historical frequency.
    """
    log.info("Building per-supplier Poisson disruption forecasts...")

    # Disruption rate: events per day
    # disruption_frequency is events/year in feature engineering
    df["rate_per_day"] = df.get(
        "disruption_frequency", pd.Series(0.5, index=df.index)
    ).fillna(0.5) / 365.0

    # Adjust rate by composite risk score (higher risk → higher rate multiplier)
    risk_multiplier = 1 + df.get(
        "composite_risk_score", pd.Series(0.5, index=df.index)
    ).fillna(0.5)
    df["adjusted_rate_per_day"] = df["rate_per_day"] * risk_multiplier

    # Compute probability for each horizon
    for h in HORIZONS_DAYS:
        col = f"disruption_prob_{h}d"
        df[col] = df["adjusted_rate_per_day"].apply(
            lambda r: round(poisson_disruption_probability(r, h), 4)
        )

    # Risk tier per horizon
    df["forecast_risk_30d"] = pd.cut(
        df["disruption_prob_30d"],
        bins=[-0.01, 0.20, 0.50, 1.01],
        labels=["Low", "Medium", "High"],
    )

    # Days until next likely disruption (expected value under Poisson = 1/λ)
    df["expected_days_to_disruption"] = (
        1 / (df["adjusted_rate_per_day"].clip(lower=1e-6))
    ).clip(upper=3650).round(0).astype(int)

    # Early warning flag: high probability in 30 days
    df["early_warning_flag"] = (df["disruption_prob_30d"] >= 0.50).astype(int)
    df["early_warning_reason"] = np.where(
        df["early_warning_flag"] == 1,
        "Disruption probability >50% in next 30 days based on historical pattern",
        ""
    )

    log.info(f"  Early warnings generated: {df['early_warning_flag'].sum()}")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# PORTFOLIO TIME SERIES (monthly)
# ─────────────────────────────────────────────────────────────────────────────

def build_portfolio_time_series(disruptions: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate disruption events into monthly counts for the whole portfolio.
    Project forward using trend + seasonality.
    """
    log.info("Building portfolio-level time series forecast...")

    today = datetime.today()

    if disruptions.empty:
        # Simulate a plausible 3-year history
        log.warning("Simulating portfolio disruption history (no real data).")
        months = pd.date_range(end=today, periods=36, freq="MS")
        base = np.array([
            4, 3, 5, 4, 6, 5, 8, 7, 5, 4, 3, 6,   # Year 1
            5, 4, 6, 5, 7, 8, 9, 8, 6, 5, 4, 7,   # Year 2
            6, 5, 8, 7, 9, 10, 12, 11, 8, 7, 6, 9  # Year 3
        ])
        noise = np.random.default_rng(42).integers(-1, 2, size=36)
        history = pd.DataFrame({"ds": months, "y": np.clip(base + noise, 1, None)})
    else:
        # Parse dates
        date_col = next((c for c in disruptions.columns if "date" in c.lower()), None)
        if not date_col:
            log.warning("No date column in disruptions — simulating.")
            months = pd.date_range(end=today, periods=24, freq="MS")
            history = pd.DataFrame({"ds": months, "y": np.random.randint(3, 12, size=24)})
        else:
            disruptions[date_col] = pd.to_datetime(disruptions[date_col], errors="coerce")
            disruptions = disruptions.dropna(subset=[date_col])
            disruptions["month"] = disruptions[date_col].dt.to_period("M").dt.to_timestamp()
            history = (disruptions.groupby("month").size()
                       .reset_index(name="y")
                       .rename(columns={"month": "ds"}))

    history = history.sort_values("ds").reset_index(drop=True)

    # Prophet forecast (if available)
    if PROPHET_AVAILABLE and len(history) >= 12:
        log.info("Using Prophet for portfolio forecast...")
        m = Prophet(yearly_seasonality=True, weekly_seasonality=False,
                    daily_seasonality=False, changepoint_prior_scale=0.15)
        m.fit(history)
        future = m.make_future_dataframe(periods=3, freq="MS")
        forecast = m.predict(future)
        future_rows = forecast[forecast["ds"] > history["ds"].max()][
            ["ds", "yhat", "yhat_lower", "yhat_upper"]
        ].copy()
        future_rows.columns = ["month", "predicted_disruptions",
                               "pred_lower", "pred_upper"]
    else:
        # Simple linear trend extrapolation
        log.info("Using linear trend for portfolio forecast (Prophet not installed).")
        y = history["y"].values.astype(float)
        x = np.arange(len(y))
        slope, intercept, r, p, se = stats.linregress(x, y)

        future_months = [
            history["ds"].iloc[-1] + pd.DateOffset(months=i)
            for i in range(1, 4)
        ]
        future_x = np.arange(len(y), len(y) + 3)
        preds = np.clip(slope * future_x + intercept, 0, None)

        future_rows = pd.DataFrame({
            "month":                  future_months,
            "predicted_disruptions":  preds.round(1),
            "pred_lower":             np.clip(preds - se * 2, 0, None).round(1),
            "pred_upper":             (preds + se * 2).round(1),
        })
        log.info(f"  Trend: slope={slope:.3f}, R²={r**2:.3f}")

    future_rows["forecast_type"] = "Prophet" if PROPHET_AVAILABLE else "LinearTrend"
    log.info(f"  Next 3 months forecast: {future_rows['predicted_disruptions'].tolist()}")
    return history, future_rows


# ─────────────────────────────────────────────────────────────────────────────
# EARLY WARNING ALERTS
# ─────────────────────────────────────────────────────────────────────────────

def build_early_warning_report(df: pd.DataFrame) -> pd.DataFrame:
    """Extract high-priority suppliers for alert report."""
    name_col = next((c for c in df.columns if "name" in c.lower() or "supplier" in c.lower()),
                    df.columns[0])

    alert_cols = [
        name_col,
        "disruption_prob_30d", "disruption_prob_60d", "disruption_prob_90d",
        "forecast_risk_30d",
        "expected_days_to_disruption",
        "composite_risk_score",
        "criticality_index",
        "early_warning_flag",
        "early_warning_reason",
    ]
    alert_cols = [c for c in alert_cols if c in df.columns]

    alerts = df[df["early_warning_flag"] == 1][alert_cols].copy()
    alerts = alerts.sort_values("disruption_prob_30d", ascending=False)

    log.info(f"Early warning alerts: {len(alerts)} suppliers flagged")
    return alerts


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("PHASE 3: DISRUPTION FORECASTING")
    log.info("=" * 60)

    df          = load_features()
    disruptions = load_disruptions()

    # ── Per-supplier Poisson model ────────────────────────────────────────────
    df = build_supplier_forecast(df)

    # ── Portfolio time series ─────────────────────────────────────────────────
    history, future_forecast = build_portfolio_time_series(disruptions)

    # ── Early warning report ──────────────────────────────────────────────────
    alerts = build_early_warning_report(df)

    # ── Save outputs ──────────────────────────────────────────────────────────
    name_col = next((c for c in df.columns if "name" in c.lower() or "supplier" in c.lower()),
                    df.columns[0])

    forecast_cols = [
        name_col,
        "disruption_prob_30d", "disruption_prob_60d", "disruption_prob_90d",
        "forecast_risk_30d",
        "expected_days_to_disruption",
        "composite_risk_score",
        "early_warning_flag",
    ]
    forecast_cols = [c for c in forecast_cols if c in df.columns]
    df[forecast_cols].sort_values("disruption_prob_30d", ascending=False).to_csv(
        OUTPUT_DIR / "disruption_forecast.csv", index=False
    )
    log.info("Saved disruption_forecast.csv")

    pd.concat([history.assign(type="historical"), future_forecast.rename(columns={"month": "ds"}).assign(type="forecast")],
              ignore_index=True).to_csv(OUTPUT_DIR / "portfolio_forecast.csv", index=False)
    log.info("Saved portfolio_forecast.csv")

    if not alerts.empty:
        alerts.to_csv(OUTPUT_DIR / "early_warning_alerts.csv", index=False)
        log.info("Saved early_warning_alerts.csv")

    summary = {
        "total_suppliers":       len(df),
        "early_warning_count":   int(df["early_warning_flag"].sum()),
        "high_risk_30d_count":   int((df["disruption_prob_30d"] >= 0.5).sum()),
        "avg_prob_30d":          round(float(df["disruption_prob_30d"].mean()), 4),
        "avg_prob_90d":          round(float(df["disruption_prob_90d"].mean()), 4),
        "portfolio_next_3_months": future_forecast["predicted_disruptions"].tolist(),
        "forecast_method":       "Prophet" if PROPHET_AVAILABLE else "LinearTrend+Poisson",
    }
    with open(OUTPUT_DIR / "forecast_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    log.info("\n" + "=" * 60)
    log.info("DISRUPTION FORECASTING COMPLETE")
    log.info("=" * 60)
    log.info(f"  Suppliers forecasted  : {summary['total_suppliers']}")
    log.info(f"  Early warnings        : {summary['early_warning_count']}")
    log.info(f"  High-risk (30d)       : {summary['high_risk_30d_count']}")
    log.info(f"  Portfolio forecast    : {summary['portfolio_next_3_months']}")
    log.info(f"  Method                : {summary['forecast_method']}")
    log.info("\nOutputs saved to phase3_forecasting/")
    log.info("  - disruption_forecast.csv")
    log.info("  - portfolio_forecast.csv")
    log.info("  - early_warning_alerts.csv")
    log.info("  - forecast_summary.json")
    log.info("Ready for recommendation engine!")


if __name__ == "__main__":
    main()
