"""
Phase 3: Streamlit Dashboard
==============================
Unified interactive dashboard for all Phase 3 ML outputs.

Run with:
    streamlit run phase3_dashboard.py

Features:
  - Executive summary KPI cards
  - Risk prediction heatmap + rankings
  - Supplier segmentation scatter (K-Means + Kraljic)
  - Disruption forecast (30/60/90 days)
  - Alternative supplier finder
  - Anomaly detector
  - Spend analytics (ABC, concentration)
  - Export to CSV
"""

import streamlit as st
import pandas as pd
import numpy as np
import json
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")

try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    PLOTLY = True
except ImportError:
    PLOTLY = False
    st.warning("Install plotly for richer charts: pip install plotly")

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SRSID — Supplier Risk & Spend Intelligence",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Colour palette ────────────────────────────────────────────────────────────
COLORS = {
    "High":        "#E74C3C",
    "Medium":      "#F39C12",
    "Low":         "#27AE60",
    "Critical":    "#C0392B",
    "Monitor":     "#E67E22",
    "Invest":      "#2980B9",
    "Optimise":    "#1ABC9C",
    "Strategic":   "#8E44AD",
    "Leverage":    "#2ECC71",
    "Bottleneck":  "#E74C3C",
    "Tactical":    "#95A5A6",
}

# ── Data loading ──────────────────────────────────────────────────────────────

@st.cache_data
def load_csv(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.exists():
        return pd.read_csv(p)
    return pd.DataFrame()


@st.cache_data
def load_json(path: str) -> dict:
    p = Path(path)
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return {}


def load_all():
    return {
        "features":        load_csv("phase3_features/supplier_features.csv"),
        "risk_pred":       load_csv("phase3_risk_predictions/risk_predictions.csv"),
        "segments":        load_csv("phase3_segmentation/supplier_segments.csv"),
        "forecast":        load_csv("phase3_forecasting/disruption_forecast.csv"),
        "portfolio_fc":    load_csv("phase3_forecasting/portfolio_forecast.csv"),
        "early_warning":   load_csv("phase3_forecasting/early_warning_alerts.csv"),
        "alternatives":    load_csv("phase3_recommendations/alternative_suppliers.csv"),
        "anomalies":       load_csv("phase3_anomalies/anomaly_report.csv"),
        # XAI outputs
        "xai_explanations": load_csv("phase3_xai/supplier_explanations.csv"),
        "xai_global":       load_csv("phase3_xai/global_importance.csv"),
        "xai_shap":         load_csv("phase3_xai/shap_values.csv"),
        "xai_lime":         load_csv("phase3_xai/lime_explanations.csv"),
        # Summaries
        "feature_sum":     load_json("phase3_features/feature_summary.json"),
        "model_eval":      load_json("phase3_risk_predictions/model_evaluation.json"),
        "seg_sum":         load_json("phase3_segmentation/segmentation_summary.json"),
        "forecast_sum":    load_json("phase3_forecasting/forecast_summary.json"),
        "xai_sum":         load_json("phase3_xai/xai_summary.json"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_name_col(df: pd.DataFrame) -> str:
    return next((c for c in df.columns
                 if "name" in c.lower() or "supplier" in c.lower()), df.columns[0])


def risk_color(tier):
    return COLORS.get(str(tier), "#95A5A6")


def metric_card(label, value, delta=None, color="#2C3E50"):
    delta_html = f"<small style='color:gray'>{delta}</small>" if delta else ""
    st.markdown(
        f"""
        <div style='background:{color}22; border-left:4px solid {color};
                    padding:12px 16px; border-radius:6px; margin-bottom:8px'>
            <div style='font-size:0.8rem; color:#666'>{label}</div>
            <div style='font-size:1.6rem; font-weight:700; color:{color}'>{value}</div>
            {delta_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR FILTERS
# ─────────────────────────────────────────────────────────────────────────────

def render_sidebar(data: dict):
    st.sidebar.image("https://img.icons8.com/fluency/48/supply-chain.png", width=48)
    st.sidebar.title("SRSID v3.0")
    st.sidebar.caption("Supplier Risk & Spend Intelligence")
    st.sidebar.divider()

    features = data["features"]
    filters  = {}

    if not features.empty:
        # Risk tier filter
        if "risk_label_3class" in features.columns:
            tiers = ["All"] + sorted(features["risk_label_3class"].dropna().unique().tolist())
            filters["risk_tier"] = st.sidebar.selectbox("Risk Tier", tiers)
        else:
            filters["risk_tier"] = "All"

        # Kraljic filter
        k_col = next((c for c in features.columns if "kraljic" in c.lower() or "segment" in c.lower()), None)
        if k_col:
            quadrants = ["All"] + sorted(features[k_col].dropna().unique().tolist())
            filters["quadrant"] = st.sidebar.selectbox("Kraljic Quadrant", quadrants)
        else:
            filters["quadrant"] = "All"

        # Industry filter
        industry_col = next((c for c in features.columns if "industry" in c.lower()), None)
        if industry_col:
            industries = ["All"] + sorted(features[industry_col].dropna().unique().tolist())
            filters["industry"] = st.sidebar.selectbox("Industry", industries)
        else:
            filters["industry"] = "All"

        # Spend range
        spend_col = "total_annual_spend"
        if spend_col in features.columns:
            mn = float(features[spend_col].min())
            mx = float(features[spend_col].max())
            if mn < mx:
                filters["spend_range"] = st.sidebar.slider(
                    "Annual Spend Range ($)",
                    min_value=mn, max_value=mx,
                    value=(mn, mx),
                    format="$%.0f",
                )
            else:
                filters["spend_range"] = (mn, mx)
        else:
            filters["spend_range"] = (0, 1e12)

    st.sidebar.divider()
    st.sidebar.caption("📁 Data loaded from phase3_*/")
    return filters


def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    if df.empty:
        return df

    if filters.get("risk_tier", "All") != "All":
        risk_cols = [c for c in df.columns if "risk" in c.lower() and "label" in c.lower()]
        if risk_cols:
            df = df[df[risk_cols[0]].astype(str) == filters["risk_tier"]]

    if filters.get("quadrant", "All") != "All":
        k_col = next((c for c in df.columns if "kraljic" in c.lower() or "segment" in c.lower()), None)
        if k_col:
            df = df[df[k_col].astype(str) == filters["quadrant"]]

    if filters.get("industry", "All") != "All":
        ind_col = next((c for c in df.columns if "industry" in c.lower()), None)
        if ind_col:
            df = df[df[ind_col].astype(str).str.contains(filters["industry"], case=False, na=False)]

    spend_col = "total_annual_spend"
    if spend_col in df.columns and "spend_range" in filters:
        lo, hi = filters["spend_range"]
        df = df[(df[spend_col] >= lo) & (df[spend_col] <= hi)]

    return df


# ─────────────────────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────────────────────

def tab_overview(data, filters):
    st.header("📊 Executive Overview")

    feat = data["features"]
    feat_sum = data["feature_sum"]
    fc_sum = data["forecast_sum"]
    anom = data["anomalies"]

    # KPI row
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        metric_card("Total Suppliers", feat_sum.get("total_suppliers", len(feat)), color="#2C3E50")
    with c2:
        hr = feat_sum.get("high_risk_count", 0)
        metric_card("High Risk Suppliers", hr, color="#E74C3C")
    with c3:
        ew = fc_sum.get("early_warning_count", 0)
        metric_card("Early Warnings (30d)", ew, color="#F39C12")
    with c4:
        an = anom["is_anomalous"].sum() if not anom.empty and "is_anomalous" in anom.columns else "—"
        metric_card("Anomalies Detected", an, color="#8E44AD")
    with c5:
        sc = feat_sum.get("spend_concentrated", 0)
        metric_card("Spend Concentrated", sc, color="#2980B9")

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Risk Distribution")
        rd = feat_sum.get("risk_distribution", {})
        if rd and PLOTLY:
            fig = px.pie(
                names=list(rd.keys()), values=list(rd.values()),
                color=list(rd.keys()),
                color_discrete_map=COLORS,
                hole=0.45,
            )
            fig.update_layout(margin=dict(t=0, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.json(rd)

    with col2:
        st.subheader("Kraljic Quadrant Distribution")
        qd = feat_sum.get("quadrant_distribution", {})
        if qd and PLOTLY:
            fig = px.bar(
                x=list(qd.keys()), y=list(qd.values()),
                color=list(qd.keys()),
                color_discrete_map=COLORS,
                labels={"x": "Quadrant", "y": "Count"},
            )
            fig.update_layout(showlegend=False, margin=dict(t=10, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.json(qd)

    # Model performance
    model_eval = data.get("model_eval", {})
    if model_eval:
        st.subheader("ML Model Performance")
        best = model_eval.get("best_model", "Unknown")
        models = model_eval.get("models", {})
        if models:
            rows = []
            for name, m in models.items():
                rows.append({
                    "Model": name,
                    "Accuracy": m.get("accuracy", "—"),
                    "F1 (weighted)": m.get("f1_weighted", "—"),
                    "AUC-ROC": m.get("auc_roc", "—"),
                    "Best": "✅" if name == best else "",
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def tab_risk(data, filters):
    st.header("⚠️ Risk Predictions")

    risk_df = data["risk_pred"]
    if risk_df.empty:
        st.info("No risk predictions found. Run phase3_risk_prediction.py first.")
        return

    risk_df = apply_filters(risk_df, filters)
    name_col = get_name_col(risk_df)

    # High risk table
    st.subheader("High Risk Suppliers")
    if "risk_tier" in risk_df.columns:
        high = risk_df[risk_df["risk_tier"] == "High"].head(20)
        st.dataframe(
            high[[name_col, "risk_tier", "risk_probability"] +
                 [c for c in ["kraljic_quadrant", "criticality_index", "disruption_count"]
                  if c in high.columns]],
            use_container_width=True, hide_index=True,
        )

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Risk Score Distribution")
        if "risk_probability" in risk_df.columns and PLOTLY:
            fig = px.histogram(risk_df, x="risk_probability", nbins=20,
                               color_discrete_sequence=["#E74C3C"])
            fig.add_vline(x=0.65, line_dash="dash", line_color="red",
                          annotation_text="High threshold")
            fig.add_vline(x=0.35, line_dash="dash", line_color="orange",
                          annotation_text="Medium threshold")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Feature Importance")
        fi_df = load_csv("phase3_risk_predictions/feature_importance.csv")
        if not fi_df.empty and PLOTLY:
            fig = px.bar(fi_df.head(10), x="importance_pct", y="feature",
                         orientation="h",
                         color="importance_pct",
                         color_continuous_scale="Reds",
                         labels={"importance_pct": "Importance (%)", "feature": ""})
            fig.update_layout(yaxis=dict(autorange="reversed"), margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)
        elif not fi_df.empty:
            st.dataframe(fi_df.head(10), use_container_width=True, hide_index=True)

    st.subheader("All Supplier Risk Scores")
    disp_cols = [name_col, "risk_tier", "risk_probability"] + \
                [c for c in ["kraljic_quadrant", "total_annual_spend", "criticality_index"]
                 if c in risk_df.columns]
    st.dataframe(risk_df[disp_cols].sort_values("risk_probability", ascending=False),
                 use_container_width=True, hide_index=True)

    # Export
    st.download_button("📥 Export Risk Predictions",
                       risk_df.to_csv(index=False).encode(),
                       "risk_predictions.csv", "text/csv")


def tab_segmentation(data, filters):
    st.header("🗂️ Supplier Segmentation")

    seg = data["segments"]
    if seg.empty:
        st.info("No segmentation data found. Run phase3_supplier_segmentation.py first.")
        return

    seg = apply_filters(seg, filters)
    name_col = get_name_col(seg)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Kraljic Matrix")
        if PLOTLY and "composite_risk_score" in seg.columns:
            fig = px.scatter(
                seg,
                x="composite_risk_score",
                y=seg.get("profit_impact_score", seg.get("criticality_index", seg["composite_risk_score"])),
                color="kraljic_segment" if "kraljic_segment" in seg.columns else None,
                color_discrete_map=COLORS,
                hover_name=name_col,
                size="total_annual_spend" if "total_annual_spend" in seg.columns else None,
                size_max=30,
                labels={"x": "Supply Risk →", "y": "Profit Impact →"},
            )
            fig.add_vline(x=seg["composite_risk_score"].median(), line_dash="dash", opacity=0.4)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("K-Means Clusters")
        if PLOTLY and "pca_x" in seg.columns:
            fig = px.scatter(
                seg,
                x="pca_x", y="pca_y",
                color="cluster_label" if "cluster_label" in seg.columns else "cluster_id",
                hover_name=name_col,
                labels={"pca_x": "PCA 1", "pca_y": "PCA 2"},
            )
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("ABC Spend Analysis")
    if "abc_class" in seg.columns and PLOTLY:
        abc_summary = seg.groupby("abc_class").agg(
            count=("abc_class", "count"),
            total_spend=("total_annual_spend", "sum") if "total_annual_spend" in seg.columns else ("abc_class", "count"),
        ).reset_index()
        fig = px.bar(abc_summary, x="abc_class", y="total_spend",
                     color="abc_class",
                     color_discrete_sequence=["#2ECC71", "#F39C12", "#E74C3C"],
                     labels={"total_spend": "Total Annual Spend ($)", "abc_class": "ABC Class"})
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Segmentation Table")
    seg_cols = [name_col,
                "kraljic_segment", "abc_class", "cluster_label",
                "risk_spend_quadrant", "composite_risk_score",
                "total_annual_spend", "strategic_action"]
    seg_cols = [c for c in seg_cols if c in seg.columns]
    st.dataframe(seg[seg_cols], use_container_width=True, hide_index=True)

    st.download_button("📥 Export Segments",
                       seg.to_csv(index=False).encode(),
                       "supplier_segments.csv", "text/csv")


def tab_forecast(data, filters):
    st.header("📈 Disruption Forecasting")

    fc = data["forecast"]
    portfolio = data["portfolio_fc"]
    ew = data["early_warning"]

    if fc.empty:
        st.info("No forecast data found. Run phase3_disruption_forecasting.py first.")
        return

    fc = apply_filters(fc, filters)
    name_col = get_name_col(fc)

    # Early warning alert box
    if not ew.empty:
        ew_count = len(ew)
        st.error(f"🚨 {ew_count} supplier(s) have disruption probability >50% in next 30 days")
        with st.expander("View Early Warning Alerts"):
            st.dataframe(ew, use_container_width=True, hide_index=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        hi_30 = int((fc["disruption_prob_30d"] >= 0.50).sum()) if "disruption_prob_30d" in fc.columns else 0
        metric_card("High Risk Next 30d", hi_30, color="#E74C3C")
    with col2:
        hi_60 = int((fc["disruption_prob_60d"] >= 0.50).sum()) if "disruption_prob_60d" in fc.columns else 0
        metric_card("High Risk Next 60d", hi_60, color="#F39C12")
    with col3:
        hi_90 = int((fc["disruption_prob_90d"] >= 0.50).sum()) if "disruption_prob_90d" in fc.columns else 0
        metric_card("High Risk Next 90d", hi_90, color="#F39C12")

    st.subheader("Portfolio Disruption Forecast")
    if not portfolio.empty and PLOTLY:
        if "type" in portfolio.columns:
            hist = portfolio[portfolio["type"] == "historical"].copy()
            fore = portfolio[portfolio["type"] == "forecast"].copy()
        else:
            hist = portfolio.head(len(portfolio) - 3).copy()
            fore = portfolio.tail(3).copy()
        fig = go.Figure()
        ds_col = "ds" if "ds" in hist.columns else hist.columns[0]
        y_col  = "y" if "y" in hist.columns else "predicted_disruptions"
        if y_col in hist.columns:
            fig.add_trace(go.Scatter(x=hist[ds_col], y=hist[y_col],
                                     name="Historical", line=dict(color="#2C3E50")))
        if not fore.empty and "predicted_disruptions" in fore.columns:
            fc_ds = "ds" if "ds" in fore.columns else "month"
            if fc_ds in fore.columns:
                fig.add_trace(go.Scatter(
                    x=fore[fc_ds], y=fore["predicted_disruptions"],
                    name="Forecast", line=dict(color="#E74C3C", dash="dash"),
                    mode="lines+markers",
                ))
                if "pred_upper" in fore.columns and "pred_lower" in fore.columns:
                    x_band = list(fore[fc_ds]) + list(fore[fc_ds])[::-1]
                    y_band = list(fore["pred_upper"]) + list(fore["pred_lower"])[::-1]
                    fig.add_trace(go.Scatter(
                        x=x_band, y=y_band,
                        fill="toself", fillcolor="rgba(231,76,60,0.1)",
                        line=dict(color="rgba(0,0,0,0)"), name="Confidence Band",
                    ))
        fig.update_layout(xaxis_title="Month", yaxis_title="Disruption Events",
                          legend=dict(orientation="h"))
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Supplier Disruption Probabilities")
    prob_cols = [name_col, "disruption_prob_30d", "disruption_prob_60d",
                 "disruption_prob_90d", "forecast_risk_30d", "expected_days_to_disruption"]
    prob_cols = [c for c in prob_cols if c in fc.columns]
    st.dataframe(fc[prob_cols].sort_values("disruption_prob_30d", ascending=False),
                 use_container_width=True, hide_index=True)


def tab_recommendations(data, filters):
    st.header("💡 Alternative Supplier Recommendations")

    alts = data["alternatives"]
    if alts.empty:
        st.info("No recommendations found. Run phase3_recommendation_anomaly.py first.")
        return

    name_col = "supplier"

    # Search
    search = st.text_input("Search by supplier name", "")
    if search:
        alts = alts[alts[name_col].str.contains(search, case=False, na=False)]

    # Show by supplier
    suppliers = alts[name_col].unique().tolist()
    selected = st.selectbox("Select a supplier to view alternatives", ["— Show All —"] + suppliers)

    if selected != "— Show All —":
        filtered = alts[alts[name_col] == selected]
    else:
        filtered = alts.head(60)

    st.dataframe(filtered, use_container_width=True, hide_index=True)
    st.download_button("📥 Export Recommendations",
                       alts.to_csv(index=False).encode(),
                       "alternative_suppliers.csv", "text/csv")


def tab_anomalies(data, filters):
    st.header("🔍 Anomaly Detection")

    anom = data["anomalies"]
    if anom.empty:
        st.info("No anomaly data found. Run phase3_recommendation_anomaly.py first.")
        return

    name_col = get_name_col(anom)
    anomalous_mask = anom["is_anomalous"].fillna(0) == 1 if "is_anomalous" in anom.columns else pd.Series(False, index=anom.index)
    anomalous = anom[anomalous_mask]

    st.metric("Total Anomalies Detected", len(anomalous))

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Anomaly Score Distribution")
        if PLOTLY and "anomaly_score" in anom.columns:
            fig = px.histogram(anom, x="anomaly_score", nbins=20,
                               color_discrete_sequence=["#8E44AD"])
            st.plotly_chart(fig, use_container_width=True)
    with col2:
        st.subheader("Top Anomalies")
        disp_cols = [name_col, "anomaly_score", "total_anomaly_flags", "rule_based_reason"]
        disp_cols = [c for c in disp_cols if c in anom.columns]
        st.dataframe(anomalous[disp_cols].head(15), use_container_width=True, hide_index=True)

    st.subheader("Full Anomaly Report")
    st.dataframe(anom.head(50), use_container_width=True, hide_index=True)
    st.download_button("📥 Export Anomaly Report",
                       anom.to_csv(index=False).encode(),
                       "anomaly_report.csv", "text/csv")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────────────────────────────────────

def tab_xai(data, filters):
    st.header("🧠 Explainable AI — Why is a Supplier High Risk?")

    xai_sum  = data.get("xai_sum", {})
    exp_df   = data["xai_explanations"]
    global_df = data["xai_global"]
    shap_df  = data["xai_shap"]
    lime_df  = data["xai_lime"]

    if exp_df.empty:
        st.warning(
            "No XAI data found. Run `python phase3_scripts/phase3_explainability.py` first.\n\n"
            "Install dependencies: `pip install shap lime`"
        )
        return

    # ── Method badges ─────────────────────────────────────────────────────────
    method = xai_sum.get("model_explained", "Unknown")
    shap_ok = xai_sum.get("shap_available", False)
    lime_ok = xai_sum.get("lime_available", False)
    agree   = xai_sum.get("method_agreement_pct")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        metric_card("Model Explained", method, color="#8E44AD")
    with c2:
        metric_card("SHAP", "✅ Active" if shap_ok else "❌ Not installed", color="#27AE60" if shap_ok else "#E74C3C")
    with c3:
        metric_card("LIME", "✅ Active" if lime_ok else "❌ Not installed", color="#27AE60" if lime_ok else "#95A5A6")
    with c4:
        agree_display = f"{float(agree):.0f}%" if agree is not None else "N/A"
        metric_card("Method Agreement", agree_display,
                    delta="SHAP & Permutation agree", color="#2980B9")

    st.divider()

    # ── SECTION 1: Global feature importance ─────────────────────────────────
    st.subheader("📊 Global Feature Importance — What Drives Risk Most?")
    st.caption("Combining SHAP (exact attribution) + Permutation Importance (accuracy-drop based). "
               "Features where both methods agree are the most trustworthy.")

    if not global_df.empty and PLOTLY:
        top_n = min(15, len(global_df))
        plot_df = global_df.head(top_n).copy()
        plot_df["color"] = plot_df["methods_agree"].map({"YES": "#E74C3C", "no": "#95A5A6"})
        plot_df["label"] = plot_df["feature_label"].fillna(plot_df["feature"])

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**SHAP — Mean |SHAP Value|** (higher = more impact)")
            if "mean_abs_shap" in plot_df.columns and plot_df["mean_abs_shap"].notna().any():
                fig = px.bar(
                    plot_df.dropna(subset=["mean_abs_shap"]).sort_values("mean_abs_shap"),
                    x="mean_abs_shap", y="label",
                    orientation="h",
                    color="methods_agree",
                    color_discrete_map={"YES": "#E74C3C", "no": "#BDC3C7"},
                    labels={"mean_abs_shap": "Mean |SHAP|", "label": ""},
                )
                fig.update_layout(
                    showlegend=True,
                    legend_title="Both Methods Agree",
                    yaxis=dict(autorange="reversed"),
                    margin=dict(t=10, b=0),
                    height=420,
                )
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Permutation Importance** (accuracy drop when feature shuffled)")
            if "perm_importance" in plot_df.columns and plot_df["perm_importance"].notna().any():
                fig = px.bar(
                    plot_df.dropna(subset=["perm_importance"]).sort_values("perm_importance"),
                    x="perm_importance", y="label",
                    orientation="h",
                    color="methods_agree",
                    color_discrete_map={"YES": "#2980B9", "no": "#BDC3C7"},
                    error_x="perm_std" if "perm_std" in plot_df.columns else None,
                    labels={"perm_importance": "Permutation Importance", "label": ""},
                )
                fig.update_layout(
                    showlegend=True,
                    legend_title="Both Methods Agree",
                    yaxis=dict(autorange="reversed"),
                    margin=dict(t=10, b=0),
                    height=420,
                )
                st.plotly_chart(fig, use_container_width=True)

        with st.expander("📋 Full Global Importance Table"):
            st.dataframe(global_df, use_container_width=True, hide_index=True)

    st.divider()

    # ── SECTION 2: Per-supplier waterfall ─────────────────────────────────────
    st.subheader("🔍 Individual Supplier Explanation")
    st.caption("Select a supplier to see exactly which factors drove their risk score and by how much.")

    name_col = "supplier_name" if "supplier_name" in exp_df.columns else exp_df.columns[0]
    supplier_list = exp_df[name_col].tolist()
    selected = st.selectbox("Select supplier", supplier_list)

    if selected:
        row = exp_df[exp_df[name_col] == selected].iloc[0]

        # Safe accessor: pandas Series rows don't support .get() like dicts
        def rget(key, default=""):
            if key not in row.index:
                return default
            val = row[key]
            return default if (val is None or (isinstance(val, float) and pd.isna(val))) else val

        tier  = rget("predicted_risk_tier", "Unknown")
        prob  = rget("risk_probability", 0)
        color = {"High": "#E74C3C", "Medium": "#F39C12", "Low": "#27AE60"}.get(str(tier), "#95A5A6")

        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            metric_card("Risk Tier", tier, color=color)
        with col2:
            metric_card("Risk Probability", f"{float(prob)*100:.1f}%", color=color)
        with col3:
            agree_val = rget("shap_perm_agreement", 0)
            confidence = "High" if float(agree_val) >= 0.67 else "Medium" if float(agree_val) >= 0.33 else "Low"
            metric_card("Explanation Confidence",
                        f"{confidence} ({float(agree_val)*100:.0f}% method agreement)",
                        color="#2980B9")

        # ── Waterfall chart from SHAP values ──────────────────────────────────
        if not shap_df.empty and PLOTLY:
            shap_row = shap_df[shap_df[name_col] == selected] if name_col in shap_df.columns else pd.DataFrame()
            if not shap_row.empty:
                feat_cols  = [c for c in shap_df.columns if c != name_col]
                shap_vals  = shap_row[feat_cols].iloc[0]
                sorted_idx = shap_vals.abs().sort_values(ascending=False).index[:10]

                FEATURE_LABELS_DASH = {
                    "financial_stability": "Financial Stability",
                    "delivery_performance": "Delivery Performance",
                    "supply_risk_score": "Supply Risk Score",
                    "profit_impact_score": "Profit Impact",
                    "performance_composite": "Overall Performance",
                    "total_annual_spend": "Annual Spend",
                    "transaction_count": "Transaction Volume",
                    "spend_pct_of_portfolio": "Spend Share",
                    "spend_concentration_flag": "Spend Concentration",
                    "disruption_count": "Disruption Count",
                    "disruption_frequency": "Disruption Frequency",
                    "days_since_last_disruption": "Days Since Disruption",
                    "high_disruption_flag": "High Disruption Flag",
                    "recency_risk": "Recent Disruption Risk",
                    "historical_risk_numeric": "Historical Risk",
                    "geo_risk_flag": "Geographic Risk",
                    "industry_risk_score": "Industry Risk",
                    "quadrant_score": "Kraljic Position",
                    "criticality_index": "Criticality Index",
                }

                waterfall_df = pd.DataFrame({
                    "feature": [FEATURE_LABELS_DASH.get(f, f) for f in sorted_idx],
                    "shap":    shap_vals[sorted_idx].values,
                }).sort_values("shap")

                waterfall_df["color"] = waterfall_df["shap"].apply(
                    lambda v: "#E74C3C" if v > 0 else "#27AE60"
                )
                waterfall_df["direction"] = waterfall_df["shap"].apply(
                    lambda v: "Increases Risk ▲" if v > 0 else "Reduces Risk ▼"
                )

                st.markdown("**SHAP Waterfall — How each feature moves the risk score**")
                fig = px.bar(
                    waterfall_df,
                    x="shap", y="feature",
                    orientation="h",
                    color="direction",
                    color_discrete_map={
                        "Increases Risk ▲": "#E74C3C",
                        "Reduces Risk ▼":  "#27AE60",
                    },
                    labels={"shap": "SHAP Value (impact on risk)", "feature": ""},
                )
                fig.add_vline(x=0, line_color="black", line_width=1)
                fig.update_layout(
                    yaxis=dict(autorange="reversed"),
                    margin=dict(t=10, b=0),
                    height=380,
                    legend_title="Effect on Risk",
                )
                st.plotly_chart(fig, use_container_width=True)

        # ── Narrative explanation ──────────────────────────────────────────────
        st.markdown("**📝 Plain-English Explanation**")
        narrative = str(rget("narrative", "Explanation not available."))
        st.markdown(narrative)

        # ── Top drivers table ──────────────────────────────────────────────────
        with st.expander("📋 Detailed Driver Table"):
            drivers = []
            for i in range(1, 4):
                feat  = rget(f"driver_{i}_feature", "")
                label = rget(f"driver_{i}_label", "")
                val   = rget(f"driver_{i}_value", "")
                shap  = rget(f"driver_{i}_shap", "")
                if feat:
                    drivers.append({"Rank": i, "Feature": label or feat,
                                    "Value": val, "SHAP Impact": shap,
                                    "Effect": "⬆ Increases Risk"})
            mit_feat = rget("mitigator_feature", "")
            if mit_feat:
                drivers.append({
                    "Rank": "M",
                    "Feature": rget("mitigator_label", mit_feat),
                    "Value":   rget("mitigator_value", ""),
                    "SHAP Impact": rget("mitigator_shap", ""),
                    "Effect": "⬇ Reduces Risk",
                })
            if drivers:
                st.dataframe(pd.DataFrame(drivers), use_container_width=True, hide_index=True)

    st.divider()

    # ── SECTION 3: LIME (high-risk suppliers) ─────────────────────────────────
    if not lime_df.empty:
        st.subheader("🔬 LIME Second Opinion — High-Risk Suppliers")
        st.caption(
            "LIME builds a local linear model around each prediction. "
            "Positive weights push toward High Risk; negative push away. "
            "Use this as a cross-check against SHAP."
        )
        lime_sup = st.selectbox("LIME — select supplier",
                                lime_df["supplier_name"].unique().tolist(),
                                key="lime_select")
        lime_row = lime_df[lime_df["supplier_name"] == lime_sup].copy()
        if not lime_row.empty and PLOTLY:
            lime_row["direction"] = lime_row["lime_weight"].apply(
                lambda v: "Increases Risk" if v > 0 else "Reduces Risk"
            )
            fig = px.bar(
                lime_row.sort_values("lime_weight"),
                x="lime_weight", y="lime_feature",
                orientation="h",
                color="direction",
                color_discrete_map={"Increases Risk": "#E74C3C", "Reduces Risk": "#27AE60"},
                labels={"lime_weight": "LIME Weight", "lime_feature": "Feature Condition"},
            )
            fig.add_vline(x=0, line_color="black", line_width=1)
            fig.update_layout(yaxis=dict(autorange="reversed"), height=280,
                              margin=dict(t=10, b=0), legend_title="")
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── SECTION 4: All suppliers table ────────────────────────────────────────
    st.subheader("📋 All Supplier Explanations")
    disp_cols = [name_col, "predicted_risk_tier", "risk_probability",
                 "driver_1_label", "driver_1_value", "driver_1_shap",
                 "driver_2_label", "driver_2_value",
                 "mitigator_label", "mitigator_value",
                 "explanation_method", "shap_perm_agreement"]
    disp_cols = [c for c in disp_cols if c in exp_df.columns]
    st.dataframe(exp_df[disp_cols], use_container_width=True, hide_index=True)
    st.download_button("📥 Export Explanations",
                       exp_df.to_csv(index=False).encode(),
                       "supplier_explanations.csv", "text/csv")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────────────────────────────────────

def main():
    data    = load_all()
    filters = render_sidebar(data)

    st.title("🔍 SRSID — Supplier Risk & Spend Intelligence Dashboard")
    st.caption("Phase 3 ML Outputs | Entity Resolution → Feature Engineering → ML Models")

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "📊 Overview",
        "⚠️ Risk Prediction",
        "🗂️ Segmentation",
        "📈 Forecasting",
        "💡 Recommendations",
        "🔍 Anomalies",
        "🧠 Explainability (XAI)",
    ])

    with tab1:
        tab_overview(data, filters)
    with tab2:
        tab_risk(data, filters)
    with tab3:
        tab_segmentation(data, filters)
    with tab4:
        tab_forecast(data, filters)
    with tab5:
        tab_recommendations(data, filters)
    with tab6:
        tab_anomalies(data, filters)
    with tab7:
        tab_xai(data, filters)


if __name__ == "__main__":
    main()
