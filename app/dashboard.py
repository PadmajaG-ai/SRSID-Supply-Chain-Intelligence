"""
SRSID Dashboard  —  app/dashboard.py
======================================
Streamlit dashboard adapted to query Postgres directly.

Changes from phase3_dashboard.py:
  - All data comes from Postgres via DBClient (no CSV reading)
  - Portfolio summary from portfolio_summary view
  - Filters applied via SQL WHERE clauses
  - Spend analytics tab added (new)
  - News feed tab added (new)

Run:
    streamlit run app/dashboard.py
"""

import sys
import json
import warnings
from pathlib import Path

import streamlit as st
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import APP_CONFIG, PATHS
from db.db_client import DBClient

try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY = True
except ImportError:
    PLOTLY = False

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title=APP_CONFIG["page_title"],
    page_icon=APP_CONFIG["page_icon"],
    layout=APP_CONFIG["layout"],
    initial_sidebar_state="expanded",
)

# ── Colors ────────────────────────────────────────────────────────────────────
COLORS = {
    "High":       "#E74C3C",
    "Medium":     "#F39C12",
    "Low":        "#27AE60",
    "Strategic":  "#8E44AD",
    "Leverage":   "#2ECC71",
    "Bottleneck": "#E74C3C",
    "Tactical":   "#95A5A6",
    "A":          "#E74C3C",
    "B":          "#F39C12",
    "C":          "#27AE60",
}


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADERS  (all from Postgres)
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_db():
    return DBClient().connect()


@st.cache_data(ttl=300)
def load_portfolio_summary() -> dict:
    with DBClient() as db:
        return db.get_portfolio_summary()


@st.cache_data(ttl=300)
def load_vendors(risk_tier=None, industry=None, country=None,
                  spend_min=None, spend_max=None) -> pd.DataFrame:
    with DBClient() as db:
        filters, params = [], []
        filters.append("is_active = TRUE")
        if risk_tier and risk_tier != "All":
            filters.append("risk_label = %s"); params.append(risk_tier)
        if industry and industry != "All":
            filters.append("industry_category ILIKE %s"); params.append(f"%{industry}%")
        if country and country != "All":
            filters.append("country_code = %s"); params.append(country)
        if spend_min is not None:
            filters.append("total_annual_spend >= %s"); params.append(spend_min)
        if spend_max is not None:
            filters.append("total_annual_spend <= %s"); params.append(spend_max)

        where = " AND ".join(filters)
        sql = f"""
            SELECT vendor_id, supplier_name, country_code,
                   industry_category, risk_label,
                   composite_risk_score, total_annual_spend,
                   transaction_count, delivery_performance,
                   financial_stability, otif_rate, avg_delay_days,
                   news_sentiment_30d, disruption_count_30d,
                   spend_pct_of_portfolio, geo_risk
            FROM vendors
            WHERE {where}
            ORDER BY total_annual_spend DESC NULLS LAST
        """
        return db.fetch_df(sql, tuple(params) if params else None)


@st.cache_data(ttl=300)
def load_risk_scores() -> pd.DataFrame:
    with DBClient() as db:
        return db.fetch_df("""
            SELECT rs.vendor_id, rs.supplier_name,
                   rs.risk_label, rs.risk_probability,
                   rs.model_type, rs.run_date,
                   rs.financial_stability, rs.delivery_performance,
                   rs.news_sentiment_30d, rs.disruption_count_30d,
                   v.total_annual_spend, v.country_code,
                   v.industry_category
            FROM latest_risk_scores rs
            LEFT JOIN vendors v ON rs.vendor_id = v.vendor_id
            ORDER BY rs.risk_probability DESC NULLS LAST
        """)


@st.cache_data(ttl=300)
def load_segments() -> pd.DataFrame:
    with DBClient() as db:
        return db.fetch_df("""
            SELECT s.vendor_id, s.supplier_name,
                   s.kraljic_segment, s.supply_risk_score,
                   s.profit_impact_score, s.cluster_label,
                   s.abc_class, s.spend_rank,
                   s.risk_spend_quadrant, s.strategic_action,
                   v.total_annual_spend, v.composite_risk_score,
                   v.country_code, v.industry_category
            FROM latest_segments s
            LEFT JOIN vendors v ON s.vendor_id = v.vendor_id
            ORDER BY v.total_annual_spend DESC NULLS LAST
        """)


@st.cache_data(ttl=300)
def load_explanations() -> pd.DataFrame:
    with DBClient() as db:
        return db.fetch_df("""
            SELECT e.vendor_id, e.supplier_name,
                   e.predicted_risk_tier,
                   e.driver_1_label, e.driver_1_shap,
                   e.driver_2_label, e.driver_2_shap,
                   e.driver_3_label, e.driver_3_shap,
                   e.mitigator_label, e.mitigator_shap,
                   e.narrative,
                   v.total_annual_spend, v.risk_label
            FROM latest_explanations e
            LEFT JOIN vendors v ON e.vendor_id = v.vendor_id
        """)


@st.cache_data(ttl=120)
def load_news(days: int = 30, disruption_only: bool = False) -> pd.DataFrame:
    with DBClient() as db:
        return db.get_recent_news(days=days, disruption_only=disruption_only)


@st.cache_data(ttl=300)
def load_spend_report() -> dict:
    p = PATHS["reports"] / "spend_intelligence_report.json"
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return {}


@st.cache_data(ttl=300)
def load_quarterly_spend() -> pd.DataFrame:
    with DBClient() as db:
        df = db.fetch_df("""
            SELECT
                year, quarter,
                CAST(year AS TEXT) || '-Q' || CAST(quarter AS TEXT) AS year_quarter,
                SUM(transaction_amount)    AS total_spend,
                COUNT(DISTINCT vendor_id)  AS active_vendors,
                COUNT(*)                   AS transaction_count
            FROM transactions
            WHERE year IS NOT NULL AND quarter IS NOT NULL
              AND transaction_amount > 0
            GROUP BY year, quarter
            ORDER BY year, quarter
        """)
    return df


@st.cache_data(ttl=300)
def load_feature_importance() -> pd.DataFrame:
    p = PATHS["reports"] / "feature_importance.csv"
    return pd.read_csv(p) if p.exists() else pd.DataFrame()


@st.cache_data(ttl=300)
def load_model_eval() -> dict:
    p = PATHS["reports"] / "model_evaluation.json"
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# UI HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def metric_card(label: str, value, delta=None, color="#2C3E50"):
    delta_html = f"<small style='color:gray'>{delta}</small>" if delta else ""
    st.markdown(
        f"""<div style='background:{color}22;border-left:4px solid {color};
                        padding:12px 16px;border-radius:6px;margin-bottom:8px'>
            <div style='font-size:0.8rem;color:#666'>{label}</div>
            <div style='font-size:1.6rem;font-weight:700;color:{color}'>{value}</div>
            {delta_html}</div>""",
        unsafe_allow_html=True,
    )


def fmt_spend(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    if abs(v) >= 1e9: return f"${v/1e9:.1f}B"
    if abs(v) >= 1e6: return f"${v/1e6:.1f}M"
    if abs(v) >= 1e3: return f"${v/1e3:.0f}K"
    return f"${v:.0f}"


def risk_badge(tier: str) -> str:
    c = COLORS.get(tier, "#95A5A6")
    return f"<span style='background:{c};color:white;padding:2px 8px;border-radius:4px;font-size:0.8rem'>{tier}</span>"


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

def render_sidebar() -> dict:
    st.sidebar.image(
        "https://img.icons8.com/fluency/48/supply-chain.png", width=48
    )
    st.sidebar.title("SRSID")
    st.sidebar.caption("Supplier Risk & Spend Intelligence")
    st.sidebar.divider()

    # Portfolio KPIs
    summary = load_portfolio_summary()
    if summary:
        st.sidebar.metric("Total Vendors",  summary.get("total_vendors", "—"))
        st.sidebar.metric("🔴 High Risk",   summary.get("high_risk_count", "—"))
        st.sidebar.metric("🟡 Medium Risk", summary.get("medium_risk_count", "—"))
        total = summary.get("total_portfolio_spend")
        if total:
            st.sidebar.metric("Portfolio Spend", fmt_spend(total))
        otif = summary.get("avg_otif_rate")
        if otif:
            st.sidebar.metric("Avg OTIF", f"{otif*100:.1f}%")
    st.sidebar.divider()

    # Filters
    st.sidebar.subheader("Filters")

    filters = {
        "risk_tier": st.sidebar.selectbox(
            "Risk Tier", ["All", "High", "Medium", "Low"]
        ),
        "industry": st.sidebar.selectbox(
            "Industry", ["All"] + _get_industries()
        ),
        "country": st.sidebar.selectbox(
            "Country", ["All"] + _get_countries()
        ),
    }

    spend_range = st.sidebar.slider(
        "Annual Spend ($)", 0, 10_000_000,
        (0, 10_000_000), step=100_000,
        format="$%d"
    )
    filters["spend_min"] = spend_range[0] if spend_range[0] > 0 else None
    filters["spend_max"] = spend_range[1] if spend_range[1] < 10_000_000 else None

    st.sidebar.divider()
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.caption("Data from PostgreSQL · srsid_db")
    return filters


@st.cache_data(ttl=600)
def _get_industries() -> list:
    with DBClient() as db:
        df = db.fetch_df(
            "SELECT DISTINCT industry_category FROM vendors "
            "WHERE industry_category IS NOT NULL ORDER BY industry_category"
        )
    return df["industry_category"].tolist() if not df.empty else []


@st.cache_data(ttl=600)
def _get_countries() -> list:
    with DBClient() as db:
        df = db.fetch_df(
            "SELECT DISTINCT country_code FROM vendors "
            "WHERE country_code IS NOT NULL ORDER BY country_code"
        )
    return df["country_code"].tolist() if not df.empty else []


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — OVERVIEW
# ─────────────────────────────────────────────────────────────────────────────

def tab_overview():
    st.header("📊 Executive Overview")

    summary = load_portfolio_summary()

    # KPI row
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        metric_card("Total Vendors",
                    summary.get("total_vendors", "—"), color="#2C3E50")
    with c2:
        metric_card("High Risk",
                    summary.get("high_risk_count", "—"), color="#E74C3C")
    with c3:
        metric_card("Medium Risk",
                    summary.get("medium_risk_count", "—"), color="#F39C12")
    with c4:
        metric_card("Portfolio Spend",
                    fmt_spend(summary.get("total_portfolio_spend")),
                    color="#2980B9")
    with c5:
        otif = summary.get("avg_otif_rate")
        metric_card("Avg OTIF",
                    f"{otif*100:.1f}%" if otif else "—",
                    color="#27AE60")

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Risk Distribution")
        vendors = load_vendors()
        if not vendors.empty and "risk_label" in vendors.columns:
            rd = vendors["risk_label"].value_counts().to_dict()
            if PLOTLY:
                fig = px.pie(names=list(rd.keys()), values=list(rd.values()),
                             color=list(rd.keys()),
                             color_discrete_map=COLORS, hole=0.45)
                fig.update_layout(margin=dict(t=0, b=0))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.bar_chart(rd)

    with col2:
        st.subheader("Spend by Country (Top 10)")
        if not vendors.empty:
            by_country = (vendors.groupby("country_code")["total_annual_spend"]
                          .sum().sort_values(ascending=False).head(10))
            if PLOTLY:
                fig = px.bar(x=by_country.values, y=by_country.index,
                             orientation="h",
                             labels={"x": "Spend ($)", "y": "Country"},
                             color=by_country.values,
                             color_continuous_scale="Blues")
                fig.update_layout(yaxis=dict(autorange="reversed"),
                                  margin=dict(t=10), showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

    # Quarterly spend trend
    st.subheader("Quarterly Spend Trend")
    qoq = load_quarterly_spend()
    if not qoq.empty and "total_spend" in qoq.columns:
        # Build year_quarter label if not already present
        if "year_quarter" not in qoq.columns and "year" in qoq.columns:
            qoq["year_quarter"] = qoq["year"].astype(str) + "-Q" + qoq["quarter"].astype(str)
        x_col = "year_quarter" if "year_quarter" in qoq.columns else "quarter"
        if PLOTLY:
            fig = px.line(qoq, x=x_col, y="total_spend",
                          labels={x_col: "Quarter", "total_spend": "Total Spend ($)"},
                          markers=True)
            fig.update_layout(margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.line_chart(qoq.set_index(x_col)["total_spend"])

    # Model performance
    model_eval = load_model_eval()
    if model_eval:
        st.subheader("ML Model Performance")
        best  = model_eval.get("best_model", "")
        evals = model_eval.get("evaluation", {})
        rows  = [{
            "Model":        best,
            "Accuracy":     evals.get("accuracy", "—"),
            "F1 (weighted)":evals.get("f1_weighted", "—"),
            "Best":         "✅",
        }]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — RISK
# ─────────────────────────────────────────────────────────────────────────────

def tab_risk(filters: dict):
    st.header("⚠️ Risk Predictions")

    risk_df = load_vendors(**{k: v for k, v in filters.items()
                               if k in ["risk_tier","industry","country",
                                        "spend_min","spend_max"]})
    if risk_df.empty:
        st.info("No risk data. Run ml/risk_model.py first.")
        return

    # High risk table
    st.subheader("High Risk Suppliers")
    high = risk_df[risk_df["risk_label"] == "High"].head(20)
    if not high.empty:
        disp = high[["supplier_name","risk_label","composite_risk_score",
                      "total_annual_spend","country_code","industry_category",
                      "delivery_performance","news_sentiment_30d"]].copy()
        disp["total_annual_spend"] = disp["total_annual_spend"].apply(fmt_spend)
        st.dataframe(disp, use_container_width=True, hide_index=True)
    else:
        st.success("✅ No High Risk suppliers in current filter")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Risk Score Distribution")
        if PLOTLY and "composite_risk_score" in risk_df.columns:
            fig = px.histogram(risk_df, x="composite_risk_score",
                               nbins=30, color_discrete_sequence=["#E74C3C"])
            from config import RISK_THRESHOLDS
            fig.add_vline(x=RISK_THRESHOLDS["high"], line_dash="dash",
                          line_color="red",   annotation_text="High threshold")
            fig.add_vline(x=RISK_THRESHOLDS["medium"], line_dash="dash",
                          line_color="orange", annotation_text="Medium threshold")
            fig.update_layout(margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Feature Importance")
        fi = load_feature_importance()
        if not fi.empty and PLOTLY:
            label_col = "feature_label" if "feature_label" in fi.columns else "feature"
            fig = px.bar(fi.head(12), x="importance", y=label_col,
                         orientation="h",
                         color="importance", color_continuous_scale="Reds",
                         labels={"importance": "Importance", label_col: ""})
            fig.update_layout(yaxis=dict(autorange="reversed"),
                              margin=dict(t=10), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    # Full table
    st.subheader("All Supplier Risk Scores")
    disp_cols = ["supplier_name","risk_label","composite_risk_score",
                 "total_annual_spend","country_code","industry_category",
                 "delivery_performance","otif_rate"]
    disp = risk_df[[c for c in disp_cols if c in risk_df.columns]].copy()
    disp.sort_values("composite_risk_score", ascending=False, inplace=True)
    st.dataframe(disp, use_container_width=True, hide_index=True)

    st.download_button("📥 Export",
                       risk_df.to_csv(index=False).encode(),
                       "risk_scores.csv", "text/csv")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 — SEGMENTATION
# ─────────────────────────────────────────────────────────────────────────────

def tab_segmentation(filters: dict):
    st.header("🗂️ Supplier Segmentation")

    seg = load_segments()
    if seg.empty:
        st.info("No segmentation data. Run ml/segmentation.py first.")
        return

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Kraljic Matrix")
        if PLOTLY and "supply_risk_score" in seg.columns:
            fig = px.scatter(
                seg,
                x="supply_risk_score",
                y="profit_impact_score",
                color="kraljic_segment",
                color_discrete_map=COLORS,
                hover_name="supplier_name",
                hover_data=["total_annual_spend","country_code"],
                size="total_annual_spend",
                size_max=30,
                labels={"supply_risk_score": "Supply Risk →",
                        "profit_impact_score": "Profit Impact →"},
            )
            fig.add_vline(x=seg["supply_risk_score"].median(),
                          line_dash="dash", line_color="gray")
            fig.add_hline(y=seg["profit_impact_score"].median(),
                          line_dash="dash", line_color="gray")
            fig.update_layout(margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("ABC Spend Classification")
        if "abc_class" in seg.columns and "total_annual_spend" in seg.columns:
            abc_spend = (seg.groupby("abc_class")["total_annual_spend"]
                         .sum().reindex(["A","B","C"]))
            if PLOTLY:
                fig = px.bar(x=abc_spend.index, y=abc_spend.values,
                             color=abc_spend.index,
                             color_discrete_map=COLORS,
                             labels={"x": "Class", "y": "Total Spend ($)"},
                             text=[fmt_spend(v) for v in abc_spend.values])
                fig.update_traces(textposition="outside")
                fig.update_layout(showlegend=False, margin=dict(t=10))
                st.plotly_chart(fig, use_container_width=True)

    # Kraljic distribution
    st.subheader("Segment Distribution")
    c1, c2 = st.columns(2)
    with c1:
        if "kraljic_segment" in seg.columns:
            kd = seg["kraljic_segment"].value_counts()
            st.bar_chart(kd)
    with c2:
        if "cluster_label" in seg.columns:
            cd = seg["cluster_label"].value_counts()
            st.bar_chart(cd)

    # Strategic actions table
    st.subheader("Strategic Actions by Segment")
    if "strategic_action" in seg.columns:
        action_df = (seg[["supplier_name","kraljic_segment",
                           "strategic_action","total_annual_spend"]]
                     .drop_duplicates("supplier_name")
                     .sort_values("total_annual_spend", ascending=False)
                     .head(30))
        action_df["total_annual_spend"] = action_df["total_annual_spend"].apply(fmt_spend)
        st.dataframe(action_df, use_container_width=True, hide_index=True)

    st.download_button("📥 Export Segments",
                       seg.to_csv(index=False).encode(),
                       "segments.csv", "text/csv")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 4 — SPEND ANALYTICS
# ─────────────────────────────────────────────────────────────────────────────

def tab_spend():
    st.header("💰 Spend Analytics")

    report  = load_spend_report()
    vendors = load_vendors()

    if report:
        si = report.get("spend_intelligence", {})
        co = report.get("concentration", {})
        op = report.get("opportunity", {})

        # KPIs
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            metric_card("Portfolio Spend",
                        fmt_spend(si.get("total_portfolio_spend", 0)),
                        color="#2980B9")
        with c2:
            sum_pct = si.get("sum_percentage", 0)
            color = "#27AE60" if sum_pct >= 80 else "#F39C12" if sum_pct >= 60 else "#E74C3C"
            metric_card("Spend Under Management",
                        f"{sum_pct:.1f}%",
                        delta=f"Target: {si.get('sum_target','>80%')}",
                        color=color)
        with c3:
            mav_pct = si.get("maverick_percentage", 0)
            color = "#27AE60" if mav_pct <= 10 else "#F39C12" if mav_pct <= 20 else "#E74C3C"
            metric_card("Maverick Spend",
                        f"{mav_pct:.1f}%",
                        delta=f"Target: {si.get('maverick_target','<10%')}",
                        color=color)
        with c4:
            metric_card("Savings Opportunity",
                        fmt_spend(op.get("total_savings_opportunity", 0)),
                        color="#8E44AD")

        st.divider()

        # Concentration metrics
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Concentration Risk")
            hhi = co.get("hhi", 0)
            hhi_status = co.get("hhi_status", "Unknown")
            hhi_color  = "#E74C3C" if hhi_status == "HIGH" else \
                         "#F39C12" if hhi_status == "MODERATE" else "#27AE60"
            metric_card(f"HHI Index ({hhi_status})", f"{hhi:,.0f}",
                        delta="Healthy < 1,500 | Moderate < 2,500",
                        color=hhi_color)
            st.metric("Top 5 Vendor Concentration",
                      f"{co.get('top5_pct', 0):.1f}%",
                      delta="⚠️ Exceeds 40% limit"
                      if co.get("top5_exceeds_limit") else "✅ Within limit")
            st.metric("Vendors Needing Diversification",
                      co.get("diversification_needed", 0))

        with col2:
            st.subheader("Contract Coverage")
            vendors_no_contract = op.get("vendors_needing_contracts", 0)
            total_v = vendors.shape[0] if not vendors.empty else 1
            covered_pct = (1 - vendors_no_contract / total_v) * 100

            if PLOTLY:
                fig = go.Figure(go.Indicator(
                    mode="gauge+number+delta",
                    value=covered_pct,
                    delta={"reference": 80, "valueformat": ".1f"},
                    number={"suffix": "%"},
                    title={"text": "Contract Coverage"},
                    gauge={
                        "axis": {"range": [0, 100]},
                        "bar":  {"color": "#27AE60"},
                        "steps": [
                            {"range": [0,  60], "color": "#FADBD8"},
                            {"range": [60, 80], "color": "#FDEBD0"},
                            {"range": [80, 100],"color": "#D5F5E3"},
                        ],
                        "threshold": {
                            "line": {"color": "red", "width": 2},
                            "value": 80,
                        },
                    },
                ))
                fig.update_layout(height=250, margin=dict(t=20, b=0))
                st.plotly_chart(fig, use_container_width=True)

    # Top spend vendors
    st.subheader("Top Vendors by Spend")
    if not vendors.empty:
        top = vendors.head(20)[
            ["supplier_name","total_annual_spend","risk_label",
             "country_code","industry_category","spend_pct_of_portfolio"]
        ].copy()
        top["total_annual_spend"] = top["total_annual_spend"].apply(fmt_spend)
        top["spend_pct_of_portfolio"] = top["spend_pct_of_portfolio"].apply(
            lambda x: f"{x:.2f}%" if x else "—"
        )
        st.dataframe(top, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 5 — EXPLAINABILITY
# ─────────────────────────────────────────────────────────────────────────────

def tab_explainability():
    st.header("🔍 Risk Explainability (SHAP)")

    expl = load_explanations()
    if expl.empty:
        st.info("No explanations. Run ml/explainability.py first.")
        return

    # Vendor search
    vendor_names = expl["supplier_name"].dropna().unique().tolist()
    selected = st.selectbox("Select a vendor to explain", sorted(vendor_names))

    if selected:
        row = expl[expl["supplier_name"] == selected].iloc[0]

        tier  = row.get("predicted_risk_tier", "Unknown")
        color = COLORS.get(tier, "#95A5A6")
        st.markdown(
            f"**Risk Classification:** "
            f"<span style='background:{color};color:white;padding:3px 10px;"
            f"border-radius:4px'>{tier}</span>",
            unsafe_allow_html=True
        )

        if pd.notna(row.get("narrative")):
            st.info(row["narrative"])

        # Driver chart
        drivers = []
        for i in range(1, 4):
            label = row.get(f"driver_{i}_label")
            shap  = row.get(f"driver_{i}_shap")
            if label and pd.notna(shap):
                drivers.append({"Feature": label, "SHAP": float(shap)})
        if row.get("mitigator_label") and pd.notna(row.get("mitigator_shap")):
            drivers.append({
                "Feature": f"✅ {row['mitigator_label']} (mitigator)",
                "SHAP": float(row["mitigator_shap"])
            })

        if drivers and PLOTLY:
            d_df = pd.DataFrame(drivers)
            colors = ["#E74C3C" if v > 0 else "#27AE60" for v in d_df["SHAP"]]
            fig = go.Figure(go.Bar(
                x=d_df["SHAP"], y=d_df["Feature"],
                orientation="h",
                marker_color=colors,
                text=[f"{v:+.3f}" for v in d_df["SHAP"]],
                textposition="outside",
            ))
            fig.update_layout(
                title="Risk Drivers (red = increases risk, green = reduces risk)",
                xaxis_title="SHAP Value",
                yaxis=dict(autorange="reversed"),
                margin=dict(t=40, b=10),
                height=300,
            )
            st.plotly_chart(fig, use_container_width=True)

    # Global feature importance
    st.subheader("Global Feature Importance")
    fi = load_feature_importance()
    if not fi.empty and PLOTLY:
        label_col = "feature_label" if "feature_label" in fi.columns else "feature"
        fig = px.bar(fi.head(15), x="importance", y=label_col,
                     orientation="h",
                     color="importance", color_continuous_scale="Reds",
                     labels={"importance": "Importance", label_col: ""})
        fig.update_layout(yaxis=dict(autorange="reversed"),
                          margin=dict(t=10), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 6 — NEWS FEED
# ─────────────────────────────────────────────────────────────────────────────

def tab_news():
    st.header("📰 Supplier News & Disruptions")

    col1, col2 = st.columns([3, 1])
    with col2:
        days = st.selectbox("Lookback", [7, 14, 30, 60], index=2)
        disruption_only = st.checkbox("Disruptions only", value=False)

    news = load_news(days=days, disruption_only=disruption_only)

    if news.empty:
        st.info(f"No news articles in the last {days} days. "
                f"Run news_ingestion.py to fetch articles.")
        return

    # Summary KPIs
    c1, c2, c3 = st.columns(3)
    with c1:
        metric_card("Total Articles", len(news), color="#2C3E50")
    with c2:
        d_count = news["disruption_flag"].sum() if "disruption_flag" in news.columns else 0
        metric_card("Disruption Alerts", d_count, color="#E74C3C")
    with c3:
        avg_sent = news["sentiment_score"].mean() if "sentiment_score" in news.columns else 0
        color = "#E74C3C" if avg_sent < -0.1 else "#27AE60" if avg_sent > 0.1 else "#F39C12"
        metric_card("Avg Sentiment", f"{avg_sent:+.2f}", color=color)

    st.divider()

    # Disruption type breakdown
    if "disruption_type" in news.columns and PLOTLY:
        dtypes = news[news["disruption_type"].notna()]["disruption_type"].value_counts()
        if not dtypes.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Disruption Types")
                fig = px.pie(names=dtypes.index, values=dtypes.values, hole=0.4)
                fig.update_layout(margin=dict(t=0, b=0))
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                st.subheader("Sentiment by Source")
                if "api_source" in news.columns:
                    sent_by_src = news.groupby("api_source")["sentiment_score"].mean()
                    fig = px.bar(x=sent_by_src.index, y=sent_by_src.values,
                                 labels={"x": "Source", "y": "Avg Sentiment"},
                                 color=sent_by_src.values,
                                 color_continuous_scale="RdYlGn",
                                 range_color=[-1, 1])
                    fig.update_layout(margin=dict(t=10), showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)

    # Article feed
    st.subheader("Latest Articles")
    for _, row in news.head(20).iterrows():
        sentiment = row.get("sentiment_score", 0)
        sent_color = "#E74C3C" if sentiment < -0.1 else \
                     "#27AE60" if sentiment > 0.1 else "#95A5A6"
        flag = "🚨 " if row.get("disruption_flag") else ""
        with st.expander(f"{flag}{row.get('supplier_name','?')} — {row.get('title','')[:80]}"):
            c1, c2, c3 = st.columns(3)
            c1.caption(f"Source: {row.get('source_name','?')}")
            c2.caption(f"Published: {str(row.get('published_at',''))[:10]}")
            c3.markdown(
                f"<span style='color:{sent_color}'>Sentiment: {sentiment:+.2f}</span>",
                unsafe_allow_html=True
            )
            if row.get("disruption_type"):
                st.warning(f"Disruption type: {row['disruption_type']}")
            url = row.get("url", "")
            if url:
                st.markdown(f"[Read article →]({url})")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 7 — VENDOR PROFILE
# ─────────────────────────────────────────────────────────────────────────────

def tab_vendor_profile():
    st.header("🏢 Vendor Deep Dive")

    with DBClient() as db:
        vendor_list = db.fetch_df(
            "SELECT vendor_id, supplier_name FROM vendors "
            "WHERE is_active = TRUE ORDER BY supplier_name"
        )

    if vendor_list.empty:
        st.info("No vendors loaded.")
        return

    selected = st.selectbox(
        "Select vendor",
        options=vendor_list["vendor_id"].tolist(),
        format_func=lambda vid: vendor_list[
            vendor_list["vendor_id"] == vid
        ]["supplier_name"].iloc[0]
    )

    if selected:
        with DBClient() as db:
            profile = db.get_vendor_profile(selected)

        if not profile:
            st.warning("Vendor profile not found.")
            return

        v = profile["vendor"]
        risk  = profile.get("risk", {}) or {}
        seg   = profile.get("segment", {}) or {}
        expl  = profile.get("explanation", {}) or {}
        news_items = profile.get("news", []) or []

        # Header
        tier  = v.get("risk_label", "Unknown")
        color = COLORS.get(tier, "#95A5A6")
        st.markdown(
            f"## {v.get('supplier_name','?')}"
            f" &nbsp; <span style='background:{color};color:white;"
            f"padding:3px 10px;border-radius:4px;font-size:0.85rem'>{tier}</span>",
            unsafe_allow_html=True
        )
        st.caption(
            f"{v.get('country_code','')} · "
            f"{v.get('industry_category','')} · "
            f"ID: {v.get('vendor_id','')}"
        )

        # Key metrics
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Annual Spend",    fmt_spend(v.get("total_annual_spend")))
        c2.metric("OTIF Rate",       f"{(v.get('otif_rate') or 0)*100:.1f}%")
        c3.metric("Risk Score",      f"{v.get('composite_risk_score') or 0:.3f}")
        c4.metric("Disruptions (30d)", v.get("disruption_count_30d", 0))

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Risk Drivers")
            if expl:
                for i in range(1, 4):
                    label = expl.get(f"driver_{i}_label")
                    shap  = expl.get(f"driver_{i}_shap")
                    if label and shap:
                        bar_color = "🔴" if float(shap) > 0 else "🟢"
                        st.write(f"{bar_color} **{label}** ({float(shap):+.3f})")
                narrative = expl.get("narrative")
                if narrative:
                    st.info(narrative)
            else:
                st.caption("No explanation data. Run ml/explainability.py")

        with col2:
            st.subheader("Segmentation")
            if seg:
                st.write(f"**Kraljic:** {seg.get('kraljic_segment','—')}")
                st.write(f"**ABC Class:** {seg.get('abc_class','—')}")
                st.write(f"**Cluster:** {seg.get('cluster_label','—')}")
                action = seg.get("strategic_action")
                if action:
                    st.success(f"💡 {action}")
            else:
                st.caption("No segment data. Run ml/segmentation.py")

        # Recent news
        if news_items:
            st.subheader("Recent News")
            for article in news_items[:5]:
                with st.expander(article.get("title", "Article")[:80]):
                    st.caption(
                        f"{article.get('source_name','')} · "
                        f"{str(article.get('published_at',''))[:10]}"
                    )
                    if article.get("disruption_type"):
                        st.warning(f"Disruption: {article['disruption_type']}")
                    url = article.get("url","")
                    if url:
                        st.markdown(f"[Read →]({url})")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_alternatives() -> pd.DataFrame:
    with DBClient() as db:
        if not db.table_exists("vendor_alternatives"):
            return pd.DataFrame()
        return db.fetch_df("""
            SELECT vendor_id, supplier_name, risk_score, risk_tier,
                   alt_supplier_name, alt_risk_score, alt_risk_tier,
                   alt_country, alt_industry,
                   alternative_rank, similarity_score,
                   recommendation_reason
            FROM vendor_alternatives
            ORDER BY risk_score DESC, alternative_rank ASC
        """)


@st.cache_data(ttl=300)
def load_anomalies() -> pd.DataFrame:
    with DBClient() as db:
        if not db.table_exists("vendor_anomalies"):
            return pd.DataFrame()
        return db.fetch_df("""
            SELECT vendor_id, supplier_name,
                   is_anomalous, total_anomaly_flags,
                   anomaly_if_score, anomaly_if_flag,
                   max_zscore, zscore_feature,
                   rule_based_flag, rule_based_reason,
                   composite_risk_score, total_annual_spend,
                   financial_stability
            FROM vendor_anomalies
            ORDER BY total_anomaly_flags DESC, anomaly_if_score DESC
        """)


def tab_alternatives():
    st.header("🔄 Alternative Suppliers & Anomalies")

    alt_df  = load_alternatives()
    anom_df = load_anomalies()

    if alt_df.empty and anom_df.empty:
        st.info(
            "No recommendations or anomaly data yet.\n\n"
            "Run: `python ml/recommendations.py`"
        )
        return

    tab_a, tab_b = st.tabs(["🔄 Alternative Suppliers", "⚡ Anomaly Detection"])

    # ── Tab A: Alternatives ───────────────────────────────────────────────────
    with tab_a:
        if alt_df.empty:
            st.info("No alternatives yet. Run: `python ml/recommendations.py`")
        else:
            # KPIs
            c1, c2, c3 = st.columns(3)
            c1.metric("Vendors Needing Alternatives",
                      alt_df["supplier_name"].nunique())
            c2.metric("Total Recommendations",   len(alt_df))
            avg_sim = alt_df["similarity_score"].mean()
            c3.metric("Avg Similarity Score",    f"{avg_sim:.2f}")

            st.divider()

            # Vendor selector
            at_risk_vendors = sorted(alt_df["supplier_name"].unique().tolist())
            selected = st.selectbox(
                "Select at-risk vendor to see alternatives",
                ["— All —"] + at_risk_vendors
            )

            if selected != "— All —":
                vendor_alts = alt_df[alt_df["supplier_name"] == selected]
                row0 = vendor_alts.iloc[0]

                col1, col2 = st.columns(2)
                col1.markdown(
                    f"**{selected}**  \n"
                    f"Risk Score: `{row0.get('risk_score', 0):.3f}`  \n"
                    f"Risk Tier: {row0.get('risk_tier','?')}"
                )

                st.subheader("Recommended Alternatives")
                for _, r in vendor_alts.sort_values("alternative_rank").iterrows():
                    alt_color = COLORS.get(r.get("alt_risk_tier",""), "#95A5A6")
                    with st.expander(
                        f"#{r['alternative_rank']}  {r['alt_supplier_name']}  "
                        f"— Similarity: {r['similarity_score']:.2f}"
                    ):
                        c1, c2, c3 = st.columns(3)
                        c1.markdown(
                            f"<span style='background:{alt_color};color:white;"
                            f"padding:2px 8px;border-radius:4px'>"
                            f"{r.get('alt_risk_tier','?')}</span>",
                            unsafe_allow_html=True
                        )
                        c2.write(f"🌍 {r.get('alt_country','?')}")
                        c3.write(f"🏭 {r.get('alt_industry','?')}")
                        st.info(f"💡 {r.get('recommendation_reason','')}")
            else:
                # Show all high-risk vendors and their top-1 alternative
                top1 = alt_df[alt_df["alternative_rank"] == 1].copy()
                top1["risk_score"] = top1["risk_score"].round(3)
                top1["similarity_score"] = top1["similarity_score"].round(3)
                disp_cols = ["supplier_name","risk_tier","risk_score",
                             "alt_supplier_name","alt_risk_tier","alt_country",
                             "similarity_score","recommendation_reason"]
                st.dataframe(
                    top1[[c for c in disp_cols if c in top1.columns]],
                    use_container_width=True, hide_index=True
                )

            st.download_button(
                "📥 Export Alternatives",
                alt_df.to_csv(index=False).encode(),
                "alternative_suppliers.csv", "text/csv"
            )

    # ── Tab B: Anomalies ──────────────────────────────────────────────────────
    with tab_b:
        if anom_df.empty:
            st.info("No anomaly data yet. Run: `python ml/recommendations.py`")
        else:
            anomalous = anom_df[anom_df["is_anomalous"] == True]

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Vendors Analysed", len(anom_df))
            c2.metric("🚨 Anomalies Flagged",   len(anomalous),
                      delta=f"{len(anomalous)/len(anom_df)*100:.1f}%")
            if_count   = anom_df["anomaly_if_flag"].sum() \
                         if "anomaly_if_flag" in anom_df.columns else 0
            rule_count = anom_df["rule_based_flag"].sum() \
                         if "rule_based_flag" in anom_df.columns else 0
            c3.metric("Isolation Forest",   if_count)
            c4.metric("Rule-Based Flags",   rule_count)

            st.divider()

            if PLOTLY and not anomalous.empty:
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("Anomaly Score Distribution")
                    fig = px.histogram(
                        anom_df, x="anomaly_if_score", nbins=30,
                        color_discrete_sequence=["#8E44AD"]
                    )
                    fig.update_layout(margin=dict(t=10))
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    st.subheader("Flags by Type")
                    flag_counts = {
                        "Isolation Forest": if_count,
                        "Z-Score":  anom_df.get("anomaly_zscore_flag",
                                        pd.Series(False)).sum(),
                        "Rule-Based": rule_count,
                    }
                    fig = px.bar(
                        x=list(flag_counts.keys()),
                        y=list(flag_counts.values()),
                        color=list(flag_counts.keys()),
                        labels={"x": "Detection Method", "y": "Count"}
                    )
                    fig.update_layout(showlegend=False, margin=dict(t=10))
                    st.plotly_chart(fig, use_container_width=True)

            st.subheader("Anomalous Vendors")
            if not anomalous.empty:
                disp = anomalous[[
                    "supplier_name", "total_anomaly_flags",
                    "anomaly_if_score", "rule_based_reason",
                    "composite_risk_score", "total_annual_spend",
                    "financial_stability"
                ]].copy()
                disp["total_annual_spend"] = disp["total_annual_spend"].apply(fmt_spend)
                disp["anomaly_if_score"]   = disp["anomaly_if_score"].round(3)
                st.dataframe(disp, use_container_width=True, hide_index=True)

            st.download_button(
                "📥 Export Anomalies",
                anom_df.to_csv(index=False).encode(),
                "anomaly_report.csv", "text/csv"
            )


def tab_vendor_scorecard():
    st.header("📋 Vendor Scorecard")
    st.caption(
        "All vendor evaluation metrics in one view. "
        "SAP-computed metrics update automatically when you re-run sap_loader.py. "
        "Manual scores (Defect Rate, Innovation, Cybersecurity, ESG) can be entered and saved."
    )

    with DBClient() as db:
        vendor_list = db.fetch_df(
            "SELECT vendor_id, supplier_name FROM vendors "
            "WHERE is_active = TRUE ORDER BY supplier_name"
        )

    if vendor_list.empty:
        st.info("No vendors loaded.")
        return

    selected_id = st.selectbox(
        "Select vendor",
        options=vendor_list["vendor_id"].tolist(),
        format_func=lambda vid: vendor_list[
            vendor_list["vendor_id"] == vid
        ]["supplier_name"].iloc[0],
        key="scorecard_vendor"
    )
    if not selected_id:
        return

    # Build column list dynamically — skip columns that don't exist yet
    with DBClient() as db:
        existing_cols = set(db.fetch_df(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'vendors'"
        )["column_name"].tolist())

    base_cols = [
        "vendor_id","supplier_name","country_code","industry_category",
        "risk_label","composite_risk_score","total_annual_spend",
        "delivery_performance","otif_rate","avg_delay_days",
        "financial_stability","sum_percentage","savings_opportunity",
        "concentration_risk","disruption_count_30d","news_sentiment_30d","geo_risk",
    ]
    optional_cols = [
        "ottr_rate","lead_time_variability","order_accuracy_rate",
        "avg_price_variance_pct","defect_rate_ppm","innovation_score",
        "cybersecurity_score","esg_score","scorecard_notes","scorecard_updated_at",
    ]
    select_cols = base_cols + [c for c in optional_cols if c in existing_cols]
    col_str = ", ".join(select_cols)

    with DBClient() as db:
        v = db.fetch_one(f"SELECT {col_str} FROM vendors WHERE vendor_id = %s",
                         (selected_id,))
    if not v:
        st.warning("Vendor not found.")
        return

    tier  = v.get("risk_label","Unknown")
    color = COLORS.get(tier,"#95A5A6")
    st.markdown(
        f"## {v.get('supplier_name','')} &nbsp;"
        f"<span style='background:{color};color:white;padding:3px 10px;"
        f"border-radius:4px;font-size:0.85rem'>{tier} Risk</span>",
        unsafe_allow_html=True
    )
    st.caption(f"{v.get('country_code','')} · {v.get('industry_category','')} · "
               f"Annual Spend: {fmt_spend(v.get('total_annual_spend'))}")
    st.divider()

    # ── Operational ───────────────────────────────────────────────────────────
    st.subheader("🔧 Operational Performance")
    c1, c2, c3, c4, c5 = st.columns(5)

    def pct_m(col, label, val, target=None):
        if val is None:
            col.metric(label, "N/A"); return
        pct = val * 100 if val <= 1.0 else val
        delta = f"{pct - target:+.1f}% vs {target:.0f}% target" if target else None
        col.metric(label, f"{pct:.1f}%", delta=delta,
                   delta_color="normal" if (target is None or pct >= target) else "inverse")

    pct_m(c1, "OTD",            v.get("delivery_performance"), 95)
    pct_m(c2, "OTIF",           v.get("otif_rate"),             95)
    pct_m(c3, "OTTR",           v.get("ottr_rate"),             90)
    pct_m(c4, "Order Accuracy", v.get("order_accuracy_rate"),   98)
    lt = v.get("lead_time_variability")
    c5.metric("Lead Time Variability",
              f"±{lt:.1f}d" if lt is not None else "N/A",
              delta="High" if lt and lt > 10 else None,
              delta_color="inverse" if lt and lt > 10 else "normal")

    st.divider()

    # ── Financial ─────────────────────────────────────────────────────────────
    st.subheader("💰 Financial & Strategic")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Financial Stability",     f"{v.get('financial_stability') or 0:.0f}/100")
    ppv = v.get("avg_price_variance_pct")
    c2.metric("PPV (Price Variance)",    f"{ppv:.1f}%" if ppv else "N/A",
              delta="High" if ppv and ppv > 15 else None,
              delta_color="inverse" if ppv and ppv > 15 else "normal")
    sum_pct = v.get("sum_percentage")
    c3.metric("Contract Compliance",
              f"{sum_pct:.1f}%" if sum_pct else "N/A",
              delta="Below 80% target" if sum_pct and sum_pct < 80 else "✅ On target",
              delta_color="inverse" if sum_pct and sum_pct < 80 else "normal")
    c4.metric("Savings Opportunity",     fmt_spend(v.get("savings_opportunity")))

    st.divider()

    # ── Risk ──────────────────────────────────────────────────────────────────
    st.subheader("🛡️ Risk & Compliance")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Risk Score",             f"{v.get('composite_risk_score') or 0:.3f}")
    c2.metric("Concentration Risk",     v.get("concentration_risk") or "—")
    c3.metric("Disruptions (30d)",      v.get("disruption_count_30d") or 0)
    sent = v.get("news_sentiment_30d")
    c4.metric("News Sentiment",         f"{sent:+.2f}" if sent is not None else "N/A")

    st.divider()

    # ── Manual Scorecard ──────────────────────────────────────────────────────
    st.subheader("✍️ Manual Scorecard Inputs")
    st.caption("Enter external assessment scores. These are saved to the database.")

    col1, col2 = st.columns(2)
    with col1:
        defect_ppm  = st.number_input("Defect Rate (PPM)",
                                       min_value=0.0, max_value=1_000_000.0,
                                       value=float(v.get("defect_rate_ppm") or 0),
                                       step=10.0,
                                       help="From QM module or inspection reports")
        innovation  = st.slider("Innovation Score (0–100)", 0, 100,
                                 int(v.get("innovation_score") or 0),
                                 help="How often does this vendor suggest improvements?")
    with col2:
        cybersec    = st.slider("Cybersecurity Score (0–100)", 0, 100,
                                 int(v.get("cybersecurity_score") or 0),
                                 help="ISO 27001 / SOC2 compliance level")
        esg         = st.slider("ESG Score (0–100)", 0, 100,
                                 int(v.get("esg_score") or 0),
                                 help="Environmental, Social, Governance practices")

    notes = st.text_area("Notes", value=v.get("scorecard_notes") or "", height=80)

    if st.button("💾 Save Scorecard", type="primary"):
        with DBClient() as db:
            for col, dtype in [
                ("defect_rate_ppm","FLOAT"),("innovation_score","FLOAT"),
                ("cybersecurity_score","FLOAT"),("esg_score","FLOAT"),
                ("scorecard_notes","TEXT"),("scorecard_updated_at","TIMESTAMP")
            ]:
                db.add_column_if_missing("vendors", col, dtype)
            db.execute("""
                UPDATE vendors
                SET defect_rate_ppm=%(d)s, innovation_score=%(i)s,
                    cybersecurity_score=%(c)s, esg_score=%(e)s,
                    scorecard_notes=%(n)s, scorecard_updated_at=NOW()
                WHERE vendor_id=%(vid)s
            """, None)
            # Use positional params instead
            db.execute(
                "UPDATE vendors SET defect_rate_ppm=%s, innovation_score=%s,"
                " cybersecurity_score=%s, esg_score=%s, scorecard_notes=%s,"
                " scorecard_updated_at=NOW() WHERE vendor_id=%s",
                (defect_ppm or None, innovation or None,
                 cybersec or None, esg or None,
                 notes or None, selected_id)
            )
        st.success("✅ Scorecard saved")
        st.cache_data.clear()

    if v.get("scorecard_updated_at"):
        st.caption(f"Last updated: {str(v['scorecard_updated_at'])[:16]}")

def main():
    filters = render_sidebar()

    tabs = st.tabs([
        "📊 Overview",
        "⚠️ Risk",
        "🗂️ Segmentation",
        "💰 Spend",
        "🔄 Alternatives",
        "🔍 Explainability",
        "📰 News",
        "🏢 Vendor Profile",
        "📋 Scorecard",
    ])

    with tabs[0]: tab_overview()
    with tabs[1]: tab_risk(filters)
    with tabs[2]: tab_segmentation(filters)
    with tabs[3]: tab_spend()
    with tabs[4]: tab_alternatives()
    with tabs[5]: tab_explainability()
    with tabs[6]: tab_news()
    with tabs[7]: tab_vendor_profile()
    with tabs[8]: tab_vendor_scorecard()


if __name__ == "__main__":
    main()