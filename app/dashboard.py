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
    page_title="SRSID — Supplier Risk & Spend Intelligence",
    page_icon="assets/favicon.png" if Path("assets/favicon.png").exists() else None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Professional Design System ────────────────────────────────────────────────
# Corporate colour palette — dark navy primary, clean greys, semantic accents
BRAND = {
    "navy":       "#0F2644",   # primary brand
    "navy_light": "#1A3A5C",   # sidebar bg
    "navy_mid":   "#2E5484",   # active elements
    "accent":     "#1A6FBF",   # interactive blue
    "accent_light":"#E8F2FB",  # blue tint bg
    "gold":       "#C9A84C",   # secondary accent
    "bg":         "#F5F7FA",   # page background
    "card":       "#FFFFFF",   # card surface
    "border":     "#E2E8F0",   # subtle borders
    "text_primary":"#1A202C",  # main text
    "text_muted":  "#64748B",  # secondary text
}

COLORS = {
    "High":       "#C0392B",   # deep red
    "Medium":     "#B7670A",   # amber-brown (accessible on white)
    "Low":        "#1E7E4B",   # forest green
    "Strategic":  "#5B3FA0",   # deep purple
    "Leverage":   "#1E7E4B",   # green
    "Bottleneck": "#C0392B",   # red
    "Tactical":   "#64748B",   # slate
    "A":          "#C0392B",
    "B":          "#B7670A",
    "C":          "#1E7E4B",
}

st.markdown(f"""
<style>
/* ── Google Font ── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* ── Global ── */
html, body, [class*="css"] {{
    font-family: 'Inter', -apple-system, sans-serif !important;
    color: {BRAND['text_primary']};
}}
.stApp {{
    background: {BRAND['bg']} !important;
}}

/* ── Sidebar ── */
[data-testid="stSidebar"] {{
    background: {BRAND['navy']} !important;
    border-right: none !important;
}}
[data-testid="stSidebar"] * {{
    color: #FFFFFF !important;
}}
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stSlider label {{
    color: #B8C9DB !important;
    font-size: 0.78rem !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
}}
[data-testid="stSidebar"] [data-testid="stMetricValue"] {{
    color: #FFFFFF !important;
    font-size: 1.3rem !important;
    font-weight: 600 !important;
}}
[data-testid="stSidebar"] [data-testid="stMetricLabel"] {{
    color: #B8C9DB !important;
    font-size: 0.75rem !important;
}}
[data-testid="stSidebar"] hr {{
    border-color: rgba(255,255,255,0.12) !important;
}}
[data-testid="stSidebar"] .stButton > button {{
    background: rgba(255,255,255,0.08) !important;
    border: 1px solid rgba(255,255,255,0.15) !important;
    color: #FFFFFF !important;
    border-radius: 6px !important;
    font-size: 0.82rem !important;
    transition: all 0.15s !important;
}}
[data-testid="stSidebar"] .stButton > button:hover {{
    background: rgba(255,255,255,0.16) !important;
    border-color: rgba(255,255,255,0.30) !important;
}}

/* ── Main content ── */
.block-container {{
    padding: 2rem 2.5rem 2rem 2.5rem !important;
    max-width: 1440px !important;
}}

/* ── Page header ── */
.page-header {{
    background: {BRAND['card']};
    border-bottom: 3px solid {BRAND['accent']};
    border-radius: 8px 8px 0 0;
    padding: 1.25rem 1.75rem;
    margin-bottom: 1.5rem;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}}
.page-header h2 {{
    margin: 0;
    color: {BRAND['navy']};
    font-size: 1.25rem;
    font-weight: 600;
    letter-spacing: -0.01em;
}}
.page-header p {{
    margin: 0.25rem 0 0;
    color: {BRAND['text_muted']};
    font-size: 0.82rem;
}}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {{
    background: {BRAND['card']} !important;
    border-bottom: 2px solid {BRAND['border']} !important;
    gap: 0 !important;
    border-radius: 8px 8px 0 0;
    padding: 0 0.5rem;
}}
.stTabs [data-baseweb="tab"] {{
    background: transparent !important;
    color: {BRAND['text_muted']} !important;
    font-size: 0.85rem !important;
    font-weight: 500 !important;
    padding: 0.8rem 1.2rem !important;
    border-bottom: 3px solid transparent !important;
    border-radius: 0 !important;
    transition: all 0.15s !important;
}}
.stTabs [aria-selected="true"] {{
    color: {BRAND['accent']} !important;
    border-bottom: 3px solid {BRAND['accent']} !important;
    background: transparent !important;
}}
.stTabs [data-baseweb="tab-panel"] {{
    background: {BRAND['card']};
    border-radius: 0 0 8px 8px;
    padding: 1.5rem;
    border: 1px solid {BRAND['border']};
    border-top: none;
}}

/* ── Cards / sections ── */
div[data-testid="stVerticalBlock"] > div[data-testid="element-container"] > div {{
    border-radius: 6px;
}}

/* ── Metric cards ── */
[data-testid="stMetric"] {{
    background: {BRAND['card']};
    border: 1px solid {BRAND['border']};
    border-radius: 8px;
    padding: 1rem 1.25rem;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}}
[data-testid="stMetricValue"] {{
    color: {BRAND['navy']} !important;
    font-weight: 700 !important;
}}
[data-testid="stMetricLabel"] {{
    color: {BRAND['text_muted']} !important;
    font-size: 0.78rem !important;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}}

/* ── Dataframes ── */
[data-testid="stDataFrame"] {{
    border: 1px solid {BRAND['border']} !important;
    border-radius: 6px;
    overflow: hidden;
}}

/* ── Buttons ── */
.stButton > button {{
    background: {BRAND['accent']} !important;
    color: #FFFFFF !important;
    border: none !important;
    border-radius: 6px !important;
    font-weight: 500 !important;
    font-size: 0.85rem !important;
    padding: 0.5rem 1.25rem !important;
    transition: all 0.15s !important;
}}
.stButton > button:hover {{
    background: {BRAND['navy_mid']} !important;
    box-shadow: 0 2px 8px rgba(26,111,191,0.3) !important;
}}

/* ── Selectbox / inputs ── */
.stSelectbox [data-baseweb="select"] {{
    border-radius: 6px !important;
    border-color: {BRAND['border']} !important;
    font-size: 0.85rem !important;
}}

/* ── Section headers inside tabs ── */
h3 {{
    color: {BRAND['navy']} !important;
    font-size: 0.95rem !important;
    font-weight: 600 !important;
    margin-top: 1.25rem !important;
    margin-bottom: 0.5rem !important;
    padding-bottom: 0.4rem;
    border-bottom: 1px solid {BRAND['border']};
}}

/* ── Dividers ── */
hr {{
    border-color: {BRAND['border']} !important;
    margin: 1rem 0 !important;
}}

/* ── Expanders ── */
.streamlit-expanderHeader {{
    background: {BRAND['bg']} !important;
    border: 1px solid {BRAND['border']} !important;
    border-radius: 6px !important;
    font-weight: 500 !important;
    color: {BRAND['navy']} !important;
}}

/* ── Success / warning / info ── */
.stSuccess {{ background: #F0FDF4 !important; border-left: 4px solid #1E7E4B !important; }}
.stWarning {{ background: #FFFBEB !important; border-left: 4px solid #B7670A !important; }}
.stInfo    {{ background: {BRAND['accent_light']} !important; border-left: 4px solid {BRAND['accent']} !important; }}

/* ── Download button ── */
.stDownloadButton > button {{
    background: transparent !important;
    color: {BRAND['accent']} !important;
    border: 1px solid {BRAND['accent']} !important;
}}
.stDownloadButton > button:hover {{
    background: {BRAND['accent_light']} !important;
}}

/* ── CSS variables so metric_card works on any background ── */
:root {{
    --srsid-card:   #FFFFFF;
    --srsid-border: #E2E8F0;
    --srsid-muted:  #64748B;
    --srsid-navy:   #0F2644;
    --srsid-bg:     #F5F7FA;
}}

/* ── Text visibility fixes ── */
.stMarkdown p, .stMarkdown li, .stMarkdown td, .stMarkdown th {{
    color: {BRAND['text_primary']} !important;
}}
.stCaption, .stCaption p {{
    color: {BRAND['text_muted']} !important;
}}
[data-testid="stText"] {{
    color: {BRAND['text_primary']} !important;
}}
.stAlert p, .stInfo p, .stWarning p, .stSuccess p, .stError p {{
    color: inherit !important;
}}

/* ── Elevated dataframe tables ── */
[data-testid="stDataFrame"] {{
    border: 1px solid {BRAND['border']} !important;
    border-radius: 10px !important;
    overflow: hidden !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06) !important;
}}
[data-testid="stDataFrame"] table {{
    border-collapse: separate !important;
    border-spacing: 0 !important;
    width: 100% !important;
}}
[data-testid="stDataFrame"] thead tr th {{
    background: {BRAND['navy']} !important;
    color: #FFFFFF !important;
    font-size: 0.75rem !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
    padding: 10px 14px !important;
    border-bottom: 2px solid {BRAND['accent']} !important;
    white-space: nowrap !important;
}}
[data-testid="stDataFrame"] tbody tr:nth-child(even) td {{
    background: {BRAND['bg']} !important;
}}
[data-testid="stDataFrame"] tbody tr:nth-child(odd) td {{
    background: #FFFFFF !important;
}}
[data-testid="stDataFrame"] tbody tr:hover td {{
    background: {BRAND['accent_light']} !important;
    transition: background 0.12s ease !important;
}}
[data-testid="stDataFrame"] tbody td {{
    font-size: 0.82rem !important;
    color: {BRAND['text_primary']} !important;
    padding: 9px 14px !important;
    border-bottom: 1px solid {BRAND['border']} !important;
}}
[data-testid="stDataFrame"] tbody tr:last-child td {{
    border-bottom: none !important;
}}

/* ── Subheader text visibility ── */
h2, h3, .stSubheader {{
    color: {BRAND['navy']} !important;
}}

/* ── Info / warning boxes text ── */
.stInfo, .stWarning, .stSuccess, .stError {{
    border-radius: 6px !important;
}}
div[data-testid="stMarkdownContainer"] p {{
    color: {BRAND['text_primary']};
}}

/* ── Expander content text ── */
.streamlit-expanderContent p,
.streamlit-expanderContent li,
.streamlit-expanderContent td {{
    color: {BRAND['text_primary']} !important;
}}
</style>
""", unsafe_allow_html=True)

# ── Colors ────────────────────────────────────────────────────────────────────

# ── Plotly chart defaults ──────────────────────────────────────────────────────
PLOTLY_LAYOUT = dict(
    font=dict(family="Inter, -apple-system, sans-serif", size=12, color="#334155"),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(t=40, b=20, l=10, r=10),
    legend=dict(
        bgcolor="rgba(255,255,255,0.9)",
        bordercolor="#E2E8F0",
        borderwidth=1,
        font=dict(size=11),
    ),
    xaxis=dict(
        gridcolor="#F1F5F9",
        linecolor="#E2E8F0",
        tickfont=dict(size=11, color="#64748B"),
    ),
    yaxis=dict(
        gridcolor="#F1F5F9",
        linecolor="#E2E8F0",
        tickfont=dict(size=11, color="#64748B"),
    ),
)

def apply_layout(fig, title=None, height=300):
    fig.update_layout(**PLOTLY_LAYOUT, height=height)
    if title:
        fig.update_layout(
            title=dict(text=title, font=dict(size=13, color="#1A3A5C"),
                       x=0, xanchor="left", pad=dict(l=0, b=8))
        )
    return fig



# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADERS  (all from Postgres)
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_db():
    return DBClient().connect()


@st.cache_data(ttl=300)
def load_portfolio_summary() -> dict:
    try:
        with DBClient() as db:
            return db.get_portfolio_summary() or {}
    except Exception as e:
        st.error(
            f"**Database not connected** — configure DB credentials in Streamlit secrets. *{e}*"
        )
        return {}


@st.cache_data(ttl=300)
def load_vendors(risk_tier=None, industry=None, country=None,
                  spend_min=None, spend_max=None) -> pd.DataFrame:
    try:
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
    except Exception:
        return pd.DataFrame()


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
def load_segments(vendor_ids: tuple = None) -> pd.DataFrame:
    with DBClient() as db:
        where = ""
        params = None
        if vendor_ids:
            placeholders = ",".join(["%s"] * len(vendor_ids))
            where  = f"WHERE s.vendor_id IN ({placeholders})"
            params = vendor_ids
        return db.fetch_df(f"""
            SELECT s.vendor_id, s.supplier_name,
                   s.kraljic_segment, s.supply_risk_score,
                   s.profit_impact_score, s.cluster_label,
                   s.abc_class, s.spend_rank,
                   s.risk_spend_quadrant, s.strategic_action,
                   v.total_annual_spend, v.composite_risk_score,
                   v.country_code, v.industry_category
            FROM latest_segments s
            LEFT JOIN vendors v ON s.vendor_id = v.vendor_id
            {where}
            ORDER BY v.total_annual_spend DESC NULLS LAST
        """, params)


@st.cache_data(ttl=300)
def load_explanations() -> pd.DataFrame:
    with DBClient() as db:
        # Check which columns actually exist — LIME cols added by explainability.py
        existing = set(db.fetch_df(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'explanations'"
        )["column_name"].tolist())

        # Core SHAP columns (always present)
        cols = [
            "e.vendor_id", "e.supplier_name", "e.predicted_risk_tier",
            "e.driver_1_label", "e.driver_1_shap", "e.driver_1_value",
            "e.driver_2_label", "e.driver_2_shap", "e.driver_2_value",
            "e.driver_3_label", "e.driver_3_shap", "e.driver_3_value",
            "e.mitigator_label", "e.mitigator_shap",
            "e.narrative",
            "v.total_annual_spend", "v.risk_label",
        ]
        # LIME columns — only add if they exist in the table
        lime_cols = [
            ("lime_driver_1_label",  "e.lime_driver_1_label"),
            ("lime_driver_1_weight", "e.lime_driver_1_weight"),
            ("lime_driver_2_label",  "e.lime_driver_2_label"),
            ("lime_driver_2_weight", "e.lime_driver_2_weight"),
            ("lime_driver_3_label",  "e.lime_driver_3_label"),
            ("lime_driver_3_weight", "e.lime_driver_3_weight"),
            ("lime_narrative",       "e.lime_narrative"),
            ("methods_agree",        "e.methods_agree"),
        ]
        for col_name, col_expr in lime_cols:
            if col_name in existing:
                cols.append(col_expr)

        select_clause = ",\n                   ".join(cols)
        return db.fetch_df(f"""
            SELECT {select_clause}
            FROM latest_explanations e
            LEFT JOIN vendors v ON e.vendor_id = v.vendor_id
        """)


@st.cache_data(ttl=120)
def load_news(days: int = 30, disruption_only: bool = False,
              vendor_ids: tuple = None) -> pd.DataFrame:
    with DBClient() as db:
        extra_where = ""
        params_extra = ()
        if vendor_ids:
            placeholders = ",".join(["%s"] * len(vendor_ids))
            extra_where  = f"AND n.vendor_id IN ({placeholders})"
            params_extra = vendor_ids
        return db.fetch_df(f"""
            SELECT n.vendor_id, n.supplier_name, n.title, n.source_name,
                   n.published_at, n.sentiment_score, n.disruption_type,
                   n.disruption_flag, n.url
            FROM vendor_news n
            WHERE n.published_at >= NOW() - INTERVAL '{days} days'
            {"AND n.disruption_flag = TRUE" if disruption_only else ""}
            {extra_where}
            ORDER BY n.published_at DESC
        """, params_extra if params_extra else None)


@st.cache_data(ttl=300)
def load_spend_report() -> dict:
    """
    Load spend intelligence report. Tries local JSON first (faster),
    then falls back to recomputing from Supabase (works on Streamlit Cloud
    where the local JSON file doesn't exist).
    """
    p = PATHS["reports"] / "spend_intelligence_report.json"
    if p.exists():
        try:
            with open(p) as f:
                return json.load(f)
        except Exception:
            pass

    # Fallback — recompute from database
    try:
        with DBClient() as db:
            stats = db.fetch_one("""
                SELECT
                    SUM(total_annual_spend)       AS total_spend,
                    SUM(CASE WHEN under_contract THEN total_annual_spend ELSE 0 END)
                                                  AS spend_under_contract,
                    SUM(CASE WHEN is_maverick    THEN total_annual_spend ELSE 0 END)
                                                  AS maverick_spend,
                    SUM(savings_opportunity)      AS total_savings,
                    COUNT(*) FILTER (WHERE NOT COALESCE(under_contract, FALSE))
                                                  AS vendors_no_contract
                FROM vendors
            """)
            if not stats:
                return {}

            total = stats.get("total_spend") or 0
            sum_v = stats.get("spend_under_contract") or 0
            mav   = stats.get("maverick_spend") or 0
            sum_pct = (sum_v / total * 100) if total else 0
            mav_pct = (mav   / total * 100) if total else 0

            # Get HHI from concentration calculation
            conc_df = db.fetch_df("""
                SELECT 
                    SUM(POWER(spend_pct_of_portfolio, 2)) AS hhi
                FROM vendors
                WHERE spend_pct_of_portfolio > 0
            """)
            hhi = (conc_df.iloc[0]["hhi"] * 10000) if not conc_df.empty else 0

            return {
                "spend_intelligence": {
                    "total_portfolio_spend":   round(float(total), 2),
                    "spend_under_management":  round(float(sum_v), 2),
                    "sum_percentage":          round(sum_pct, 2),
                    "sum_target":              ">80%",
                    "maverick_spend":          round(float(mav), 2),
                    "maverick_percentage":     round(mav_pct, 2),
                    "maverick_target":         "<10%",
                },
                "concentration": {
                    "hhi":                     round(float(hhi), 0),
                    "hhi_status":              "HIGH" if hhi > 2500 else "MODERATE" if hhi > 1500 else "HEALTHY",
                },
                "opportunity": {
                    "total_savings_opportunity": round(
                        float(stats.get("total_savings") or 0), 2
                    ),
                    "vendors_needing_contracts": int(
                        stats.get("vendors_no_contract") or 0
                    ),
                },
            }
    except Exception as e:
        st.warning(f"Could not load spend data: {e}")
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
@st.cache_data(ttl=300)
def load_feature_importance() -> pd.DataFrame:
    """Load feature importance — from DB first, fallback to local CSV."""
    try:
        with DBClient() as db:
            if db.table_exists("feature_importance"):
                df = db.fetch_df(
                    "SELECT feature, feature_label, importance "
                    "FROM feature_importance "
                    "ORDER BY importance DESC LIMIT 20"
                )
                if not df.empty:
                    return df
    except Exception:
        pass
    # Fallback: local CSV (only available when running locally)
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

def metric_card(label: str, value, delta=None, color="#1A6FBF", border_top=True):
    border = f"border-top: 3px solid {color};" if border_top else ""
    delta_html = (f"<div style='font-size:0.72rem;color:var(--srsid-muted,#64748B);"
                  f"margin-top:4px'>{delta}</div>") if delta else ""
    st.markdown(
        f"""<div style='background:var(--srsid-card,#FFFFFF);{border}border-radius:8px;
                        padding:1rem 1.25rem;border:1px solid var(--srsid-border,#E2E8F0);
                        box-shadow:0 2px 6px rgba(0,0,0,0.06)'>
            <div style='font-size:0.70rem;color:var(--srsid-muted,#64748B);
                        text-transform:uppercase;letter-spacing:0.06em;
                        font-weight:600;margin-bottom:6px'>{label}</div>
            <div style='font-size:1.55rem;font-weight:700;color:{color};
                        letter-spacing:-0.02em;line-height:1.1'>{value}</div>
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

def styled_table(df: pd.DataFrame, height: int = 400,
                 hide_index: bool = True) -> None:
    """
    Render a dataframe with elevated styling:
    — dark navy header row
    — alternating row shading
    — hover highlight
    — consistent font and padding
    All controlled via CSS injected in the global style block.
    """
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=hide_index,
        height=height,
    )


def risk_badge(tier: str) -> str:
    c = COLORS.get(tier, "#95A5A6")
    return f"<span style='background:{c};color:white;padding:2px 8px;border-radius:4px;font-size:0.8rem'>{tier}</span>"


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

def render_sidebar() -> dict:
    st.sidebar.markdown(
        f"""<div style='padding:1.25rem 1rem 0.75rem;
                        border-bottom:1px solid rgba(255,255,255,0.12);
                        margin-bottom:1rem'>
            <div style='font-size:1.1rem;font-weight:700;color:#FFFFFF;
                        letter-spacing:0.02em'>SRSID</div>
            <div style='font-size:0.72rem;color:#B8C9DB;margin-top:2px;
                        text-transform:uppercase;letter-spacing:0.08em'>
                Supplier Risk Intelligence</div>
        </div>""",
        unsafe_allow_html=True,
    )

    # Portfolio KPIs — custom HTML cards (st.metric is invisible on dark bg)
    summary = load_portfolio_summary()
    if summary:
        total_vendors  = summary.get("total_vendors", "—")
        high_risk      = summary.get("high_risk_count", "—")
        medium_risk    = summary.get("medium_risk_count", "—")
        total_spend    = fmt_spend(summary.get("total_portfolio_spend")) \
                         if summary.get("total_portfolio_spend") else "—"
        otif           = summary.get("avg_otif_rate")
        otif_str       = f"{otif*100:.1f}%" if otif else "—"

        def kpi_card(label, value, accent="#1A6FBF"):
            return (
                f"<div style='background:rgba(255,255,255,0.07);"
                f"border-radius:6px;padding:10px 12px;margin-bottom:6px;"
                f"border-left:3px solid {accent}'>"
                f"<div style='font-size:0.68rem;color:#B8C9DB;text-transform:uppercase;"
                f"letter-spacing:0.07em;margin-bottom:3px'>{label}</div>"
                f"<div style='font-size:1.2rem;font-weight:700;color:#FFFFFF'>{value}</div>"
                f"</div>"
            )

        st.sidebar.markdown(
            "<div style='padding:0 0.1rem;font-size:0.68rem;color:#B8C9DB;"
            "text-transform:uppercase;letter-spacing:0.08em;margin-bottom:6px'>"
            "Portfolio Overview</div>",
            unsafe_allow_html=True,
        )
        col1, col2 = st.sidebar.columns(2)
        col1.markdown(kpi_card("Vendors", total_vendors), unsafe_allow_html=True)
        col2.markdown(kpi_card("High Risk", high_risk, "#C0392B"), unsafe_allow_html=True)
        col1.markdown(kpi_card("Med Risk", medium_risk, "#B7670A"), unsafe_allow_html=True)
        col2.markdown(kpi_card("OTIF", otif_str, "#1E7E4B"), unsafe_allow_html=True)
        st.sidebar.markdown(kpi_card("Portfolio Spend", total_spend), unsafe_allow_html=True)

    st.sidebar.markdown(
        "<hr style='border-color:rgba(255,255,255,0.12);margin:0.75rem 0'>",
        unsafe_allow_html=True,
    )

    # Filters — inject CSS to make selectbox readable on dark background
    st.sidebar.markdown("""
    <style>
    [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div {
        background: rgba(255,255,255,0.10) !important;
        border-color: rgba(255,255,255,0.20) !important;
        color: #FFFFFF !important;
    }
    [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] span {
        color: #FFFFFF !important;
    }
    [data-testid="stSidebar"] .stSlider [data-testid="stTickBarMin"],
    [data-testid="stSidebar"] .stSlider [data-testid="stTickBarMax"] {
        color: #B8C9DB !important;
    }
    </style>
    """, unsafe_allow_html=True)

    st.sidebar.markdown(
        "<div style='font-size:0.68rem;color:#B8C9DB;text-transform:uppercase;"
        "letter-spacing:0.08em;margin-bottom:6px'>Filters</div>",
        unsafe_allow_html=True,
    )

    filters = {
        "risk_tier": st.sidebar.selectbox("Risk Tier", ["All", "High", "Medium", "Low"]),
        "industry":  st.sidebar.selectbox("Industry",  ["All"] + _get_industries()),
        "country":   st.sidebar.selectbox("Country",   ["All"] + _get_countries()),
    }

    spend_range = st.sidebar.slider(
        "Annual Spend ($)", 0, 10_000_000,
        (0, 10_000_000), step=100_000, format="$%d"
    )
    filters["spend_min"] = spend_range[0] if spend_range[0] > 0 else None
    filters["spend_max"] = spend_range[1] if spend_range[1] < 10_000_000 else None

    st.sidebar.markdown(
        "<hr style='border-color:rgba(255,255,255,0.12);margin:0.75rem 0'>",
        unsafe_allow_html=True,
    )
    if st.sidebar.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.markdown(
        "<div style='font-size:0.68rem;color:rgba(255,255,255,0.30);margin-top:6px'>"
        "PostgreSQL · srsid_db</div>",
        unsafe_allow_html=True,
    )
    return filters


@st.cache_data(ttl=300)
def load_filtered_vendor_ids(risk_tier=None, industry=None, country=None,
                              spend_min=None, spend_max=None):
    """Return tuple of vendor_ids matching current filters (tuple for cache key hashing)."""
    is_filtered = any([
        risk_tier  and risk_tier  != "All",
        industry   and industry   != "All",
        country    and country    != "All",
        spend_min  is not None,
        spend_max  is not None,
    ])
    if not is_filtered:
        return None   # None = "no filter" → loaders return all data
    df = load_vendors(risk_tier=risk_tier, industry=industry, country=country,
                      spend_min=spend_min, spend_max=spend_max)
    return tuple(df["vendor_id"].tolist()) if not df.empty else ()

def _fkw(filters):
    """Extract filter kwargs for load_vendors calls."""
    return {k: v for k, v in filters.items()
            if k in ["risk_tier","industry","country","spend_min","spend_max"]}

def _vids(filters):
    """Get filtered vendor_ids tuple for segment/news/alternatives loaders."""
    return load_filtered_vendor_ids(**{k: v for k, v in filters.items()
                                       if k in ["risk_tier","industry","country",
                                                "spend_min","spend_max"]})




def active_filter_bar(filters: dict):
    """Show a compact banner when non-default filters are active."""
    active = {k: v for k, v in filters.items()
              if v not in (None, "All", 0) and k not in ("spend_min","spend_max")}
    if filters.get("spend_min") or filters.get("spend_max"):
        active["spend"] = (
            f"${filters.get('spend_min', 0):,} – "
            f"${filters.get('spend_max', 10_000_000):,}"
        )
    if active:
        tags = " · ".join(f"<b>{v}</b>" for v in active.values())
        st.markdown(
            f"<div style='background:#E8F2FB;border-left:3px solid #1A6FBF;"
            f"border-radius:4px;padding:6px 12px;margin-bottom:1rem;"
            f"font-size:0.8rem;color:#0F2644'>"
            f"Filtered by: {tags} — charts and tables reflect this selection.</div>",
            unsafe_allow_html=True,
        )
    with DBClient() as db:
        df = db.fetch_df(
            "SELECT DISTINCT industry_category FROM vendors "
            "WHERE industry_category IS NOT NULL ORDER BY industry_category"
        )
    return df["industry_category"].tolist() if not df.empty else []


@st.cache_data(ttl=600)
def _get_industries() -> list:
    try:
        with DBClient() as db:
            df = db.fetch_df(
                "SELECT DISTINCT industry_category FROM vendors "
                "WHERE industry_category IS NOT NULL ORDER BY industry_category"
            )
        return df["industry_category"].tolist() if not df.empty else []
    except Exception:
        return []


@st.cache_data(ttl=600)
def _get_countries() -> list:
    try:
        with DBClient() as db:
            df = db.fetch_df(
                "SELECT DISTINCT country_code FROM vendors "
                "WHERE country_code IS NOT NULL ORDER BY country_code"
            )
        return df["country_code"].tolist() if not df.empty else []
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — OVERVIEW
# ─────────────────────────────────────────────────────────────────────────────

def tab_overview(filters: dict):
    st.header("Executive Overview")
    active_filter_bar(filters)

    f = {k: v for k, v in filters.items()
         if k in ["risk_tier","industry","country","spend_min","spend_max"]}
    is_filtered = any(v not in (None,"All") for v in f.values())

    # KPIs — from filtered vendors if filter active, else portfolio summary
    vendors = load_vendors(**_fkw(filters))
    if is_filtered and not vendors.empty:
        total_v  = len(vendors)
        high     = (vendors["risk_label"]=="High").sum()
        med      = (vendors["risk_label"]=="Medium").sum()
        spend    = vendors["total_annual_spend"].sum()
        otif_v   = vendors["otif_rate"].mean() if "otif_rate" in vendors.columns else None
        otif_str = f"{otif_v*100:.1f}%" if pd.notna(otif_v) else "—"
    else:
        summary  = load_portfolio_summary()
        total_v  = summary.get("total_vendors","—")
        high     = summary.get("high_risk_count","—")
        med      = summary.get("medium_risk_count","—")
        spend    = summary.get("total_portfolio_spend")
        otif_val = summary.get("avg_otif_rate")
        otif_str = f"{otif_val*100:.1f}%" if otif_val else "—"

    # KPI cards
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: metric_card("Total Vendors",    total_v,            color=BRAND["navy"])
    with c2: metric_card("High Risk",        high,               color=COLORS["High"])
    with c3: metric_card("Medium Risk",      med,                color=COLORS["Medium"])
    with c4: metric_card("Portfolio Spend",  fmt_spend(spend),   color=BRAND["accent"])
    with c5: metric_card("Avg OTIF",         otif_str,           color=COLORS["Low"])

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Risk Distribution")
        if not vendors.empty and "risk_label" in vendors.columns:
            rd = vendors["risk_label"].value_counts().to_dict()
            if PLOTLY:
                fig = px.pie(names=list(rd.keys()), values=list(rd.values()),
                             color=list(rd.keys()),
                             color_discrete_map=COLORS, hole=0.45)
                apply_layout(fig, height=280)
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
                fig.update_layout(yaxis=dict(autorange="reversed"), showlegend=False)
                apply_layout(fig, height=280)
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
            apply_layout(fig)
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
    st.header("Risk Predictions")

    risk_df = load_vendors(**{k: v for k, v in filters.items()
                               if k in ["risk_tier","industry","country",
                                        "spend_min","spend_max"]})
    if risk_df.empty:
        st.info("No risk data. Run ml/risk_model.py first.")
        return

    # Load alternatives and segments for enrichment
    alts = load_alternatives(vendor_ids=_vids(filters))
    segs = load_segments(vendor_ids=_vids(filters))

    def enrich(df: pd.DataFrame) -> pd.DataFrame:
        """Add best alternative + strategic action to each vendor row."""
        out = df.copy()
        # Best alternative per vendor (rank 1)
        if not alts.empty:
            best_alt = (alts[alts["alternative_rank"] == 1]
                        [["vendor_id","alt_supplier_name",
                          "alt_risk_tier","recommendation_reason"]]
                        .drop_duplicates("vendor_id"))
            out = out.merge(best_alt, on="vendor_id", how="left")
        else:
            out["alt_supplier_name"]    = "—"
            out["alt_risk_tier"]        = "—"
            out["recommendation_reason"]= "—"

        # Strategic action from segmentation
        if not segs.empty and "strategic_action" in segs.columns:
            out = out.merge(
                segs[["vendor_id","strategic_action"]].drop_duplicates("vendor_id"),
                on="vendor_id", how="left"
            )
        else:
            out["strategic_action"] = "—"

        # Fill nulls
        for c in ["alt_supplier_name","alt_risk_tier",
                  "recommendation_reason","strategic_action"]:
            if c in out.columns:
                out[c] = out[c].fillna("—")
        return out

    # ── High Risk table ──────────────────────────────────────────────────────
    st.subheader("High Risk Suppliers")
    high = risk_df[risk_df["risk_label"] == "High"].head(20)
    if not high.empty:
        high_enr = enrich(high)
        disp_cols = ["supplier_name","composite_risk_score","total_annual_spend",
                     "country_code","industry_category","delivery_performance",
                     "alt_supplier_name","alt_risk_tier",
                     "recommendation_reason","strategic_action"]
        disp = high_enr[[c for c in disp_cols if c in high_enr.columns]].copy()
        disp["total_annual_spend"] = disp["total_annual_spend"].apply(fmt_spend)
        disp.columns = [c.replace("_"," ").title() for c in disp.columns]
        styled_table(disp)
    else:
        st.success("No High Risk suppliers in current filter.")

    # ── Risk Score Distribution (full width now that FI is removed) ──────────
    st.subheader("Risk Score Distribution")
    if PLOTLY and "composite_risk_score" in risk_df.columns:
        fig = px.histogram(risk_df, x="composite_risk_score",
                           nbins=30, color_discrete_sequence=["#E74C3C"])
        from config import RISK_THRESHOLDS
        fig.add_vline(x=RISK_THRESHOLDS["high"],   line_dash="dash",
                      line_color="red",    annotation_text="High threshold")
        fig.add_vline(x=RISK_THRESHOLDS["medium"], line_dash="dash",
                      line_color="orange", annotation_text="Medium threshold")
        apply_layout(fig)
        st.plotly_chart(fig, use_container_width=True)

    # ── All Vendors table ────────────────────────────────────────────────────
    st.subheader("All Supplier Risk Scores")
    all_enr = enrich(risk_df)
    disp_cols = ["supplier_name","risk_label","composite_risk_score",
                 "total_annual_spend","country_code","industry_category",
                 "delivery_performance","otif_rate",
                 "alt_supplier_name","recommendation_reason","strategic_action"]
    disp = all_enr[[c for c in disp_cols if c in all_enr.columns]].copy()
    disp.sort_values("composite_risk_score", ascending=False, inplace=True)
    disp["total_annual_spend"] = disp["total_annual_spend"].apply(fmt_spend)
    disp.columns = [c.replace("_"," ").title() for c in disp.columns]
    styled_table(disp)

    st.download_button("Export",
                       risk_df.to_csv(index=False).encode(),
                       "risk_scores.csv", "text/csv")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 — SEGMENTATION
# ─────────────────────────────────────────────────────────────────────────────

def tab_segmentation(filters: dict):
    st.header("Supplier Segmentation")

    seg = load_segments(vendor_ids=_vids(filters))
    if seg.empty:
        st.info("No segmentation data. Run ml/segmentation.py first.")
        return

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Kraljic Matrix")
        if PLOTLY and "supply_risk_score" in seg.columns:
            seg_plot = seg.copy()

            # Bug fix: bubble size — raw spend makes one vendor dominate the chart
            # Use log10(spend) so $50M and $500K are distinguishable, not 100:1 pixels
            if "total_annual_spend" in seg_plot.columns:
                seg_plot["_bubble"] = np.log10(
                    seg_plot["total_annual_spend"].clip(lower=1)
                ).clip(lower=1)
            else:
                seg_plot["_bubble"] = 5

            fig = px.scatter(
                seg_plot,
                x="supply_risk_score",
                y="profit_impact_score",
                color="kraljic_segment",
                color_discrete_map=COLORS,
                hover_name="supplier_name",
                hover_data={"total_annual_spend": True,
                            "country_code": True,
                            "_bubble": False},
                size="_bubble",
                size_max=20,
                opacity=0.75,
                labels={"supply_risk_score": "Supply Risk →",
                        "profit_impact_score": "Profit Impact →"},
            )
            # Quadrant dividers at the median of each axis
            fig.add_vline(x=seg["supply_risk_score"].median(),
                          line_dash="dash", line_color="gray", line_width=1)
            fig.add_hline(y=seg["profit_impact_score"].median(),
                          line_dash="dash", line_color="gray", line_width=1)
            # Quadrant labels (inside the plot area)
            sr_mid = seg["supply_risk_score"].median()
            pi_mid = seg["profit_impact_score"].median()
            for txt, ax, ay in [
                ("Strategic",  sr_mid + 0.03, pi_mid + 0.03),
                ("Leverage",   sr_mid - 0.03, pi_mid + 0.03),
                ("Bottleneck", sr_mid + 0.03, pi_mid - 0.03),
                ("Tactical",   sr_mid - 0.03, pi_mid - 0.03),
            ]:
                fig.add_annotation(
                    x=ax, y=ay, text=txt,
                    showarrow=False,
                    font=dict(size=10, color="gray"),
                    xanchor="left" if "Strategic" in txt or "Bottleneck" in txt else "right",
                )
            apply_layout(fig, height=380)
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
        styled_table(action_df)

    st.download_button("Export Segments",
                       seg.to_csv(index=False).encode(),
                       "segments.csv", "text/csv")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 4 — SPEND ANALYTICS
# ─────────────────────────────────────────────────────────────────────────────

def tab_spend(filters: dict):
    st.header("Spend Analytics")
    active_filter_bar(filters)
    f = {k: v for k, v in filters.items()
         if k in ["risk_tier","industry","country","spend_min","spend_max"]}
    vendors = load_vendors(**_fkw(filters))

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
                        delta="40% addressable × 15–25% savings rate",
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
                      delta="Exceeds 40% limit"
                      if co.get("top5_exceeds_limit") else "Within limit")
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
        styled_table(top)

    st.divider()

    # ── Spend by UNSPSC Segment (UN standard taxonomy) ───────────────────────
    st.subheader("Spend by UNSPSC Segment")
    st.caption(
        "UN Standard Products and Services Code (UNSPSC v25) — "
        "internationally recognised spend taxonomy, mapped from SAP material groups."
    )
    try:
        with DBClient() as db:
            unspsc_df = db.fetch_df("""
                SELECT unspsc_segment, unspsc_code, unspsc_segment_name,
                       vendor_count, transaction_count,
                       total_spend, spend_pct, classification_method
                FROM unspsc_spend_summary
                ORDER BY total_spend DESC
                LIMIT 25
            """) if db.table_exists("unspsc_spend_summary") else pd.DataFrame()
    except Exception:
        unspsc_df = pd.DataFrame()

    if unspsc_df.empty:
        st.info(
            "UNSPSC classification not yet run. "
            "Execute `python ingestion/unspsc_classifier.py` to classify "
            "transactions against the UNSPSC v25 taxonomy."
        )
    else:
        col1, col2 = st.columns(2)
        with col1:
            if PLOTLY:
                fig = px.bar(
                    unspsc_df.head(15),
                    x="total_spend", y="unspsc_segment_name",
                    orientation="h",
                    color="spend_pct",
                    color_continuous_scale="Blues",
                    labels={
                        "total_spend":         "Total Spend",
                        "unspsc_segment_name": "UNSPSC Segment",
                        "spend_pct":           "% of Portfolio",
                    },
                    title="Spend by UNSPSC segment"
                )
                fig.update_layout(yaxis=dict(autorange="reversed"),
                                  margin=dict(t=40), height=450)
                apply_layout(fig)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            if PLOTLY and "classification_method" in unspsc_df.columns:
                method_counts = (
                    unspsc_df.groupby("classification_method")["total_spend"]
                    .sum().reset_index()
                )
                method_counts.columns = ["Method", "Spend"]
                fig2 = px.pie(
                    method_counts, values="Spend", names="Method",
                    title="Classification coverage by method",
                    hole=0.45,
                    color_discrete_sequence=["#1A6FBF","#1D9E75",
                                             "#E07B00","#95A5A6"],
                )
                fig2.update_traces(textposition="outside",
                                   textinfo="percent+label")
                fig2.update_layout(margin=dict(t=40, b=10),
                                   showlegend=False, height=450)
                st.plotly_chart(fig2, use_container_width=True)

        disp = unspsc_df.copy()
        disp["total_spend"] = disp["total_spend"].apply(fmt_spend)
        disp["spend_pct"]   = disp["spend_pct"].apply(lambda x: f"{x:.1f}%")
        disp.rename(columns={
            "unspsc_segment":        "Segment",
            "unspsc_code":           "UNSPSC Code",
            "unspsc_segment_name":   "Segment Name",
            "vendor_count":          "Vendors",
            "transaction_count":     "Transactions",
            "total_spend":           "Spend",
            "spend_pct":             "% Portfolio",
            "classification_method": "Method",
        }, inplace=True)
        styled_table(disp)

    with st.expander("How is savings opportunity calculated?"):
        st.markdown("""
**Methodology (Ardent Partners / Hackett Group benchmarks):**

| Vendor Type | Addressable Portion | Savings Rate | Basis |
|---|---|---|---|
| Off-Contract (High Value) | 40% | 20% | Volume consolidation |
| Off-Contract (Regular) | 40% | 15% | Standard renegotiation |
| Emergency / One-Off | 40% | 25% | Eliminate one-off purchases |

**Savings opportunity = off-contract spend × 40% addressable × savings rate**

*Note: All vendors show as off-contract because the SAP dataset contracts table has
no active contracts. In a live deployment, contracted vendors are excluded.*
        """)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 5 — EXPLAINABILITY
# ─────────────────────────────────────────────────────────────────────────────

def tab_explainability():
    st.header("Risk Explainability — SHAP & LIME")
    st.caption(
        "Dual-method explainability. **SHAP** computes globally consistent "
        "feature attributions using game theory. **LIME** fits a local "
        "linear model around each vendor to explain why *this specific* "
        "prediction was made. Cross-validation between methods increases "
        "trust in the explanation."
    )

    expl = load_explanations()
    if expl.empty:
        st.info("No explanations. Run ml/explainability.py first.")
        return

    # Vendor search
    vendor_names = expl["supplier_name"].dropna().unique().tolist()
    selected = st.selectbox("Select a vendor to explain", sorted(vendor_names))

    if selected:
        row = expl[expl["supplier_name"] == selected].iloc[0]

        # ── Risk tier + agreement badge ─────────────────────────────────────
        tier  = row.get("predicted_risk_tier", "Unknown")
        color = COLORS.get(tier, "#95A5A6")
        agree = row.get("methods_agree", "")

        badge_html = (
            f"<span style='background:{color};color:white;padding:3px 10px;"
            f"border-radius:4px;font-size:0.85rem'>{tier}</span>"
        )
        if agree:
            agree_color = "#1E7E4B" if agree == "YES" else "#B7670A"
            badge_html += (
                f" &nbsp; <span style='background:{agree_color};color:white;"
                f"padding:3px 10px;border-radius:4px;font-size:0.75rem'>"
                f"SHAP & LIME agree: {agree}</span>"
            )
        st.markdown(f"**Risk Classification:** {badge_html}",
                    unsafe_allow_html=True)

        narr = row.get("narrative")
        if narr and pd.notna(narr) and str(narr) != "nan":
            st.info(str(narr))

        # ── Side-by-side SHAP + LIME charts ─────────────────────────────────
        col1, col2 = st.columns(2)

        # SHAP drivers
        shap_drivers = []
        for i in range(1, 4):
            label = row.get(f"driver_{i}_label")
            shap  = row.get(f"driver_{i}_shap")
            if label and pd.notna(shap):
                shap_drivers.append({"Feature": label, "SHAP": float(shap)})
        if row.get("mitigator_label") and pd.notna(row.get("mitigator_shap")):
            shap_drivers.append({
                "Feature": f"{row['mitigator_label']} (mitigator)",
                "SHAP": float(row["mitigator_shap"])
            })

        with col1:
            st.markdown("**SHAP — global attribution**")
            if shap_drivers and PLOTLY:
                d_df = pd.DataFrame(shap_drivers)
                colors = ["#E74C3C" if v > 0 else "#27AE60" for v in d_df["SHAP"]]
                fig = go.Figure(go.Bar(
                    x=d_df["SHAP"], y=d_df["Feature"],
                    orientation="h",
                    marker_color=colors,
                    text=[f"{v:+.3f}" for v in d_df["SHAP"]],
                    textposition="outside",
                ))
                fig.update_layout(
                    xaxis_title="SHAP Value",
                    yaxis=dict(autorange="reversed"),
                    margin=dict(t=20, b=10),
                    height=300,
                )
                apply_layout(fig)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.caption("No SHAP data for this vendor.")

        # LIME drivers
        lime_drivers = []
        for i in range(1, 4):
            ll = row.get(f"lime_driver_{i}_label")
            lw = row.get(f"lime_driver_{i}_weight")
            if ll and pd.notna(lw):
                lime_drivers.append({"Feature": ll, "LIME": float(lw)})

        with col2:
            st.markdown("**LIME — local approximation**")
            if lime_drivers and PLOTLY:
                l_df   = pd.DataFrame(lime_drivers)
                colors = ["#E74C3C" if v > 0 else "#27AE60" for v in l_df["LIME"]]
                # Format text labels so tiny weights still show sign
                def _fmt(v):
                    if abs(v) < 1e-6:   return "≈ 0"
                    if abs(v) < 0.001:  return f"{v:+.2e}"
                    return f"{v:+.3f}"
                fig = go.Figure(go.Bar(
                    x=l_df["LIME"], y=l_df["Feature"],
                    orientation="h",
                    marker_color=colors,
                    text=[_fmt(v) for v in l_df["LIME"]],
                    textposition="outside",
                ))
                fig.update_layout(
                    xaxis_title="LIME Weight",
                    yaxis=dict(autorange="reversed"),
                    margin=dict(t=20, b=10),
                    height=300,
                )
                apply_layout(fig)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.caption("No LIME data for this vendor. "
                           "Install lime and re-run ml/explainability.py")

        # ── LIME narrative expander ─────────────────────────────────────────
        lime_narr = row.get("lime_narrative")
        if lime_narr and pd.notna(lime_narr) and str(lime_narr) != "nan":
            with st.expander("LIME local-explanation narrative"):
                st.write(str(lime_narr))

        st.divider()

    # ── Global feature importance ───────────────────────────────────────────
    st.subheader("Global Feature Importance")
    st.caption(
        "Average feature importance computed by the risk model across all "
        "2,541 vendors. Use this to understand which features the model "
        "relies on most globally, independent of any single supplier."
    )
    fi = load_feature_importance()
    if not fi.empty and PLOTLY:
        label_col = "feature_label" if "feature_label" in fi.columns else "feature"
        fig = px.bar(fi.head(15), x="importance", y=label_col,
                     orientation="h",
                     color="importance", color_continuous_scale="Reds",
                     labels={"importance": "Importance", label_col: ""})
        fig.update_layout(yaxis=dict(autorange="reversed"),
                          margin=dict(t=10), showlegend=False,
                          height=420)
        apply_layout(fig)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(
            "Feature importance not yet available. "
            "It is generated when the risk model runs. "
            "Run `python ml/risk_model.py` locally — the chart will appear "
            "on the next pipeline run once results are saved to the database."
        )


# ─────────────────────────────────────────────────────────────────────────────
# TAB 6 — NEWS FEED
# ─────────────────────────────────────────────────────────────────────────────

def tab_news(filters: dict):
    st.header("Supplier News & Disruptions")
    active_filter_bar(filters)

    col1, col2 = st.columns([3, 1])
    with col2:
        days = st.selectbox("Lookback", [7, 14, 30, 60], index=2)
        disruption_only = st.checkbox("Disruptions only", value=False)

    news = load_news(days=days, disruption_only=disruption_only,
                     vendor_ids=_vids(filters))

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
    """Combined Vendor Profile + Scorecard tab."""
    st.header("Vendor Profile & Scorecard")

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
    if not selected:
        return

    # Load full profile
    with DBClient() as db:
        profile = db.get_vendor_profile(selected)
        existing_cols = set(db.fetch_df(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'vendors'"
        )["column_name"].tolist())

    if not profile:
        st.warning("Vendor profile not found.")
        return

    v          = profile["vendor"]
    seg        = profile.get("segment", {}) or {}
    expl       = profile.get("explanation", {}) or {}
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
        f"ID: {v.get('vendor_id','')} · "
        f"Annual Spend: {fmt_spend(v.get('total_annual_spend'))}"
    )
    st.divider()

    # ── Key metrics ──────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Annual Spend",       fmt_spend(v.get("total_annual_spend")))
    c2.metric("OTIF Rate",          f"{(v.get('otif_rate') or 0)*100:.1f}%")
    c3.metric("Risk Score",         f"{v.get('composite_risk_score') or 0:.3f}")
    c4.metric("Disruptions (30d)",  v.get("disruption_count_30d", 0))
    st.divider()

    # ── Operational Performance ───────────────────────────────────────────────
    st.subheader("Operational Performance")
    c1, c2, c3, c4, c5 = st.columns(5)

    def pct_m(col, label, val, target=None):
        if val is None:
            col.metric(label, "N/A"); return
        pct   = val * 100 if val <= 1.0 else val
        delta = f"{pct - target:+.1f}% vs {target:.0f}% target" if target else None
        col.metric(label, f"{pct:.1f}%", delta=delta,
                   delta_color="normal" if (target is None or pct >= target) else "inverse")

    pct_m(c1, "OTD",            v.get("delivery_performance"), 95)
    pct_m(c2, "OTIF",           v.get("otif_rate"),            95)
    pct_m(c3, "OTTR",           v.get("ottr_rate"),            90)
    pct_m(c4, "Order Accuracy", v.get("order_accuracy_rate"),  98)
    lt = v.get("lead_time_variability")
    c5.metric("Lead Time Var.",
              f"±{lt:.1f}d" if lt is not None else "N/A",
              delta="High" if lt and lt > 10 else None,
              delta_color="inverse" if lt and lt > 10 else "normal")
    st.divider()

    # ── Financial & Strategic ─────────────────────────────────────────────────
    st.subheader("Financial & Strategic")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Financial Stability",  f"{v.get('financial_stability') or 0:.0f}/100")
    ppv = v.get("avg_price_variance_pct")
    c2.metric("PPV",                  f"{ppv:.1f}%" if ppv else "N/A",
              delta="High" if ppv and ppv > 15 else None,
              delta_color="inverse" if ppv and ppv > 15 else "normal")
    sum_pct = v.get("sum_percentage")
    c3.metric("Contract Compliance",
              f"{sum_pct:.1f}%" if sum_pct else "N/A",
              delta="Below target" if sum_pct and sum_pct < 80 else "On target",
              delta_color="inverse" if sum_pct and sum_pct < 80 else "normal")
    c4.metric("Savings Opportunity",  fmt_spend(v.get("savings_opportunity")))
    st.divider()

    # ── Risk Drivers + Segmentation ───────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Risk Drivers")
        if expl:
            drivers_found = False

            # ── SHAP drivers ─────────────────────────────────────────────────
            shap_drivers = []
            for i in range(1, 4):
                label = expl.get(f"driver_{i}_label")
                shap  = expl.get(f"driver_{i}_shap")
                if label and shap is not None:
                    shap_drivers.append((label, shap,
                                         expl.get(f"driver_{i}_value", "")))

            if shap_drivers:
                drivers_found = True
                st.caption("**SHAP** — global feature attribution")
                for label, shap, value in shap_drivers:
                    icon    = "🔴" if float(shap) > 0 else "🟢"
                    val_str = f" = {value}" if value else ""
                    st.markdown(
                        f"{icon} **{label}**{val_str} "
                        f"<span style='color:#64748B;font-size:0.82rem'>"
                        f"(SHAP: {float(shap):+.3f})</span>",
                        unsafe_allow_html=True
                    )
                mit_label = expl.get("mitigator_label")
                mit_shap  = expl.get("mitigator_shap")
                if mit_label and mit_shap is not None:
                    st.markdown(
                        f"🟢 **{mit_label}** "
                        f"<span style='color:#64748B;font-size:0.82rem'>"
                        f"(mitigator, SHAP: {float(mit_shap):+.3f})</span>",
                        unsafe_allow_html=True
                    )

            # ── LIME drivers ──────────────────────────────────────────────────
            lime_drivers = []
            for i in range(1, 4):
                ll = expl.get(f"lime_driver_{i}_label")
                lw = expl.get(f"lime_driver_{i}_weight")
                if ll and lw is not None:
                    lime_drivers.append((ll, lw))

            if lime_drivers:
                drivers_found = True
                st.caption("**LIME** — local approximation for this vendor")
                for label, weight in lime_drivers:
                    w = float(weight)
                    # Format based on magnitude so sign + precision are both clear
                    if abs(w) < 0.001:
                        # Very small — show 'negligible' with actual direction
                        if abs(w) < 1e-6:
                            weight_str = "≈ 0 (negligible local effect)"
                            icon = "⚪"
                        else:
                            # Use scientific notation to preserve sign + magnitude
                            weight_str = f"{w:+.2e}"
                            icon = "🔴" if w > 0 else "🟢"
                    else:
                        weight_str = f"{w:+.3f}"
                        icon = "🔴" if w > 0 else "🟢"
                    st.markdown(
                        f"{icon} **{label}** "
                        f"<span style='color:#64748B;font-size:0.82rem'>"
                        f"(LIME weight: {weight_str})</span>",
                        unsafe_allow_html=True
                    )
                agree = expl.get("methods_agree", "")
                if agree:
                    badge_color = "#1E7E4B" if agree == "YES" else "#B7670A"
                    st.markdown(
                        f"<span style='background:{badge_color};color:white;"
                        f"padding:2px 8px;border-radius:4px;font-size:0.75rem'>"
                        f"SHAP & LIME agree: {agree}</span>",
                        unsafe_allow_html=True
                    )

            if not drivers_found:
                st.caption("Explanation data exists but values are NULL — "
                           "re-run ml/explainability.py to regenerate.")

            # Narratives
            narrative = expl.get("narrative")
            if narrative and pd.notna(narrative) and str(narrative) != "nan":
                st.info(str(narrative))
            lime_narr = expl.get("lime_narrative")
            if lime_narr:
                with st.expander("LIME local explanation"):
                    st.write(lime_narr)
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
                st.success(f"{action}")
        else:
            st.caption("No segment data. Run ml/segmentation.py")

    st.divider()

    # ── Manual Scorecard ──────────────────────────────────────────────────────
    st.subheader("Scorecard Inputs")
    st.caption("Enter external scores (EcoVadis, ISO 27001, QM inspections). Saved to Supabase.")

    col1, col2 = st.columns(2)
    with col1:
        defect_ppm = st.number_input(
            "Defect Rate (PPM)", min_value=0.0, max_value=1_000_000.0,
            value=float(v.get("defect_rate_ppm") or 0), step=10.0)
        innovation = st.slider("Innovation Score (0–100)", 0, 100,
                               int(v.get("innovation_score") or 0))
    with col2:
        cybersec   = st.slider("Cybersecurity Score (0–100)", 0, 100,
                               int(v.get("cybersecurity_score") or 0))
        esg        = st.slider("ESG Score (0–100)", 0, 100,
                               int(v.get("esg_score") or 0))

    notes = st.text_area("Notes", value=v.get("scorecard_notes") or "", height=80)

    if st.button("Save Scorecard", type="primary"):
        with DBClient() as db:
            for col, dtype in [
                ("defect_rate_ppm","FLOAT"),("innovation_score","FLOAT"),
                ("cybersecurity_score","FLOAT"),("esg_score","FLOAT"),
                ("scorecard_notes","TEXT"),("scorecard_updated_at","TIMESTAMP")
            ]:
                db.add_column_if_missing("vendors", col, dtype)
            db.execute(
                "UPDATE vendors SET defect_rate_ppm=%s, innovation_score=%s,"
                " cybersecurity_score=%s, esg_score=%s, scorecard_notes=%s,"
                " scorecard_updated_at=NOW() WHERE vendor_id=%s",
                (defect_ppm or None, innovation or None,
                 cybersec or None, esg or None,
                 notes or None, selected)
            )
        st.success("Scorecard saved.")
        st.cache_data.clear()

    if v.get("scorecard_updated_at"):
        st.caption(f"Last updated: {str(v['scorecard_updated_at'])[:16]}")
    st.divider()

    # ── Recent News ───────────────────────────────────────────────────────────
    if news_items:
        st.subheader("Recent News")
        for article in news_items[:5]:
            with st.expander(article.get("title","Article")[:80]):
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
def load_alternatives(vendor_ids: tuple = None) -> pd.DataFrame:
    with DBClient() as db:
        if not db.table_exists("vendor_alternatives"):
            return pd.DataFrame()
        where  = ""
        params = None
        if vendor_ids:
            placeholders = ",".join(["%s"] * len(vendor_ids))
            where  = f"WHERE vendor_id IN ({placeholders})"
            params = vendor_ids
        return db.fetch_df(f"""
            SELECT vendor_id, supplier_name, risk_score, risk_tier,
                   alt_supplier_name, alt_risk_score, alt_risk_tier,
                   alt_country, alt_industry,
                   alternative_rank, similarity_score,
                   recommendation_reason
            FROM vendor_alternatives
            {where}
            ORDER BY risk_score DESC, alternative_rank ASC
        """, params)


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


def tab_alternatives(filters: dict):
    st.header("Alternative Suppliers & Anomalies")
    active_filter_bar(filters)

    alt_df  = load_alternatives(vendor_ids=_vids(filters))
    anom_df = load_anomalies()

    if alt_df.empty and anom_df.empty:
        st.info(
            "No recommendations or anomaly data yet.\n\n"
            "Run: `python ml/recommendations.py`"
        )
        return

    tab_a, tab_b = st.tabs(["Alternative Suppliers", "⚡ Anomaly Detection"])

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
                        c2.write(f"{r.get('alt_country','?')}")
                        c3.write(f"{r.get('alt_industry','?')}")
                        st.info(f"{r.get('recommendation_reason','')}")
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
                "Export Alternatives",
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
                    apply_layout(fig)
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
                styled_table(disp)

            # ── Anomalies by spend category ───────────────────────────────────
            st.subheader("Anomalies by Spend Category")
            st.caption("Which material groups contain the most anomalous vendors?")
            try:
                with DBClient() as db:
                    cat_anom = db.fetch_df("""
                        SELECT
                            COALESCE(NULLIF(TRIM(t.material_group),''), 'Unclassified')
                                                        AS material_group,
                            COUNT(DISTINCT t.vendor_id) AS total_vendors,
                            COUNT(DISTINCT t.vendor_id) FILTER (
                                WHERE va.is_anomalous = TRUE
                            )                           AS anomalous_vendors,
                            SUM(t.transaction_amount)   AS total_spend,
                            SUM(t.transaction_amount) FILTER (
                                WHERE va.is_anomalous = TRUE
                            )                           AS anomalous_spend
                        FROM transactions t
                        JOIN vendor_anomalies va ON t.vendor_id = va.vendor_id
                        WHERE t.transaction_amount > 0
                        GROUP BY 1
                        HAVING COUNT(DISTINCT t.vendor_id) FILTER (
                            WHERE va.is_anomalous = TRUE) > 0
                        ORDER BY anomalous_vendors DESC
                        LIMIT 20
                    """) if db.table_exists("vendor_anomalies") else pd.DataFrame()
            except Exception:
                cat_anom = pd.DataFrame()

            if not cat_anom.empty:
                cat_anom["anomaly_rate"] = (
                    cat_anom["anomalous_vendors"] / cat_anom["total_vendors"] * 100
                ).round(1)
                cat_anom["anomalous_spend"] = cat_anom["anomalous_spend"].fillna(0)

                if PLOTLY:
                    fig = px.bar(
                        cat_anom.head(15),
                        x="anomalous_vendors",
                        y="material_group",
                        orientation="h",
                        color="anomaly_rate",
                        color_continuous_scale="Reds",
                        labels={
                            "anomalous_vendors": "Anomalous Vendor Count",
                            "material_group":    "Material Group",
                            "anomaly_rate":      "Anomaly Rate %"
                        },
                    )
                    fig.update_layout(yaxis=dict(autorange="reversed"),
                                      margin=dict(t=10), height=380)
                    apply_layout(fig)
                    st.plotly_chart(fig, use_container_width=True)

                disp2 = cat_anom.copy()
                disp2["total_spend"]     = disp2["total_spend"].apply(fmt_spend)
                disp2["anomalous_spend"] = disp2["anomalous_spend"].apply(fmt_spend)
                disp2["anomaly_rate"]    = disp2["anomaly_rate"].apply(
                    lambda x: f"{x:.1f}%")
                disp2.columns = [c.replace("_"," ").title() for c in disp2.columns]
                styled_table(disp2)
            else:
                st.caption("Run `python ml/spend_analytics.py` first to generate category data.")

            st.download_button(
                "Export Anomalies",
                anom_df.to_csv(index=False).encode(),
                "anomaly_report.csv", "text/csv"
            )


def main():
    filters = render_sidebar()

    tabs = st.tabs([
        "Overview",
        "Risk",
        "Segmentation",
        "Spend",
        "Alternatives",
        "Explainability",
        "News",
        "Vendor Profile & Scorecard",
    ])

    with tabs[0]: tab_overview(filters)
    with tabs[1]: tab_risk(filters)
    with tabs[2]: tab_segmentation(filters)
    with tabs[3]: tab_spend(filters)
    with tabs[4]: tab_alternatives(filters)
    with tabs[5]: tab_explainability()
    with tabs[6]: tab_news(filters)
    with tabs[7]: tab_vendor_profile()


if __name__ == "__main__":
    main()