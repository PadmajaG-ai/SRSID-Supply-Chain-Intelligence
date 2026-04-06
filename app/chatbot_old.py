"""
SRSID Procurement Intelligence Chatbot  —  app/chatbot.py
===========================================================
Covers all 105 user queries across 8 categories:
  1. High-Level Summary & Risk Alerts
  2. Individual Supplier Deep Dives
  3. Spend Analysis & Financial Intelligence
  4. Operational Performance & Delays
  5. Comparative Analysis & Alternatives
  6. Strategic Sourcing & Decision Support
  7. Chatbot Logic & Data Integrity
  8. What-If & Predictive Scenarios

Run:
    streamlit run app/chatbot.py
"""

import sys, re, json
from pathlib import Path
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.db_client import DBClient

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG + CSS
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SRSID Procurement Assistant",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', -apple-system, sans-serif !important; }

/* Page */
.stApp { background: #F0F4F8 !important; }
.block-container { max-width: 860px !important; padding-top: 0 !important; padding-bottom: 2rem !important; }

/* Sidebar — dark navy */
[data-testid="stSidebar"] { background: #0F2644 !important; border-right: none !important; }
[data-testid="stSidebar"] * { color: #FFFFFF !important; }
[data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.12) !important; margin: 0.6rem 0 !important; }
[data-testid="stSidebar"] .stButton > button {
    background: rgba(255,255,255,0.07) !important; border: 1px solid rgba(255,255,255,0.14) !important;
    color: #E2EAF4 !important; border-radius: 6px !important; font-size: 0.8rem !important;
    padding: 6px 10px !important; text-align: left !important; width: 100% !important;
    transition: all 0.15s !important; line-height: 1.4 !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(255,255,255,0.14) !important; border-color: rgba(255,255,255,0.28) !important;
}
[data-testid="stSidebar"] .streamlit-expanderHeader {
    background: rgba(255,255,255,0.06) !important; border: 1px solid rgba(255,255,255,0.12) !important;
    border-radius: 6px !important; font-size: 0.8rem !important; font-weight: 500 !important;
}
[data-testid="stSidebar"] .streamlit-expanderContent {
    background: transparent !important; border: none !important; padding: 4px 0 !important;
}

/* Chat messages */
[data-testid="stChatMessage"] p,
[data-testid="stChatMessage"] li { color: #1A202C !important; font-size: 0.89rem !important; line-height: 1.75 !important; }
[data-testid="stChatMessage"] strong { color: #0F2644 !important; font-weight: 600 !important; }
[data-testid="stChatMessage"] code {
    background: #EEF2F7 !important; color: #1A6FBF !important;
    border-radius: 3px !important; font-size: 0.83rem !important; padding: 1px 5px !important;
}

/* User bubble */
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]) {
    background: #0F2644 !important; border-radius: 16px 16px 4px 16px !important;
    margin-left: 16% !important; padding: 12px 16px !important;
    box-shadow: 0 2px 8px rgba(15,38,68,0.2) !important;
}
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]) p,
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]) li,
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]) strong { color: #FFFFFF !important; }

/* Assistant bubble */
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) {
    background: #FFFFFF !important; border: 1px solid #DDE4EE !important;
    border-left: 3px solid #1A6FBF !important; border-radius: 4px 16px 16px 16px !important;
    margin-right: 16% !important; padding: 12px 16px !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05) !important;
}

/* Hide default avatars — we use custom ones */
[data-testid="chatAvatarIcon-user"],
[data-testid="chatAvatarIcon-assistant"] { display: none !important; }

/* Chat input */
[data-testid="stChatInput"] textarea {
    border-radius: 8px !important; font-size: 0.89rem !important;
    border: 1px solid #CBD5E0 !important;
    background: #FFFFFF !important;
    color: #1A202C !important;
}
[data-testid="stChatInput"] textarea::placeholder {
    color: #94A3B8 !important;
}
[data-testid="stChatInput"]:focus-within {
    border-color: #1A6FBF !important;
    box-shadow: 0 0 0 3px rgba(26,111,191,0.1) !important;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADER  — cached, all from Postgres
# ─────────────────────────────────────────────────────────────────────────────

def _empty_data() -> dict:
    """Return empty dataframes for all keys — used when DB is unreachable."""
    empty = pd.DataFrame()
    return dict(
        summary={}, vendors=empty, segments=empty, explanations=empty,
        news=empty, contracts=empty, delivery=empty, alternatives=empty,
        anomalies=empty, quarterly=empty, feat_imp=empty,
        spend_categories=empty,
    )


@st.cache_data(ttl=300)
def load_data() -> dict:
    try:
        with DBClient() as db:
            summary     = db.get_portfolio_summary() or {}
            vendors     = db.fetch_df("""
            SELECT vendor_id, supplier_name, country_code, industry_category,
                   risk_label, composite_risk_score, total_annual_spend,
                   transaction_count, delivery_performance, financial_stability,
                   otif_rate, ottr_rate, lead_time_variability,
                   order_accuracy_rate, avg_price_variance_pct,
                   avg_delay_days, news_sentiment_30d, disruption_count_30d,
                   spend_pct_of_portfolio, sum_percentage, maverick_type,
                   geo_risk, is_maverick, concentration_risk,
                   esg_score, cybersecurity_score, innovation_score
            FROM vendors WHERE is_active = TRUE
            ORDER BY composite_risk_score DESC NULLS LAST
        """)
            segments    = db.fetch_df("""
                SELECT s.vendor_id, s.supplier_name, s.kraljic_segment,
                       s.abc_class, s.cluster_label, s.strategic_action,
                       s.supply_risk_score, s.profit_impact_score,
                       v.total_annual_spend, v.composite_risk_score,
                       v.country_code, v.industry_category
                FROM latest_segments s
                LEFT JOIN vendors v ON s.vendor_id = v.vendor_id
            """)
            explanations = db.fetch_df("""
                SELECT e.vendor_id, e.supplier_name, e.predicted_risk_tier,
                       e.driver_1_label, e.driver_2_label, e.driver_3_label,
                       e.mitigator_label, e.narrative
                FROM latest_explanations e
            """)
            news        = db.fetch_df("""
                SELECT n.vendor_id, n.supplier_name, n.title, n.source_name,
                       n.published_at, n.sentiment_score, n.disruption_type,
                       n.disruption_flag, n.url, v.country_code, v.industry_category
                FROM vendor_news n
                LEFT JOIN vendors v ON n.vendor_id = v.vendor_id
                WHERE n.published_at >= NOW() - INTERVAL '30 days'
                ORDER BY n.published_at DESC
            """)
            contracts   = db.fetch_df("""
                SELECT c.vendor_id, c.supplier_name, c.contract_end,
                       c.days_to_expiry, c.contract_status,
                       v.total_annual_spend, v.risk_label
                FROM contracts c
                LEFT JOIN vendors v ON c.vendor_id = v.vendor_id
            """)
            delivery    = db.fetch_df("""
                SELECT vendor_id, supplier_name,
                       AVG(delay_days) AS avg_delay,
                       STDDEV(delay_days) AS delay_std,
                       AVG(otif::FLOAT)*100 AS otif_pct,
                       COUNT(*) AS deliveries,
                       SUM(CASE WHEN delay_days > 5 THEN 1 ELSE 0 END) AS sig_delays
                FROM delivery_events
                GROUP BY vendor_id, supplier_name
            """)
            alt_exists  = db.table_exists("vendor_alternatives")
            alternatives = db.fetch_df("""
                SELECT * FROM vendor_alternatives ORDER BY risk_score DESC, alternative_rank
            """) if alt_exists else pd.DataFrame()
            anom_exists = db.table_exists("vendor_anomalies")
            anomalies   = db.fetch_df("""
                SELECT * FROM vendor_anomalies WHERE is_anomalous = TRUE
                ORDER BY total_anomaly_flags DESC
            """) if anom_exists else pd.DataFrame()
            quarterly   = db.fetch_df("""
                SELECT year, quarter,
                       CAST(year AS TEXT)||'-Q'||CAST(quarter AS TEXT) AS period,
                       SUM(transaction_amount) AS total_spend,
                       COUNT(DISTINCT vendor_id) AS vendor_count
                FROM transactions WHERE year IS NOT NULL
                GROUP BY year, quarter ORDER BY year, quarter
            """)
            fi_path     = Path(__file__).parent.parent / "reports" / "feature_importance.csv"
            feat_imp    = pd.read_csv(fi_path) if fi_path.exists() else pd.DataFrame()
            cat_exists  = db.table_exists("spend_by_category")
            spend_categories = db.fetch_df("""
                SELECT material_group, total_spend, spend_pct,
                       maverick_pct, high_risk_pct, savings_opportunity,
                       vendor_count, transaction_count
                FROM spend_by_category
                ORDER BY total_spend DESC
            """) if cat_exists else pd.DataFrame()

        return dict(
            summary=summary, vendors=vendors, segments=segments,
            explanations=explanations, news=news, contracts=contracts,
            delivery=delivery, alternatives=alternatives, anomalies=anomalies,
            quarterly=quarterly, feat_imp=feat_imp,
            spend_categories=spend_categories,
        )

    except Exception as e:
        # DB unreachable (e.g. Streamlit Cloud without a configured DB)
        st.error(
            "**Database not connected.** "
            "Configure your PostgreSQL connection in Streamlit secrets:\n\n"
            "Go to **App settings → Secrets** and add:\n"
            "```\nDB_HOST = 'your-supabase-host.supabase.co'\n"
            "DB_PORT = '5432'\n"
            "DB_NAME = 'postgres'\n"
            "DB_USER = 'postgres'\n"
            "DB_PASSWORD = 'your-password'\n```\n\n"
            f"*Error: {e}*"
        )
        return _empty_data()


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def fmt_money(v) -> str:
    try:
        v = float(v)
        if v >= 1e9: return f"${v/1e9:.2f}B"
        if v >= 1e6: return f"${v/1e6:.1f}M"
        if v >= 1e3: return f"${v/1e3:.0f}K"
        return f"${v:.0f}"
    except: return "N/A"

def fmt_pct(v, scale=1) -> str:
    try: return f"{float(v)*scale:.1f}%"
    except: return "N/A"

def risk_icon(t) -> str:
    return {"High":"","Medium":"","Low":""}.get(str(t),"")

def action_tier(t) -> str:
    return {
        "High":   "**Immediate action required** — escalate to procurement manager, "
                  "start dual-sourcing, place safety stock orders.",
        "Medium": "**Active monitoring needed** — schedule a supplier review this quarter "
                  "and identify at least one backup supplier.",
        "Low":    "**Routine oversight** — maintain current terms, review at next performance cycle.",
    }.get(str(t), "Review with your procurement manager.")

def action_segment(s) -> str:
    return {
        "Strategic":  "Invest in the relationship — long-term contracts, joint planning, safety stock.",
        "Bottleneck": "Urgently qualify alternatives — do not allow sole-source dependency.",
        "Leverage":   "Negotiate aggressively — consolidate spend, run competitive tenders.",
        "Tactical":   "Automate — move to e-catalog, reduce POs, use preferred-supplier frameworks.",
    }.get(str(s), "Review with category manager.")

def find_vendor(name: str, vendors: pd.DataFrame):
    """Fuzzy-find a vendor by name."""
    mask = vendors["supplier_name"].astype(str).str.lower().str.contains(
        re.escape(name.lower()), na=False)
    return vendors[mask].iloc[0] if mask.any() else None

def detect_vendor_name(q: str, vendors: pd.DataFrame) -> str | None:
    """Detect a vendor name mentioned in the question."""
    for name in vendors["supplier_name"].dropna().unique():
        if len(str(name)) > 3 and str(name).lower() in q.lower():
            return str(name)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# INTENT DETECTION
# ─────────────────────────────────────────────────────────────────────────────
INTENTS = {
    # Cat 1: Summary & Alerts
    "overview":       ["overview","summary","health","overall","how many","total","portfolio",
                       "morning briefing","briefing","executive","supply chain health"],
    "high_risk":      ["high risk","most risky","riskiest","top risk","worst","high-risk",
                       "worry about","concern","9 how many","flagged as high"],
    "medium_risk":    ["medium risk","moderate risk","medium-risk","all medium"],
    "low_risk":       ["low risk","low-risk","safe supplier","all low","performing well"],
    "alerts":         ["alert","warning","urgent","30 day","next 30","imminent",
                       "watchlist","moved to high","new risk"],
    "geo_news":       ["asia","asia-pacific","region","geopolit","country impact",
                       "breaking news","global","country risk","regional"],
    "risk_heatmap":   ["heat map","heatmap","heat-map","global sourcing","visual risk"],
    "spend_at_risk":  ["spend at risk","total spend at risk","value at risk","var"],
    "risk_drivers":   ["risk driver","major risk","biggest driver","risk factor this month",
                       "summarize.*risk","top driver"],
    "health_drop":    ["health score","sudden drop","dropped","deteriorat","worsened"],

    # Cat 2: Supplier Deep Dive
    "supplier_risk":  ["risk level for","risk of","why is","why.*high risk","probability of default",
                       "risk rating","categorized as"],
    "supplier_news":  ["news.*for","headlines for","latest news","sentiment trend","scanned for",
                       "last.*article","negative sentiment"],
    "supplier_spend": ["spend with","spend.*fiscal","percentage.*spend","how much.*spend",
                       "spend dependency","26 what percent"],
    "supplier_delivery": ["delivery delay","delivery performance","lead time","otif",
                          "shipment","in transit","on-time"],
    "supplier_sla":   ["sla","service level","audit","single.source","multi.source",
                       "last.*audit","legal","lawsuit","red flag"],
    "supplier_explain": ["why.*medium","explain.*rating","explain.*risk","why.*score",
                         "primary risk factor","risk factor for","driver for","factor for"],

    # Cat 3: Spend Analytics
    "spend_quarter":  ["total spend","current quarter","this quarter","last quarter",
                       "quarterly spend","spend for q","spend this month","spend last month",
                       "spend trend","spend breakdown","month-over-month","spend report"],
    "maverick":       ["maverick","off.contract","tail.spend","consolidat","no.*contract",
                       "spend.*no.*contract","over.budget","budget"],
    "spend_category": ["category spend","by category","by risk category",
                       "by department","which department",
                       "spend.*industry","industry.*spend",
                       "spend by industry","breakdown.*industry",
                       "industry.*breakdown","average transaction",
                       "material group","material.*group","spend.*category",
                       "category.*spend","which category","by material",
                       "spend breakdown","breakdown.*spend"],
    "top_vendors":    ["top.*vendor","top.*supplier.*spend","highest.*spend.*supplier",
                       "most.*spend.*supplier","biggest.*spend",
                       "spend.*vs.*perform","spend.*performance"],
    "savings":        ["saving.*opportun","opportun.*saving","how much.*save",
                       "cost.*saving","saving.*potential","off.contract.*saving",
                       "maverick.*saving","reduce.*spend","consolidat.*saving",
                       "roi.*diversif","price hike","commodity","cost impact"],

    # Cat 4: Operational Performance
    "otif":           ["otif","on.time.in.full","failing.*delivery","delivery fail"],
    "delay":          ["delay","late delivery","more than 5 day","logistics disruption",
                       "delay.*semiconductor","returns","rejection","variance.*promised"],
    "news_delivery_corr": ["correlation.*news","news.*delay","negative news.*delay"],
    "predict_delay":  ["predict.*delay","might have a delay","next week.*delay"],

    # Cat 5: Alternatives
    "alternatives":   ["alternative","replace","backup","substitute","switch","option",
                       "who can absorb","safest.*alternative","local supplier","replace.*international"],
    "compare":        ["compare","vs","versus","side.by.side","benchmark","rank all",
                       "comparison","risk adjusted cost","best balance"],
    "zero_spend":     ["zero.*spend","no.*spend","unused supplier","lowest risk.*no spend"],

    # Cat 6: Strategic
    "contracts_expiry": ["contract.*renew","renewal","expiry","expiring","up for renewal",
                         "should i renew","renew.*contract","60 day","90 day"],
    "pip":            ["performance improvement","offboard","consistent poor","should.*offboard"],
    "concentration":  ["concentration risk","single source","sole.source","if we lose",
                       "concentration.*if","89 how much.*single"],
    "esg":            ["esg","sustainability","carbon footprint","environmental","green",
                       "social","governance","84 how many"],
    "whatif":         ["what if","what happens","if.*bankrupt","10% increase","20% increase",
                       "shift.*spend","if i stop","predict.*quarter","scenario"],

    # Cat 7: Data & System
    "data_source":    ["where.*data","source.*data","how.*updated","how often","accuracy",
                       "financial data source","delivery.*data.*from","news.*updated"],
    "how_score":      ["how.*risk score","calculated","how.*define","how.*sentiment",
                       "what.*negative sentiment","how is.*rated"],
    "export":         ["export","download","excel","report","generate.*report"],
    "help":           ["help","what can you","what do you","how to use","what questions",
                       "capabilities","what can i ask"],
}

def detect_intent(q: str) -> str:
    ql = q.lower()
    scores = {}
    for intent, keywords in INTENTS.items():
        score = sum(1 for kw in keywords if re.search(kw.replace(".*", ".*"), ql))
        if score > 0:
            scores[intent] = score
    if not scores:
        return "general"
    return max(scores, key=scores.get)


# ─────────────────────────────────────────────────────────────────────────────
# RESPONSE BUILDERS — one per intent
# ─────────────────────────────────────────────────────────────────────────────

def resp_help() -> str:
    return """**SRSID Procurement Assistant — Verified Question Guide**

**Portfolio Summary**
- "Give me a supply chain health summary"
- "Which suppliers are high risk right now?"
- "List all medium risk suppliers"
- "Show low risk suppliers"
- "Show disruption alerts"
- "What is our total spend at risk?"

**Spend & Contracts**
- "What is our total spend this quarter?"
- "What is our total spend last quarter?"
- "Where is our maverick spend?"
- "Show spend concentration risk"
- "Show spend by industry"
- "Show spend by material group"
- "What is our savings opportunity?"
- "Which vendors have no long-term contract?"
- "Which contracts expire in the next 60 days?"
- "Predict spend for next quarter"

**Operational Performance**
- "Which suppliers are failing on OTIF?"
- "Is there a correlation between news and delivery delays?"
- "Which suppliers should be on a performance improvement plan?"

**Supplier Deep Dives** *(replace [Supplier Name] with an actual vendor name)*
- "What is the risk level for [Supplier Name]?"
- "Why is [Supplier Name] high risk?"
- "What % of our spend goes to [Supplier Name]?"
- "What is our concentration risk if we lose [Supplier Name]?"
- "What happens if [Supplier Name] goes bankrupt?"

**Alternatives & What-If** *(requires vendor names)*
- "Who are our top 3 backup suppliers?"
- "Who is the safest alternative to [Supplier Name]?"
- "Compare [Supplier A] and [Supplier B] by risk and spend"
- "Move 20% spend from [Supplier A] to [Supplier B]"
- "Shift 20% spend from [Supplier Name]"

**ESG & Strategy**
- "Show ESG status"
- "Show spend by industry"

*Tip: For vendor-specific questions (Why is X high risk? Who replaces Y?) the assistant uses AI-powered search to retrieve the most relevant vendor records.*"""


def resp_overview(d: dict) -> str:
    s   = d["summary"]
    v   = d["vendors"]
    seg = d["segments"]
    fi  = d["feat_imp"]

    total   = int(s.get("total_vendors", len(v)))
    high    = int(s.get("high_risk_count", 0))
    med     = int(s.get("medium_risk_count", 0))
    low     = int(s.get("low_risk_count", 0))
    spend   = s.get("total_portfolio_spend")
    otif    = s.get("avg_otif_rate")
    disrupt = int(s.get("total_disruption_alerts", 0))

    kd = seg["kraljic_segment"].value_counts().to_dict() if not seg.empty else {}
    top_driver = fi.iloc[0].get("feature_label", fi.iloc[0].get("feature","")) \
                 if not fi.empty else ""

    worst3 = v[v["risk_label"]=="High"].head(3)["supplier_name"].tolist() \
             if not v[v["risk_label"]=="High"].empty else []

    parts = [
        "**Supply Chain Health Summary**\n",
        f"Monitoring **{total:,} active suppliers** across your portfolio.",
        f"Total portfolio spend: **{fmt_money(spend)}**\n",
        "**Risk breakdown:**",
        f"-  High Risk: **{high} suppliers** — immediate attention needed",
        f"-  Medium Risk: **{med} suppliers** — monitor actively",
        f"-  Low Risk: **{low} suppliers** — performing well\n",
    ]
    if disrupt:
        parts.append(f"️ **{disrupt} disruption alerts** from news signals in the last 30 days.\n")
    if otif:
        parts.append(f" Average portfolio OTIF: **{otif*100:.1f}%**\n")
    if kd:
        parts += ["**Kraljic segmentation:**",
                  f"- {kd.get('Strategic',0)} Strategic · {kd.get('Bottleneck',0)} Bottleneck",
                  f"- {kd.get('Leverage',0)} Leverage · {kd.get('Tactical',0)} Tactical\n"]
    if worst3:
        parts.append(f"**Most urgent to review:** {', '.join(worst3)}\n")
    if top_driver:
        parts.append(f"**Biggest risk driver across portfolio:** {top_driver}\n")
    parts.append("Ask *'show high risk suppliers'* or *'show disruption alerts'* for details.")
    return "\n".join(parts)


def _risk_list(d: dict, tier: str, label: str) -> str:
    v = d["vendors"]
    filtered = v[v["risk_label"] == tier].copy()
    if filtered.empty:
        counts = {t: int((v["risk_label"]==t).sum()) for t in ["High","Medium","Low"]}
        return (f" No **{tier} Risk** suppliers currently.\n"
                f"Portfolio:  {counts['High']} High |  {counts['Medium']} Medium | "
                f" {counts['Low']} Low")

    filtered = filtered.sort_values("composite_risk_score",
                                    ascending=(tier=="Low"), na_position="last")
    lines = [f"**{len(filtered)} {label} Supplier(s):**\n"]
    for i, (_, r) in enumerate(filtered.head(20).iterrows(), 1):
        parts = [f"**{r['supplier_name']}**"]
        if pd.notna(r.get("composite_risk_score")):
            parts.append(f"Risk: {r['composite_risk_score']:.3f}")
        if pd.notna(r.get("total_annual_spend")):
            parts.append(f"Spend: {fmt_money(r['total_annual_spend'])}")
        if pd.notna(r.get("delivery_performance")):
            parts.append(f"Delivery: {r['delivery_performance']:.0f}/100")
        if pd.notna(r.get("country_code")):
            parts.append(f" {r['country_code']}")
        lines.append(f"{i}. {' | '.join(parts)}")
    lines.append(f"\n{action_tier(tier)}")
    return "\n".join(lines)


def resp_high_risk(d):  return _risk_list(d, "High",   " High-Risk")
def resp_medium_risk(d): return _risk_list(d, "Medium", " Medium-Risk")
def resp_low_risk(d):   return _risk_list(d, "Low",    " Low-Risk")


def resp_alerts(d: dict) -> str:
    news = d["news"]
    v    = d["vendors"]
    disruptions = news[news["disruption_flag"] == True] if not news.empty else pd.DataFrame()
    high_risk_v = v[v["risk_label"] == "High"]

    lines = []
    if not disruptions.empty:
        lines.append(f"️ **{disruptions['supplier_name'].nunique()} vendors** "
                     f"have disruption news in the last 30 days:\n")
        for _, r in disruptions.drop_duplicates("supplier_name").head(10).iterrows():
            lines.append(f"• **{r['supplier_name']}** — {r.get('disruption_type','')} "
                         f"| Sentiment: {r.get('sentiment_score',0):+.2f}")

    if not high_risk_v.empty:
        lines.append(f"\n **{len(high_risk_v)} High Risk vendors** need immediate attention:")
        for _, r in high_risk_v.head(5).iterrows():
            lines.append(f"• **{r['supplier_name']}** — Score: "
                         f"{r.get('composite_risk_score',0):.3f} | "
                         f"Spend: {fmt_money(r.get('total_annual_spend'))}")

    if not lines:
        return " No active alerts. All disruption signals are within normal range."

    lines += ["\n**Immediate actions:**",
              "1. Contact flagged vendors this week to check capacity",
              "2. Verify inventory levels for components from these suppliers",
              "3. Activate backup suppliers where possible",
              "4. Escalate Strategic-category vendors to procurement manager"]
    return "\n".join(lines)


def resp_geo_news(d: dict, q: str) -> str:
    news = d["news"]
    v    = d["vendors"]

    # Detect region from question
    region_map = {
        "asia": ["CN","JP","KR","TW","IN","VN","TH","SG","MY","ID","PH"],
        "europe": ["DE","FR","GB","IT","ES","NL","PL","SE","AT","CH","BE"],
        "americas": ["US","CA","MX","BR","AR"],
        "middle east": ["SA","AE","TR","IL","EG"],
        "africa": ["ZA","NG","KE","GH","ET"],
    }
    target_codes, region_label = [], "Global"
    for region, codes in region_map.items():
        if region in q.lower():
            target_codes, region_label = codes, region.title()
            break

    if target_codes:
        region_vendors = v[v["country_code"].isin(target_codes)]["vendor_id"].tolist()
        region_news = news[news["vendor_id"].isin(region_vendors)]
    else:
        region_news = news

    disruptions = region_news[region_news["disruption_flag"] == True] \
                  if not region_news.empty else pd.DataFrame()

    lines = [f"**{region_label} News & Disruption Summary (Last 30 Days)**\n"]

    if not disruptions.empty:
        lines.append(f"️ **{len(disruptions)} disruption articles** found:\n")
        for _, r in disruptions.head(8).iterrows():
            lines.append(f"• **{r['supplier_name']}** — {r.get('disruption_type','')} "
                         f"| {str(r.get('published_at',''))[:10]}")
            if r.get("title"):
                lines.append(f"   *{str(r['title'])[:100]}*")
    else:
        lines.append(f" No disruption news for {region_label} suppliers in the last 30 days.")

    if not region_news.empty and "sentiment_score" in region_news.columns:
        avg_sent = region_news["sentiment_score"].mean()
        lines.append(f"\n Average news sentiment: **{avg_sent:+.2f}** "
                     f"({'Negative ️' if avg_sent < -0.1 else 'Positive ' if avg_sent > 0.1 else 'Neutral'})")

    return "\n".join(lines)


def resp_spend_at_risk(d: dict) -> str:
    v = d["vendors"]
    v = v[v["total_annual_spend"].notna()]
    v = v[v["composite_risk_score"].notna()]

    v = v.copy()
    v["spend_at_risk"] = v["total_annual_spend"] * v["composite_risk_score"]

    total_spend    = v["total_annual_spend"].sum()
    high_risk_v    = v[v["risk_label"] == "High"]
    med_risk_v     = v[v["risk_label"] == "Medium"]
    total_at_risk  = v["spend_at_risk"].sum()
    high_spend     = high_risk_v["total_annual_spend"].sum()
    med_spend      = med_risk_v["total_annual_spend"].sum()

    lines = [
        "**Total Value at Risk — Supply Chain Portfolio**\n",
        f"Total portfolio spend: **{fmt_money(total_spend)}**",
        f"Risk-weighted spend at risk: **{fmt_money(total_at_risk)}** "
        f"({total_at_risk/total_spend*100:.1f}% of portfolio)\n",
        "**Breakdown by risk tier:**",
        f"-  High Risk: **{fmt_money(high_spend)}** across {len(high_risk_v)} vendors",
        f"-  Medium Risk: **{fmt_money(med_spend)}** across {len(med_risk_v)} vendors\n",
        "**Top 5 highest spend-at-risk vendors:**",
    ]
    for _, r in v.nlargest(5, "spend_at_risk").iterrows():
        lines.append(f"• **{r['supplier_name']}** — "
                     f"Spend: {fmt_money(r['total_annual_spend'])} | "
                     f"Risk: {r['composite_risk_score']:.3f} | "
                     f"At Risk: {fmt_money(r['spend_at_risk'])}")
    return "\n".join(lines)


def resp_risk_drivers(d: dict) -> str:
    fi = d["feat_imp"]
    v  = d["vendors"]

    lines = ["**Major Risk Drivers — Portfolio Analysis**\n"]
    if not fi.empty:
        label_col = "feature_label" if "feature_label" in fi.columns else "feature"
        lines.append("**Top predictive features (from ML model):**")
        for i, (_, r) in enumerate(fi.head(8).iterrows(), 1):
            lines.append(f"{i}. **{r[label_col]}** — importance: {r.get('importance',0):.3f}")
        lines.append("")

    # Geographic concentration
    country_risk = v[v["geo_risk"]=="High"]["supplier_name"].count()
    if country_risk:
        lines.append(f" **{country_risk} vendors** in high geo-risk countries\n")

    # Industry concentration
    if "industry_category" in v.columns:
        ind_risk = v[v["risk_label"]=="High"]["industry_category"].value_counts().head(3)
        if not ind_risk.empty:
            lines.append("**Industries with most High-Risk vendors:**")
            for ind, cnt in ind_risk.items():
                lines.append(f"• {ind}: {cnt} vendors")
    return "\n".join(lines)


def resp_supplier_risk(d: dict, q: str) -> str:
    name = detect_vendor_name(q, d["vendors"])
    if not name:
        return ("Please name a specific supplier. Example: *'What is the risk level for Siemens?'*\n"
                "Or ask *'show high risk suppliers'* for the full list.")

    v   = find_vendor(name, d["vendors"])
    exp = d["explanations"]
    expl_row = None
    if not exp.empty:
        m = exp["supplier_name"].astype(str).str.lower().str.contains(name.lower(), na=False)
        if m.any():
            expl_row = exp[m].iloc[0]

    if v is None:
        return f"I couldn't find **{name}** in the vendor database. Check the spelling."

    tier  = v.get("risk_label","Unknown")
    score = v.get("composite_risk_score", 0)
    icon  = risk_icon(tier)

    lines = [
        f"**{v['supplier_name']} — Risk Profile**\n",
        f"{icon} Risk Tier: **{tier}** | Score: **{score:.3f}/1.000**",
        f" Country: {v.get('country_code','?')} | Industry: {v.get('industry_category','?')}",
        f" Annual Spend: {fmt_money(v.get('total_annual_spend'))} "
        f"({fmt_pct(v.get('spend_pct_of_portfolio', 0), 100)} of portfolio)\n",
    ]

    if pd.notna(v.get("delivery_performance")):
        lines.append(f" Delivery Performance: {v['delivery_performance']:.0f}/100")
    if pd.notna(v.get("otif_rate")):
        lines.append(f"️ OTIF Rate: {v['otif_rate']*100:.1f}%")
    if pd.notna(v.get("financial_stability")):
        lines.append(f" Financial Stability: {v['financial_stability']:.0f}/100")
    if pd.notna(v.get("news_sentiment_30d")):
        lines.append(f" News Sentiment (30d): {v['news_sentiment_30d']:+.2f}")
    if pd.notna(v.get("disruption_count_30d")) and v["disruption_count_30d"] > 0:
        lines.append(f"️ News Disruptions (30d): {int(v['disruption_count_30d'])}")

    lines.append("")
    if expl_row is not None and pd.notna(expl_row.get("narrative")):
        lines.append(f"**Why this score:** {expl_row['narrative']}")
    else:
        lines.append(action_tier(tier))

    return "\n".join(lines)


def resp_supplier_explain(d: dict, q: str) -> str:
    name = detect_vendor_name(q, d["vendors"])
    if not name:
        return "Please name a supplier. Example: *'Why is Siemens classified as medium risk?'*"

    exp = d["explanations"]
    v   = find_vendor(name, d["vendors"])

    lines = [f"**Why {name} is rated this way:**\n"]

    if not exp.empty:
        m = exp["supplier_name"].astype(str).str.lower().str.contains(name.lower(), na=False)
        if m.any():
            e = exp[m].iloc[0]
            if pd.notna(e.get("narrative")):
                lines.append(e["narrative"] + "\n")
            for i in range(1, 4):
                lbl = e.get(f"driver_{i}_label")
                shp = e.get(f"driver_{i}_shap")
                if lbl and pd.notna(shp):
                    direction = "↑ increases risk" if float(shp) > 0 else "↓ reduces risk"
                    lines.append(f"• **{lbl}** — {direction} (SHAP: {float(shp):+.3f})")
            if pd.notna(e.get("mitigator_label")):
                lines.append(f"\n **Mitigating factor:** {e['mitigator_label']}")
            return "\n".join(lines)

    if v is not None:
        tier = v.get("risk_label","Unknown")
        lines.append(f"Risk tier: **{tier}** | Score: {v.get('composite_risk_score',0):.3f}\n")
        lines.append("Key contributing factors:")
        if pd.notna(v.get("geo_risk")) and v["geo_risk"] == "High":
            lines.append("• Geographic risk — supplier country is classified high-risk")
        if pd.notna(v.get("otif_rate")) and v["otif_rate"] < 0.8:
            lines.append(f"• Low OTIF rate ({v['otif_rate']*100:.1f}%) — delivery reliability concern")
        if pd.notna(v.get("financial_stability")) and v["financial_stability"] < 50:
            lines.append(f"• Financial stability score ({v['financial_stability']:.0f}/100) below threshold")
        if pd.notna(v.get("disruption_count_30d")) and v["disruption_count_30d"] > 0:
            lines.append(f"• {int(v['disruption_count_30d'])} news disruptions in last 30 days")
        lines.append("\nRun `ml/explainability.py` for full SHAP explanation.")
    return "\n".join(lines)


def resp_supplier_news(d: dict, q: str) -> str:
    name = detect_vendor_name(q, d["vendors"])
    news = d["news"]

    if name:
        m = news["supplier_name"].astype(str).str.lower().str.contains(name.lower(), na=False)
        vendor_news = news[m].sort_values("published_at", ascending=False)
        if vendor_news.empty:
            return f"No news articles found for **{name}** in the last 30 days."
        lines = [f"**Latest news for {name}** ({len(vendor_news)} articles):\n"]
        for _, r in vendor_news.head(5).iterrows():
            sent = r.get("sentiment_score", 0)
            icon = "" if sent < -0.2 else "" if sent > 0.2 else ""
            lines.append(f"{icon} {str(r.get('published_at',''))[:10]} — "
                         f"**{str(r.get('title',''))[:90]}**")
            lines.append(f"   Source: {r.get('source_name','?')} | Sentiment: {sent:+.2f}")
            if r.get("disruption_type"):
                lines.append(f"   ️ Disruption: {r['disruption_type']}")
        return "\n".join(lines)

    # Generic: most negative sentiment vendors
    if news.empty:
        return "No news data loaded. Run `python news_ingestion.py` to fetch articles."
    neg = (news.groupby("supplier_name")["sentiment_score"].mean()
           .sort_values().head(10))
    lines = ["**Vendors with most negative news sentiment (30 days):**\n"]
    for name, sent in neg.items():
        lines.append(f"• **{name}** — avg sentiment: {sent:+.2f}")
    return "\n".join(lines)


def resp_spend_by_category(d: dict) -> str:
    """Show spend breakdown by SAP material group (spend category)."""
    cat = d.get("spend_categories", pd.DataFrame())

    if cat.empty:
        # Fallback to industry breakdown if category table not yet generated
        return (
            "Spend by material group not yet generated.\n\n"
            "Run `python ml/spend_analytics.py` to build the category breakdown.\n\n"
            "In the meantime, here's spend by industry:\n\n"
            + resp_spend_by_industry(d)
        )

    total = cat["total_spend"].sum()
    lines = ["**Spend by Material Group (SAP MATKL):**\n"]
    for _, r in cat.head(12).iterrows():
        risk_flag = " ⚠️" if r.get("high_risk_pct", 0) > 30 else ""
        mav_flag  = " 🔴" if r.get("maverick_pct",  0) > 50 else ""
        lines.append(
            f"• **{r['material_group']}** — "
            f"{fmt_money(r['total_spend'])} ({r['spend_pct']:.1f}%)"
            f" | {int(r.get('vendor_count',0))} vendors"
            f" | {r.get('maverick_pct',0):.0f}% off-contract{mav_flag}"
            f" | {r.get('high_risk_pct',0):.0f}% high risk{risk_flag}"
        )
    lines.append(f"\n**Total: {fmt_money(total)}**")
    lines.append(
        "\n*🔴 = >50% off-contract spend  ⚠️ = >30% high-risk spend*"
    )
    return "\n".join(lines)


def resp_savings_opportunity(d: dict) -> str:
    """Show savings opportunity from off-contract spend."""
    v   = d["vendors"]
    cat = d.get("spend_categories", pd.DataFrame())

    total_spend = v["total_annual_spend"].sum() if not v.empty else 0

    # Get savings from vendor-level data
    if "savings_opportunity" in v.columns:
        vendor_savings = v["savings_opportunity"].sum()
        mav_vendors    = v[v["is_maverick"] == True] if "is_maverick" in v.columns \
                         else pd.DataFrame()
        mav_spend      = mav_vendors["total_annual_spend"].sum() \
                         if not mav_vendors.empty else 0
        mav_count      = len(mav_vendors)
    else:
        vendor_savings = mav_spend = mav_count = 0

    lines = [
        "**Savings Opportunity — Off-Contract Spend**\n",
        f"Total portfolio spend: **{fmt_money(total_spend)}**",
        f"Off-contract (maverick) vendors: **{mav_count}**",
        f"Off-contract spend: **{fmt_money(mav_spend)}** "
        f"({mav_spend/total_spend*100:.1f}% of portfolio)\n" if total_spend else "",
        f"**Estimated savings opportunity: {fmt_money(vendor_savings)}**",
        f"*(40% addressable × 15–25% off-contract savings rate)*\n",
    ]

    # Top categories with savings potential
    if not cat.empty and "savings_opportunity" in cat.columns:
        lines.append("**Biggest savings by material group:**")
        for _, r in cat.nlargest(5, "savings_opportunity").iterrows():
            lines.append(
                f"• **{r['material_group']}** — "
                f"Est. {fmt_money(r['savings_opportunity'])} savings "
                f"({r.get('maverick_pct',0):.0f}% off-contract)"
            )
        lines.append("")

    lines += [
        "**How to capture these savings:**",
        "1. Create framework agreements for high-spend off-contract vendors",
        "2. Consolidate Emergency/One-Off purchases into approved supplier lists",
        "3. Mandate POs above $10K threshold to capture off-contract tail spend",
        "4. Prioritise Strategic and Leverage segment vendors for contract coverage",
    ]
    return "\n".join(lines)


def resp_spend_by_industry(d: dict) -> str:
    """Show spend breakdown aggregated by industry category."""
    v = d["vendors"]
    if "industry_category" not in v.columns or v.empty:
        return "No industry data available."

    total = v["total_annual_spend"].sum()
    by_ind = (
        v.groupby("industry_category")
         .agg(
             spend=("total_annual_spend", "sum"),
             vendors=("vendor_id", "count"),
             high_risk=("risk_label", lambda x: (x == "High").sum()),
         )
         .sort_values("spend", ascending=False)
         .reset_index()
    )

    lines = ["**Spend by Industry:**\n"]
    for _, r in by_ind.iterrows():
        pct  = r["spend"] / total * 100 if total else 0
        risk_note = f" | {int(r['high_risk'])} high risk" if r["high_risk"] > 0 else ""
        lines.append(
            f"• **{r['industry_category']}** — "
            f"{fmt_money(r['spend'])} ({pct:.1f}%) "
            f"| {int(r['vendors'])} vendors{risk_note}"
        )
    lines.append(f"\n**Total portfolio spend: {fmt_money(total)}**")
    return "\n".join(lines)


def resp_supplier_spend(d: dict, q: str) -> str:
    name = detect_vendor_name(q, d["vendors"])
    v    = find_vendor(name, d["vendors"]) if name else None

    if v is not None:
        spend     = v.get("total_annual_spend")
        pct       = v.get("spend_pct_of_portfolio", 0) or 0
        tx_count  = v.get("transaction_count", 0)
        sum_pct   = v.get("sum_percentage")
        is_mav    = v.get("is_maverick")
        lines = [
            f"**Spend Profile — {v['supplier_name']}**\n",
            f"Annual Spend: **{fmt_money(spend)}**",
            f"% of Portfolio: **{pct*100:.2f}%**" if pct <= 1 else f"% of Portfolio: **{pct:.2f}%**",
            f"Transaction Count: {int(tx_count) if pd.notna(tx_count) else 'N/A'}",
        ]
        if pd.notna(sum_pct):
            lines.append(f"Contract Coverage (SUM%): **{sum_pct:.1f}%** "
                         f"{'' if sum_pct >= 80 else '️ Below 80% target'}")
        if is_mav:
            lines.append("️ **Maverick flag** — spend outside active contracts detected")
        return "\n".join(lines)

    # Generic spend overview
    v = d["vendors"]
    total = v["total_annual_spend"].sum()
    top10 = v.nlargest(10, "total_annual_spend")
    lines = ["**Top 10 Vendors by Annual Spend:**\n"]
    for i, (_, r) in enumerate(top10.iterrows(), 1):
        pct = (r["total_annual_spend"] / total * 100) if total else 0
        lines.append(f"{i}. **{r['supplier_name']}** — "
                     f"{fmt_money(r['total_annual_spend'])} ({pct:.1f}%) "
                     f"| {risk_icon(r.get('risk_label'))} {r.get('risk_label','?')}")
    lines.append(f"\n**Total portfolio spend: {fmt_money(total)}**")
    return "\n".join(lines)


def resp_supplier_delivery(d: dict, q: str) -> str:
    name = detect_vendor_name(q, d["vendors"])
    deliv = d["delivery"]

    if name:
        m = deliv["supplier_name"].astype(str).str.lower().str.contains(name.lower(), na=False)
        row = deliv[m].iloc[0] if m.any() else None
        if row is None:
            return f"No delivery data found for **{name}** in the database."
        lines = [f"**Delivery Performance — {name}**\n",
                 f"Total Deliveries Recorded: {int(row.get('deliveries',0))}",
                 f"OTIF Rate: **{row.get('otif_pct',0):.1f}%**",
                 f"Average Delay: **{row.get('avg_delay',0):.1f} days**",
                 f"Delay Variability (STDDEV): {row.get('delay_std',0):.1f} days",
                 f"Significant Delays (>5 days): {int(row.get('sig_delays',0))}"]
        otif = row.get("otif_pct", 0)
        if otif < 80:
            lines.append("\n️ OTIF below 80% — consider escalating to supplier account manager.")
        elif otif >= 95:
            lines.append("\n Strong delivery performance — maintain current terms.")
        return "\n".join(lines)

    # Generic: worst OTIF
    worst = deliv.nsmallest(10, "otif_pct") if "otif_pct" in deliv.columns else deliv.head(10)
    lines = ["**Suppliers with Lowest OTIF Performance:**\n"]
    for _, r in worst.iterrows():
        lines.append(f"• **{r['supplier_name']}** — OTIF: {r.get('otif_pct',0):.1f}% | "
                     f"Avg Delay: {r.get('avg_delay',0):.1f}d | "
                     f"Sig. Delays: {int(r.get('sig_delays',0))}")
    return "\n".join(lines)


def resp_spend_quarter(d: dict, q: str) -> str:
    ql  = q.lower()
    qdf = d["quarterly"]
    v   = d["vendors"]

    if qdf.empty:
        total = v["total_annual_spend"].sum()
        return (f"No quarterly transaction data available.\n"
                f"Total annual spend on record: **{fmt_money(total)}**")

    today   = datetime.today()
    cur_q   = (today.month - 1) // 3 + 1
    cur_y   = today.year

    q_match = re.search(r'\bq([1-4])\b', ql)
    if "this quarter" in ql or "current quarter" in ql:
        tq, ty = cur_q, cur_y
    elif "last quarter" in ql:
        tq = cur_q - 1 if cur_q > 1 else 4
        ty = cur_y if cur_q > 1 else cur_y - 1
    elif q_match:
        tq, ty = int(q_match.group(1)), cur_y
    else:
        tq = ty = None  # show trend

    if tq and ty:
        row = qdf[(qdf["quarter"] == tq) & (qdf["year"] == ty)]
        if row.empty:
            return f"No spend data found for Q{tq} {ty}."
        r = row.iloc[0]
        lines = [
            f"**Spend Report — Q{tq} {ty}**\n",
            f"Total Spend: **{fmt_money(r['total_spend'])}**",
            f"Active Vendors: **{int(r.get('vendor_count', 0))}**\n",
            "**Quarterly trend (all periods):**",
        ]
        for _, qr in qdf.iterrows():
            lines.append(f"  {qr['period']}: {fmt_money(qr['total_spend'])}")
        return "\n".join(lines)

    lines = ["**Quarterly Spend Trend:**\n"]
    for _, r in qdf.iterrows():
        lines.append(f"• **{r['period']}** — {fmt_money(r['total_spend'])} "
                     f"| {int(r.get('vendor_count',0))} vendors")
    return "\n".join(lines)


def resp_maverick(d: dict) -> str:
    v = d["vendors"]
    mav = v[v["is_maverick"] == True] if "is_maverick" in v.columns else pd.DataFrame()
    no_contract = v[v["sum_percentage"].notna() & (v["sum_percentage"] < 10)] \
                  if "sum_percentage" in v.columns else pd.DataFrame()

    lines = ["**Maverick Spend & Contract Gaps**\n"]

    if not mav.empty:
        mav_spend = mav["total_annual_spend"].sum()
        total     = v["total_annual_spend"].sum()
        lines += [
            f"️ **{len(mav)} vendors** flagged as maverick (off-contract) spend",
            f"Maverick spend total: **{fmt_money(mav_spend)}** "
            f"({mav_spend/total*100:.1f}% of portfolio)\n",
            "**Top maverick vendors by spend:**",
        ]
        for _, r in mav.nlargest(8, "total_annual_spend").iterrows():
            lines.append(f"• **{r['supplier_name']}** — {fmt_money(r['total_annual_spend'])} "
                         f"| Type: {r.get('maverick_type','Off-Contract')}")

    if not no_contract.empty:
        nc_spend = no_contract["total_annual_spend"].sum()
        lines += [
            f"\n **{len(no_contract)} vendors** with <10% spend under contract",
            f"Exposed spend: **{fmt_money(nc_spend)}**",
        ]
        lines.append("\n**Largest unmanaged vendors:**")
        for _, r in no_contract.nlargest(5, "total_annual_spend").iterrows():
            lines.append(f"• **{r['supplier_name']}** — {fmt_money(r['total_annual_spend'])}")

    if mav.empty and no_contract.empty:
        return " No maverick spend detected. All vendors have active contract coverage."

    lines += ["\n**Actions to reduce maverick spend:**",
              "1. Create framework agreements for top off-contract vendors",
              "2. Mandate purchase orders for all transactions above $10K",
              "3. Review and consolidate tail spend into preferred supplier lists"]
    return "\n".join(lines)


def resp_alternatives(d: dict, q: str) -> str:
    name = detect_vendor_name(q, d["vendors"])
    alts = d["alternatives"]

    if name and not alts.empty:
        m = alts["supplier_name"].astype(str).str.lower().str.contains(name.lower(), na=False)
        vendor_alts = alts[m].sort_values("alternative_rank")
        if not vendor_alts.empty:
            v = find_vendor(name, d["vendors"])
            lines = [f"**Alternative Suppliers for {name}**\n"]
            if v is not None:
                lines.append(f"Current: {risk_icon(v.get('risk_label'))} {v.get('risk_label')} | "
                             f"Score: {v.get('composite_risk_score',0):.3f} | "
                             f"Spend: {fmt_money(v.get('total_annual_spend'))}\n")
            lines.append("**Recommended alternatives:**")
            for _, r in vendor_alts.head(3).iterrows():
                lines.append(f"#{int(r['alternative_rank'])} **{r['alt_supplier_name']}** "
                             f"— {risk_icon(r.get('alt_risk_tier'))} {r.get('alt_risk_tier')} "
                             f"| Similarity: {r.get('similarity_score',0):.2f} "
                             f"|  {r.get('alt_country','?')}")
                if r.get("recommendation_reason"):
                    lines.append(f"    {r['recommendation_reason']}")
            return "\n".join(lines)

    # Generic: top backup suppliers
    v = d["vendors"]
    low_risk = v[v["risk_label"] == "Low"].nlargest(10, "delivery_performance")
    lines = ["**Top Low-Risk Backup Suppliers:**\n"]
    for i, (_, r) in enumerate(low_risk.head(8).iterrows(), 1):
        lines.append(f"{i}. **{r['supplier_name']}** — "
                     f"Delivery: {r.get('delivery_performance',0):.0f}/100 | "
                     f"Spend: {fmt_money(r.get('total_annual_spend'))} | "
                     f" {r.get('country_code','?')} | {r.get('industry_category','?')}")

    if alts.empty:
        lines.append("\n*Run `python ml/recommendations.py` for personalised alternative matching.*")
    return "\n".join(lines)


def resp_compare(d: dict, q: str) -> str:
    v = d["vendors"]
    # Find all vendor names in the question
    found = [name for name in v["supplier_name"].dropna().unique()
             if len(str(name)) > 3 and str(name).lower() in q.lower()]

    if len(found) < 2:
        return ("Please name two suppliers to compare. "
                "Example: *'Compare Siemens and BASF by risk and spend'*")

    rows = [find_vendor(n, v) for n in found[:2] if find_vendor(n, v) is not None]
    if len(rows) < 2:
        return f"Could not find both suppliers. Found: {found}"

    a, b = rows[0], rows[1]
    lines = [f"**Comparison: {a['supplier_name']} vs {b['supplier_name']}**\n"]

    metrics = [
        ("Risk Tier",           a.get("risk_label","?"),      b.get("risk_label","?")),
        ("Risk Score",          f"{a.get('composite_risk_score',0):.3f}",
                                f"{b.get('composite_risk_score',0):.3f}"),
        ("Annual Spend",        fmt_money(a.get("total_annual_spend")),
                                fmt_money(b.get("total_annual_spend"))),
        ("Delivery Perf.",      f"{a.get('delivery_performance',0):.0f}/100",
                                f"{b.get('delivery_performance',0):.0f}/100"),
        ("OTIF Rate",           f"{(a.get('otif_rate') or 0)*100:.1f}%",
                                f"{(b.get('otif_rate') or 0)*100:.1f}%"),
        ("Financial Stability", f"{a.get('financial_stability',0):.0f}/100",
                                f"{b.get('financial_stability',0):.0f}/100"),
        ("Geo Risk",            a.get("geo_risk","?"),         b.get("geo_risk","?")),
        ("Country",             a.get("country_code","?"),     b.get("country_code","?")),
        ("News Sentiment",      f"{(a.get('news_sentiment_30d') or 0):+.2f}",
                                f"{(b.get('news_sentiment_30d') or 0):+.2f}"),
    ]

    header = f"{'Metric':<22} {a['supplier_name'][:20]:<22} {b['supplier_name'][:20]:<22}"
    lines.append(f"`{header}`")
    for m, va, vb in metrics:
        lines.append(f"`{m:<22} {str(va):<22} {str(vb):<22}`")

    # Verdict
    a_score = float(a.get("composite_risk_score", 0.5))
    b_score = float(b.get("composite_risk_score", 0.5))
    safer   = a["supplier_name"] if a_score < b_score else b["supplier_name"]
    lines.append(f"\n **{safer}** has the lower risk profile.")
    return "\n".join(lines)


def resp_contracts(d: dict, q: str) -> str:
    c = d["contracts"]
    ql = q.lower()

    if c.empty:
        return ("No contracts found in the database. "
                "This SAP dataset may not contain separate contract documents (BSTYP=K). "
                "All POs are framework orders.")

    days_match = re.search(r'(\d+)\s*day', ql)
    days = int(days_match.group(1)) if days_match else (30 if "30" in ql else 60)

    expiring = c[c["days_to_expiry"].between(0, days)] if "days_to_expiry" in c.columns else c
    lines = [f"**Contracts Expiring in the Next {days} Days**\n"]

    if expiring.empty:
        lines.append(f" No contracts expiring in the next {days} days.")
    else:
        lines.append(f"️ **{len(expiring)} contract(s)** require renewal:\n")
        for _, r in expiring.sort_values("days_to_expiry").head(15).iterrows():
            risk_flag = f" | {risk_icon(r.get('risk_label'))} {r.get('risk_label','?')}" \
                        if pd.notna(r.get("risk_label")) else ""
            lines.append(f"• **{r['supplier_name']}** — "
                         f"Expires: {str(r.get('contract_end','?'))} "
                         f"({int(r.get('days_to_expiry',0))} days){risk_flag} | "
                         f"Spend: {fmt_money(r.get('total_annual_spend'))}")

    lines += ["\n**Renewal priorities:**",
              "1. High-risk expiring contracts — negotiate risk clauses",
              "2. High-spend expiring contracts — strategic renegotiation opportunity",
              "3. Consider multi-year agreements for stable low-risk suppliers"]
    return "\n".join(lines)


def resp_concentration(d: dict, q: str) -> str:
    name = detect_vendor_name(q, d["vendors"])
    v    = d["vendors"]
    total = v["total_annual_spend"].sum()

    lines = ["**Concentration Risk Analysis**\n"]

    if name:
        row = find_vendor(name, v)
        if row is not None:
            spend = row.get("total_annual_spend", 0) or 0
            pct   = spend / total * 100 if total else 0
            lines += [
                f"**If we lose {name}:**",
                f"Spend exposure: **{fmt_money(spend)}** ({pct:.2f}% of portfolio)\n",
                "Impact assessment:",
            ]
            if pct > 10:
                lines.append(f" **CRITICAL** — {pct:.1f}% dependency exceeds 10% threshold.")
                lines.append("Immediate action: qualify at least 2 alternative suppliers.")
            elif pct > 5:
                lines.append(f" **MODERATE** — {pct:.1f}% dependency. Build backup relationships.")
            else:
                lines.append(f" **LOW** — {pct:.1f}% dependency. Manageable with normal sourcing.")
            return "\n".join(lines)

    # Portfolio concentration
    top5 = v.nlargest(5, "total_annual_spend")
    top5_spend  = top5["total_annual_spend"].sum()
    top10_spend = v.nlargest(10, "total_annual_spend")["total_annual_spend"].sum()
    shares      = (v["total_annual_spend"] / total * 100) ** 2
    hhi         = shares.sum()

    lines += [
        f"Total portfolio spend: **{fmt_money(total)}**\n",
        f"**Concentration metrics:**",
        f"- Top 5 vendors: **{top5_spend/total*100:.1f}%** of spend "
        f"{'️ >40% limit' if top5_spend/total > 0.4 else ' Within limit'}",
        f"- Top 10 vendors: **{top10_spend/total*100:.1f}%** of spend",
        f"- HHI Index: **{hhi:,.0f}** "
        f"({'HIGH' if hhi > 2500 else 'MODERATE' if hhi > 1500 else 'HEALTHY'})\n",
        "**Top 5 vendors by spend:**",
    ]
    for _, r in top5.iterrows():
        pct = r["total_annual_spend"] / total * 100
        lines.append(f"• **{r['supplier_name']}** — {fmt_money(r['total_annual_spend'])} "
                     f"({pct:.1f}%) | {risk_icon(r.get('risk_label'))} {r.get('risk_label','?')}")
    return "\n".join(lines)


def resp_whatif(d: dict, q: str) -> str:
    ql   = q.lower()
    name = detect_vendor_name(q, d["vendors"])
    v    = d["vendors"]
    total_spend = v["total_annual_spend"].sum()

    # Bankruptcy scenario
    if "bankrupt" in ql and name:
        row = find_vendor(name, v)
        if row:
            spend = row.get("total_annual_spend", 0) or 0
            pct   = spend / total_spend * 100 if total_spend else 0
            tier  = row.get("risk_label","?")
            return (
                f"**What-If: {name} goes bankrupt**\n\n"
                f"Immediate spend exposure: **{fmt_money(spend)}** ({pct:.1f}% of portfolio)\n\n"
                f"**Risk assessment:**\n"
                f"• Current risk tier: {risk_icon(tier)} **{tier}**\n"
                f"• Portfolio impact: {'CRITICAL' if pct > 10 else 'SIGNIFICANT' if pct > 5 else 'MANAGEABLE'}\n\n"
                f"**Actions to take now:**\n"
                f"1. Identify alternative suppliers in the same industry\n"
                f"2. Check inventory for components/services from this vendor\n"
                f"3. Review contract force-majeure clauses\n"
                f"4. Place safety stock orders immediately if strategic component\n"
                f"5. Brief the procurement manager and relevant business units"
            )

    # Spend shift scenario — handle both "shift 20% from X" and "move 20% from X to Y"
    # Pattern A: number before verb  → "20% shift from X"
    # Pattern B: verb before number  → "move 20% from X to Y"  (most natural phrasing)
    shift_match = (
        re.search(r'(\d+)%?\s*(?:shift|move|transfer|reallocate)', ql)
        or re.search(r'(?:shift|move|transfer|reallocate)[^\d]*(\d+)%?', ql)
    )
    # Also detect "from X to Y" with two vendor names
    from_to_match = re.search(r'from\s+(.+?)\s+to\s+(.+?)(?:\s*[,?.]|$)', q, re.IGNORECASE)

    if shift_match:
        pct_shift = int(shift_match.group(1))

        # If "from X to Y" pattern — show impact on both vendors
        if from_to_match:
            name_from = detect_vendor_name(from_to_match.group(1).strip(), v)
            name_to   = detect_vendor_name(from_to_match.group(2).strip(), v)

            if name_from and name_to:
                row_from = find_vendor(name_from, v)
                row_to   = find_vendor(name_to, v)
                if row_from and row_to:
                    spend_from = row_from.get("total_annual_spend", 0) or 0
                    shifted    = spend_from * pct_shift / 100
                    new_from   = spend_from - shifted
                    spend_to   = row_to.get("total_annual_spend", 0) or 0
                    new_to     = spend_to + shifted
                    return (
                        f"**What-If: Move {pct_shift}% spend from {name_from} to {name_to}**\n\n"
                        f"**{name_from} (source):**\n"
                        f"• Current spend: **{fmt_money(spend_from)}** → New: **{fmt_money(new_from)}**\n"
                        f"• Risk tier: {risk_icon(row_from.get('risk_label'))} {row_from.get('risk_label','?')}\n"
                        f"• Portfolio concentration change: "
                        f"{spend_from/total_spend*100:.1f}% → {new_from/total_spend*100:.1f}%\n\n"
                        f"**{name_to} (destination):**\n"
                        f"• Current spend: **{fmt_money(spend_to)}** → New: **{fmt_money(new_to)}**\n"
                        f"• Risk tier: {risk_icon(row_to.get('risk_label'))} {row_to.get('risk_label','?')}\n"
                        f"• Portfolio concentration change: "
                        f"{spend_to/total_spend*100:.1f}% → {new_to/total_spend*100:.1f}%\n\n"
                        f"**Net effect:** Shifting **{fmt_money(shifted)}** from "
                        f"{row_from.get('risk_label','?')} risk to {row_to.get('risk_label','?')} risk vendor.\n"
                        f"*Verify {name_to} has the capacity to absorb this volume before committing.*"
                    )

        # Single-vendor shift (away from that vendor)
        if name:
            row = find_vendor(name, v)
            if row:
                spend   = row.get("total_annual_spend", 0) or 0
                shifted = spend * pct_shift / 100
                return (
                    f"**What-If: Shift {pct_shift}% spend away from {name}**\n\n"
                    f"Current spend: **{fmt_money(spend)}**\n"
                    f"Amount to shift: **{fmt_money(shifted)}**\n\n"
                    f"**Portfolio impact:**\n"
                    f"• Concentration reduces by {spend/total_spend*pct_shift/100*100:.2f}%\n"
                    f"• You need alternative suppliers capable of absorbing {fmt_money(shifted)}\n\n"
                    f"*Ask 'who is the safest alternative to {name}?' to find candidates.*"
                )

        # Percentage detected but no vendor identified
        return (
            f"I found a {pct_shift}% spend shift request but couldn't identify the supplier.\n\n"
            f"Try: *'Move {pct_shift}% spend from [Supplier A] to [Supplier B]'*\n"
            f"or:  *'Shift {pct_shift}% spend from [Supplier Name]'*"
        )

    # Quarter prediction
    if "predict" in ql and "quarter" in ql:
        qdf = d["quarterly"]
        v   = d["vendors"]

        # Drop quarters where total_spend is NULL/NaN/0 — can't use them for trend
        if not qdf.empty and "total_spend" in qdf.columns:
            qdf_clean = qdf[qdf["total_spend"].notna() & (qdf["total_spend"] > 0)].copy()
        else:
            qdf_clean = pd.DataFrame()

        if len(qdf_clean) >= 2:
            # Sort chronologically and use last 2 periods for trend
            qdf_clean = qdf_clean.sort_values(["year", "quarter"])
            last2     = qdf_clean.tail(2)
            s_prev    = float(last2.iloc[0]["total_spend"])
            s_last    = float(last2.iloc[1]["total_spend"])

            if s_prev > 0:
                growth    = (s_last - s_prev) / s_prev
                predicted = s_last * (1 + growth)
            else:
                growth    = 0
                predicted = s_last

            period_last = last2.iloc[1].get("period", "last quarter")
            period_prev = last2.iloc[0].get("period", "prior quarter")

            # Cap extreme extrapolation (>±50%) with a note
            capped = abs(growth) > 0.5
            if capped:
                growth_display = growth
                predicted = s_last * (1 + max(min(growth, 0.5), -0.5))
            else:
                growth_display = growth

            lines = [
                "**Spend Forecast — Next Quarter**\n",
                f"{period_prev} spend: **{fmt_money(s_prev)}**",
                f"{period_last} spend: **{fmt_money(s_last)}**",
                f"Quarter-on-quarter change: **{growth_display*100:+.1f}%**",
                f"Predicted next quarter: **{fmt_money(predicted)}**",
            ]
            if capped:
                lines.append(
                    f"\n*Growth capped at ±50% — raw trend was {growth*100:+.1f}%, "
                    "which may reflect one-off events rather than a real trend.*"
                )
            else:
                lines.append(
                    f"\n*Linear trend from {len(qdf_clean)} quarters of data. "
                    "Actual may vary with seasonality and new contracts.*"
                )
            return "\n".join(lines)

        elif len(qdf_clean) == 1:
            # Only one valid quarter — show flat estimate
            s_only = float(qdf_clean.iloc[0]["total_spend"])
            return (
                "**Spend Forecast — Next Quarter (Flat Estimate)**\n\n"
                f"Only one quarter of transaction data available.\n"
                f"Last recorded quarter: **{fmt_money(s_only)}**\n"
                f"Flat estimate (no trend): **{fmt_money(s_only)}**\n\n"
                "*At least 2 quarters are needed for a trend-based forecast.*"
            )

        elif not v.empty and "total_annual_spend" in v.columns:
            # No quarterly data at all — fall back to annual ÷ 4
            total_annual  = v["total_annual_spend"].dropna().sum()
            quarterly_est = total_annual / 4
            return (
                "**Spend Forecast — Next Quarter (Annual Estimate)**\n\n"
                f"No quarterly transaction breakdown found in the transactions table.\n"
                f"Total annual spend (vendor master): **{fmt_money(total_annual)}**\n"
                f"Estimated quarterly spend (÷4): **{fmt_money(quarterly_est)}**\n\n"
                "*For a trend-based forecast, run `python ingestion/sap_loader.py` "
                "to load transaction records with year/quarter fields.*"
            )

        return "No spend data available for prediction. Run the SAP loader first."

    return ("I can model these what-if scenarios:\n"
            "- *'What happens if [Supplier] goes bankrupt?'*\n"
            "- *'If we shift 20% spend from [Supplier A] to [Supplier B], what changes?'*\n"
            "- *'Predict spend for next quarter based on current trends'*")


def resp_esg(d: dict) -> str:
    v = d["vendors"]
    lines = ["**ESG & Sustainability Assessment**\n"]

    if "esg_score" in v.columns:
        with_esg = v[v["esg_score"].notna() & (v["esg_score"] > 0)]
        if not with_esg.empty:
            avg_esg = with_esg["esg_score"].mean()
            lines += [
                f"ESG data available for **{len(with_esg)}** vendors",
                f"Average ESG score: **{avg_esg:.0f}/100**\n",
                "**Top ESG performers:**",
            ]
            for _, r in with_esg.nlargest(5,"esg_score").iterrows():
                lines.append(f"• **{r['supplier_name']}** — ESG: {r['esg_score']:.0f}/100 | "
                             f"Spend: {fmt_money(r.get('total_annual_spend'))}")
            lines += ["\n**Lowest ESG scores:**"]
            for _, r in with_esg.nsmallest(5,"esg_score").iterrows():
                lines.append(f"• **{r['supplier_name']}** — ESG: {r['esg_score']:.0f}/100 | "
                             f"Spend: {fmt_money(r.get('total_annual_spend'))}")
            return "\n".join(lines)

    lines += [
        "ESG scores not yet populated for your vendors.\n",
        "**How to add ESG scores:**",
        "1. Open the **Scorecard** tab in the dashboard",
        "2. Select each vendor and enter their ESG score (0–100)",
        "3. Source: EcoVadis, CDP, Sustainalytics, or internal assessments",
        "4. Scores are saved to Postgres and used in future ML runs\n",
        "**Why ESG matters:**",
        "- Increasingly a legal requirement (EU CSRD, UK Modern Slavery Act)",
        "- Drives brand and reputational risk",
        "- Correlates with supplier resilience and long-term stability",
    ]
    return "\n".join(lines)


def resp_data_source(q: str) -> str:
    ql = q.lower()
    if "news" in ql or "sentiment" in ql:
        return ("**News Data Sources:**\n"
                "- **NewsAPI** — company-specific news articles\n"
                "- **The Guardian API** — global news coverage\n"
                "- **GDELT** — free, global event database (no key needed)\n\n"
                "Data is fetched per-vendor by querying each API with the vendor name. "
                "Sentiment is scored using a lexicon of positive/negative procurement keywords. "
                "Run `python news_ingestion.py --days 30` to refresh.")
    if "delivery" in ql or "delay" in ql:
        return ("**Delivery Data Source: SAP**\n"
                "- **EKET** — purchase order schedule lines (promised dates and quantities)\n"
                "- **EKBE** — goods receipt history (actual delivery dates and quantities)\n\n"
                "OTIF is calculated as: actual GR date ≤ promised date AND actual qty ≥ 98% of promised qty.")
    if "risk score" in ql or "calculated" in ql:
        return resp_how_score()
    if "financial" in ql:
        return ("**Financial Stability Source:**\n"
                "Computed from SAP transaction data as a proxy score (0–100):\n"
                "- 50% weight: log-normalised total annual spend\n"
                "- 30% weight: transaction count (consistency of activity)\n"
                "- 20% base score\n\n"
                "For D&B or RapidRatings integration, add their scores via "
                "the Scorecard tab in the dashboard.")
    return ("**Data Sources:**\n"
            "- **SAP BigQuery Dataset** — LFA1 (vendors), EKKO+EKPO (transactions), "
            "EKET+EKBE (delivery), EKKO BSTYP=K (contracts)\n"
            "- **NewsAPI + Guardian + GDELT** — news and sentiment signals\n"
            "- **Manual Scorecard** — ESG, cybersecurity, innovation scores\n"
            "- **PostgreSQL srsid_db** — all data stored and queried live")


def resp_how_score() -> str:
    return ("**How the Risk Score is Calculated:**\n\n"
            "The composite risk score (0–1) uses a weighted formula:\n\n"
            "```\nComposite Risk = (\n"
            "  geo_risk          × 30%   +\n"
            "  industry_risk     × 25%   +\n"
            "  delivery_risk     × 25%   +\n"
            "  concentration_risk× 20%\n"
            ") × 85%  +  disruption_news × 15%  +  spend_compliance × 10%\n```\n\n"
            "**Geo risk:** Evidence-based per-country score from World Bank LPI + EM-DAT disaster data\n"
            "**Industry risk:** Pre-scored by industry (Electronics=0.85, Chemicals=0.75...)\n"
            "**Delivery risk:** 1 - OTIF rate\n"
            "**Concentration:** Flag if vendor >10% of portfolio spend\n"
            "**News:** Disruption frequency from NewsAPI/Guardian/GDELT (last 30 days)\n"
            "**Spend compliance:** Maverick flag + low SUM%\n\n"
            "Tiers: **High** ≥ 0.45 | **Medium** ≥ 0.25 | **Low** < 0.25")


def resp_export(d: dict) -> str:
    return ("**Export Options:**\n\n"
            "Each dashboard tab has a ** Export** button that downloads the displayed "
            "data as CSV.\n\n"
            "**Available exports:**\n"
            "- Risk Scores → `risk_scores.csv`\n"
            "- Segments → `segments.csv`\n"
            "- Alternatives → `alternative_suppliers.csv`\n"
            "- Anomalies → `anomaly_report.csv`\n\n"
            "For a full Excel report, the `reports/` folder contains:\n"
            "- `supplier_features.csv` — full feature matrix\n"
            "- `spend_intelligence_report.json` — spend KPIs\n"
            "- `anomaly_summary.json` — anomaly summary\n"
            "- `model_evaluation.json` — ML model performance")


def resp_general(q: str, d: dict) -> str:
    """Catch-all: try to find a vendor name, else give generic guidance."""
    name = detect_vendor_name(q, d["vendors"])
    if name:
        return resp_supplier_risk(d, q)
    return (f"I'm not sure I understood that. Try asking:\n"
            "- *'Give me a supply chain health summary'*\n"
            "- *'Which suppliers are high risk?'*\n"
            "- *'What is the risk level for [Supplier Name]?'*\n"
            "- *'Help'* to see all capabilities.")


# ─────────────────────────────────────────────────────────────────────────────
# VISUALISATIONS — shown inline after responses
# ─────────────────────────────────────────────────────────────────────────────

def show_chart(intent: str, d: dict, q: str):
    """Show an appropriate chart based on intent."""
    try:
        import plotly.express as px
        import plotly.graph_objects as go
    except ImportError:
        return

    v   = d["vendors"]
    qdf = d["quarterly"]

    if intent == "overview" and not v.empty:
        col1, col2 = st.columns(2)
        with col1:
            rd = v["risk_label"].value_counts().to_dict()
            colors = {"High":"#E74C3C","Medium":"#F39C12","Low":"#27AE60"}
            fig = px.pie(names=list(rd.keys()), values=list(rd.values()),
                         color=list(rd.keys()), color_discrete_map=colors,
                         hole=0.45, title="Risk Distribution")
            fig.update_layout(margin=dict(t=40,b=0), height=280)
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            if not qdf.empty:
                fig = px.line(qdf, x="period", y="total_spend",
                              title="Quarterly Spend Trend", markers=True)
                fig.update_layout(margin=dict(t=40,b=0), height=280)
                st.plotly_chart(fig, use_container_width=True)

    elif intent in ("high_risk","medium_risk","low_risk"):
        tier_map = {"high_risk":"High","medium_risk":"Medium","low_risk":"Low"}
        tier = tier_map[intent]
        subset = v[v["risk_label"]==tier].nlargest(
            10, "total_annual_spend" if "total_annual_spend" in v.columns else "composite_risk_score"
        )
        if not subset.empty and "total_annual_spend" in subset.columns:
            fig = px.bar(subset, x="total_annual_spend", y="supplier_name",
                         orientation="h", color="composite_risk_score",
                         color_continuous_scale="Reds",
                         title=f"Top {tier}-Risk Vendors by Spend",
                         labels={"total_annual_spend":"Annual Spend","supplier_name":"Vendor"})
            fig.update_layout(yaxis=dict(autorange="reversed"),
                              margin=dict(t=40,b=0), height=350)
            st.plotly_chart(fig, use_container_width=True)

    elif intent == "spend_quarter" and not qdf.empty:
        fig = px.bar(qdf, x="period", y="total_spend",
                     title="Quarterly Spend", text_auto=True,
                     color="total_spend", color_continuous_scale="Blues")
        fig.update_layout(margin=dict(t=40,b=0), height=300)
        st.plotly_chart(fig, use_container_width=True)

    elif intent == "concentration" and not v.empty:
        top10 = v.nlargest(10, "total_annual_spend")
        fig = px.treemap(top10, path=["supplier_name"],
                         values="total_annual_spend",
                         color="composite_risk_score",
                         color_continuous_scale="RdYlGn_r",
                         title="Spend Concentration (size=spend, colour=risk)")
        fig.update_layout(margin=dict(t=40,b=0), height=350)
        st.plotly_chart(fig, use_container_width=True)

    elif intent == "otif" and not d["delivery"].empty:
        worst = d["delivery"].nsmallest(10,"otif_pct")
        fig = px.bar(worst, x="otif_pct", y="supplier_name",
                     orientation="h", color="otif_pct",
                     color_continuous_scale="RdYlGn",
                     title="OTIF Rate — Lowest Performers",
                     labels={"otif_pct":"OTIF %","supplier_name":"Vendor"})
        fig.update_layout(yaxis=dict(autorange="reversed"),
                          margin=dict(t=40,b=0), height=350)
        st.plotly_chart(fig, use_container_width=True)

    elif intent == "compare":
        name_matches = [n for n in v["supplier_name"].dropna().unique()
                        if len(str(n)) > 3 and str(n).lower() in q.lower()]
        if len(name_matches) >= 2:
            subset = v[v["supplier_name"].isin(name_matches[:2])]
            metrics = ["composite_risk_score","delivery_performance",
                       "financial_stability","news_sentiment_30d"]
            metrics = [m for m in metrics if m in subset.columns]
            if metrics:
                fig = go.Figure()
                for _, r in subset.iterrows():
                    vals = [float(r.get(m,0) or 0) for m in metrics]
                    labels = [m.replace("_"," ").title() for m in metrics]
                    fig.add_trace(go.Scatterpolar(
                        r=vals, theta=labels, fill="toself",
                        name=r["supplier_name"]
                    ))
                fig.update_layout(polar=dict(radialaxis=dict(visible=True)),
                                  title="Radar Comparison", height=350)
                st.plotly_chart(fig, use_container_width=True)

    elif intent == "risk_drivers" and not d["feat_imp"].empty:
        fi = d["feat_imp"]
        label_col = "feature_label" if "feature_label" in fi.columns else "feature"
        fig = px.bar(fi.head(10), x="importance", y=label_col,
                     orientation="h", color="importance",
                     color_continuous_scale="Reds",
                     title="Top Risk Drivers (Feature Importance)")
        fig.update_layout(yaxis=dict(autorange="reversed"),
                          margin=dict(t=40,b=0), height=320)
        st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# ROUTER
# ─────────────────────────────────────────────────────────────────────────────

def route(q: str, d: dict) -> tuple[str, str]:
    """
    Hybrid router — decides whether to answer from:
      A) Postgres aggregation (portfolio-level questions)
      B) RAG + Flan-T5 (vendor-specific questions)

    Returns (response_text, intent).
    """
    from rag.retriever import route_to_rag

    use_rag = route_to_rag(q)

    if use_rag:
        response = _rag_response(q, d)
        return response, "rag"
    else:
        return _postgres_route(q, d)


def _rag_response(q: str, d: dict) -> str:
    """Generate response using RAG + Flan-T5."""
    from rag.retriever import retrieve, build_prompt, is_available as rag_available
    import rag.llm as llm

    # Graceful fallback if pgvector index not built yet
    if not rag_available():
        return (
            "The vector index has not been built yet.\n\n"
            "Run: `python rag/build_index.py`\n\n"
            "Falling back to rule-based answer...\n\n"
            + _postgres_route(q, d)[0]
        )

    if not llm.is_available():
        return (
            "Flan-T5 is not installed. Run:\n"
            "`pip install transformers torch`\n\n"
            "Falling back to rule-based answer...\n\n"
            + _postgres_route(q, d)[0]
        )

    try:
        # Retrieve relevant vendor chunks
        chunks = retrieve(q, n=5)

        if not chunks:
            # No relevant vendors found — fall back to Postgres
            return _postgres_route(q, d)[0]

        # Show which vendors were retrieved (transparency)
        retrieved_names = [c["metadata"]["name"] for c in chunks]

        # Build prompt and generate
        prompt   = build_prompt(q, chunks, max_tokens=380)
        answer   = llm.generate(prompt, model_size="base", max_new_tokens=220)

        # Append source attribution
        sources = ", ".join(retrieved_names[:3])
        if len(retrieved_names) > 3:
            sources += f" + {len(retrieved_names)-3} more"

        return (
            f"{answer}\n\n"
            f"---\n"
            f"*Sources retrieved: {sources}*"
        )

    except Exception as e:
        # Any RAG failure → fall back gracefully
        fallback_response, _ = _postgres_route(q, d)
        return (
            f"{fallback_response}\n\n"
            f"*(RAG unavailable: {e})*"
        )


def _postgres_route(q: str, d: dict) -> tuple[str, str]:
    """Original rule-based Postgres router — unchanged."""
    intent = detect_intent(q)
    resp_map = {
        "help":            lambda: resp_help(),
        "overview":        lambda: resp_overview(d),
        "high_risk":       lambda: resp_high_risk(d),
        "medium_risk":     lambda: resp_medium_risk(d),
        "low_risk":        lambda: resp_low_risk(d),
        "alerts":          lambda: resp_alerts(d),
        "geo_news":        lambda: resp_geo_news(d, q),
        "spend_at_risk":   lambda: resp_spend_at_risk(d),
        "risk_drivers":    lambda: resp_risk_drivers(d),
        "health_drop":     lambda: resp_alerts(d),
        "supplier_risk":   lambda: resp_supplier_risk(d, q),
        "supplier_explain":lambda: resp_supplier_explain(d, q),
        "supplier_news":   lambda: resp_supplier_news(d, q),
        "supplier_spend":  lambda: resp_supplier_spend(d, q),
        "supplier_delivery":lambda: resp_supplier_delivery(d, q),
        "supplier_sla":    lambda: resp_supplier_risk(d, q),
        "spend_quarter":   lambda: resp_spend_quarter(d, q),
        "maverick":        lambda: resp_maverick(d),
        "spend_category":  lambda: resp_spend_by_category(d)
                                   if any(kw in q.lower() for kw in
                                          ["material","category","matkl"])
                                   else resp_spend_by_industry(d),
        "top_vendors":     lambda: resp_supplier_spend(d, q),
        "savings":         lambda: resp_savings_opportunity(d),
        "otif":            lambda: resp_supplier_delivery(d, q),
        "delay":           lambda: resp_supplier_delivery(d, q),
        "news_delivery_corr": lambda: _resp_news_delay_corr(d),
        "predict_delay":   lambda: _resp_predict_delay(d),
        "alternatives":    lambda: resp_alternatives(d, q),
        "compare":         lambda: resp_compare(d, q),
        "zero_spend":      lambda: _resp_zero_spend(d),
        "contracts_expiry":lambda: resp_contracts(d, q),
        "pip":             lambda: _resp_pip(d),
        "concentration":   lambda: resp_concentration(d, q),
        "esg":             lambda: resp_esg(d),
        "whatif":          lambda: resp_whatif(d, q),
        "risk_heatmap":    lambda: resp_concentration(d, q),
        "data_source":     lambda: resp_data_source(q),
        "how_score":       lambda: resp_how_score(),
        "export":          lambda: resp_export(d),
        "general":         lambda: resp_general(q, d),
    }
    fn = resp_map.get(intent, resp_map["general"])
    return fn(), intent


def _resp_news_delay_corr(d: dict) -> str:
    news  = d["news"]
    deliv = d["delivery"]
    if news.empty or deliv.empty:
        return "Need both news and delivery data to compute correlation."
    neg_news_vendors = set(
        news[news["sentiment_score"] < -0.2]["vendor_id"].dropna().unique()
    )
    deliv_neg = deliv[deliv["vendor_id"].isin(neg_news_vendors)]
    deliv_pos = deliv[~deliv["vendor_id"].isin(neg_news_vendors)]
    avg_delay_neg = deliv_neg["avg_delay"].mean() if not deliv_neg.empty else 0
    avg_delay_pos = deliv_pos["avg_delay"].mean() if not deliv_pos.empty else 0

    if avg_delay_neg > avg_delay_pos:
        verdict = ("️ Vendors with negative news show higher delivery delays — "
                   "news signals may be an early warning for operational issues.")
    else:
        verdict = " No strong correlation detected in current data."

    return (f"**News Sentiment vs Delivery Delay Correlation**\n\n"
            f"Vendors with **negative news sentiment**:\n"
            f"  Average delay: **{avg_delay_neg:.1f} days** ({len(deliv_neg)} vendors)\n\n"
            f"Vendors with **neutral/positive news sentiment**:\n"
            f"  Average delay: **{avg_delay_pos:.1f} days** ({len(deliv_pos)} vendors)\n\n"
            f"{verdict}")


def _resp_predict_delay(d: dict) -> str:
    news  = d["news"]
    v     = d["vendors"]
    at_risk = v[(v["disruption_count_30d"] > 0) |
                (v["news_sentiment_30d"] < -0.2) if
                "disruption_count_30d" in v.columns and
                "news_sentiment_30d" in v.columns else
                v["risk_label"] == "High"]
    if at_risk.empty:
        return " No vendors currently flagged with disruption news signals."
    lines = ["**Suppliers with elevated delay risk (based on news signals):**\n"]
    for _, r in at_risk.head(10).iterrows():
        lines.append(f"• **{r['supplier_name']}** — "
                     f"Disruptions: {int(r.get('disruption_count_30d') or 0)} | "
                     f"Sentiment: {(r.get('news_sentiment_30d') or 0):+.2f}")
    lines.append("\n*Based on current news signals. Not a deterministic forecast.*")
    return "\n".join(lines)


def _resp_zero_spend(d: dict) -> str:
    v = d["vendors"]
    zero = v[v["transaction_count"].fillna(0) == 0] if "transaction_count" in v.columns \
           else v[v["total_annual_spend"].fillna(0) == 0]
    low_risk_zero = zero[zero["risk_label"] == "Low"] if not zero.empty else pd.DataFrame()
    lines = [f"**{len(zero)} vendors with zero recorded spend:**\n"]
    for _, r in low_risk_zero.head(10).iterrows():
        lines.append(f"• **{r['supplier_name']}** — {r.get('country_code','?')} | "
                     f"{r.get('industry_category','?')}")
    lines.append("\n*These may be preferred suppliers not yet activated, or duplicates.*")
    return "\n".join(lines)


def _resp_pip(d: dict) -> str:
    v     = d["vendors"]
    deliv = d["delivery"]
    high  = v[v["risk_label"] == "High"]
    poor_del = deliv[deliv["otif_pct"] < 70] if "otif_pct" in deliv.columns else pd.DataFrame()
    pip_candidates = set(high["supplier_name"].tolist())
    if not poor_del.empty:
        pip_candidates |= set(poor_del["supplier_name"].tolist())
    lines = [f"**Performance Improvement Plan (PIP) Candidates:**\n",
             f"Criteria: High Risk tier OR OTIF < 70%\n"]
    for name in list(pip_candidates)[:15]:
        row = find_vendor(name, v)
        if row is not None:
            lines.append(f"• **{name}** — {risk_icon(row.get('risk_label'))} "
                         f"{row.get('risk_label','?')} | "
                         f"OTIF: {(row.get('otif_rate') or 0)*100:.1f}% | "
                         f"Spend: {fmt_money(row.get('total_annual_spend'))}")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

QUICK_QUESTIONS = [
    ("Summary",      [
        "Give me a supply chain health summary",
        "Which suppliers are high risk right now?",
        "Show disruption alerts",
        "What is our total spend at risk?",
    ]),
    ("Suppliers",    [
        "List all medium risk suppliers",
        "Show low risk suppliers",
        "Who has the most negative news today?",
        "Which suppliers are failing on OTIF?",
    ]),
    ("Spend",        [
        "What is our total spend this quarter?",
        "Where is our maverick spend?",
        "Show spend by material group",
        "What is our savings opportunity?",
    ]),
    ("Alternatives", [
        "Who are our top backup suppliers?",
        "Which contracts expire in 60 days?",
        "Which suppliers should be on a PIP?",
        "Show ESG status",
    ]),
    ("What-If Scenarios",      [
        "Predict spend for next quarter",
        "What is the correlation between news and delays?",
        "How is the risk score calculated?",
        "How often is news data updated?",
    ]),
]


def render_sidebar():
    # Brand header
    st.sidebar.markdown("""
<div style='padding:1.1rem 1rem 0.8rem;border-bottom:1px solid rgba(255,255,255,0.12);margin-bottom:0.8rem'>
  <div style='display:flex;align-items:center;gap:10px'>
    <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
      <rect width="28" height="28" rx="6" fill="#1A6FBF"/>
      <path d="M7 20L10 12L14 17L17 9L21 20" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" fill="none"/>
    </svg>
    <div>
      <div style='font-size:1rem;font-weight:700;color:#FFFFFF;letter-spacing:0.01em'>SRSID</div>
      <div style='font-size:0.65rem;color:#9BB5D0;text-transform:uppercase;letter-spacing:0.08em'>Procurement Assistant</div>
    </div>
  </div>
</div>""", unsafe_allow_html=True)

    # KPI cards
    def kpi_card(icon_path, label, value, accent="#1A6FBF"):
        return (f"<div style='background:rgba(255,255,255,0.07);border-radius:6px;"
                f"padding:8px 11px;margin-bottom:5px;border-left:3px solid {accent};"
                f"display:flex;align-items:center;gap:10px'>"
                f"<svg width='16' height='16' viewBox='0 0 16 16' fill='none'>{icon_path}</svg>"
                f"<div><div style='font-size:0.62rem;color:#9BB5D0;text-transform:uppercase;"
                f"letter-spacing:0.07em'>{label}</div>"
                f"<div style='font-size:1.05rem;font-weight:700;color:#FFFFFF'>{value}</div></div></div>")

    ICON_VENDORS = '<rect x="2" y="3" width="5" height="10" rx="1" fill="#9BB5D0"/><rect x="9" y="6" width="5" height="7" rx="1" fill="#9BB5D0"/><rect x="2" y="14" width="12" height="1" rx="0.5" fill="#9BB5D0"/>'
    ICON_RISK    = '<circle cx="8" cy="8" r="6" stroke="#C0392B" stroke-width="1.5" fill="none"/><path d="M8 5v4M8 11v1" stroke="#C0392B" stroke-width="1.5" stroke-linecap="round"/>'
    ICON_SPEND   = '<path d="M2 12L6 7l3 3 3-5 3 3" stroke="#1D9E75" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" fill="none"/>'

    try:
        d = load_data()
        s = d["summary"]
        total_v = s.get("total_vendors", "—")
        high    = s.get("high_risk_count", "—")
        spend   = fmt_money(s.get("total_portfolio_spend")) if s.get("total_portfolio_spend") else "—"

        st.sidebar.markdown(
            "<div style='font-size:0.62rem;color:#9BB5D0;text-transform:uppercase;"
            "letter-spacing:0.08em;margin-bottom:5px'>Portfolio</div>",
            unsafe_allow_html=True)
        c1, c2 = st.sidebar.columns(2)
        c1.markdown(kpi_card(ICON_VENDORS, "Vendors",   total_v),           unsafe_allow_html=True)
        c2.markdown(kpi_card(ICON_RISK,    "High Risk", high,    "#C0392B"), unsafe_allow_html=True)
        st.sidebar.markdown(kpi_card(ICON_SPEND, "Portfolio Spend", spend, "#1D9E75"), unsafe_allow_html=True)
    except Exception:
        pass

    st.sidebar.markdown(
        "<hr style='border-color:rgba(255,255,255,0.12);margin:0.7rem 0'>",
        unsafe_allow_html=True)

    st.sidebar.markdown(
        "<div style='font-size:0.62rem;color:#9BB5D0;text-transform:uppercase;"
        "letter-spacing:0.08em;margin-bottom:5px'>Quick Questions</div>",
        unsafe_allow_html=True)

    # st.expander labels are plain text only — HTML is not supported there.
    # Use clean Unicode prefixes instead.
    SECTION_PREFIX = {
        "Summary":           "  Portfolio Summary",
        "Suppliers":         "  Supplier Lists",
        "Spend":             "  Spend & Contracts",
        "Alternatives":      "  Alternatives & PIP",
        "What-If Scenarios": "  What-If & Help",
    }

    for section, questions in QUICK_QUESTIONS:
        label = SECTION_PREFIX.get(section, section)
        with st.sidebar.expander(label, expanded=False):
            for q in questions:
                if st.button(q, key=f"sq_{q[:30]}"):
                    st.session_state["sidebar_q"] = q
                    st.rerun()

    st.sidebar.markdown(
        "<hr style='border-color:rgba(255,255,255,0.12);margin:0.7rem 0'>",
        unsafe_allow_html=True)
    col1, col2 = st.sidebar.columns(2)
    if col1.button("Clear chat"):
        st.session_state["messages"] = []
        st.rerun()
    if col2.button("Refresh data"):
        load_data.clear()
        st.rerun()
    st.sidebar.markdown(
        "<div style='font-size:0.62rem;color:rgba(255,255,255,0.3);margin-top:5px'>"
        "PostgreSQL · srsid_db</div>",
        unsafe_allow_html=True)



# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    render_sidebar()

    st.markdown("""
<div style='background:#FFFFFF;border-bottom:3px solid #1A6FBF;border-radius:0;
            padding:1rem 1.5rem;margin-bottom:0;display:flex;align-items:center;gap:14px'>
  <svg width="32" height="32" viewBox="0 0 32 32" fill="none">
    <rect width="32" height="32" rx="8" fill="#0F2644"/>
    <path d="M7 23L11 13L16 19L20 9L25 23" stroke="#1A6FBF" stroke-width="2.5"
          stroke-linecap="round" stroke-linejoin="round" fill="none"/>
    <circle cx="16" cy="19" r="2" fill="#1A6FBF"/>
    <circle cx="11" cy="13" r="1.5" fill="#B8C9DB"/>
    <circle cx="20" cy="9" r="1.5" fill="#B8C9DB"/>
  </svg>
  <div>
    <div style='font-size:1.05rem;font-weight:700;color:#0F2644;letter-spacing:-0.01em'>
      SRSID Procurement Assistant</div>
    <div style='font-size:0.78rem;color:#64748B;margin-top:1px'>
      Ask about supplier risk, spend, performance or alternatives</div>
  </div>
  <div style='margin-left:auto;display:flex;gap:8px;align-items:center'>
    <div style='width:8px;height:8px;border-radius:50%;background:#1D9E75'></div>
    <div style='font-size:0.72rem;color:#64748B'>Live data</div>
  </div>
</div>""", unsafe_allow_html=True)

    if "messages" not in st.session_state:
        st.session_state["messages"] = []

    # ── Determine what question to answer this run ────────────────────────────
    # Either from sidebar button (sidebar_q) or from chat input
    pending_q = None

    if "sidebar_q" in st.session_state:
        pending_q = st.session_state.pop("sidebar_q")

    # Render existing history first
    for msg in st.session_state["messages"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Chat input (returns None if not submitted this run)
    user_input = st.chat_input("Ask about your suppliers, spend, risk, or alternatives...")

    if user_input:
        pending_q = user_input

    # ── Process the question (sidebar OR typed) ───────────────────────────────
    if pending_q:
        # Show user message
        st.session_state["messages"].append({"role": "user", "content": pending_q})
        with st.chat_message("user"):
            st.markdown(pending_q)

        # Generate and show assistant response
        with st.chat_message("assistant"):
            # Show different spinner depending on route
            from rag.retriever import route_to_rag, is_available as rag_available
            import rag.llm as _llm
            rag_ready = rag_available() and _llm.is_available()
            will_rag  = route_to_rag(pending_q) and rag_ready

            spinner_msg = "Searching vendor index..." if will_rag else "Querying Postgres..."
            with st.spinner(spinner_msg):
                try:
                    d = load_data()
                    response, intent = route(pending_q, d)
                except Exception as e:
                    response = (f"Error: {e}\n\n"
                                "Check that Postgres is running: `pg_isready`")
                    intent   = "general"

            st.markdown(response)

            # Inline chart where relevant
            try:
                show_chart(intent, load_data(), pending_q)
            except Exception:
                pass

        st.session_state["messages"].append({"role": "assistant", "content": response})


if __name__ == "__main__":
    main()
