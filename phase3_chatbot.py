"""
SRSID Procurement Intelligence Chatbot  v3
============================================
Self-contained — NO API KEY required.
Single source of truth: supplier_features.csv (risk_label_3class)
matches exactly what the sidebar shows from feature_summary.json.

Run: streamlit run phase3_chatbot.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import json, re
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SRSID Procurement Assistant",
    page_icon="🤝",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html,body,[class*="css"]{ font-family:'Inter',sans-serif; }
.stApp { background:#f1f5f9; }
.block-container { max-width:900px !important; padding-top:1.5rem !important; }

/* sidebar */
[data-testid="stSidebar"]{ background:#ffffff !important; border-right:1px solid #e2e8f0; }

/* ALL text inside chat messages — guaranteed dark */
[data-testid="stChatMessage"] p,
[data-testid="stChatMessage"] li,
[data-testid="stChatMessage"] span:not([data-testid]),
[data-testid="stChatMessage"] div:not([data-testid]) {
    color:#1e293b !important; font-size:.92rem !important; line-height:1.72 !important;
}
[data-testid="stChatMessage"] strong,
[data-testid="stChatMessage"] b { color:#1d4ed8 !important; font-weight:600 !important; }
[data-testid="stChatMessage"] ul{ padding-left:18px !important; margin:6px 0 !important; }
[data-testid="stChatMessage"] li{ margin:3px 0 !important; }
[data-testid="stChatMessage"] h3{ color:#0f172a !important; font-size:.95rem !important; }

/* user bubble background */
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]){
    background:#1d4ed8 !important;
    border-radius:18px 18px 4px 18px !important;
    margin-left:20% !important;
}
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]) p,
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]) span{
    color:#ffffff !important;
}
/* assistant bubble */
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]){
    background:#ffffff !important;
    border:1px solid #e2e8f0 !important;
    border-radius:4px 18px 18px 18px !important;
    margin-right:20% !important;
    box-shadow:0 1px 4px rgba(0,0,0,.06) !important;
}
/* hide avatars */
[data-testid="chatAvatarIcon-user"],
[data-testid="chatAvatarIcon-assistant"]{ display:none !important; }

/* header */
.chat-header{
    background:#ffffff; border:1px solid #e2e8f0;
    border-left:4px solid #1d4ed8; border-radius:10px;
    padding:16px 24px; margin-bottom:20px;
    box-shadow:0 1px 4px rgba(0,0,0,.05);
}
.chat-header h1{ font-size:1.3rem; font-weight:700; color:#0f172a; margin:0; }
.chat-header p { color:#64748b; font-size:.84rem; margin:4px 0 0 0; }

/* sidebar buttons */
.stButton>button{
    background:#f8fafc !important; border:1px solid #e2e8f0 !important;
    border-radius:8px !important; color:#334155 !important;
    font-size:.82rem !important; font-weight:400 !important;
    padding:8px 12px !important; text-align:left !important;
    transition:all .15s !important; width:100% !important;
}
.stButton>button:hover{
    background:#eff6ff !important; border-color:#93c5fd !important;
    color:#1d4ed8 !important;
}
.clear-btn>button{ background:#fff5f5 !important; border-color:#fca5a5 !important; color:#dc2626 !important; }
.clear-btn>button:hover{ background:#fee2e2 !important; }

/* chat input */
[data-testid="stChatInputContainer"]{
    background:#ffffff !important; border:1px solid #cbd5e1 !important;
    border-radius:12px !important; box-shadow:0 1px 4px rgba(0,0,0,.06) !important;
}
[data-testid="stChatInputContainer"] textarea{
    background:#ffffff !important; color:#0f172a !important;
    font-size:.93rem !important; border:none !important;
}
[data-testid="stChatInputContainer"] button{
    background:#1d4ed8 !important; border-radius:8px !important; color:#fff !important;
}
hr{ border-color:#e2e8f0 !important; }
.footer{ text-align:center; color:#94a3b8; font-size:.72rem;
         margin-top:24px; padding-top:14px; border-top:1px solid #e2e8f0; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING  — single source of truth for risk: supplier_features.csv
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_data() -> dict:
    def csv(p):
        f = Path(p)
        return pd.read_csv(f) if f.exists() else pd.DataFrame()
    def js(p):
        f = Path(p)
        return json.loads(f.read_text(encoding="utf-8")) if f.exists() else {}

    feat    = csv("phase3_features/supplier_features.csv")
    seg     = csv("phase3_segmentation/supplier_segments.csv")
    fc      = csv("phase3_forecasting/disruption_forecast.csv")
    alerts  = csv("phase3_forecasting/early_warning_alerts.csv")
    alts    = csv("phase3_recommendations/alternative_suppliers.csv")
    anom    = csv("phase3_anomalies/anomaly_report.csv")
    expl    = csv("phase3_xai/supplier_explanations.csv")
    gimp    = csv("phase3_xai/global_importance.csv")
    feat_s  = js("phase3_features/feature_summary.json")
    seg_s   = js("phase3_segmentation/segmentation_summary.json")
    fc_s    = js("phase3_forecasting/forecast_summary.json")

    # ── Build the canonical risk table from supplier_features.csv ──────────
    # This is the ONLY risk classification used — it matches feature_summary.json
    risk_label_col = None
    canonical_risk = pd.DataFrame()

    if not feat.empty:
        # Find the risk label column — try multiple possible names
        for candidate in ["risk_label_3class", "risk_label", "historical_risk_category",
                          "historical_risk_raw", "risk_category"]:
            if candidate in feat.columns:
                risk_label_col = candidate
                break

        if risk_label_col:
            nc = next((c for c in feat.columns
                       if "supplier" in c.lower() or "name" in c.lower()), feat.columns[0])
            canonical_risk = feat[[nc, risk_label_col]].copy()
            canonical_risk.columns = ["supplier_name", "risk_tier"]

            # Normalise labels to High / Medium / Low
            label_map = {
                "high": "High", "h": "High",   "3": "High",
                "medium": "Medium", "med": "Medium", "m": "Medium", "2": "Medium",
                "low": "Low",  "l": "Low",   "1": "Low",
            }
            canonical_risk["risk_tier"] = (
                canonical_risk["risk_tier"]
                .astype(str).str.strip().str.lower()
                .map(label_map)
                .fillna("Low")
            )

            # Merge spend and criticality from features for richer output
            for extra_col in ["total_annual_spend", "criticality_index",
                              "composite_risk_score", "financial_stability",
                              "delivery_performance"]:
                if extra_col in feat.columns:
                    canonical_risk[extra_col] = feat[extra_col].values

    # Rebuild feat_sum risk_distribution from canonical_risk if json is empty/wrong
    if canonical_risk.empty is False and "risk_tier" in canonical_risk.columns:
        computed_rd = canonical_risk["risk_tier"].value_counts().to_dict()
        if not feat_s.get("risk_distribution"):
            feat_s["risk_distribution"] = computed_rd
        if not feat_s.get("total_suppliers"):
            feat_s["total_suppliers"] = len(canonical_risk)

    return {
        "features":     feat,
        "risk":         canonical_risk,       # ← canonical, consistent with sidebar
        "segments":     seg,
        "forecast":     fc,
        "alerts":       alerts,
        "alternatives": alts,
        "anomalies":    anom,
        "explanations": expl,
        "global_imp":   gimp,
        "feat_sum":     feat_s,
        "seg_sum":      seg_s,
        "fc_sum":       fc_s,
    }


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def ncol(df: pd.DataFrame) -> str:
    return next((c for c in df.columns
                 if "supplier" in c.lower() or "name" in c.lower()), df.columns[0])


def find_row(df: pd.DataFrame, name: str):
    if df.empty:
        return None
    nc = ncol(df)
    mask = df[nc].astype(str).str.lower().str.contains(
        re.escape(name.lower()), na=False)
    return df[mask].iloc[0] if mask.any() else None


def safe(row, key, default=""):
    if row is None:
        return default
    try:
        val = row[key] if key in row.index else default
        return default if (val is None or (isinstance(val, float) and np.isnan(val))) else val
    except Exception:
        return default


def fmt_money(v) -> str:
    try:
        v = float(v)
        return f"${v/1_000_000:.1f}M" if v >= 1_000_000 else f"${v:,.0f}"
    except Exception:
        return str(v)


def fmt_pct(v) -> str:
    try:
        return f"{float(v)*100:.1f}%"
    except Exception:
        return str(v)


def risk_icon(tier: str) -> str:
    return {"High": "🔴", "Medium": "🟡", "Low": "🟢"}.get(str(tier), "⚪")


def action_tier(tier: str) -> str:
    return {
        "High":   "**Immediate action required** — escalate to your procurement manager, "
                  "start the dual-sourcing process, and place safety stock orders now.",
        "Medium": "**Active monitoring needed** — schedule a supplier review this quarter "
                  "and identify at least one backup supplier.",
        "Low":    "**Routine oversight** — maintain current terms and review at the next "
                  "scheduled performance cycle.",
    }.get(str(tier), "Review with your procurement manager.")


def action_segment(seg: str) -> str:
    return {
        "Strategic":  "Invest in the relationship — long-term contracts, joint planning, "
                      "safety stock, and dual-sourcing to protect this critical dependency.",
        "Bottleneck": "Urgently qualify alternative suppliers. This supplier is hard to "
                      "replace — do not allow sole-source dependency to continue.",
        "Leverage":   "Negotiate aggressively — consolidate spend, run competitive tenders, "
                      "and push for volume discounts. You have the power here.",
        "Tactical":   "Automate and simplify — move to e-catalog procurement, reduce "
                      "purchase orders, and use preferred-supplier frameworks.",
    }.get(str(seg), "Review supplier management strategy with your category manager.")


def get_risk_counts(data: dict) -> dict:
    """Get risk distribution from canonical risk table — same as sidebar."""
    risk_df = data["risk"]
    if risk_df.empty:
        # Fallback to json summary
        rd = data["feat_sum"].get("risk_distribution", {})
        return {
            "High":   rd.get("High",   rd.get("high",   0)),
            "Medium": rd.get("Medium", rd.get("medium", 0)),
            "Low":    rd.get("Low",    rd.get("low",    0)),
        }
    vc = risk_df["risk_tier"].value_counts()
    return {
        "High":   int(vc.get("High",   0)),
        "Medium": int(vc.get("Medium", 0)),
        "Low":    int(vc.get("Low",    0)),
    }


# ─────────────────────────────────────────────────────────────────────────────
# INTENT + SUPPLIER DETECTION
# ─────────────────────────────────────────────────────────────────────────────

INTENTS = {
    "help":         ["help", "what can you", "what do you", "how to use", "what questions"],
    "alerts":       ["alert", "warning", "urgent", "30 day", "next month", "imminent", "at risk"],
    "high_risk":    ["high risk", "most risky", "riskiest", "dangerous", "top risk",
                     "worst", "concern", "worried", "worry", "all high"],
    "medium_risk":  ["medium risk", "medium-risk", "moderate risk", "all medium", "21 medium"],
    "low_risk":     ["low risk", "low-risk", "safe supplier", "all low", "performing well"],
    "forecast":     ["forecast", "predict", "future", "upcoming", "probability", "disruption"],
    "alternatives": ["alternative", "replace", "backup", "substitute", "switch", "option"],
    "segments":     ["segment", "category", "strategic", "tactical", "leverage",
                     "bottleneck", "kraljic", "quadrant", "classify", "group"],
    "spend":        ["spend", "cost", "budget", "money", "payment", "invoice", "purchase"],
    "explain":      ["why", "reason", "explain", "because", "driver", "cause", "factor"],
    "anomaly":      ["anomal", "unusual", "strange", "odd", "outlier", "suspicious", "flag"],
    "overview":     ["overview", "summary", "status", "health", "overall", "how many",
                     "total", "portfolio", "executive", "brief"],
}


def detect_intent(q: str) -> str:
    ql = q.lower()
    scores = {i: sum(1 for kw in kws if kw in ql) for i, kws in INTENTS.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "general"


def detect_suppliers(q: str, data: dict) -> list:
    found = []
    for key in ["risk", "segments", "features"]:
        df = data[key]
        if df.empty:
            continue
        nc = ncol(df)
        for name in df[nc].dropna().unique():
            if len(str(name)) > 3 and str(name).lower() in q.lower():
                found.append(str(name))
        if found:
            break
    return list(dict.fromkeys(found))


# ─────────────────────────────────────────────────────────────────────────────
# RESPONSE BUILDERS — all use canonical risk table
# ─────────────────────────────────────────────────────────────────────────────

def resp_overview(data: dict) -> str:
    counts = get_risk_counts(data)
    high, med, low = counts["High"], counts["Medium"], counts["Low"]
    total = data["feat_sum"].get("total_suppliers", len(data["features"]))
    ss    = data["seg_sum"]
    fcs   = data["fc_sum"]
    ew    = len(data["alerts"]) if not data["alerts"].empty \
            else fcs.get("early_warning_count", 0)
    qd    = ss.get("kraljic_distribution", {})

    gi = data["global_imp"]
    top_driver = gi.iloc[0]["feature_label"] \
        if not gi.empty and "feature_label" in gi.columns else ""

    parts = [
        "**Supply Chain Health Summary**\n",
        f"We are monitoring **{total} suppliers** across your portfolio.\n",
        "**Risk breakdown:**",
        f"- 🔴 High Risk: **{high} suppliers** — immediate attention needed",
        f"- 🟡 Medium Risk: **{med} suppliers** — active monitoring required",
        f"- 🟢 Low Risk: **{low} suppliers** — performing well\n",
    ]

    if ew:
        parts.append(f"⚠️ **{ew} supplier(s)** have elevated disruption risk in the next "
                     f"30 days — ask *'show disruption alerts'* for details.\n")

    if qd:
        parts += [
            "**Strategic classification (Kraljic):**",
            f"- {qd.get('Strategic',0)} Strategic (critical, high spend, hard to replace)",
            f"- {qd.get('Bottleneck',0)} Bottleneck (hard to replace — find alternatives now)",
            f"- {qd.get('Leverage',0)} Leverage (negotiate harder — you have the power)",
            f"- {qd.get('Tactical',0)} Tactical (routine — automate and simplify)\n",
        ]

    if top_driver:
        parts.append(f"**Biggest risk driver across your portfolio:** {top_driver}\n")

    # Top 3 high risk from canonical table
    risk_df = data["risk"]
    if not risk_df.empty and high > 0:
        top3 = risk_df[risk_df["risk_tier"] == "High"]
        if "composite_risk_score" in top3.columns:
            top3 = top3.sort_values("composite_risk_score", ascending=False)
        nc = ncol(top3)
        names = top3[nc].head(3).tolist()
        parts.append(f"**Most urgent to review:** {', '.join(names)}\n")

    if high == 0:
        parts.append("✅ No High Risk suppliers currently. Focus on monitoring the "
                     f"{med} Medium Risk suppliers this quarter.")
    else:
        parts.append("**Recommended action:** Start with High Risk suppliers — "
                     "schedule review meetings this week and verify backup suppliers "
                     "are in place for each flagged name.")
    return "\n".join(parts)


def _risk_list(data: dict, tier: str, label: str) -> str:
    """Generic function to list suppliers by risk tier."""
    risk_df = data["risk"]
    if risk_df.empty:
        return (f"No risk data found. "
                f"Run `python run_phase3.py --step feature_engineering` first.")

    nc = ncol(risk_df)
    filtered = risk_df[risk_df["risk_tier"] == tier].copy()

    if filtered.empty:
        counts = get_risk_counts(data)
        return (f"✅ No suppliers are currently classified as **{tier} Risk** "
                f"in the feature data.\n\n"
                f"Current portfolio: "
                f"🔴 {counts['High']} High | "
                f"🟡 {counts['Medium']} Medium | "
                f"🟢 {counts['Low']} Low")

    # Sort by risk score if available
    if "composite_risk_score" in filtered.columns:
        filtered = filtered.sort_values("composite_risk_score", ascending=(tier == "Low"))
    elif "criticality_index" in filtered.columns:
        filtered = filtered.sort_values("criticality_index", ascending=(tier == "Low"))

    lines = [f"**{len(filtered)} {label} Supplier(s):**\n"]

    for i, (_, r) in enumerate(filtered.iterrows(), 1):
        name   = r[nc]
        parts  = [f"**{name}**"]
        score  = safe(r, "composite_risk_score")
        spend  = safe(r, "total_annual_spend")
        fin    = safe(r, "financial_stability")
        deliv  = safe(r, "delivery_performance")
        if score:
            parts.append(f"Risk score: {float(score):.2f}/1.00")
        if spend:
            parts.append(f"Spend: {fmt_money(spend)}")
        if fin:
            parts.append(f"Fin. stability: {float(fin):.0f}/100")
        if deliv:
            parts.append(f"Delivery: {float(deliv):.0f}/100")
        lines.append(f"{i}. {' | '.join(parts)}")

    lines.append(f"\n{action_tier(tier)}")
    return "\n".join(lines)


def resp_high_risk(data: dict)   -> str:
    return _risk_list(data, "High",   "🔴 High-Risk")

def resp_medium_risk(data: dict) -> str:
    return _risk_list(data, "Medium", "🟡 Medium-Risk")

def resp_low_risk(data: dict)    -> str:
    return _risk_list(data, "Low",    "🟢 Low-Risk")


def resp_alerts(data: dict) -> str:
    alerts = data["alerts"]
    fcs    = data["fc_sum"]

    if alerts.empty:
        count = fcs.get("early_warning_count", 0)
        if count == 0:
            return ("✅ No disruption alerts active. No suppliers have a disruption "
                    "probability above 50% in the next 30 days.")
        return (f"⚠️ **{count} supplier(s)** flagged for elevated disruption risk "
                f"in the next 30 days. Re-run the forecasting step for full details.")

    nc = ncol(alerts)
    lines = [f"⚠️ **{len(alerts)} Early Warning Alert(s)** — "
             f"disruption risk >50% in the next 30 days:\n"]
    for _, r in alerts.head(10).iterrows():
        p30 = safe(r, "disruption_prob_30d")
        p60 = safe(r, "disruption_prob_60d")
        line = f"• **{r[nc]}**"
        if p30:
            line += f" — 30-day: **{fmt_pct(p30)}**"
        if p60:
            line += f" | 60-day: {fmt_pct(p60)}"
        lines.append(line)

    lines += [
        "\n**Immediate actions:**",
        "1. Contact each flagged supplier this week to check their capacity",
        "2. Verify inventory levels for components from these suppliers",
        "3. Activate backup suppliers or place buffer stock orders where possible",
        "4. Escalate to your procurement manager for any Strategic suppliers listed",
    ]
    return "\n".join(lines)


def resp_forecast(data: dict) -> str:
    fc_df = data["forecast"]
    fcs   = data["fc_sum"]
    lines = ["**Disruption Forecast — Next 30 / 60 / 90 Days**\n"]

    preds = fcs.get("portfolio_next_3_months", [])
    if preds:
        lines.append("**Portfolio-level forecast:**")
        for lbl, val in zip(["Next 30 days", "31–60 days", "61–90 days"], preds[:3]):
            lines.append(f"  • {lbl}: ~**{float(val):.0f} disruption events**")
        lines.append("")

    if not fc_df.empty and "disruption_prob_30d" in fc_df.columns:
        nc   = ncol(fc_df)
        top5 = fc_df.sort_values("disruption_prob_30d", ascending=False).head(5)
        lines.append("**Highest-risk suppliers for the next 30 days:**")
        for _, r in top5.iterrows():
            p30 = safe(r, "disruption_prob_30d", 0)
            p90 = safe(r, "disruption_prob_90d", 0)
            if float(p30) > 0.05:
                lines.append(f"• **{r[nc]}** — {fmt_pct(p30)} (30d) | {fmt_pct(p90)} (90d)")
        lines.append("")

    lines += [
        "**What to do:**",
        "- Contact suppliers above 40% probability proactively this week",
        "- Review safety stock for components from these suppliers",
        "- Re-run the forecast monthly to catch emerging patterns early",
    ]
    return "\n".join(lines)


def resp_segments(data: dict) -> str:
    seg_df = data["segments"]
    ss     = data["seg_sum"]
    if seg_df.empty:
        return "Segmentation data not found. Run `python run_phase3.py --step segmentation`."

    nc      = ncol(seg_df)
    qd      = ss.get("kraljic_distribution", {})
    abd     = ss.get("abc_distribution", {})
    seg_col = "kraljic_segment" if "kraljic_segment" in seg_df.columns else None

    lines = ["**Supplier Segmentation Overview**\n"]

    for seg, desc, icon in [
        ("Strategic",  "critical — high spend, very hard to replace",  "🔴"),
        ("Bottleneck", "hard to replace, lower spend — act now",       "🟠"),
        ("Leverage",   "high spend, alternatives exist — negotiate",   "🟡"),
        ("Tactical",   "routine, easy to replace — automate",          "🟢"),
    ]:
        count = qd.get(seg, 0)
        if count == 0 and seg_col:
            count = int((seg_df[seg_col] == seg).sum())
        if count == 0:
            continue
        examples = seg_df[seg_df[seg_col] == seg][nc].head(4).tolist() if seg_col else []
        ex = f" — e.g. {', '.join(examples)}" if examples else ""
        lines.append(f"{icon} **{seg}** ({count} suppliers): *{desc}*{ex}")
        lines.append(f"   → {action_segment(seg)}\n")

    if abd:
        lines += [
            "**Spend Ranking (ABC Analysis):**",
            f"- Class A ({abd.get('A',0)} suppliers): top 70% of spend",
            f"- Class B ({abd.get('B',0)} suppliers): next 20%",
            f"- Class C ({abd.get('C',0)} suppliers): remaining 10%",
        ]
    return "\n".join(lines)


def resp_spend(data: dict) -> str:
    src = data["segments"] if not data["segments"].empty else data["features"]
    if src.empty:
        return "Spend data not found. Check the Segmentation tab in the main dashboard."

    nc = ncol(src)
    spend_col = next((c for c in src.columns
                      if "spend" in c.lower()
                      and not any(x in c.lower() for x in
                                  ["pct", "flag", "concentration", "trend"])), None)
    if not spend_col:
        return ("Spend breakdown is available in the Segmentation tab of the dashboard. "
                "The ABC analysis there shows suppliers ranked by spend value.")

    top      = src.nlargest(10, spend_col)
    total_sp = src[spend_col].sum()
    top10_sp = top[spend_col].sum()
    pct_top  = top10_sp / total_sp * 100 if total_sp else 0

    lines = [
        "**Top 10 Suppliers by Annual Spend**\n",
        f"Top 10 account for **{pct_top:.0f}%** of portfolio spend "
        f"({fmt_money(top10_sp)} of {fmt_money(total_sp)} total).\n",
    ]

    for i, (_, r) in enumerate(top.iterrows(), 1):
        name  = r[nc]
        spend = fmt_money(r[spend_col])
        pct   = r[spend_col] / total_sp * 100 if total_sp else 0

        # Get risk from canonical risk table
        rrow = find_row(data["risk"], name)
        tier_str = ""
        if rrow is not None:
            tier = safe(rrow, "risk_tier", "")
            if tier:
                tier_str = f" {risk_icon(tier)} {tier} Risk"

        srow = find_row(data["segments"], name)
        seg_str = ""
        if srow is not None and "kraljic_segment" in srow.index:
            seg_str = f" | {srow['kraljic_segment']}"

        lines.append(f"{i}. **{name}** — {spend} ({pct:.1f}%){tier_str}{seg_str}")

    lines.append("\n**Action:** Focus contract negotiations on Class A suppliers "
                 "where you have the most spend and therefore the most leverage.")
    return "\n".join(lines)


def resp_anomalies(data: dict) -> str:
    anom = data["anomalies"]
    if anom.empty:
        return ("Anomaly data not found. "
                "Run `python run_phase3.py --step recommendation_anomaly` first.")

    nc      = ncol(anom)
    flagged = anom[anom["is_anomalous"] == 1] \
              if "is_anomalous" in anom.columns else anom.head(10)

    if flagged.empty:
        return ("✅ No unusual patterns detected. All suppliers are behaving within "
                "expected ranges based on historical performance and spend.")

    lines = [f"**{len(flagged)} Supplier(s) Showing Unusual Patterns**\n",
             "These suppliers have been flagged because something doesn't match "
             "what we normally expect:\n"]

    for _, r in flagged.head(8).iterrows():
        reason = safe(r, "rule_based_reason", "")
        score  = safe(r, "anomaly_score", "")
        sc_str = f" (score: {float(score):.2f})" if score else ""
        if reason:
            lines.append(f"• **{r[nc]}**{sc_str}\n  → {str(reason).lower()}")
        else:
            lines.append(f"• **{r[nc]}**{sc_str} — unusual pattern detected")

    lines += ["\n**What to do:**",
              "- Contact each flagged supplier to understand if there is an issue",
              "- Cross-reference with accounts payable for payment patterns",
              "- Flag persistent anomalies for quarterly supplier reviews"]
    return "\n".join(lines)


def resp_single_supplier(name: str, data: dict) -> str:
    sections = []

    # Risk — from canonical table
    r = find_row(data["risk"], name)
    if r is not None:
        tier  = safe(r, "risk_tier", "Unknown")
        score = safe(r, "composite_risk_score", "")
        fin   = safe(r, "financial_stability", "")
        deliv = safe(r, "delivery_performance", "")
        risk_parts = [f"{risk_icon(tier)} **{tier} Risk**"]
        if score:
            risk_parts.append(f"overall risk score: {float(score):.2f}/1.00")
        if fin:
            risk_parts.append(f"financial health: {float(fin):.0f}/100")
        if deliv:
            risk_parts.append(f"on-time delivery: {float(deliv):.0f}/100")
        sections.append("**Risk Assessment:** " + " | ".join(risk_parts))

    # Segment
    s = find_row(data["segments"], name)
    if s is not None:
        parts = []
        for col, lbl in [("kraljic_segment","Category"),
                         ("abc_class","Spend class"),
                         ("risk_spend_quadrant","Position")]:
            val = safe(s, col)
            if val:
                parts.append(f"{lbl}: **{val}**")
        spend = safe(s, "total_annual_spend")
        if spend:
            parts.append(f"Annual spend: **{fmt_money(spend)}**")
        if parts:
            sections.append("**Profile:** " + " | ".join(parts))
        if "strategic_action" in s.index and safe(s, "strategic_action"):
            sections.append(f"**Recommended procurement approach:** {s['strategic_action']}")

    # Forecast
    f = find_row(data["forecast"], name)
    if f is not None:
        p30 = safe(f, "disruption_prob_30d")
        p60 = safe(f, "disruption_prob_60d")
        p90 = safe(f, "disruption_prob_90d")
        if p30:
            sections.append(
                f"**Disruption Forecast:** "
                f"30 days: {fmt_pct(p30)} | "
                f"60 days: {fmt_pct(p60)} | "
                f"90 days: {fmt_pct(p90)}"
            )

    # XAI explanation
    x = find_row(data["explanations"], name)
    if x is not None:
        narrative = safe(x, "narrative", "")
        if narrative:
            clean = str(narrative).replace("SHAP:", "impact:").replace("[SHAP:", "[impact:")
            sections.append(f"**Why this risk level:**\n{clean[:600]}")
        else:
            drivers = []
            for i in range(1, 4):
                lbl = safe(x, f"driver_{i}_label")
                val = safe(x, f"driver_{i}_value")
                if lbl:
                    drivers.append(f"{lbl} ({val})")
            if drivers:
                sections.append(f"**Key risk factors:** {', '.join(drivers)}")

    # Alternatives
    alts_df = data["alternatives"]
    if not alts_df.empty:
        ac = "supplier" if "supplier" in alts_df.columns else ncol(alts_df)
        matches = alts_df[alts_df[ac].astype(str).str.lower().str.contains(
            re.escape(name.lower()), na=False)]
        if not matches.empty and "alternative_supplier" in matches.columns:
            alt_names = matches["alternative_supplier"].head(3).tolist()
            sections.append(f"**Backup suppliers identified:** {', '.join(alt_names)}")

    # Anomaly
    an = find_row(data["anomalies"], name)
    if an is not None and safe(an, "is_anomalous", 0) == 1:
        reason = safe(an, "rule_based_reason", "unusual pattern detected")
        sections.append(f"⚠️ **Anomaly flag:** {reason}")

    if not sections:
        all_names = []
        for key in ["risk", "segments", "features"]:
            df = data[key]
            if not df.empty:
                all_names = df[ncol(df)].dropna().tolist()
                break
        suggestions = [n for n in all_names
                       if any(w in n.lower()
                              for w in name.lower().split() if len(w) > 3)][:4]
        if suggestions:
            return (f"I couldn't find an exact match for **'{name}'**.\n\n"
                    f"Did you mean one of these?\n" +
                    "\n".join(f"• {s}" for s in suggestions))
        return (f"No data found for **'{name}'**. "
                f"Ask *'show all high risk suppliers'* to see available names.")

    # Action
    tier = "Unknown"
    r2 = find_row(data["risk"], name)
    if r2 is not None:
        tier = safe(r2, "risk_tier", "Unknown")
    seg = "Unknown"
    s2 = find_row(data["segments"], name)
    if s2 is not None:
        seg = str(safe(s2, "kraljic_segment", "Unknown"))

    action = action_tier(tier) if tier not in ("Unknown", "") else action_segment(seg)

    return (f"**Supplier Profile: {name.title()}**\n\n"
            + "\n\n".join(sections)
            + f"\n\n**Recommended Action:** {action}")


def resp_alternatives(name: str, data: dict) -> str:
    alts_df = data["alternatives"]
    if alts_df.empty:
        return ("Alternative supplier data not found. "
                "Run `python run_phase3.py --step recommendation_anomaly` first.")

    ac = "supplier" if "supplier" in alts_df.columns else ncol(alts_df)
    matches = alts_df[alts_df[ac].astype(str).str.lower().str.contains(
        re.escape(name.lower()), na=False)]

    if matches.empty:
        return (f"No alternatives found for **'{name}'**. "
                f"This may mean the supplier is currently Low or Medium risk. "
                f"Contact your category manager to explore market options.")

    lines = [f"**Alternative Suppliers for {name.title()}**\n",
             "Our analysis found these suppliers with similar capabilities:\n"]

    for _, r in matches.sort_values("alternative_rank").iterrows():
        alt    = safe(r, "alternative_supplier", "Unknown")
        sim    = safe(r, "similarity_score", "")
        sim_s  = f" — {float(sim)*100:.0f}% capability match" if sim else ""
        reason = safe(r, "recommendation_reason", "")
        rs     = f"\n   *{reason}*" if reason else ""
        lines.append(f"• **{alt}**{sim_s}{rs}")

    lines += [
        "\n**Next steps:**",
        "1. Request quotes from these alternatives this week",
        "2. Verify quality certifications and lead times",
        "3. Run a small trial order before committing to a switch",
        "4. Let your current supplier know you are evaluating alternatives",
    ]
    return "\n".join(lines)


def resp_explain(name: str, data: dict) -> str:
    x = find_row(data["explanations"], name)
    if x is None:
        return resp_single_supplier(name, data)

    # Get tier from canonical risk table
    r = find_row(data["risk"], name)
    tier = safe(r, "risk_tier", safe(x, "predicted_risk_tier", "Unknown")) if r is not None \
           else safe(x, "predicted_risk_tier", "Unknown")

    lines = [f"**Why is {name.title()} classified as {tier} Risk?**\n"]

    narrative = safe(x, "narrative", "")
    if narrative:
        clean = (str(narrative)
                 .replace("SHAP:", "impact:")
                 .replace("[SHAP:", "[impact:")
                 .replace("[impact:", "(impact:"))
        lines.append(clean[:700])
    else:
        lines.append("The three main factors driving this classification:\n")
        for i in range(1, 4):
            lbl  = safe(x, f"driver_{i}_label")
            val  = safe(x, f"driver_{i}_value")
            shap = safe(x, f"driver_{i}_shap", 0)
            if lbl:
                try:
                    direction = "pushing risk **up** ⬆" if float(shap) > 0 \
                                else "keeping risk **down** ⬇"
                except Exception:
                    direction = ""
                lines.append(f"{i}. **{lbl}**: {val} — {direction}")

        mit = safe(x, "mitigator_label")
        mit_val = safe(x, "mitigator_value")
        if mit:
            lines.append(f"\nOne factor working in their favour: "
                         f"**{mit}** is {mit_val}, which helps reduce risk.")

    lines.append(f"\n**What to do:** {action_tier(tier)}")
    return "\n".join(lines)


def resp_help() -> str:
    return """**Here's what you can ask me:**

🔍 **About a specific supplier:**
- *"Tell me about Zentis GmbH"*
- *"What is the risk level of Rich's?"*
- *"Why is DHL flagged as medium risk?"*
- *"Who can replace Kellogg Company if they fail?"*

📊 **About your portfolio:**
- *"Give me a supply chain health overview"*
- *"Show me all high risk suppliers"*
- *"Show me all medium risk suppliers"*
- *"Who are our most critical strategic suppliers?"*

⚠️ **Alerts and forecasting:**
- *"Any disruption alerts I should know about?"*
- *"What does the forecast look like for next month?"*

💰 **Spend:** *"Which suppliers are we spending the most with?"*

🔎 **Anomalies:** *"Are there any suppliers showing unusual behaviour?"*

Just type naturally — no special commands needed!"""


def resp_general(data: dict) -> str:
    return (resp_overview(data)
            + "\n\n---\n*Type **'help'** to see all questions I can answer, "
            "or ask about a specific supplier by name.*")


def _explain_global(data: dict) -> str:
    gi = data["global_imp"]
    if gi.empty or "feature_label" not in gi.columns:
        return resp_general(data)
    lines = ["**Top factors driving supplier risk across your portfolio:**\n"]
    for _, r in gi.head(5).iterrows():
        lbl    = r.get("feature_label", r.get("feature", ""))
        agreed = "✅ confirmed by both analysis methods" \
                 if r.get("methods_agree") == "YES" else ""
        lines.append(f"• **{lbl}** {agreed}")
    lines.append("\nFocus supplier review conversations on these areas "
                 "for the highest impact on managing risk.")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# ROUTER
# ─────────────────────────────────────────────────────────────────────────────

def answer(question: str, data: dict) -> str:
    q         = question.strip()
    intent    = detect_intent(q)
    suppliers = detect_suppliers(q, data)

    if suppliers:
        primary = suppliers[0]
        if intent == "explain":
            return resp_explain(primary, data)
        if intent == "alternatives":
            return resp_alternatives(primary, data)
        return resp_single_supplier(primary, data)

    dispatch = {
        "help":        lambda: resp_help(),
        "overview":    lambda: resp_overview(data),
        "high_risk":   lambda: resp_high_risk(data),
        "medium_risk": lambda: resp_medium_risk(data),
        "low_risk":    lambda: resp_low_risk(data),
        "alerts":      lambda: resp_alerts(data),
        "forecast":    lambda: resp_forecast(data),
        "segments":    lambda: resp_segments(data),
        "spend":       lambda: resp_spend(data),
        "anomaly":     lambda: resp_anomalies(data),
        "explain":     lambda: _explain_global(data),
    }

    fn = dispatch.get(intent)
    return fn() if fn else resp_general(data)


# ─────────────────────────────────────────────────────────────────────────────
# SUGGESTIONS
# ─────────────────────────────────────────────────────────────────────────────

SUGGESTIONS = [
    "Which suppliers should I worry about right now?",
    "Give me a supply chain health summary",
    "Any disruption alerts in the next 30 days?",
    "Who are our most strategic suppliers?",
    "Which suppliers are we spending the most with?",
    "Are there any unusual patterns to investigate?",
    "What drives the most risk in our portfolio?",
    "Show me all medium risk suppliers",
]


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

def render_sidebar(data: dict):
    st.sidebar.markdown("""
    <div style='padding:20px 4px 12px 4px;text-align:center'>
        <div style='font-size:1.5rem'>🤝</div>
        <div style='font-size:1rem;font-weight:700;color:#0f172a;margin-top:6px'>
            SRSID Assistant</div>
        <div style='font-size:.75rem;color:#94a3b8'>Procurement Intelligence</div>
    </div>""", unsafe_allow_html=True)

    st.sidebar.divider()
    st.sidebar.markdown(
        "<div style='font-size:.72rem;font-weight:600;color:#64748b;"
        "text-transform:uppercase;letter-spacing:.07em;margin-bottom:6px'>"
        "📊 Portfolio Snapshot — click to explore</div>",
        unsafe_allow_html=True)

    counts = get_risk_counts(data)
    high, med, low = counts["High"], counts["Medium"], counts["Low"]
    total = data["feat_sum"].get("total_suppliers", "—")
    ew    = len(data["alerts"]) if not data["alerts"].empty \
            else data["fc_sum"].get("early_warning_count", 0)

    with st.sidebar:
        if st.button(f"🏭  {total} Total Suppliers",    key="sb_total",  use_container_width=True):
            st.session_state.sidebar_q = "Give me a full supply chain health overview"
        if st.button(f"🔴  {high} High Risk Suppliers",  key="sb_high",   use_container_width=True):
            st.session_state.sidebar_q = "Show me all high risk suppliers"
        if st.button(f"🟡  {med} Medium Risk Suppliers", key="sb_med",    use_container_width=True):
            st.session_state.sidebar_q = "Show me all medium risk suppliers"
        if st.button(f"🟢  {low} Low Risk Suppliers",    key="sb_low",    use_container_width=True):
            st.session_state.sidebar_q = "Show me all low risk suppliers"
        if st.button(f"⚠️  {ew} Disruption Alerts",     key="sb_alerts", use_container_width=True):
            st.session_state.sidebar_q = "Show me all disruption alerts"

    st.sidebar.divider()
    st.sidebar.markdown(
        "<div style='font-size:.78rem;color:#64748b;line-height:1.5'>"
        "💡 <b>Tip:</b> Click a number above, or type any supplier name or question below.</div>",
        unsafe_allow_html=True)
    st.sidebar.divider()

    with st.sidebar:
        st.markdown("<div class='clear-btn'>", unsafe_allow_html=True)
        if st.button("🗑️  Clear conversation", key="sb_clear", use_container_width=True):
            st.session_state.messages  = []
            st.session_state.sidebar_q = ""
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    if "messages"  not in st.session_state:
        st.session_state.messages  = []
    if "sidebar_q" not in st.session_state:
        st.session_state.sidebar_q = ""

    with st.spinner("Loading supplier data..."):
        data = load_data()

    render_sidebar(data)

    st.markdown("""
    <div class='chat-header'>
        <h1>🤝 Procurement Intelligence Assistant</h1>
        <p>Ask me anything about your suppliers — risk levels, spend, disruption forecasts,
           alternatives — in plain English. No technical knowledge needed. No login required.</p>
    </div>""", unsafe_allow_html=True)

    # Process sidebar button click
    if st.session_state.sidebar_q:
        q = st.session_state.sidebar_q
        st.session_state.sidebar_q = ""
        st.session_state.messages.append({"role": "user",      "content": q})
        st.session_state.messages.append({"role": "assistant", "content": answer(q, data)})
        st.rerun()

    # Suggestion chips on empty chat
    if not st.session_state.messages:
        st.markdown(
            "<div style='color:#64748b;font-size:.82rem;margin-bottom:10px'>"
            "💡 <b>Quick questions — click any to get started:</b></div>",
            unsafe_allow_html=True)
        cols = st.columns(2)
        for i, q in enumerate(SUGGESTIONS):
            with cols[i % 2]:
                if st.button(q, key=f"sugg_{i}", use_container_width=True):
                    st.session_state.messages.append({"role": "user",      "content": q})
                    st.session_state.messages.append({"role": "assistant", "content": answer(q, data)})
                    st.rerun()
        st.markdown("<br>", unsafe_allow_html=True)

    # Render conversation
    for msg in st.session_state.messages:
        if msg["role"] == "user":
            with st.chat_message("user"):
                st.markdown(
                    f"<span style='color:#ffffff;font-size:.93rem'>{msg['content']}</span>",
                    unsafe_allow_html=True)
        else:
            with st.chat_message("assistant", avatar="🤝"):
                st.markdown(
                    f"<div style='color:#1e293b;font-size:.92rem;line-height:1.72'>"
                    f"{msg['content']}</div>",
                    unsafe_allow_html=True)

    # Chat input
    user_input = st.chat_input("Ask about any supplier, risk, spend, forecast, or disruption…")

    if user_input and user_input.strip():
        q = user_input.strip()
        st.session_state.messages.append({"role": "user", "content": q})
        with st.spinner("Looking up supplier data…"):
            resp = answer(q, data)
        st.session_state.messages.append({"role": "assistant", "content": resp})
        st.rerun()

    if st.session_state.messages:
        st.markdown(
            "<div class='footer'>SRSID Procurement Assistant · "
            "Answers drawn from Phase 3 ML pipeline · No API key · No external services · "
            "Verify critical decisions with your procurement manager</div>",
            unsafe_allow_html=True)


if __name__ == "__main__":
    main()
