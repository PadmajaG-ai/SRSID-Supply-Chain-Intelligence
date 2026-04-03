"""
SRSID  —  rag/retriever.py
============================
RAG retrieval using pgvector in Supabase.

retrieve(question, n=5)  → list of vendor chunks from pgvector similarity search
build_prompt(question, chunks)  → prompt string for Flan-T5
route_to_rag(question)  → True=RAG, False=Postgres aggregation
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

_encoder = None


def _get_encoder():
    global _encoder
    if _encoder is None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise RuntimeError("pip install sentence-transformers")
        _encoder = SentenceTransformer("all-MiniLM-L6-v2")
    return _encoder


def is_available() -> bool:
    """Check pgvector is set up — vendors table has embedding column with data."""
    try:
        from db.db_client import DBClient
        with DBClient() as db:
            count = db.scalar(
                "SELECT COUNT(*) FROM vendors WHERE embedding IS NOT NULL"
            )
            return (count or 0) > 0
    except Exception:
        return False


def retrieve(question: str, n: int = 5,
             risk_filter: str = None) -> list[dict]:
    """
    Find top-n vendors most semantically similar to the question.
    Uses pgvector cosine similarity on the embedding column in Supabase.
    Works on Streamlit Cloud — no local files needed.
    """
    from db.db_client import DBClient

    # Embed the question
    encoder = _get_encoder()
    q_vec   = encoder.encode([question], show_progress_bar=False)[0].tolist()
    vec_str = "[" + ",".join(f"{v:.6f}" for v in q_vec) + "]"

    # Build optional risk filter
    risk_clause = ""
    params      = [vec_str, n]
    if risk_filter:
        risk_clause = "AND risk_label = %s"
        params.insert(1, risk_filter)

    sql = f"""
        SELECT vendor_id, supplier_name, risk_label,
               country_code, industry_category,
               total_annual_spend, composite_risk_score,
               vendor_text,
               1 - (embedding <=> %s::vector) AS similarity
        FROM vendors
        WHERE embedding IS NOT NULL
          {risk_clause}
        ORDER BY embedding <=> %s::vector
        LIMIT %s
    """
    # Adjust params for the ORDER BY clause (needs vec_str again)
    if risk_filter:
        params = [vec_str, risk_filter, vec_str, n]
    else:
        params = [vec_str, vec_str, n]

    try:
        with DBClient() as db:
            rows = db.fetch_df(sql, tuple(params))
    except Exception as e:
        raise RuntimeError(f"pgvector query failed: {e}")

    chunks = []
    for _, row in rows.iterrows():
        chunks.append({
            "text":       row.get("vendor_text", ""),
            "metadata": {
                "vendor_id":  str(row["vendor_id"]),
                "name":       str(row["supplier_name"]),
                "risk_label": str(row.get("risk_label", "Unknown")),
                "country":    str(row.get("country_code", "")),
                "industry":   str(row.get("industry_category", "")),
                "risk_score": float(row.get("composite_risk_score") or 0),
                "spend":      float(row.get("total_annual_spend") or 0),
            },
            "similarity": float(row.get("similarity") or 0),
        })
    return chunks


def build_prompt(question: str, chunks: list[dict],
                 max_tokens: int = 400) -> str:
    """Build a Flan-T5 prompt from retrieved vendor chunks."""
    context_parts = []
    token_count   = 0

    for chunk in chunks:
        text         = chunk["text"]
        chunk_tokens = len(text) * 0.25   # ~4 chars per token
        if token_count + chunk_tokens > max_tokens:
            break
        context_parts.append(text)
        token_count += chunk_tokens

    context = "\n\n".join(context_parts)
    return f"""You are a procurement analyst assistant for a supplier risk dashboard.
Answer the question below using only the vendor data provided.
Be concise. If the data does not contain the answer, say "Data not available".

Vendor data:
{context}

Question: {question}
Answer:"""


# ── Intent router ─────────────────────────────────────────────────────────────

POSTGRES_INTENTS = [
    r"(list|show|all|which|get).*(high|medium|low).*(risk|supplier|vendor)",
    r"(high|medium|low).*(risk).*(supplier|vendor|list)",
    r"all supplier", r"all vendor",
    r"list.*supplier", r"list.*vendor",
    r"show.*supplier", r"show.*vendor",
    r"how many.*supplier", r"how many.*vendor",
    r"total.*spend", r"portfolio.*spend", r"spend.*portfolio",
    r"risk distribution", r"overall.*risk",
    r"supply chain.*health", r"summary", r"overview", r"briefing",
    r"maverick.*total", r"total.*maverick", r"maverick spend",
    r"hhi", r"concentration.*index",
    r"quarterly.*spend", r"spend.*quarter",
    r"this quarter", r"last quarter", r"predict.*spend",
    r"which contract.*expir", r"contracts.*expir",
    r"how.*risk.*score.*calculat", r"how.*score.*calculat",
    r"export", r"data.*source", r"news.*updat", r"help", r"what can you",
    r"failing.*otif", r"otif.*failing",
    r"disruption.*alert", r"alert", r"esg.*status",
    r"backup.*supplier", r"top.*backup",
    r"spend.*risk", r"risk.*spend",
    r"pip", r"performance improvement",
    r"concentration risk", r"spend.*concentration",
]

RAG_SIGNALS = [
    r"\b(for|about|of)\s+[A-Z][a-z]",
    r"compare\s+\w+\s+(and|vs|versus)",
    r"why is .* (high|medium|low|risk)",
    r"risk.*level.*for\s+[A-Z]",
    r"delivery.*for\s+[A-Z]",
    r"news.*for\s+[A-Z]",
    r"alternative.*to\s+[A-Z]",
    r"replace\s+[A-Z]",
    r"what.*risk.*[A-Z][a-z]{2,}",
    r"explain.*[A-Z][a-z]{2,}.*risk",
]

QUESTION_WORDS = {
    "list","show","what","which","who","where","when","how","why",
    "give","tell","find","get","display","compare","are","is","do",
    "does","can","will","should","has","have","predict","explain",
    "summarize","describe",
}


def route_to_rag(question: str) -> bool:
    """True = use RAG, False = use Postgres aggregation."""
    ql = question.lower()

    # Postgres patterns checked first
    for pattern in POSTGRES_INTENTS:
        if re.search(pattern, ql):
            return False

    # Strong RAG signal
    for pattern in RAG_SIGNALS:
        if re.search(pattern, question, re.IGNORECASE):
            return True

    # Fallback: proper noun check (skip first word + question words)
    for i, w in enumerate(question.split()):
        clean = w.strip("?.,!\"'")
        if (len(clean) > 3
                and clean[0].isupper()
                and not clean.isupper()
                and clean.lower() not in QUESTION_WORDS
                and i > 0):
            return True

    return False
