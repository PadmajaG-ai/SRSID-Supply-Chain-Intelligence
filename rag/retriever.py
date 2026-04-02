"""
SRSID  —  rag/retriever.py
============================
Core RAG retrieval layer.

retrieve(question, n=5)  →  list of vendor text chunks
build_prompt(question, chunks)  →  prompt string for Flan-T5

Also holds the intent router:
  route_to_rag(question)  →  True if question needs vector search
                              False if it needs a Postgres aggregation
"""

import re
from pathlib import Path

CHROMA_PATH     = Path(__file__).parent / "chroma_store"
COLLECTION_NAME = "srsid_vendors"

# ── Lazy-loaded globals (loaded once on first call) ───────────────────────────
_collection  = None
_emb_fn      = None


def _load_collection():
    global _collection, _emb_fn
    if _collection is not None:
        return _collection

    try:
        import chromadb
        from chromadb.utils import embedding_functions
    except ImportError:
        raise RuntimeError(
            "chromadb not installed. Run: pip install chromadb sentence-transformers"
        )

    if not CHROMA_PATH.exists():
        raise RuntimeError(
            f"Vector index not found at {CHROMA_PATH}. "
            "Run: python rag/build_index.py"
        )

    client   = chromadb.PersistentClient(path=str(CHROMA_PATH))
    _emb_fn  = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="all-MiniLM-L6-v2"
    )
    _collection = client.get_collection(COLLECTION_NAME, embedding_function=_emb_fn)
    return _collection


def retrieve(question: str, n: int = 5,
             risk_filter: str = None) -> list[dict]:
    """
    Return top-n vendor chunks most relevant to the question.
    Optional risk_filter: 'High', 'Medium', 'Low'
    """
    collection = _load_collection()

    where = {"risk_label": risk_filter} if risk_filter else None

    results = collection.query(
        query_texts=[question],
        n_results=min(n, collection.count()),
        where=where,
        include=["documents", "metadatas", "distances"]
    )

    chunks = []
    for doc, meta, dist in zip(
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0]
    ):
        chunks.append({
            "text":       doc,
            "metadata":   meta,
            "similarity": round(1 - dist, 3),  # cosine: 1=identical, 0=unrelated
        })
    return chunks


def build_prompt(question: str, chunks: list[dict],
                 max_tokens: int = 400) -> str:
    """
    Build a prompt for Flan-T5 that fits within its token limit.
    Flan-T5-base: 512 tokens total input.
    Reserve ~100 for instructions + question, ~400 for vendor context.
    """
    context_parts = []
    token_count   = 0
    tokens_per_char = 0.25   # rough estimate: 1 token ≈ 4 chars

    for chunk in chunks:
        text        = chunk["text"]
        chunk_tokens = len(text) * tokens_per_char
        if token_count + chunk_tokens > max_tokens:
            break
        context_parts.append(text)
        token_count += chunk_tokens

    context = "\n\n".join(context_parts)

    prompt = f"""You are a procurement analyst assistant for a supplier risk dashboard.
Answer the question below using only the vendor data provided.
Be concise. If the data does not contain the answer, say "Data not available".

Vendor data:
{context}

Question: {question}
Answer:"""

    return prompt


# ── Intent router ─────────────────────────────────────────────────────────────
# Questions that need PORTFOLIO aggregations → Postgres (not RAG)
# Questions about SPECIFIC vendors or comparisons → RAG

POSTGRES_INTENTS = [
    # Risk lists — must be asking for a LIST, not about a specific vendor
    r"(list|show|all|which|get).*(high|medium|low).*(risk|supplier|vendor)",
    r"(high|medium|low).*(risk).*(supplier|vendor|list)",
    r"all supplier",
    r"all vendor",
    r"list.*supplier",
    r"list.*vendor",
    r"show.*supplier",
    r"show.*vendor",
    # Portfolio aggregations
    r"how many.*supplier",
    r"how many.*vendor",
    r"total.*spend",
    r"portfolio.*spend",
    r"spend.*portfolio",
    r"risk distribution",
    r"how many.*high risk",
    r"how many.*low risk",
    r"how many.*medium",
    r"overall.*risk",
    r"supply chain.*health",
    r"summary",
    r"overview",
    r"briefing",
    r"maverick.*total",
    r"total.*maverick",
    r"maverick spend",
    r"hhi",
    r"concentration.*index",
    r"quarterly.*spend",
    r"spend.*quarter",
    r"this quarter",
    r"last quarter",
    r"predict.*spend",
    r"which contract.*expir",
    r"contracts.*expir",
    r"how.*risk.*score.*calculat",
    r"how.*score.*calculat",
    r"export",
    r"data.*source",
    r"news.*updat",
    r"help",
    r"what can you",
    # Operational
    r"failing.*otif",
    r"otif.*failing",
    r"delivery.*delay",
    r"disruption.*alert",
    r"alert",
    r"warning",
    r"esg.*status",
    r"backup.*supplier",
    r"top.*backup",
    r"spend.*risk",
    r"risk.*spend",
    r"pip",
    r"performance improvement",
    r"concentration risk",
    r"spend.*concentration",
]

RAG_SIGNALS = [
    r"\b(for|about|of)\s+[A-Z][a-z]",   # "for Siemens", "about Boeing"
    r"compare\s+\w+\s+(and|vs|versus)",
    r"why is .* (high|medium|low|risk)",
    r"risk.*level.*for\s+[A-Z]",
    r"delivery.*for\s+[A-Z]",
    r"news.*for\s+[A-Z]",
    r"alternative.*to\s+[A-Z]",
    r"replace\s+[A-Z]",
    r"who.*safest.*to\s+[A-Z]",
    r"single.source",
    r"backup.*for\s+[A-Z]",
    r"what.*risk.*[A-Z][a-z]{2,}",   # "what is the risk for Siemens"
    r"explain.*[A-Z][a-z]{2,}.*risk",
]

# Common sentence-starting words that are NOT vendor names
QUESTION_WORDS = {
    "list", "show", "what", "which", "who", "where", "when", "how",
    "why", "give", "tell", "find", "get", "display", "compare",
    "are", "is", "do", "does", "can", "will", "should", "has",
    "have", "predict", "explain", "summarize", "describe",
}


def route_to_rag(question: str) -> bool:
    """
    Returns True  → use RAG (vendor-specific question)
    Returns False → use Postgres aggregation (portfolio-level question)
    """
    ql = question.lower()

    # Strong Postgres signal — check FIRST (most specific patterns)
    for pattern in POSTGRES_INTENTS:
        if re.search(pattern, ql):
            return False

    # Strong RAG signal — mentions a specific named entity
    for pattern in RAG_SIGNALS:
        if re.search(pattern, question, re.IGNORECASE):
            return True

    # Fallback: check for vendor-name proper nouns
    # Exclude: first word (sentence start), known question words, ALL-CAPS acronyms
    words = question.split()
    for i, w in enumerate(words):
        clean = w.strip("?.,!\"'")
        if (len(clean) > 3
                and clean[0].isupper()
                and not clean.isupper()         # skip OTIF, HHI, ESG etc
                and clean.lower() not in QUESTION_WORDS
                and i > 0):                     # skip first word of sentence
            return True   # likely a vendor name

    # Default → Postgres (safe: rule-based system handles most questions)
    return False
