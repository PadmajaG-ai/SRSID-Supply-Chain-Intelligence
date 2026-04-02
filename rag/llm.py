"""
SRSID  —  rag/llm.py
======================
Flan-T5 model wrapper.

Lazy-loads the model on first call (slow, ~10–30s).
After that, inference is ~2–5s per question on CPU.

Supports:
    flan-t5-base  — 250M params, ~1GB RAM, fastest
    flan-t5-large — 780M params, ~3GB RAM, better quality
    flan-t5-xl    — 3B params,   ~8GB RAM, best quality

Set MODEL_SIZE in config.py or override at call time.
"""

import logging
from pathlib import Path

log = logging.getLogger(__name__)

# ── Lazy globals ──────────────────────────────────────────────────────────────
_model     = None
_tokenizer = None
_model_name = None


def load_model(model_size: str = "base"):
    """Load Flan-T5 model. Called once, result cached globally."""
    global _model, _tokenizer, _model_name

    model_id = f"google/flan-t5-{model_size}"

    if _model is not None and _model_name == model_id:
        return _model, _tokenizer

    try:
        from transformers import T5ForConditionalGeneration, T5Tokenizer
    except ImportError:
        raise RuntimeError(
            "transformers not installed. Run: pip install transformers torch"
        )

    log.info(f"Loading {model_id} (first load takes 10–30s)...")
    _tokenizer  = T5Tokenizer.from_pretrained(model_id)
    _model      = T5ForConditionalGeneration.from_pretrained(model_id)
    _model_name = model_id
    _model.eval()   # inference mode
    log.info(f"Model loaded: {model_id}")

    return _model, _tokenizer


def generate(prompt: str,
             model_size: str   = "base",
             max_new_tokens: int = 200,
             num_beams: int    = 4,
             temperature: float = 1.0) -> str:
    """
    Generate a response from Flan-T5 given a prompt string.

    Args:
        prompt:         The assembled RAG prompt (instruction + context + question)
        model_size:     'base', 'large', or 'xl'
        max_new_tokens: Max output length (200 = ~2–3 sentences, enough for chatbot)
        num_beams:      Beam search width (4 = good quality/speed balance)
        temperature:    >1 = more creative, <1 = more focused (1.0 = default)

    Returns:
        Generated text string
    """
    import torch

    model, tokenizer = load_model(model_size)

    inputs = tokenizer(
        prompt,
        return_tensors="pt",
        max_length=512,
        truncation=True,
        padding=False,
    )

    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            num_beams=num_beams,
            early_stopping=True,
            no_repeat_ngram_size=3,    # reduce repetition
            length_penalty=1.0,
        )

    response = tokenizer.decode(outputs[0], skip_special_tokens=True).strip()

    # Flan-T5 sometimes echoes the prompt — strip if so
    if response.startswith("Answer:"):
        response = response[7:].strip()

    return response if response else "I was unable to generate an answer from the available data."


def is_available() -> bool:
    """Check if transformers and torch are importable."""
    try:
        import transformers, torch
        return True
    except ImportError:
        return False
