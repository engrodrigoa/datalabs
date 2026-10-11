"""Embedding model shared by every RAG step (build, retrieval, generation, evaluation).

One place decides the model, so the vectors written by rag01 and the query vectors of
rag02/03/04 can never come from different models.

Default: multilingual-e5-small (384 dims, ~470 MB). On the lab's CPU it embeds several
times faster than e5-base (768 dims, ~1.1 GB) and downloads in a fraction of the time,
which keeps the portfolio reproducible on a laptop. Switching models is a governed change:
rag01 detects it and rebuilds the vector store, and the evaluation gate (rag04) decides if
the new model is good enough.
"""
from __future__ import annotations

import os

MODEL_NAME = os.getenv("RAG_EMBEDDING_MODEL", "intfloat/multilingual-e5-small")

# e5 models expect these prefixes (asymmetric retrieval: short query x longer passage)
QUERY_PREFIX = "query: "
PASSAGE_PREFIX = "passage: "


def load_model(name: str = MODEL_NAME):
    """Load the sentence-transformers model (heavy import only when a step really needs it)."""
    threads = os.getenv("RAG_TORCH_THREADS")
    if threads:
        import torch

        torch.set_num_threads(int(threads))
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(name)


def model_dim(model) -> int:
    return int(model.get_sentence_embedding_dimension())


def encode_passages(model, texts: list[str], batch_size: int = 64):
    return model.encode([f"{PASSAGE_PREFIX}{t}" for t in texts], normalize_embeddings=True,
                        batch_size=batch_size, show_progress_bar=False)


def encode_query(model, text: str) -> list[float]:
    return model.encode([f"{QUERY_PREFIX}{text}"], normalize_embeddings=True)[0].tolist()
