from __future__ import annotations

from sentence_transformers import SentenceTransformer

from pipelines.examples.pdf_rag_lab.backend.services.config import DEFAULT_EMBEDDING_MODEL


_model = None


def get_embedding_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer(DEFAULT_EMBEDDING_MODEL)
    return _model


def embed_texts(texts: list[str]) -> list[list[float]]:
    model = get_embedding_model()
    embeddings = model.encode(texts, normalize_embeddings=True)
    return [embedding.tolist() for embedding in embeddings]


def embed_query(text: str) -> list[float]:
    return embed_texts([text])[0]
