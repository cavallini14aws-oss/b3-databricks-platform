from __future__ import annotations

import json

import faiss
import numpy as np

from pipelines.examples.pdf_rag_lab.backend.services.config import (
    DEFAULT_RETRIEVAL_CANDIDATES,
    VECTORSTORE_DIR,
)


INDEX_PATH = VECTORSTORE_DIR / "pdf_rag.index"
METADATA_PATH = VECTORSTORE_DIR / "pdf_rag_metadata.json"


def save_index(chunks: list[dict], embeddings: list[list[float]]) -> None:
    if not embeddings:
        raise ValueError("Nenhum embedding recebido para indexação.")

    matrix = np.array(embeddings, dtype="float32")
    dimension = matrix.shape[1]

    index = faiss.IndexFlatIP(dimension)
    index.add(matrix)

    faiss.write_index(index, str(INDEX_PATH))
    METADATA_PATH.write_text(json.dumps(chunks, ensure_ascii=False, indent=2), encoding="utf-8")


def load_index() -> tuple[faiss.IndexFlatIP, list[dict]]:
    if not INDEX_PATH.exists():
        raise FileNotFoundError("Índice vetorial não encontrado. Rode setup_docs.py primeiro.")

    if not METADATA_PATH.exists():
        raise FileNotFoundError("Metadados do índice não encontrados.")

    index = faiss.read_index(str(INDEX_PATH))
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    return index, metadata


def _rerank_results(results: list[dict], top_k: int) -> list[dict]:
    def sort_key(item: dict):
        page = item.get("page_number") or 999999
        text_len = len(item.get("chunk_text", ""))
        score = item.get("score", 0.0)
        return (-score, page, -text_len)

    ranked = sorted(results, key=sort_key)
    return ranked[:top_k]


def search_similar_chunks(
    query_embedding: list[float],
    top_k: int = 4,
    candidate_k: int = DEFAULT_RETRIEVAL_CANDIDATES,
) -> list[dict]:
    index, metadata = load_index()

    query = np.array([query_embedding], dtype="float32")
    scores, indices = index.search(query, max(top_k, candidate_k))

    results = []
    for score, idx in zip(scores[0], indices[0]):
        if idx == -1:
            continue
        item = metadata[idx].copy()
        item["score"] = float(score)
        results.append(item)

    return _rerank_results(results, top_k)
