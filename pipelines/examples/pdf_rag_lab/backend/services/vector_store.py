from __future__ import annotations

import json

import faiss
import numpy as np

from pipelines.examples.pdf_rag_lab.backend.services.config import (
    DEFAULT_RETRIEVAL_CANDIDATES,
    VECTORSTORE_DIR,
)
from pipelines.examples.pdf_rag_lab.backend.services.text_normalization import normalize_text


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


def _get_canonical_title(item: dict) -> str:
    return item.get("catalog_display_name_normalized") or item.get("document_title_normalized") or ""


def _compute_lexical_score(
    normalized_question: str,
    chunk_text_normalized: str,
    canonical_title: str,
    document_title_filter: str | None,
) -> float:
    score = 0.0
    question_tokens = [token for token in normalized_question.split() if len(token) >= 3]

    for token in question_tokens:
        if token in chunk_text_normalized:
            score += 1.0

    if document_title_filter and canonical_title == document_title_filter:
        score += 6.0

    return score


def _compute_introductory_boost(
    normalized_question: str,
    chunk_text_normalized: str,
    page_number: int | None,
) -> float:
    boost = 0.0
    page_number = page_number or 999999

    intro_question = (
        "introdu" in normalized_question
        or "tema principal" in normalized_question
        or "resuma" in normalized_question
        or "primeiras paginas" in normalized_question
        or "primeiras páginas" in normalized_question
        or "conteudo introdutorio" in normalized_question
        or "conteúdo introdutório" in normalized_question
    )

    if intro_question:
        if page_number <= 15:
            boost += 0.50
        elif page_number <= 25:
            boost += 0.30
        elif page_number <= 40:
            boost += 0.15

    intro_terms = [
        "introducao",
        "introdução",
        "prefacio",
        "prefácio",
        "apresentacao",
        "apresentação",
        "prologo",
        "prólogo",
    ]

    if any(term in chunk_text_normalized for term in intro_terms):
        boost += 0.15

    return boost


def _rerank_results(
    question: str,
    results: list[dict],
    top_k: int,
    document_title_filter: str | None,
    mode: str,
) -> list[dict]:
    normalized_question = normalize_text(question)
    reranked = []

    for item in results:
        chunk_text_normalized = item.get("chunk_text_normalized") or normalize_text(item.get("chunk_text", ""))
        canonical_title = _get_canonical_title(item)
        vector_score = item.get("score", 0.0)
        lexical_score = _compute_lexical_score(
            normalized_question=normalized_question,
            chunk_text_normalized=chunk_text_normalized,
            canonical_title=canonical_title,
            document_title_filter=document_title_filter,
        )
        intro_boost = _compute_introductory_boost(
            normalized_question=normalized_question,
            chunk_text_normalized=chunk_text_normalized,
            page_number=item.get("page_number"),
        )

        final_score = float(vector_score) + 0.05 * lexical_score + intro_boost

        enriched = item.copy()
        enriched["score"] = final_score
        reranked.append(enriched)

    reranked.sort(
        key=lambda item: (
            -item.get("score", 0.0),
            item.get("page_number") or 999999,
            -len(item.get("chunk_text", "")),
        )
    )

    deduped = []
    seen_hashes: set[str] = set()
    for item in reranked:
        chunk_hash = item.get("chunk_hash") or item.get("chunk_id")
        if chunk_hash in seen_hashes:
            continue
        seen_hashes.add(chunk_hash)
        deduped.append(item)

    if mode != "compare_documents":
        return deduped[:top_k]

    diversified = []
    per_document_counts: dict[str, int] = {}
    for item in deduped:
        document_name = item.get("catalog_display_name") or item.get("document_name", "unknown")
        count = per_document_counts.get(document_name, 0)
        if count >= 2:
            continue
        diversified.append(item)
        per_document_counts[document_name] = count + 1
        if len(diversified) >= top_k:
            break

    return diversified[:top_k] if diversified else deduped[:top_k]


def _search_within_items(
    question: str,
    query_embedding: list[float],
    items: list[dict],
    top_k: int,
    candidate_k: int,
    document_title_filter: str | None,
    mode: str,
) -> list[dict]:
    if not items:
        return []

    matrix = np.array([item["_vector"] for item in items], dtype="float32")
    dimension = matrix.shape[1]
    local_index = faiss.IndexFlatIP(dimension)
    local_index.add(matrix)

    query = np.array([query_embedding], dtype="float32")
    local_scores, local_indices = local_index.search(query, min(len(items), max(top_k, candidate_k)))

    results = []
    for score, local_idx in zip(local_scores[0], local_indices[0]):
        if local_idx == -1:
            continue
        item = items[local_idx].copy()
        item.pop("_vector", None)
        item["score"] = float(score)
        results.append(item)

    return _rerank_results(
        question=question,
        results=results,
        top_k=top_k,
        document_title_filter=document_title_filter,
        mode=mode,
    )


def search_similar_chunks(
    question: str,
    query_embedding: list[float],
    top_k: int = 4,
    candidate_k: int = DEFAULT_RETRIEVAL_CANDIDATES,
    document_title_filter: str | None = None,
    mode: str = "broad_summary",
) -> list[dict]:
    index, metadata = load_index()

    if not document_title_filter:
        query = np.array([query_embedding], dtype="float32")
        scores, indices = index.search(query, max(top_k, candidate_k))
        results = []

        for score, idx in zip(scores[0], indices[0]):
            if idx == -1:
                continue
            item = metadata[idx].copy()
            item["score"] = float(score)
            results.append(item)

        return _rerank_results(
            question=question,
            results=results,
            top_k=top_k,
            document_title_filter=document_title_filter,
            mode=mode,
        )

    filtered_items = []
    for idx, item in enumerate(metadata):
        canonical_title = _get_canonical_title(item)
        if canonical_title != document_title_filter:
            continue
        enriched = item.copy()
        enriched["_vector"] = index.reconstruct(idx)
        filtered_items.append(enriched)

    return _search_within_items(
        question=question,
        query_embedding=query_embedding,
        items=filtered_items,
        top_k=top_k,
        candidate_k=candidate_k,
        document_title_filter=document_title_filter,
        mode=mode,
    )


def search_intro_chunks(
    question: str,
    query_embedding: list[float],
    document_title_filter: str,
    top_k: int = 4,
    candidate_k: int = DEFAULT_RETRIEVAL_CANDIDATES,
    max_intro_page: int = 25,
) -> list[dict]:
    index, metadata = load_index()

    intro_items = []
    for idx, item in enumerate(metadata):
        canonical_title = _get_canonical_title(item)
        page_number = item.get("page_number") or 999999

        if canonical_title != document_title_filter:
            continue
        if page_number > max_intro_page:
            continue

        enriched = item.copy()
        enriched["_vector"] = index.reconstruct(idx)
        intro_items.append(enriched)

    return _search_within_items(
        question=question,
        query_embedding=query_embedding,
        items=intro_items,
        top_k=top_k,
        candidate_k=candidate_k,
        document_title_filter=document_title_filter,
        mode="single_document",
    )
