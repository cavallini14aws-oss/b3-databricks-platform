from __future__ import annotations

from dataclasses import dataclass

from pipelines.examples.pdf_rag_lab.backend.services.document_catalog import load_document_catalog
from pipelines.examples.pdf_rag_lab.backend.services.embedding_service import embed_query
from pipelines.examples.pdf_rag_lab.backend.services.llm_service import generate_answer
from pipelines.examples.pdf_rag_lab.backend.services.vector_store import search_intro_chunks, search_similar_chunks


@dataclass
class DocumentSummary:
    document_id: str
    display_name: str
    summary: str
    chunk_count: int
    page_numbers: list[int]


def summarize_single_document(
    display_name: str,
    document_id: str,
    query_embedding: list[float],
) -> DocumentSummary:
    chunks = search_intro_chunks(
        question=f"No documento '{display_name}', quais são os temas centrais?",
        query_embedding=query_embedding,
        document_title_filter=display_name.lower(),
        top_k=3,
        candidate_k=12,
        max_intro_page=40,
    )

    if not chunks:
        chunks = search_similar_chunks(
            question=f"No documento '{display_name}', quais são os temas centrais?",
            query_embedding=query_embedding,
            top_k=3,
            candidate_k=20,
            document_title_filter=display_name.lower(),
            mode="single_document",
        )

    if not chunks:
        return DocumentSummary(
            document_id=document_id,
            display_name=display_name,
            summary="Não encontrei contexto suficiente para resumir este documento.",
            chunk_count=0,
            page_numbers=[],
        )

    answer = generate_answer(
        f"No documento '{display_name}', resuma apenas os temas centrais com base no contexto disponível.",
        chunks,
    )

    return DocumentSummary(
        document_id=document_id,
        display_name=display_name,
        summary=answer,
        chunk_count=len(chunks),
        page_numbers=sorted({chunk.get('page_number') for chunk in chunks if chunk.get('page_number') is not None}),
    )


def summarize_collection() -> dict:
    catalog = load_document_catalog()
    if not catalog:
        return {
            "document_summaries": [],
            "collection_summary": "Nenhum documento encontrado no catálogo.",
        }

    query_embedding = embed_query("Quais são os temas centrais de cada documento do corpus?")
    document_summaries: list[DocumentSummary] = []

    for doc in catalog:
        document_summaries.append(
            summarize_single_document(
                display_name=doc["display_name"],
                document_id=doc["document_id"],
                query_embedding=query_embedding,
            )
        )

    synthetic_chunks = []
    for item in document_summaries:
        synthetic_chunks.append(
            {
                "document_id": item.document_id,
                "document_name": item.display_name,
                "catalog_display_name": item.display_name,
                "page_number": item.page_numbers[0] if item.page_numbers else None,
                "chunk_text": item.summary,
                "score": 1.0,
            }
        )

    collection_answer = generate_answer(
        "Com base nos resumos de cada documento, quais são os principais temas tratados nos PDFs carregados?",
        synthetic_chunks,
    )

    return {
        "document_summaries": [
            {
                "document_id": item.document_id,
                "display_name": item.display_name,
                "summary": item.summary,
                "chunk_count": item.chunk_count,
                "page_numbers": item.page_numbers,
            }
            for item in document_summaries
        ],
        "collection_summary": collection_answer,
    }
