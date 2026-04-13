from __future__ import annotations

import shutil
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile

from pipelines.examples.pdf_rag_lab.backend.services.config import DEFAULT_RETRIEVAL_CANDIDATES, DEFAULT_TOP_K, RAW_DIR
from pipelines.examples.pdf_rag_lab.backend.services.document_catalog import build_catalog_alias_map
from pipelines.examples.pdf_rag_lab.backend.services.embedding_service import embed_query
from pipelines.examples.pdf_rag_lab.backend.services.llm_service import generate_answer
from pipelines.examples.pdf_rag_lab.backend.services.retrieval_router import detect_intent
from pipelines.examples.pdf_rag_lab.backend.services.schemas import (
    ChatRequest,
    ChatResponse,
    HealthResponse,
    RetrievedChunk,
    UploadResponse,
)
from pipelines.examples.pdf_rag_lab.backend.services.vector_store import search_intro_chunks, search_similar_chunks


app = FastAPI(title="PDF RAG Lab", version="0.1.0")


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@app.post("/upload", response_model=UploadResponse)
async def upload_pdf(file: UploadFile = File(...)) -> UploadResponse:
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Apenas arquivos PDF são permitidos.")

    target_path = RAW_DIR / Path(file.filename).name

    with target_path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return UploadResponse(message="PDF recebido com sucesso.", file_name=file.filename)


@app.post("/chat", response_model=ChatResponse)
def chat(request: ChatRequest) -> ChatResponse:
    top_k = request.top_k or DEFAULT_TOP_K

    try:
        document_alias_map = build_catalog_alias_map()
        intent = detect_intent(request.question, document_alias_map)

        query_embedding = embed_query(request.question)
        chunks = []

        if intent.mode == "compare_documents" and intent.document_hints:
            per_document_top_k = max(2, top_k // max(1, len(intent.document_hints)))
            gathered = []

            for document_hint in intent.document_hints:
                if intent.is_introductory:
                    document_chunks = search_intro_chunks(
                        question=request.question,
                        query_embedding=query_embedding,
                        document_title_filter=document_hint,
                        top_k=per_document_top_k,
                        candidate_k=max(DEFAULT_RETRIEVAL_CANDIDATES, top_k * 5),
                        max_intro_page=40,
                    )
                else:
                    document_chunks = search_similar_chunks(
                        question=request.question,
                        query_embedding=query_embedding,
                        top_k=per_document_top_k,
                        candidate_k=max(DEFAULT_RETRIEVAL_CANDIDATES, top_k * 5),
                        document_title_filter=document_hint,
                        mode="compare_documents",
                    )
                gathered.extend(document_chunks)

            seen_chunk_ids: set[str] = set()
            deduped = []
            for chunk in gathered:
                chunk_id = chunk["chunk_id"]
                if chunk_id in seen_chunk_ids:
                    continue
                seen_chunk_ids.add(chunk_id)
                deduped.append(chunk)

            chunks = deduped[:top_k]

        else:
            if intent.mode == "single_document" and intent.document_hint and intent.is_introductory:
                chunks = search_intro_chunks(
                    question=request.question,
                    query_embedding=query_embedding,
                    document_title_filter=intent.document_hint,
                    top_k=top_k,
                    candidate_k=DEFAULT_RETRIEVAL_CANDIDATES,
                    max_intro_page=40,
                )
            else:
                candidate_k = DEFAULT_RETRIEVAL_CANDIDATES
                if intent.mode == "compare_documents":
                    candidate_k = max(DEFAULT_RETRIEVAL_CANDIDATES, top_k * 5)

                chunks = search_similar_chunks(
                    question=request.question,
                    query_embedding=query_embedding,
                    top_k=top_k,
                    candidate_k=candidate_k,
                    document_title_filter=intent.document_hint,
                    mode=intent.mode,
                )

        if intent.mode == "single_document" and intent.document_hint:
            has_document_match = any(
                chunk.get("catalog_display_name_normalized", chunk.get("document_title_normalized")) == intent.document_hint
                or chunk.get("catalog_display_name") is not None and intent.document_hint in (
                    chunk.get("catalog_display_name_normalized", "")
                )
                for chunk in chunks
            )
            if not has_document_match or not chunks:
                answer = "Não encontrei contexto suficiente do documento solicitado."
            else:
                answer = generate_answer(request.question, chunks)
        elif intent.mode == "compare_documents" and intent.document_hints:
            matched_documents = {
                chunk.get("catalog_display_name") or chunk.get("document_name")
                for chunk in chunks
            }
            if len([name for name in matched_documents if name]) < 2:
                answer = "Não encontrei contexto suficiente de ambos os documentos solicitados para uma comparação confiável."
            elif not chunks:
                answer = "Não encontrei contexto suficiente para responder com segurança."
            else:
                answer = generate_answer(request.question, chunks)
        elif not chunks:
            answer = "Não encontrei contexto suficiente para responder com segurança."
        else:
            answer = generate_answer(request.question, chunks)

    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro interno: {exc}") from exc

    response_chunks = [
        RetrievedChunk(
            document_id=chunk["document_id"],
            document_name=chunk.get("document_name"),
            document_title_normalized=chunk.get("document_title_normalized"),
            catalog_document_id=chunk.get("catalog_document_id"),
            catalog_display_name=chunk.get("catalog_display_name"),
            chunk_id=chunk["chunk_id"],
            page_number=chunk.get("page_number"),
            score=chunk["score"],
            chunk_text=chunk["chunk_text"],
        )
        for chunk in chunks
    ]

    return ChatResponse(
        answer=answer,
        chunks=response_chunks,
        retrieval_mode=intent.mode,
        document_filter_applied=intent.document_hint,
        document_filters_applied=intent.document_hints,
    )
