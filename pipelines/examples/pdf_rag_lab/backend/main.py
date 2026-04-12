from __future__ import annotations

import shutil
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile

from pipelines.examples.pdf_rag_lab.backend.services.config import DEFAULT_TOP_K, RAW_DIR
from pipelines.examples.pdf_rag_lab.backend.services.embedding_service import embed_query
from pipelines.examples.pdf_rag_lab.backend.services.llm_service import generate_answer
from pipelines.examples.pdf_rag_lab.backend.services.schemas import (
    ChatRequest,
    ChatResponse,
    HealthResponse,
    RetrievedChunk,
    UploadResponse,
)
from pipelines.examples.pdf_rag_lab.backend.services.vector_store import search_similar_chunks


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
        query_embedding = embed_query(request.question)
        chunks = search_similar_chunks(query_embedding, top_k=top_k)
        answer = generate_answer(request.question, chunks)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro interno: {exc}") from exc

    response_chunks = [
        RetrievedChunk(
            document_id=chunk["document_id"],
            chunk_id=chunk["chunk_id"],
            page_number=chunk.get("page_number"),
            score=chunk["score"],
            chunk_text=chunk["chunk_text"],
        )
        for chunk in chunks
    ]

    return ChatResponse(answer=answer, chunks=response_chunks)
