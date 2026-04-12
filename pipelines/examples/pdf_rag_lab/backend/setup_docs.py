from __future__ import annotations

from pathlib import Path

from pipelines.examples.pdf_rag_lab.backend.services.config import RAW_DIR
from pipelines.examples.pdf_rag_lab.backend.services.embedding_service import embed_texts
from pipelines.examples.pdf_rag_lab.backend.services.pdf_service import build_document_chunks
from pipelines.examples.pdf_rag_lab.backend.services.vector_store import save_index


def main() -> None:
    pdf_files = sorted(RAW_DIR.glob("*.pdf"))
    if not pdf_files:
        raise SystemExit("Nenhum PDF encontrado em backend/data/raw")

    all_chunks = []

    for pdf_file in pdf_files:
        chunks = build_document_chunks(str(pdf_file), pdf_file.name)
        all_chunks.extend(chunks)

    if not all_chunks:
        raise SystemExit("Nenhum chunk foi gerado a partir dos PDFs.")

    texts = [chunk["chunk_text"] for chunk in all_chunks]
    embeddings = embed_texts(texts)
    save_index(all_chunks, embeddings)

    print(f"OK: {len(pdf_files)} PDF(s) processado(s)")
    print(f"OK: {len(all_chunks)} chunk(s) indexado(s)")


if __name__ == "__main__":
    main()
