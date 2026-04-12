from __future__ import annotations

import uuid

import fitz

from pipelines.examples.pdf_rag_lab.backend.services.config import (
    DEFAULT_CHUNK_OVERLAP,
    DEFAULT_CHUNK_SIZE,
)


def extract_pdf_pages(pdf_path: str) -> list[dict]:
    doc = fitz.open(pdf_path)
    pages = []

    for page_index, page in enumerate(doc, start=1):
        text = page.get_text("text").strip()
        if text:
            pages.append(
                {
                    "page_number": page_index,
                    "text": text,
                }
            )

    doc.close()
    return pages


def chunk_text(text: str, chunk_size: int = DEFAULT_CHUNK_SIZE, chunk_overlap: int = DEFAULT_CHUNK_OVERLAP) -> list[str]:
    if not text:
        return []

    chunks = []
    start = 0
    step = max(1, chunk_size - chunk_overlap)

    while start < len(text):
        chunk = text[start:start + chunk_size].strip()
        if chunk:
            chunks.append(chunk)
        start += step

    return chunks


def build_document_chunks(pdf_path: str, file_name: str) -> list[dict]:
    document_id = str(uuid.uuid4())
    pages = extract_pdf_pages(pdf_path)

    output = []
    chunk_counter = 0

    for page in pages:
        page_chunks = chunk_text(page["text"])
        for chunk in page_chunks:
            output.append(
                {
                    "document_id": document_id,
                    "file_name": file_name,
                    "chunk_id": f"{document_id}_{chunk_counter}",
                    "chunk_index": chunk_counter,
                    "page_number": page["page_number"],
                    "chunk_text": chunk,
                }
            )
            chunk_counter += 1

    return output
