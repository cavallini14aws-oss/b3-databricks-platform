from __future__ import annotations

import hashlib
import re
import uuid

import fitz

from pipelines.examples.pdf_rag_lab.backend.services.text_normalization import normalize_text


def extract_pdf_pages(pdf_path: str) -> list[dict]:
    doc = fitz.open(pdf_path)
    pages = []

    for page_index in range(len(doc)):
        page = doc.load_page(page_index)
        text = page.get_text("text") or ""
        pages.append(
            {
                "page_number": page_index + 1,
                "text": text.strip(),
            }
        )

    doc.close()
    return pages


def _split_text_into_chunks(text: str, chunk_size: int = 1200, overlap: int = 150) -> list[str]:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not cleaned:
        return []

    chunks = []
    start = 0
    text_len = len(cleaned)

    while start < text_len:
        end = min(start + chunk_size, text_len)
        chunk = cleaned[start:end].strip()
        if chunk:
            chunks.append(chunk)

        if end >= text_len:
            break

        start = max(end - overlap, start + 1)

    return chunks


def build_document_chunks(
    pdf_path: str,
    file_name: str,
    pages: list[dict] | None = None,
) -> list[dict]:
    if pages is None:
        pages = extract_pdf_pages(pdf_path)

    document_id = str(uuid.uuid4())
    document_title_normalized = normalize_text(file_name.replace(".pdf", ""))

    chunks = []
    chunk_counter = 0

    for page in pages:
        page_number = page["page_number"]
        text = page.get("text", "").strip()
        extraction_method = page.get("extraction_method", "native")

        if not text:
            continue

        split_chunks = _split_text_into_chunks(text)
        for chunk_text in split_chunks:
            if len(chunk_text) < 80:
                continue

            chunk_hash = hashlib.sha1(chunk_text.encode("utf-8")).hexdigest()
            chunks.append(
                {
                    "document_id": document_id,
                    "document_name": file_name,
                    "document_title_normalized": document_title_normalized,
                    "chunk_id": f"{document_id}_{chunk_counter}",
                    "page_number": page_number,
                    "chunk_text": chunk_text,
                    "chunk_text_normalized": normalize_text(chunk_text),
                    "chunk_hash": chunk_hash,
                    "page_extraction_method": extraction_method,
                }
            )
            chunk_counter += 1

    return chunks
