from __future__ import annotations

from pipelines.examples.pdf_rag_lab.backend.services.config import RAW_DIR
from pipelines.examples.pdf_rag_lab.backend.services.document_catalog import load_document_catalog
from pipelines.examples.pdf_rag_lab.backend.services.document_ingestion_strategy import choose_ingestion_strategy
from pipelines.examples.pdf_rag_lab.backend.services.document_intelligence import inspect_document
from pipelines.examples.pdf_rag_lab.backend.services.document_quality_report import write_document_quality_report
from pipelines.examples.pdf_rag_lab.backend.services.embedding_service import embed_texts
from pipelines.examples.pdf_rag_lab.backend.services.hybrid_pdf_extractor import extract_pdf_pages_hybrid
from pipelines.examples.pdf_rag_lab.backend.services.ocr_quality import is_low_quality_ocr
from pipelines.examples.pdf_rag_lab.backend.services.pdf_service import build_document_chunks
from pipelines.examples.pdf_rag_lab.backend.services.vector_store import save_index


def _deduplicate_chunks(chunks: list[dict]) -> list[dict]:
    deduped = []
    seen_hashes: set[str] = set()

    for chunk in chunks:
        chunk_hash = chunk.get("chunk_hash")
        if not chunk_hash:
            deduped.append(chunk)
            continue
        if chunk_hash in seen_hashes:
            continue
        seen_hashes.add(chunk_hash)
        deduped.append(chunk)

    return deduped


def main() -> None:
    catalog = load_document_catalog()
    if not catalog:
        raise SystemExit("Nenhum documento encontrado no catálogo.")

    all_chunks = []
    processed_files = []

    for doc in catalog:
        pdf_file = RAW_DIR / doc["source_file_name"]
        if not pdf_file.exists():
            raise SystemExit(f"PDF do catálogo não encontrado: {pdf_file}")

        inspection = inspect_document(str(pdf_file), pdf_file.name)
        strategy = choose_ingestion_strategy(inspection)
        report_path = write_document_quality_report(inspection)

        if strategy.use_ocr_fallback:
            pages = extract_pdf_pages_hybrid(
                str(pdf_file),
                pdf_file.name,
                ocr_lang="eng",
                max_ocr_pages=12,
                ocr_page_limit=40,
            )
        else:
            pages = None

        chunks = build_document_chunks(
            pdf_path=str(pdf_file),
            file_name=pdf_file.name,
            pages=pages,
        )

        for chunk in chunks:
            chunk["catalog_document_id"] = doc["document_id"]
            chunk["catalog_display_name"] = doc["display_name"]
            chunk["catalog_display_name_normalized"] = doc["display_name_normalized"]
            chunk["catalog_aliases"] = doc["aliases"]
            chunk["document_quality_tier"] = inspection.quality_tier
            chunk["document_extraction_strategy"] = strategy.strategy_name
            chunk["document_quality_report_path"] = str(report_path)

        if strategy.drop_low_quality_pages:
            valid_pages = {page.page_number for page in inspection.page_reports if page.is_usable}
            ocr_pages = {page["page_number"] for page in pages or [] if page.get("extraction_method") == "ocr_fallback"}
            valid_pages = valid_pages.union(ocr_pages)
            chunks = [chunk for chunk in chunks if chunk.get("page_number") in valid_pages]

        if strategy.intro_page_limit is not None:
            early_chunks = [chunk for chunk in chunks if (chunk.get("page_number") or 999999) <= strategy.intro_page_limit]
            if early_chunks:
                chunks = early_chunks

        all_chunks.extend(chunks)
        processed_files.append(pdf_file.name)

        print(f"[INFO] file={pdf_file.name}")
        print(f"[INFO] quality_tier={inspection.quality_tier}")
        print(f"[INFO] strategy={strategy.strategy_name}")
        print(f"[INFO] report={report_path}")
        print()

    if not all_chunks:
        raise SystemExit("Nenhum chunk foi gerado a partir dos PDFs do catálogo.")

    deduped_chunks = _deduplicate_chunks(all_chunks)

    quality_filtered_chunks = []
    dropped_low_quality = 0

    for chunk in deduped_chunks:
        if is_low_quality_ocr(chunk["chunk_text"]):
            dropped_low_quality += 1
            continue
        quality_filtered_chunks.append(chunk)

    if not quality_filtered_chunks:
        raise SystemExit("Todos os chunks foram descartados pelo filtro de qualidade OCR.")

    texts = [chunk["chunk_text"] for chunk in quality_filtered_chunks]
    embeddings = embed_texts(texts)
    save_index(quality_filtered_chunks, embeddings)

    print(f"OK: {len(processed_files)} PDF(s) do catálogo processado(s)")
    for file_name in processed_files:
        print(f"OK: arquivo indexado -> {file_name}")
    print(f"OK: {len(all_chunks)} chunk(s) bruto(s)")
    print(f"OK: {len(deduped_chunks)} chunk(s) após deduplicação")
    print(f"OK: {dropped_low_quality} chunk(s) descartado(s) por OCR ruim")
    print(f"OK: {len(quality_filtered_chunks)} chunk(s) indexado(s) após filtro de qualidade")


if __name__ == "__main__":
    main()
