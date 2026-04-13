from __future__ import annotations

from pipelines.examples.pdf_rag_lab.backend.services.document_intelligence import inspect_document
from pipelines.examples.pdf_rag_lab.backend.services.ocr_fallback import extract_text_with_ocr
from pipelines.examples.pdf_rag_lab.backend.services.pdf_service import extract_pdf_pages


def extract_pdf_pages_hybrid(
    pdf_path: str,
    file_name: str,
    ocr_lang: str = "eng",
    max_ocr_pages: int = 12,
    ocr_page_limit: int = 40,
) -> list[dict]:
    native_pages = extract_pdf_pages(pdf_path)
    inspection = inspect_document(pdf_path, file_name)

    inspection_by_page = {page.page_number: page for page in inspection.page_reports}
    hybrid_pages = []
    ocr_used = 0

    for page in native_pages:
        page_number = page["page_number"]
        page_report = inspection_by_page.get(page_number)

        if page_report and page_report.is_usable:
            hybrid_pages.append(
                {
                    "page_number": page_number,
                    "text": page["text"],
                    "extraction_method": "native",
                }
            )
            continue

        should_try_ocr = (
            page_number <= ocr_page_limit
            and ocr_used < max_ocr_pages
        )

        if should_try_ocr:
            ocr_text = extract_text_with_ocr(
                pdf_path,
                page_number - 1,
                lang=ocr_lang,
                timeout_seconds=12,
            )
            if ocr_text:
                hybrid_pages.append(
                    {
                        "page_number": page_number,
                        "text": ocr_text,
                        "extraction_method": "ocr_fallback",
                    }
                )
                ocr_used += 1
                continue

        hybrid_pages.append(
            {
                "page_number": page_number,
                "text": page["text"],
                "extraction_method": "native_unusable",
            }
        )

    return hybrid_pages
