from __future__ import annotations

from dataclasses import dataclass

from pipelines.examples.pdf_rag_lab.backend.services.ocr_quality import estimate_ocr_noise_score, has_minimum_text_density
from pipelines.examples.pdf_rag_lab.backend.services.pdf_service import extract_pdf_pages


@dataclass
class PageInspection:
    page_number: int
    text_length: int
    alpha_density_ok: bool
    ocr_noise_score: float
    is_usable: bool


@dataclass
class DocumentInspection:
    file_name: str
    total_pages: int
    extracted_pages: int
    usable_pages: int
    unusable_pages: int
    avg_ocr_noise_score: float
    quality_tier: str
    extraction_strategy: str
    page_reports: list[PageInspection]


def inspect_document(pdf_path: str, file_name: str) -> DocumentInspection:
    pages = extract_pdf_pages(pdf_path)
    page_reports: list[PageInspection] = []

    for page in pages:
        text = page["text"]
        text_length = len(text)
        alpha_density_ok = has_minimum_text_density(text)
        noise_score = estimate_ocr_noise_score(text)
        is_usable = alpha_density_ok and noise_score < 0.06 and text_length >= 80

        page_reports.append(
            PageInspection(
                page_number=page["page_number"],
                text_length=text_length,
                alpha_density_ok=alpha_density_ok,
                ocr_noise_score=noise_score,
                is_usable=is_usable,
            )
        )

    total_pages = len(page_reports)
    usable_pages = sum(1 for p in page_reports if p.is_usable)
    unusable_pages = total_pages - usable_pages
    avg_noise = sum(p.ocr_noise_score for p in page_reports) / max(total_pages, 1)

    usable_ratio = usable_pages / max(total_pages, 1)

    if usable_ratio >= 0.85 and avg_noise < 0.03:
        quality_tier = "high"
        extraction_strategy = "native_text"
    elif usable_ratio >= 0.50:
        quality_tier = "medium"
        extraction_strategy = "native_text_with_filtering"
    else:
        quality_tier = "low"
        extraction_strategy = "ocr_fallback_recommended"

    return DocumentInspection(
        file_name=file_name,
        total_pages=total_pages,
        extracted_pages=total_pages,
        usable_pages=usable_pages,
        unusable_pages=unusable_pages,
        avg_ocr_noise_score=avg_noise,
        quality_tier=quality_tier,
        extraction_strategy=extraction_strategy,
        page_reports=page_reports,
    )
