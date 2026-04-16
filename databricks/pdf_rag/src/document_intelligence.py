from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import fitz


@dataclass
class DocumentInspection:
    file_name: str
    total_pages: int
    usable_pages: int
    unusable_pages: int
    avg_ocr_noise_score: float
    quality_tier: str
    extraction_strategy: str


def inspect_document(pdf_path: str, file_name: str) -> DocumentInspection:
    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    doc.close()

    usable_pages = total_pages
    unusable_pages = 0
    avg_ocr_noise_score = 0.0

    if total_pages >= 200:
        quality_tier = "medium"
        extraction_strategy = "native_text_with_filtering"
    else:
        quality_tier = "high"
        extraction_strategy = "native_text"

    return DocumentInspection(
        file_name=file_name,
        total_pages=total_pages,
        usable_pages=usable_pages,
        unusable_pages=unusable_pages,
        avg_ocr_noise_score=avg_ocr_noise_score,
        quality_tier=quality_tier,
        extraction_strategy=extraction_strategy,
    )
