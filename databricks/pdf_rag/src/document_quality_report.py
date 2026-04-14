from __future__ import annotations

import json
from pathlib import Path

from src.document_intelligence import DocumentInspection

REPORT_DIR = Path("/tmp/pdf_rag_reports")


def write_document_quality_report(inspection: DocumentInspection) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_DIR / f"{inspection.file_name}.quality.json"

    payload = {
        "file_name": inspection.file_name,
        "total_pages": inspection.total_pages,
        "usable_pages": inspection.usable_pages,
        "unusable_pages": inspection.unusable_pages,
        "avg_ocr_noise_score": inspection.avg_ocr_noise_score,
        "quality_tier": inspection.quality_tier,
        "extraction_strategy": inspection.extraction_strategy,
    }

    report_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return report_path
