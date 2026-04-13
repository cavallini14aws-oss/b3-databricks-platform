from __future__ import annotations

import json
from pathlib import Path

from pipelines.examples.pdf_rag_lab.backend.services.document_intelligence import DocumentInspection


REPORT_DIR = Path("pipelines/examples/pdf_rag_lab/backend/reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def write_document_quality_report(inspection: DocumentInspection) -> Path:
    output = {
        "file_name": inspection.file_name,
        "total_pages": inspection.total_pages,
        "extracted_pages": inspection.extracted_pages,
        "usable_pages": inspection.usable_pages,
        "unusable_pages": inspection.unusable_pages,
        "avg_ocr_noise_score": inspection.avg_ocr_noise_score,
        "quality_tier": inspection.quality_tier,
        "extraction_strategy": inspection.extraction_strategy,
        "page_reports": [
            {
                "page_number": page.page_number,
                "text_length": page.text_length,
                "alpha_density_ok": page.alpha_density_ok,
                "ocr_noise_score": page.ocr_noise_score,
                "is_usable": page.is_usable,
            }
            for page in inspection.page_reports
        ],
    }

    report_path = REPORT_DIR / f"{inspection.file_name}.quality.json"
    report_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    return report_path
