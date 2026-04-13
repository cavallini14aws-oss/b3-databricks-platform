from __future__ import annotations

from dataclasses import dataclass

from pipelines.examples.pdf_rag_lab.backend.services.document_intelligence import DocumentInspection


@dataclass
class IngestionStrategy:
    strategy_name: str
    use_native_text: bool
    use_ocr_fallback: bool
    intro_page_limit: int | None
    drop_low_quality_pages: bool


def choose_ingestion_strategy(inspection: DocumentInspection) -> IngestionStrategy:
    if inspection.quality_tier == "high":
        return IngestionStrategy(
            strategy_name="native_full",
            use_native_text=True,
            use_ocr_fallback=False,
            intro_page_limit=None,
            drop_low_quality_pages=False,
        )

    if inspection.quality_tier == "medium":
        return IngestionStrategy(
            strategy_name="native_filtered",
            use_native_text=True,
            use_ocr_fallback=False,
            intro_page_limit=40,
            drop_low_quality_pages=True,
        )

    return IngestionStrategy(
        strategy_name="hybrid_ocr_fallback",
        use_native_text=True,
        use_ocr_fallback=True,
        intro_page_limit=25,
        drop_low_quality_pages=True,
    )
