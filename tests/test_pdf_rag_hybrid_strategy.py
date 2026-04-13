from pipelines.examples.pdf_rag_lab.backend.services.document_ingestion_strategy import choose_ingestion_strategy
from pipelines.examples.pdf_rag_lab.backend.services.document_intelligence import DocumentInspection, PageInspection


def _inspection(quality_tier: str) -> DocumentInspection:
    return DocumentInspection(
        file_name="test.pdf",
        total_pages=10,
        extracted_pages=10,
        usable_pages=2 if quality_tier == "low" else 8,
        unusable_pages=8 if quality_tier == "low" else 2,
        avg_ocr_noise_score=0.08 if quality_tier == "low" else 0.02,
        quality_tier=quality_tier,
        extraction_strategy="ocr_fallback_recommended",
        page_reports=[
            PageInspection(
                page_number=1,
                text_length=50,
                alpha_density_ok=False,
                ocr_noise_score=0.08,
                is_usable=False,
            )
        ],
    )


def test_low_quality_document_uses_ocr_fallback():
    strategy = choose_ingestion_strategy(_inspection("low"))
    assert strategy.strategy_name == "hybrid_ocr_fallback"
    assert strategy.use_ocr_fallback is True
