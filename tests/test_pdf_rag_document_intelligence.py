from pipelines.examples.pdf_rag_lab.backend.services.document_ingestion_strategy import choose_ingestion_strategy
from pipelines.examples.pdf_rag_lab.backend.services.document_intelligence import DocumentInspection, PageInspection


def _inspection(quality_tier: str) -> DocumentInspection:
    return DocumentInspection(
        file_name="test.pdf",
        total_pages=10,
        extracted_pages=10,
        usable_pages=8 if quality_tier == "high" else 5 if quality_tier == "medium" else 2,
        unusable_pages=2 if quality_tier == "high" else 5 if quality_tier == "medium" else 8,
        avg_ocr_noise_score=0.02 if quality_tier == "high" else 0.05 if quality_tier == "medium" else 0.08,
        quality_tier=quality_tier,
        extraction_strategy="native_text",
        page_reports=[
            PageInspection(
                page_number=1,
                text_length=500 if quality_tier != "low" else 50,
                alpha_density_ok=False if quality_tier == "low" else True,
                ocr_noise_score=0.08 if quality_tier == "low" else 0.02,
                is_usable=False if quality_tier == "low" else True,
            )
        ],
    )


def test_choose_ingestion_strategy_high():
    strategy = choose_ingestion_strategy(_inspection("high"))
    assert strategy.strategy_name == "native_full"
    assert strategy.drop_low_quality_pages is False
    assert strategy.use_ocr_fallback is False


def test_choose_ingestion_strategy_medium():
    strategy = choose_ingestion_strategy(_inspection("medium"))
    assert strategy.strategy_name == "native_filtered"
    assert strategy.drop_low_quality_pages is True
    assert strategy.use_ocr_fallback is False


def test_choose_ingestion_strategy_low():
    strategy = choose_ingestion_strategy(_inspection("low"))
    assert strategy.strategy_name == "hybrid_ocr_fallback"
    assert strategy.intro_page_limit == 25
    assert strategy.use_ocr_fallback is True
