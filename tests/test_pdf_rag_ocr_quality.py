from pipelines.examples.pdf_rag_lab.backend.services.ocr_quality import (
    estimate_ocr_noise_score,
    has_minimum_text_density,
    is_low_quality_ocr,
)


def test_clean_text_has_lower_noise():
    clean = "Introdução. Este livro apresenta a experiência interior de Jung e seu desenvolvimento simbólico."
    noisy = 'O .ERU.t ITA " Tu eml., ...,,ado, encos,odo "" patt<k'
    assert estimate_ocr_noise_score(clean) < estimate_ocr_noise_score(noisy)


def test_low_quality_ocr_detection():
    noisy = 'O .ERU.t ITA " Tu eml., ...,,ado, encos,odo "" patt<k'
    assert is_low_quality_ocr(noisy) is True


def test_minimum_text_density():
    clean = "Este texto tem densidade suficiente de letras para ser útil."
    noisy = '<<<< .... //// 12345 """"'
    assert has_minimum_text_density(clean) is True
    assert has_minimum_text_density(noisy) is False
