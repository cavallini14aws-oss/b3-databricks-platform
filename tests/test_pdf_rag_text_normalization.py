from pipelines.examples.pdf_rag_lab.backend.services.text_normalization import (
    normalize_document_title,
    normalize_text,
    stable_chunk_hash,
)


def test_normalize_document_title():
    assert normalize_document_title("6_jung_tipos_psicologicos.pdf") == "6 jung tipos psicologicos"


def test_normalize_text_removes_accents_and_normalizes_spaces():
    assert normalize_text("Tipos   Psicológicos") == "tipos psicologicos"


def test_stable_chunk_hash_is_deterministic():
    a = stable_chunk_hash("doc.pdf", 1, "Trecho de teste")
    b = stable_chunk_hash("doc.pdf", 1, "Trecho de teste")
    assert a == b
