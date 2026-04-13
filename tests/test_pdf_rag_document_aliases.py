from pipelines.examples.pdf_rag_lab.backend.services.document_aliases import build_document_aliases


def test_build_document_aliases_removes_noise():
    aliases = build_document_aliases("6 jung tipos psicologicos")
    assert "6 jung tipos psicologicos" in aliases
    assert "tipos psicologicos" in aliases
