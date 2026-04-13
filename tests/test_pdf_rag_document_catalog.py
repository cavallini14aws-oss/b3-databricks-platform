from pipelines.examples.pdf_rag_lab.backend.services.document_catalog import build_catalog_alias_map, load_document_catalog


def test_load_document_catalog():
    catalog = load_document_catalog()
    assert len(catalog) >= 1
    assert "document_id" in catalog[0]
    assert "display_name" in catalog[0]
    assert "source_file_name" in catalog[0]


def test_build_catalog_alias_map():
    alias_map = build_catalog_alias_map()
    assert isinstance(alias_map, dict)
    assert len(alias_map) >= 1
