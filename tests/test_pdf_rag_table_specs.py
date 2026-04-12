from data_platform.governance.table_governance_validator import validate_table_spec_object
from data_platform.table_specs.examples.pdf_rag_lab.pdf_rag_table_specs import (
    PDF_RAG_CHUNKS_SPEC,
    PDF_RAG_DOCUMENTS_SPEC,
    PDF_RAG_EMBEDDINGS_SPEC,
    PDF_RAG_QUERIES_SPEC,
)


def test_pdf_rag_documents_spec_valid():
    assert validate_table_spec_object(PDF_RAG_DOCUMENTS_SPEC)["valid"] is True


def test_pdf_rag_chunks_spec_valid():
    assert validate_table_spec_object(PDF_RAG_CHUNKS_SPEC)["valid"] is True


def test_pdf_rag_embeddings_spec_valid():
    assert validate_table_spec_object(PDF_RAG_EMBEDDINGS_SPEC)["valid"] is True


def test_pdf_rag_queries_spec_valid():
    assert validate_table_spec_object(PDF_RAG_QUERIES_SPEC)["valid"] is True
