def test_pdf_rag_backend_imports():
    from pipelines.examples.pdf_rag_lab.backend.services import config  # noqa: F401
    from pipelines.examples.pdf_rag_lab.backend.services import schemas  # noqa: F401
    from pipelines.examples.pdf_rag_lab.backend.services import pdf_service  # noqa: F401
    from pipelines.examples.pdf_rag_lab.backend.services import embedding_service  # noqa: F401
    from pipelines.examples.pdf_rag_lab.backend.services import vector_store  # noqa: F401
    from pipelines.examples.pdf_rag_lab.backend.services import llm_service  # noqa: F401
