from pathlib import Path


def test_vector_bootstrap_exists():
    assert Path("databricks/pdf_rag/src/vector_index_bootstrap.py").exists()


def test_vector_notebook_exists():
    assert Path("databricks/pdf_rag/notebooks/03_create_vector_index.py").exists()
