from pathlib import Path
import yaml


def test_vector_search_config_exists():
    path = Path("databricks/pdf_rag/config/pdf_rag_vector_search.yml")
    assert path.exists()
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert "vector_search" in data
    cfg = data["vector_search"]
    assert "endpoint_name" in cfg
    assert "index_name" in cfg
    assert "source_table_name" in cfg
    assert "embedding_source_column" in cfg


def test_vector_bootstrap_real_exists():
    assert Path("databricks/pdf_rag/src/vector_index_bootstrap.py").exists()


def test_vector_query_script_exists():
    assert Path("databricks/pdf_rag/src/query_vector_index.py").exists()


def test_query_notebook_exists():
    assert Path("databricks/pdf_rag/notebooks/04_query_vector_index.py").exists()
