from pathlib import Path
import yaml


def test_pdf_rag_tables_config_exists():
    path = Path("databricks/pdf_rag/config/pdf_rag_tables.yml")
    assert path.exists()
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert "catalog" in data
    assert "schema" in data
    assert "tables" in data


def test_pdf_rag_runtime_config_exists():
    path = Path("databricks/pdf_rag/config/pdf_rag_runtime.yml")
    assert path.exists()
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert "ocr" in data
    assert "chunking" in data
