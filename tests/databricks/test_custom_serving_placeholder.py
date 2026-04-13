from pathlib import Path


def test_custom_serving_placeholder_exists():
    assert Path("databricks/pdf_rag/src/custom_serving_placeholder.py").exists()
