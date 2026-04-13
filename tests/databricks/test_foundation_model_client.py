from pathlib import Path


def test_foundation_model_client_exists():
    assert Path("databricks/pdf_rag/src/foundation_model_client.py").exists()
