from pathlib import Path
import yaml


def test_foundation_models_config_exists():
    path = Path("databricks/pdf_rag/config/pdf_rag_foundation_models.yml")
    assert path.exists()
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert "foundation_models" in data
    cfg = data["foundation_models"]
    assert "endpoint_name_env" in cfg
    assert "request_timeout_seconds" in cfg


def test_foundation_model_client_exists():
    assert Path("databricks/pdf_rag/src/foundation_model_client.py").exists()


def test_foundation_model_smoke_exists():
    assert Path("databricks/pdf_rag/src/smoke_foundation_model.py").exists()


def test_foundation_model_notebook_exists():
    assert Path("databricks/pdf_rag/notebooks/05_test_foundation_model.py").exists()


def test_ai_query_sql_exists():
    assert Path("databricks/pdf_rag/notebooks/06_batch_inference_ai_query.sql").exists()
