from pathlib import Path
import yaml


def test_custom_serving_config_exists():
    path = Path("databricks/pdf_rag/config/pdf_rag_custom_serving.yml")
    assert path.exists()
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert "custom_serving" in data
    cfg = data["custom_serving"]
    assert "endpoint_name" in cfg
    assert "uc_model_name" in cfg
    assert "served_model_name" in cfg


def test_custom_pyfunc_model_exists():
    assert Path("databricks/pdf_rag/src/custom_pyfunc_model.py").exists()


def test_custom_register_script_exists():
    assert Path("databricks/pdf_rag/src/register_custom_pyfunc_model.py").exists()


def test_custom_deploy_script_exists():
    assert Path("databricks/pdf_rag/src/deploy_custom_serving_endpoint.py").exists()


def test_custom_smoke_exists():
    assert Path("databricks/pdf_rag/src/smoke_custom_serving.py").exists()


def test_custom_notebooks_exist():
    assert Path("databricks/pdf_rag/notebooks/09_register_custom_pyfunc_model.py").exists()
    assert Path("databricks/pdf_rag/notebooks/10_deploy_custom_serving_endpoint.py").exists()
    assert Path("databricks/pdf_rag/notebooks/11_smoke_custom_serving.py").exists()


def test_custom_serving_docs_exist():
    assert Path("docs/databricks/pdf_rag/CUSTOM_MODEL_SERVING_ROADMAP.md").exists()
