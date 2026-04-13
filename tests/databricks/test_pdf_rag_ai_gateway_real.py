from pathlib import Path
import yaml


def test_ai_gateway_config_exists():
    path = Path("databricks/pdf_rag/config/pdf_rag_ai_gateway.yml")
    assert path.exists()
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert "ai_gateway" in data
    cfg = data["ai_gateway"]
    assert "endpoint_name_env" in cfg
    assert "inference_table_catalog" in cfg
    assert "inference_table_schema" in cfg


def test_ai_gateway_summary_script_exists():
    assert Path("databricks/pdf_rag/src/ai_gateway_config_summary.py").exists()


def test_ai_gateway_notebook_exists():
    assert Path("databricks/pdf_rag/notebooks/07_ai_gateway_enablement_check.py").exists()


def test_inference_table_query_sql_exists():
    assert Path("databricks/pdf_rag/notebooks/08_query_inference_tables.sql").exists()
