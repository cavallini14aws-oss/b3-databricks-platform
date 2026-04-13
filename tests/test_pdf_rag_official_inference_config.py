from pathlib import Path

import yaml


def test_pdf_rag_official_inference_config_is_valid():
    path = Path("config/pdf_rag_official_inference.yml")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))

    assert data["realtime"]["provider"] == "databricks"
    assert data["realtime"]["mode"] in {"foundation_model_api", "serving_endpoint"}
    assert data["realtime"]["serving_endpoint_type"] == "foundation"
    assert data["realtime"]["ollama_enabled"] is False

    assert data["batch"]["provider"] == "databricks"
    assert data["batch"]["mode"] == "ai_query"
    assert data["batch"]["ai_query_enabled"] is True

    assert data["governance"]["ai_gateway_enabled"] is True
    assert data["governance"]["inference_tables_enabled"] is True
    assert data["governance"]["endpoint_telemetry_enabled"] is False
    assert data["governance"]["endpoint_telemetry_scope"] == "custom_only"
