from pathlib import Path

import yaml


def test_official_strategy_keeps_ollama_disabled_operationally():
    data = yaml.safe_load(
        Path("config/pdf_rag_official_inference.yml").read_text(encoding="utf-8")
    )

    assert data["realtime"]["provider"] == "databricks"
    assert data["realtime"]["ollama_enabled"] is False


def test_official_strategy_uses_ai_query_for_batch():
    data = yaml.safe_load(
        Path("config/pdf_rag_official_inference.yml").read_text(encoding="utf-8")
    )

    assert data["batch"]["mode"] == "ai_query"
    assert data["batch"]["ai_query_enabled"] is True


def test_official_strategy_enables_ai_gateway_and_inference_tables():
    data = yaml.safe_load(
        Path("config/pdf_rag_official_inference.yml").read_text(encoding="utf-8")
    )

    assert data["governance"]["ai_gateway_enabled"] is True
    assert data["governance"]["inference_tables_enabled"] is True
