import pytest


def _sample_chunks():
    return [
        {
            "page_number": 1,
            "chunk_text": "Trecho de contexto para teste."
        }
    ]


def test_databricks_provider_requires_host(monkeypatch):
    from pipelines.examples.pdf_rag_lab.backend.services import llm_service

    monkeypatch.setattr(llm_service, "LLM_PROVIDER", "databricks")
    monkeypatch.setattr(llm_service, "DATABRICKS_HOST", "")
    monkeypatch.setattr(llm_service, "DATABRICKS_TOKEN", "token")
    monkeypatch.setattr(llm_service, "DATABRICKS_SERVING_ENDPOINT", "endpoint")

    with pytest.raises(ValueError, match="DATABRICKS_HOST"):
        llm_service.generate_answer("pergunta", _sample_chunks())


def test_databricks_provider_requires_token(monkeypatch):
    from pipelines.examples.pdf_rag_lab.backend.services import llm_service

    monkeypatch.setattr(llm_service, "LLM_PROVIDER", "databricks")
    monkeypatch.setattr(llm_service, "DATABRICKS_HOST", "https://workspace.databricks.com")
    monkeypatch.setattr(llm_service, "DATABRICKS_TOKEN", "")
    monkeypatch.setattr(llm_service, "DATABRICKS_SERVING_ENDPOINT", "endpoint")

    with pytest.raises(ValueError, match="DATABRICKS_TOKEN"):
        llm_service.generate_answer("pergunta", _sample_chunks())


def test_databricks_provider_requires_serving_endpoint(monkeypatch):
    from pipelines.examples.pdf_rag_lab.backend.services import llm_service

    monkeypatch.setattr(llm_service, "LLM_PROVIDER", "databricks")
    monkeypatch.setattr(llm_service, "DATABRICKS_HOST", "https://workspace.databricks.com")
    monkeypatch.setattr(llm_service, "DATABRICKS_TOKEN", "token")
    monkeypatch.setattr(llm_service, "DATABRICKS_SERVING_ENDPOINT", "")

    with pytest.raises(ValueError, match="DATABRICKS_SERVING_ENDPOINT"):
        llm_service.generate_answer("pergunta", _sample_chunks())


def test_databricks_provider_returns_text_from_choices_payload(monkeypatch):
    from pipelines.examples.pdf_rag_lab.backend.services import llm_service

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": "resposta databricks"
                        }
                    }
                ]
            }

    def fake_post(*args, **kwargs):
        return FakeResponse()

    monkeypatch.setattr(llm_service, "LLM_PROVIDER", "databricks")
    monkeypatch.setattr(llm_service, "DATABRICKS_HOST", "https://workspace.databricks.com")
    monkeypatch.setattr(llm_service, "DATABRICKS_TOKEN", "token")
    monkeypatch.setattr(llm_service, "DATABRICKS_SERVING_ENDPOINT", "endpoint")
    monkeypatch.setattr(llm_service.requests, "post", fake_post)

    result = llm_service.generate_answer("pergunta", _sample_chunks())
    assert result == "resposta databricks"


def test_databricks_provider_returns_text_from_predictions_payload(monkeypatch):
    from pipelines.examples.pdf_rag_lab.backend.services import llm_service

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "predictions": [
                    "resposta predictions"
                ]
            }

    def fake_post(*args, **kwargs):
        return FakeResponse()

    monkeypatch.setattr(llm_service, "LLM_PROVIDER", "databricks")
    monkeypatch.setattr(llm_service, "DATABRICKS_HOST", "https://workspace.databricks.com")
    monkeypatch.setattr(llm_service, "DATABRICKS_TOKEN", "token")
    monkeypatch.setattr(llm_service, "DATABRICKS_SERVING_ENDPOINT", "endpoint")
    monkeypatch.setattr(llm_service.requests, "post", fake_post)

    result = llm_service.generate_answer("pergunta", _sample_chunks())
    assert result == "resposta predictions"
