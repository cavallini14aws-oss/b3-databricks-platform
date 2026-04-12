def test_run_clientes_ai_end_to_end_executes_expected_flow(monkeypatch):
    from pipelines.examples.clientes.ai.run_clientes_ai_end_to_end import (
        run_clientes_ai_end_to_end,
    )

    call_order = []

    monkeypatch.setattr(
        "pipelines.examples.clientes.ai.run_clientes_ai_end_to_end.load_yaml_config",
        lambda path: {
            "chunking": {"enabled": True},
            "embeddings": {"enabled": True},
            "index": {"enabled": True},
            "evaluation": {"enabled": True},
        },
    )

    class DummyNaming:
        use_catalog = False

    class DummyContext:
        env = "dev"
        project = "clientes"
        naming = DummyNaming()

    monkeypatch.setattr(
        "pipelines.examples.clientes.ai.run_clientes_ai_end_to_end.get_context",
        lambda **kwargs: DummyContext(),
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ai.run_clientes_ai_end_to_end.run_ingest_knowledge",
        lambda **kwargs: call_order.append("ingest_knowledge"),
    )
    monkeypatch.setattr(
        "pipelines.examples.clientes.ai.run_clientes_ai_end_to_end.run_chunk_documents",
        lambda **kwargs: call_order.append("chunk_documents"),
    )
    monkeypatch.setattr(
        "pipelines.examples.clientes.ai.run_clientes_ai_end_to_end.run_build_embeddings",
        lambda **kwargs: call_order.append("build_embeddings"),
    )
    monkeypatch.setattr(
        "pipelines.examples.clientes.ai.run_clientes_ai_end_to_end.run_build_index",
        lambda **kwargs: call_order.append("build_index"),
    )
    monkeypatch.setattr(
        "pipelines.examples.clientes.ai.run_clientes_ai_end_to_end.run_evaluate_rag",
        lambda **kwargs: call_order.append("evaluate_rag"),
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ai.run_clientes_ai_end_to_end.run_with_observability",
        lambda **kwargs: kwargs["fn"](type("DummyLogger", (), {"info": lambda *a, **k: None})()),
    )

    run_clientes_ai_end_to_end(
        spark=object(),
        project="clientes",
        use_catalog=False,
        config_path="config/clientes_ai_pipeline.yml",
        forced_run_id="ai-runner-test",
    )

    assert call_order == [
        "ingest_knowledge",
        "chunk_documents",
        "build_embeddings",
        "build_index",
        "evaluate_rag",
    ]
