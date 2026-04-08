from types import SimpleNamespace

import pytest


def test_run_clientes_retraining_blocks_non_approved_request(monkeypatch):
    from pipelines.examples.clientes.ml.run_clientes_retraining import run_clientes_retraining

    fake_ctx = SimpleNamespace(
        env="dev",
        project="clientes",
        naming=SimpleNamespace(
            use_catalog=False,
        ),
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_retraining.get_context",
        lambda project, use_catalog: fake_ctx,
    )
    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_retraining.run_with_observability",
        lambda **kwargs: kwargs["fn"](SimpleNamespace(info=lambda *args, **kwargs: None)),
    )

    with pytest.raises(ValueError, match="Somente retraining request APPROVED"):
        run_clientes_retraining(
            spark=object(),
            request_payload={
                "model_name": "clientes_status_classifier",
                "request_status": "OPEN",
                "trigger_type": "DRIFT",
                "trigger_source": "drift_monitoring",
            },
            project="clientes",
            use_catalog=False,
            forced_run_id="retraining-run-1",
        )


def test_run_clientes_retraining_executes_full_flow(monkeypatch):
    from pipelines.examples.clientes.ml.run_clientes_retraining import run_clientes_retraining

    fake_ctx = SimpleNamespace(
        env="dev",
        project="clientes",
        naming=SimpleNamespace(
            use_catalog=False,
        ),
    )

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_retraining.get_context",
        lambda project, use_catalog: fake_ctx,
    )
    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_retraining.run_with_observability",
        lambda **kwargs: kwargs["fn"](SimpleNamespace(info=lambda *args, **kwargs: None)),
    )

    calls = []

    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_retraining.run_prepare_clientes_training_dataset",
        lambda **kwargs: calls.append("prepare_training"),
    )
    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_retraining.run_train_clientes_model",
        lambda **kwargs: (calls.append("train"), "new-model-v1")[1],
    )
    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_retraining.run_evaluate_clientes_model",
        lambda **kwargs: calls.append("evaluate"),
    )
    monkeypatch.setattr(
        "pipelines.examples.clientes.ml.run_clientes_retraining.execute_retraining_request",
        lambda **kwargs: {"request_status": "EXECUTED"},
    )

    result = run_clientes_retraining(
        spark=object(),
        request_payload={
            "model_name": "clientes_status_classifier",
            "model_version": "old-model-v0",
            "request_status": "APPROVED",
            "trigger_type": "DRIFT",
            "trigger_source": "drift_monitoring",
            "trigger_severity": "CRITICAL",
            "reason": "Drift critico",
            "requested_by": "mlops",
        },
        project="clientes",
        use_catalog=False,
        forced_run_id="retraining-run-2",
    )

    assert calls == ["prepare_training", "train", "evaluate"]
    assert result["request_status"] == "EXECUTED"
    assert result["new_model_version"] == "new-model-v1"
