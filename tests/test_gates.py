from data_platform.mlops.gates import (
    evaluate_post_retraining_gate,
    evaluate_post_rollback_gate,
)


def test_evaluate_post_retraining_gate_approves_when_metric_meets_threshold(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.gates.get_postprod_threshold",
        lambda **kwargs: 0.70,
    )

    result = evaluate_post_retraining_gate(
        config_path="config/env/dev.yml",
        metrics={"f1": 0.81},
    )

    assert result["approved"] is True


def test_evaluate_post_retraining_gate_rejects_when_metric_below_threshold(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.gates.get_postprod_threshold",
        lambda **kwargs: 0.70,
    )

    result = evaluate_post_retraining_gate(
        config_path="config/env/dev.yml",
        metrics={"f1": 0.61},
    )

    assert result["approved"] is False


def test_evaluate_post_rollback_gate_requires_smoke_when_enabled():
    result = evaluate_post_rollback_gate(
        require_smoke_after_rollback=True,
        smoke_completed=False,
    )

    assert result["approved"] is False


def test_evaluate_post_rollback_gate_approves_when_smoke_completed():
    result = evaluate_post_rollback_gate(
        require_smoke_after_rollback=True,
        smoke_completed=True,
    )

    assert result["approved"] is True
