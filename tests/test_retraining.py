from data_platform.mlops.retraining import (
    RETRAINING_REQUEST_SCHEMA,
    approve_retraining_request,
    execute_retraining_request,
    maybe_open_retraining_request_from_drift,
    maybe_open_retraining_request_from_postprod,
    reject_retraining_request,
    validate_retraining_request_status,
    validate_retraining_transition,
    validate_retraining_trigger_type,
)


def test_retraining_request_schema_has_expected_fields():
    field_names = [field.name for field in RETRAINING_REQUEST_SCHEMA.fields]

    assert field_names == [
        "event_timestamp",
        "env",
        "project",
        "model_name",
        "model_version",
        "trigger_type",
        "trigger_source",
        "trigger_severity",
        "reason",
        "request_status",
        "requested_by",
        "run_id",
    ]


def test_validate_retraining_trigger_type_accepts_valid_values():
    validate_retraining_trigger_type("DRIFT")
    validate_retraining_trigger_type("POSTPROD_DEGRADATION")
    validate_retraining_trigger_type("MANUAL")


def test_validate_retraining_trigger_type_blocks_invalid_value():
    try:
        validate_retraining_trigger_type("AUTO")
        assert False, "Era esperado ValueError"
    except ValueError as exc:
        assert "Trigger type invalido" in str(exc)


def test_validate_retraining_request_status_accepts_valid_values():
    validate_retraining_request_status("OPEN")
    validate_retraining_request_status("APPROVED")
    validate_retraining_request_status("REJECTED")
    validate_retraining_request_status("EXECUTED")


def test_validate_retraining_request_status_blocks_invalid_value():
    try:
        validate_retraining_request_status("PENDING")
        assert False, "Era esperado ValueError"
    except ValueError as exc:
        assert "Request status invalido" in str(exc)


def test_validate_retraining_transition_accepts_valid_transitions():
    validate_retraining_transition("OPEN", "APPROVED")
    validate_retraining_transition("OPEN", "REJECTED")
    validate_retraining_transition("APPROVED", "EXECUTED")


def test_validate_retraining_transition_blocks_invalid_transition():
    try:
        validate_retraining_transition("REJECTED", "EXECUTED")
        assert False, "Era esperado ValueError"
    except ValueError as exc:
        assert "Transicao invalida de retraining request" in str(exc)


def test_approve_retraining_request_returns_approved(monkeypatch):
    captured = []

    monkeypatch.setattr(
        "data_platform.mlops.retraining.persist_retraining_request",
        lambda **kwargs: captured.append(kwargs),
    )

    result = approve_retraining_request(
        spark=object(),
        model_name="clientes_status_classifier",
        model_version="v123",
        trigger_type="DRIFT",
        trigger_source="drift_monitoring",
        trigger_severity="CRITICAL",
        reason="Drift critico detectado",
        requested_by="mlops",
        run_id="run-1",
        project="clientes",
        use_catalog=False,
    )

    assert result["previous_status"] == "OPEN"
    assert result["request_status"] == "APPROVED"
    assert captured[0]["request_status"] == "APPROVED"


def test_reject_retraining_request_returns_rejected(monkeypatch):
    captured = []

    monkeypatch.setattr(
        "data_platform.mlops.retraining.persist_retraining_request",
        lambda **kwargs: captured.append(kwargs),
    )

    result = reject_retraining_request(
        spark=object(),
        model_name="clientes_status_classifier",
        model_version="v123",
        trigger_type="DRIFT",
        trigger_source="drift_monitoring",
        trigger_severity="WARNING",
        reason="Drift moderado, sem acao imediata",
        requested_by="mlops",
        run_id="run-2",
        project="clientes",
        use_catalog=False,
    )

    assert result["previous_status"] == "OPEN"
    assert result["request_status"] == "REJECTED"
    assert captured[0]["request_status"] == "REJECTED"


def test_execute_retraining_request_returns_executed(monkeypatch):
    captured = []

    monkeypatch.setattr(
        "data_platform.mlops.retraining.persist_retraining_request",
        lambda **kwargs: captured.append(kwargs),
    )

    result = execute_retraining_request(
        spark=object(),
        model_name="clientes_status_classifier",
        model_version="v124",
        trigger_type="POSTPROD_DEGRADATION",
        trigger_source="postprod_evaluation",
        trigger_severity="CRITICAL",
        reason="Queda real de F1 em producao",
        requested_by="mlops",
        run_id="run-3",
        project="clientes",
        use_catalog=False,
    )

    assert result["previous_status"] == "APPROVED"
    assert result["request_status"] == "EXECUTED"
    assert captured[0]["request_status"] == "EXECUTED"


def test_maybe_open_retraining_request_from_drift_opens_for_critical(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.retraining.open_retraining_request",
        lambda **kwargs: {"request_status": "OPEN", "trigger_type": "DRIFT"},
    )

    result = maybe_open_retraining_request_from_drift(
        spark=object(),
        drift_event={
            "model_name": "clientes_status_classifier",
            "model_version": "v123",
            "severity": "CRITICAL",
            "message": "Drift critico detectado",
        },
        requested_by="mlops",
        run_id="run-1",
        project="clientes",
        use_catalog=False,
    )

    assert result is not None
    assert result["request_status"] == "OPEN"
    assert result["trigger_type"] == "DRIFT"


def test_maybe_open_retraining_request_from_drift_skips_non_critical():
    result = maybe_open_retraining_request_from_drift(
        spark=object(),
        drift_event={
            "model_name": "clientes_status_classifier",
            "model_version": "v123",
            "severity": "WARNING",
            "message": "Drift moderado",
        },
        requested_by="mlops",
        run_id="run-2",
        project="clientes",
        use_catalog=False,
    )

    assert result is None


def test_maybe_open_retraining_request_from_postprod_opens_below_threshold(monkeypatch):
    monkeypatch.setattr(
        "data_platform.mlops.retraining.open_retraining_request",
        lambda **kwargs: {"request_status": "OPEN", "trigger_type": "POSTPROD_DEGRADATION"},
    )

    result = maybe_open_retraining_request_from_postprod(
        spark=object(),
        model_name="clientes_status_classifier",
        model_version="v123",
        metric_name="f1",
        metric_value=0.61,
        threshold_value=0.70,
        requested_by="mlops",
        run_id="run-3",
        project="clientes",
        use_catalog=False,
    )

    assert result is not None
    assert result["request_status"] == "OPEN"
    assert result["trigger_type"] == "POSTPROD_DEGRADATION"


def test_maybe_open_retraining_request_from_postprod_skips_when_metric_ok():
    result = maybe_open_retraining_request_from_postprod(
        spark=object(),
        model_name="clientes_status_classifier",
        model_version="v123",
        metric_name="f1",
        metric_value=0.81,
        threshold_value=0.70,
        requested_by="mlops",
        run_id="run-4",
        project="clientes",
        use_catalog=False,
    )

    assert result is None
