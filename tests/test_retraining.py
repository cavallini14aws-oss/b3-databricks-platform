from data_platform.mlops.retraining import (
    RETRAINING_REQUEST_SCHEMA,
    validate_retraining_request_status,
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
