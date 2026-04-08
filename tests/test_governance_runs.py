from data_platform.governance.governance_runs import GOVERNANCE_RUN_SCHEMA


def test_governance_run_schema_has_expected_fields():
    field_names = [field.name for field in GOVERNANCE_RUN_SCHEMA.fields]

    assert field_names == [
        "event_timestamp",
        "env",
        "project",
        "component",
        "model_name",
        "model_version",
        "source_env",
        "target_env",
        "artifact_path",
        "status",
        "run_id",
        "message",
    ]
