from data_platform.mlops.smoke_runs import SMOKE_RUN_SCHEMA


def test_smoke_run_schema_has_expected_fields():
    field_names = [field.name for field in SMOKE_RUN_SCHEMA.fields]

    assert field_names == [
        "event_timestamp",
        "env",
        "project",
        "component",
        "model_name",
        "model_version",
        "status",
        "input_table",
        "output_table",
        "run_id",
        "message",
    ]
