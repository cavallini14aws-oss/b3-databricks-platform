from data_platform.core.schema_validation import validate_required_schema_specs


def test_validate_required_schema_specs_returns_expected_shape(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.schema_validation.Path.exists",
        lambda self: True,
    )

    result = validate_required_schema_specs()

    assert result["valid"] is True
    assert len(result["required_specs"]) == 3
    assert result["missing_specs"] == []
