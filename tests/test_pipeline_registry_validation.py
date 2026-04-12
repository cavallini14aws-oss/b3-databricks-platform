from data_platform.core.pipeline_registry_validation import (
    validate_pipeline_registry_artifacts,
)


def test_validate_pipeline_registry_artifacts_returns_expected_shape(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.pipeline_registry_validation.Path.exists",
        lambda self: True,
    )

    result = validate_pipeline_registry_artifacts()

    assert result["valid"] is True
    assert len(result["required_artifacts"]) == 7
    assert result["missing_artifacts"] == []
