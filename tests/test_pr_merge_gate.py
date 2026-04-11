from data_platform.core.pr_merge_gate import run_pr_merge_gate


def test_run_pr_merge_gate_blocks_when_any_critical_gate_fails(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.pr_merge_gate.validate_required_schema_specs",
        lambda: {"valid": False, "missing_specs": ["x.sql"]},
    )
    monkeypatch.setattr(
        "data_platform.core.pr_merge_gate.validate_pipeline_registry_artifacts",
        lambda: {"valid": True, "missing_artifacts": []},
    )
    monkeypatch.setattr(
        "data_platform.core.pr_merge_gate.run_activation_preflight",
        lambda: {"status": "PASS"},
    )
    monkeypatch.setattr(
        "data_platform.core.pr_merge_gate.evaluate_go_no_go",
        lambda: {"decision": "GO"},
    )

    result = run_pr_merge_gate()

    assert result["decision"] == "BLOCK"
    assert "schema_validation_failed" in result["errors"]


def test_run_pr_merge_gate_allows_with_risk_on_warning_only(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.pr_merge_gate.validate_required_schema_specs",
        lambda: {"valid": True, "missing_specs": []},
    )
    monkeypatch.setattr(
        "data_platform.core.pr_merge_gate.validate_pipeline_registry_artifacts",
        lambda: {"valid": True, "missing_artifacts": []},
    )
    monkeypatch.setattr(
        "data_platform.core.pr_merge_gate.run_activation_preflight",
        lambda: {"status": "WARN"},
    )
    monkeypatch.setattr(
        "data_platform.core.pr_merge_gate.evaluate_go_no_go",
        lambda: {"decision": "GO_WITH_RISK"},
    )

    result = run_pr_merge_gate()

    assert result["decision"] == "ALLOW_WITH_RISK"
    assert "activation_preflight_warn" in result["warnings"]


def test_run_pr_merge_gate_allows_when_everything_passes(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.pr_merge_gate.validate_required_schema_specs",
        lambda: {"valid": True, "missing_specs": []},
    )
    monkeypatch.setattr(
        "data_platform.core.pr_merge_gate.validate_pipeline_registry_artifacts",
        lambda: {"valid": True, "missing_artifacts": []},
    )
    monkeypatch.setattr(
        "data_platform.core.pr_merge_gate.run_activation_preflight",
        lambda: {"status": "PASS"},
    )
    monkeypatch.setattr(
        "data_platform.core.pr_merge_gate.evaluate_go_no_go",
        lambda: {"decision": "GO"},
    )

    result = run_pr_merge_gate()

    assert result["decision"] == "ALLOW"
    assert result["errors"] == []
