from data_platform.core.activation_preflight import run_activation_preflight
from data_platform.core.go_no_go_policy import evaluate_go_no_go
from data_platform.core.pipeline_registry_validation import (
    validate_pipeline_registry_artifacts,
)
from data_platform.core.schema_validation import validate_required_schema_specs


def run_pr_merge_gate(mode: str = "full") -> dict:
    schema_validation = validate_required_schema_specs()
    pipeline_validation = validate_pipeline_registry_artifacts()
    preflight = run_activation_preflight()
    go_no_go = evaluate_go_no_go()

    errors = []
    warnings = []

    if not schema_validation["valid"]:
        errors.append("schema_validation_failed")

    if not pipeline_validation["valid"]:
        errors.append("pipeline_registry_validation_failed")

    if mode == "full":
        if preflight["status"] == "BLOCK":
            errors.append("activation_preflight_blocked")
        elif preflight["status"] == "WARN":
            warnings.append("activation_preflight_warn")

        if go_no_go["decision"] == "NO_GO":
            errors.append("go_no_go_policy_blocked")
        elif go_no_go["decision"] == "GO_WITH_RISK":
            warnings.append("go_no_go_policy_warn")
    elif mode == "technical":
        if preflight["status"] == "WARN":
            warnings.append("activation_preflight_warn")

        if go_no_go["decision"] == "GO_WITH_RISK":
            warnings.append("go_no_go_policy_warn")
    else:
        raise ValueError(f"Modo invalido para pr merge gate: {mode}")

    decision = "ALLOW"
    if errors:
        decision = "BLOCK"
    elif warnings:
        decision = "ALLOW_WITH_RISK"

    return {
        "decision": decision,
        "mode": mode,
        "errors": errors,
        "warnings": warnings,
        "schema_validation": schema_validation,
        "pipeline_registry_validation": pipeline_validation,
        "activation_preflight": preflight,
        "go_no_go": go_no_go,
    }
