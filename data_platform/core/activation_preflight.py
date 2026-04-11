from data_platform.core.activation_readiness_report import build_activation_readiness_report
from data_platform.core.go_live_report import build_go_live_report


def run_activation_preflight() -> dict:
    readiness = build_activation_readiness_report()
    go_live = build_go_live_report()

    environments = {}

    for env_name, readiness_env in readiness.get("environments", {}).items():
        go_live_env = go_live.get("environments", {}).get(env_name, {})

        errors = readiness_env.get("errors", []) + go_live_env.get("errors", [])
        warnings = readiness_env.get("warnings", []) + go_live_env.get("warnings", [])
        blockers = readiness_env.get("blockers", []) + go_live_env.get("go_live", {}).get("blockers", [])

        status = "PASS"
        if blockers or errors:
            status = "BLOCK"
        elif warnings:
            status = "WARN"

        environments[env_name] = {
            "status": status,
            "errors": errors,
            "warnings": warnings,
            "blockers": blockers,
        }

    overall_status = "PASS"
    if any(env["status"] == "BLOCK" for env in environments.values()):
        overall_status = "BLOCK"
    elif any(env["status"] == "WARN" for env in environments.values()):
        overall_status = "WARN"

    return {
        "status": overall_status,
        "environments": environments,
    }
