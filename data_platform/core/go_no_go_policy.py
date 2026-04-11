from data_platform.core.activation_preflight import run_activation_preflight


def evaluate_go_no_go() -> dict:
    preflight = run_activation_preflight()
    status = preflight.get("status", "BLOCK")

    decision = "NO_GO"
    if status == "PASS":
        decision = "GO"
    elif status == "WARN":
        decision = "GO_WITH_RISK"

    return {
        "decision": decision,
        "preflight_status": status,
        "environments": preflight.get("environments", {}),
    }
