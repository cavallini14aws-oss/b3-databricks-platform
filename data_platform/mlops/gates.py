from data_platform.mlops.policies import get_postprod_threshold


def evaluate_post_retraining_gate(
    *,
    config_path: str,
    metrics: dict[str, float],
) -> dict:
    f1_threshold = get_postprod_threshold(
        config_path=config_path,
        metric_name="f1",
    )

    current_f1 = metrics.get("f1")
    approved = f1_threshold is None or (current_f1 is not None and current_f1 >= f1_threshold)

    return {
        "gate_name": "post_retraining",
        "approved": bool(approved),
        "reason": (
            f"f1={current_f1} atende threshold={f1_threshold}"
            if approved
            else f"f1={current_f1} abaixo do threshold={f1_threshold}"
        ),
    }


def evaluate_post_rollback_gate(
    *,
    require_smoke_after_rollback: bool,
    smoke_completed: bool,
) -> dict:
    approved = (not require_smoke_after_rollback) or smoke_completed

    return {
        "gate_name": "post_rollback",
        "approved": bool(approved),
        "reason": (
            "Smoke validado apos rollback"
            if approved
            else "Smoke obrigatorio apos rollback ainda nao validado"
        ),
    }
