from data_platform.mlops.registry import get_latest_valid_model_entry, update_model_status
from data_platform.mlops.model_states import ROLLED_BACK


def validate_rollback_target(target_env: str) -> None:
    valid_targets = {"hml", "prd"}
    if target_env not in valid_targets:
        raise ValueError(
            f"Rollback invalido para target_env={target_env}. "
            f"Permitidos: {sorted(valid_targets)}"
        )


def prepare_rollback_request(
    spark,
    model_name: str,
    target_env: str,
    project: str = "template",
    use_catalog: bool = False,
):
    validate_rollback_target(target_env)

    entry = get_latest_valid_model_entry(
        spark=spark,
        model_name=model_name,
        project=project,
        use_catalog=use_catalog,
    )

    update_model_status(
        spark=spark,
        model_name=model_name,
        model_version=entry["model_version"],
        status=ROLLED_BACK,
        project=project,
        use_catalog=use_catalog,
    )

    return {
        "action": "ROLLBACK_PREPARED",
        "model_name": model_name,
        "model_version": entry["model_version"],
        "artifact_path": entry["artifact_path"],
        "target_env": target_env,
        "status": ROLLED_BACK,
    }
