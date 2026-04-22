from dataclasses import dataclass

from data_platform.core.activation_control import (
    get_activation_databricks_config,
    get_activation_jobs_config,
)
from data_platform.core.config_loader import load_yaml_config
from data_platform.core.env import get_env


@dataclass(frozen=True)
class PromotionRule:
    source_env: str
    target_env: str
    requires_approval: bool
    require_tests_passed: bool
    require_quality_gates: bool


@dataclass(frozen=True)
class MlQualityGates:
    minimum_accuracy: float
    minimum_f1: float
    minimum_auc: float


@dataclass(frozen=True)
class JobConfig:
    environment: str
    cluster_key: str
    cluster_mode: str
    workspace_root: str
    default_timeout_seconds: int
    max_retries: int
    use_catalog: bool
    default_config_path: str | None
    promotion_rules: list[PromotionRule]
    ml_quality_gates: MlQualityGates


def _load_legacy_job_yaml(env: str) -> dict:
    return load_yaml_config(f"config/jobs/{env}.yml")


def _build_promotion_rules(config: dict) -> list[PromotionRule]:
    promotion = config.get("promotion", {})
    rules = []

    for item in promotion.get("rules", []):
        rules.append(
            PromotionRule(
                source_env=item["source_env"],
                target_env=item["target_env"],
                requires_approval=bool(item.get("requires_approval", False)),
                require_tests_passed=bool(item.get("require_tests_passed", True)),
                require_quality_gates=bool(item.get("require_quality_gates", False)),
            )
        )

    return rules


def _build_ml_quality_gates(config: dict) -> MlQualityGates:
    quality_gates = config.get("quality_gates", {})
    ml = quality_gates.get("ml", {})

    return MlQualityGates(
        minimum_accuracy=float(ml.get("minimum_accuracy", 0.0)),
        minimum_f1=float(ml.get("minimum_f1", 0.0)),
        minimum_auc=float(ml.get("minimum_auc", 0.0)),
    )


def load_job_config(
    env: str | None = None,
    activation_config_path: str = "config/activation/operational_control.yml",
) -> JobConfig:
    resolved_env = env or get_env()

    legacy_config = _load_legacy_job_yaml(resolved_env)
    legacy_job = legacy_config.get("job", {})

    activation_databricks = get_activation_databricks_config(
        env=resolved_env,
        config_path=activation_config_path,
    )
    activation_jobs = get_activation_jobs_config(
        env=resolved_env,
        config_path=activation_config_path,
    )

    environment = legacy_job.get("environment", resolved_env)
    cluster_key = legacy_job.get("cluster_key", f"{resolved_env}-cluster")
    cluster_mode = activation_databricks.get("cluster_mode", legacy_job.get("cluster_mode", "existing_or_job_cluster"))
    workspace_root = legacy_job.get("workspace_root", "/Workspace/Repos")
    default_timeout_seconds = int(legacy_job.get("default_timeout_seconds", 3600))
    max_retries = int(legacy_job.get("max_retries", 0))
    default_config_path = legacy_job.get("default_config_path")

    activation_use_catalog = activation_databricks.get("use_catalog")
    if activation_use_catalog is None:
        use_catalog = bool(legacy_job.get("use_catalog", False))
    else:
        use_catalog = bool(activation_use_catalog)

    workspace_root = activation_databricks.get("workspace_root", workspace_root)
    default_config_path = activation_databricks.get(
        "default_config_path",
        default_config_path,
    )

    operational_cycle = activation_jobs.get("operational_cycle", {})
    if operational_cycle:
        default_timeout_seconds = int(
            operational_cycle.get("timeout_seconds", default_timeout_seconds)
        )
        max_retries = int(operational_cycle.get("retries", max_retries))

    return JobConfig(
        environment=environment,
        cluster_key=cluster_key,
        cluster_mode=cluster_mode,
        workspace_root=workspace_root,
        default_timeout_seconds=default_timeout_seconds,
        max_retries=max_retries,
        use_catalog=use_catalog,
        default_config_path=default_config_path,
        promotion_rules=_build_promotion_rules(legacy_config),
        ml_quality_gates=_build_ml_quality_gates(legacy_config),
    )


def load_job_runtime_from_activation_control(
    env: str | None = None,
    config_path: str = "config/activation/operational_control.yml",
) -> dict:
    databricks_cfg = get_activation_databricks_config(
        env=env,
        config_path=config_path,
    )
    jobs_cfg = get_activation_jobs_config(
        env=env,
        config_path=config_path,
    )

    return {
        "databricks": databricks_cfg,
        "jobs": jobs_cfg,
    }
