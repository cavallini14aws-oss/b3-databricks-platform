from dataclasses import dataclass

from data_platform.core.activation_control import get_activation_databricks_config, get_activation_jobs_config
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


def load_job_config(env: str | None = None) -> JobConfig:
    resolved_env = env or get_env()
    config = load_yaml_config(f"config/jobs/{resolved_env}.yml")

    job = config.get("job", {})
    promotion = config.get("promotion", {})
    quality_gates = config.get("quality_gates", {})
    ml = quality_gates.get("ml", {})

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

    return JobConfig(
        environment=job.get("environment", resolved_env),
        cluster_key=job.get("cluster_key", f"{resolved_env}-cluster"),
        cluster_mode=job.get("cluster_mode", "existing_or_job_cluster"),
        workspace_root=job.get("workspace_root", "/Workspace/Repos"),
        default_timeout_seconds=int(job.get("default_timeout_seconds", 3600)),
        max_retries=int(job.get("max_retries", 0)),
        use_catalog=bool(job.get("use_catalog", False)),
        default_config_path=job.get("default_config_path"),
        promotion_rules=rules,
        ml_quality_gates=MlQualityGates(
            minimum_accuracy=float(ml.get("minimum_accuracy", 0.0)),
            minimum_f1=float(ml.get("minimum_f1", 0.0)),
            minimum_auc=float(ml.get("minimum_auc", 0.0)),
        ),
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
