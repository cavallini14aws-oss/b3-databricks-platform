from dataclasses import dataclass

from b3_platform.core.config_loader import load_yaml_config
from b3_platform.core.env import get_env


@dataclass(frozen=True)
class PromotionConfig:
    source_env: str
    target_env: str | None
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
    promotion: PromotionConfig
    ml_quality_gates: MlQualityGates


def load_job_config(env: str | None = None) -> JobConfig:
    resolved_env = env or get_env()
    config = load_yaml_config(f"config/jobs/{resolved_env}.yml")

    job = config.get("job", {})
    promotion = config.get("promotion", {})
    quality_gates = config.get("quality_gates", {})
    ml = quality_gates.get("ml", {})

    return JobConfig(
        environment=job.get("environment", resolved_env),
        cluster_key=job.get("cluster_key", f"{resolved_env}-cluster"),
        cluster_mode=job.get("cluster_mode", "existing_or_job_cluster"),
        workspace_root=job.get("workspace_root", "/Workspace/Repos"),
        default_timeout_seconds=int(job.get("default_timeout_seconds", 3600)),
        max_retries=int(job.get("max_retries", 0)),
        promotion=PromotionConfig(
            source_env=promotion.get("source_env", resolved_env),
            target_env=promotion.get("target_env"),
            requires_approval=bool(promotion.get("requires_approval", False)),
            require_tests_passed=bool(promotion.get("require_tests_passed", True)),
            require_quality_gates=bool(promotion.get("require_quality_gates", False)),
        ),
        ml_quality_gates=MlQualityGates(
            minimum_accuracy=float(ml.get("minimum_accuracy", 0.0)),
            minimum_f1=float(ml.get("minimum_f1", 0.0)),
            minimum_auc=float(ml.get("minimum_auc", 0.0)),
        ),
    )
