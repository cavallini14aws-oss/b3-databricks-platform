from dataclasses import dataclass

from b3_platform.core.job_config import load_job_config


@dataclass(frozen=True)
class DeploymentContract:
    source_env: str
    target_env: str
    target_cluster_key: str
    target_workspace_root: str
    target_timeout_seconds: int
    target_max_retries: int


def build_deployment_contract(
    source_env: str,
    target_env: str,
) -> DeploymentContract:
    target_job_config = load_job_config(target_env)

    return DeploymentContract(
        source_env=source_env,
        target_env=target_env,
        target_cluster_key=target_job_config.cluster_key,
        target_workspace_root=target_job_config.workspace_root,
        target_timeout_seconds=target_job_config.default_timeout_seconds,
        target_max_retries=target_job_config.max_retries,
    )
