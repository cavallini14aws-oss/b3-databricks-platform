from data_platform.core.activation_control import (
    DEFAULT_ACTIVATION_CONTROL_PATH,
    load_activation_control,
)
from data_platform.core.activation_validator import validate_activation_environment


def build_activation_readiness_report(
    config_path: str = DEFAULT_ACTIVATION_CONTROL_PATH,
) -> dict:
    control = load_activation_control(config_path)
    environments = control.get("environments", {})

    env_reports = {}
    ready_count = 0

    for env_name in environments.keys():
        validation = validate_activation_environment(
            env=env_name,
            config_path=config_path,
        )

        env_cfg = environments.get(env_name, {})
        go_live_cfg = env_cfg.get("go_live", {})
        blockers = go_live_cfg.get("blockers", [])

        env_report = {
            "ready": validation["ready"] and not blockers,
            "errors": validation["errors"],
            "warnings": validation["warnings"],
            "blockers": blockers,
            "go_live_ready_flag": bool(go_live_cfg.get("ready", False)),
            "notifications_enabled": bool(env_cfg.get("notifications", {}).get("enabled", False)),
            "databricks_enabled": bool(env_cfg.get("databricks", {}).get("enabled", False)),
            "jobs_enabled": {
                job_name: bool(job_cfg.get("enabled", False))
                for job_name, job_cfg in env_cfg.get("jobs", {}).items()
            },
        }

        if env_report["ready"]:
            ready_count += 1

        env_reports[env_name] = env_report

    total_envs = len(env_reports)
    readiness_score = round(ready_count / total_envs, 4) if total_envs else 0.0

    return {
        "project": control.get("project", {}).get("name", "unknown"),
        "owner": control.get("project", {}).get("owner", "unknown"),
        "criticality": control.get("project", {}).get("criticality", "unknown"),
        "regulator_context": control.get("project", {}).get("regulator_context", "unknown"),
        "ready_environments": ready_count,
        "total_environments": total_envs,
        "readiness_score": readiness_score,
        "environments": env_reports,
    }
