from data_platform.core.activation_control import (
    DEFAULT_ACTIVATION_CONTROL_PATH,
    load_activation_control,
)
from data_platform.core.activation_validator import validate_activation_environment


def build_go_live_report(
    config_path: str = DEFAULT_ACTIVATION_CONTROL_PATH,
) -> dict:
    control = load_activation_control(config_path)
    project_cfg = control.get("project", {})
    environments = control.get("environments", {})

    report = {
        "project": project_cfg.get("name", "unknown"),
        "owner": project_cfg.get("owner", "unknown"),
        "criticality": project_cfg.get("criticality", "unknown"),
        "regulator_context": project_cfg.get("regulator_context", "unknown"),
        "environments": {},
    }

    for env_name, env_cfg in environments.items():
        validation = validate_activation_environment(
            env=env_name,
            config_path=config_path,
        )

        report["environments"][env_name] = {
            "ready": validation["ready"],
            "errors": validation["errors"],
            "warnings": validation["warnings"],
            "go_live": env_cfg.get("go_live", {}),
            "databricks": env_cfg.get("databricks", {}),
            "notifications": {
                "enabled": env_cfg.get("notifications", {}).get("enabled", False),
                "severity_min": env_cfg.get("notifications", {}).get("severity_min"),
                "email_enabled": env_cfg.get("notifications", {}).get("email", {}).get("enabled", False),
                "slack_enabled": env_cfg.get("notifications", {}).get("slack", {}).get("enabled", False),
                "teams_enabled": env_cfg.get("notifications", {}).get("teams", {}).get("enabled", False),
            },
            "retraining": env_cfg.get("retraining", {}),
            "thresholds": env_cfg.get("thresholds", {}),
            "retention": env_cfg.get("retention", {}),
            "access_control": env_cfg.get("access_control", {}),
            "jobs": env_cfg.get("jobs", {}),
            "cicd": env_cfg.get("cicd", {}),
        }

    return report
