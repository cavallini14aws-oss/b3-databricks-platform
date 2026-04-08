from data_platform.core.activation_control import (
    DEFAULT_ACTIVATION_CONTROL_PATH,
    get_activation_environment_config,
)


def validate_activation_environment(
    env: str,
    config_path: str = DEFAULT_ACTIVATION_CONTROL_PATH,
) -> dict:
    cfg = get_activation_environment_config(env=env, config_path=config_path)

    errors = []
    warnings = []

    databricks_cfg = cfg.get("databricks", {})
    notifications_cfg = cfg.get("notifications", {})
    jobs_cfg = cfg.get("jobs", {})
    go_live_cfg = cfg.get("go_live", {})

    if not databricks_cfg.get("workspace_host_key"):
        errors.append("databricks.workspace_host_key vazio")
    if not databricks_cfg.get("databricks_token_key"):
        errors.append("databricks.databricks_token_key vazio")
    if not databricks_cfg.get("cluster_id_key"):
        errors.append("databricks.cluster_id_key vazio")
    if not databricks_cfg.get("secret_scope"):
        errors.append("databricks.secret_scope vazio")

    if notifications_cfg.get("enabled", False):
        severity_min = notifications_cfg.get("severity_min")
        if not severity_min:
            errors.append("notifications.severity_min vazio")

        email_cfg = notifications_cfg.get("email", {})
        if email_cfg.get("enabled", False):
            recipients = email_cfg.get("recipients", [])
            if not recipients:
                errors.append("notifications.email.recipients vazio")

            smtp_cfg = email_cfg.get("smtp", {})
            for key in ["scope", "host_key", "port_key", "username_key", "password_key"]:
                if not smtp_cfg.get(key):
                    errors.append(f"notifications.email.smtp.{key} vazio")
            if not smtp_cfg.get("ready", False):
                warnings.append("notifications.email.smtp.ready=false")

        slack_cfg = notifications_cfg.get("slack", {})
        if slack_cfg.get("enabled", False):
            webhook_cfg = slack_cfg.get("webhook", {})
            for key in ["scope", "key"]:
                if not webhook_cfg.get(key):
                    errors.append(f"notifications.slack.webhook.{key} vazio")
            if not webhook_cfg.get("ready", False):
                warnings.append("notifications.slack.webhook.ready=false")

        teams_cfg = notifications_cfg.get("teams", {})
        if teams_cfg.get("enabled", False):
            webhook_cfg = teams_cfg.get("webhook", {})
            for key in ["scope", "key"]:
                if not webhook_cfg.get(key):
                    errors.append(f"notifications.teams.webhook.{key} vazio")
            if not webhook_cfg.get("ready", False):
                warnings.append("notifications.teams.webhook.ready=false")

    for job_name, job_cfg in jobs_cfg.items():
        if job_cfg.get("enabled", False) and not job_cfg.get("cron"):
            errors.append(f"jobs.{job_name}.cron vazio")

    blockers = go_live_cfg.get("blockers", [])
    if blockers:
        warnings.append(f"go_live.blockers presentes: {', '.join(blockers)}")

    return {
        "env": env,
        "ready": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
    }
