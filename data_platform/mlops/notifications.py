from data_platform.core.config_loader import load_yaml_config


def load_alerting_config(config_path: str) -> dict:
    config = load_yaml_config(config_path)
    return config.get("alerting", {})


def is_notification_channel_enabled(alerting_config: dict, channel: str) -> bool:
    mapping = {
        "email": "email_enabled",
        "slack": "slack_enabled",
        "teams": "teams_enabled",
    }

    key = mapping.get(channel)
    if key is None:
        raise ValueError(f"Canal de notificacao invalido: {channel}")

    return bool(alerting_config.get(key, False))


def parse_recipients(recipients: str | None) -> list[str]:
    if not recipients:
        return []

    return [
        item.strip()
        for item in recipients.split(",")
        if item.strip()
    ]


def build_email_notification_payload(
    *,
    recipients: list[str],
    subject: str,
    body: str,
) -> dict:
    return {
        "channel": "email",
        "recipients": recipients,
        "subject": subject,
        "body": body,
    }


def build_slack_notification_payload(
    *,
    message: str,
    webhook_key: str | None,
) -> dict:
    return {
        "channel": "slack",
        "message": message,
        "webhook_key": webhook_key,
    }


def build_teams_notification_payload(
    *,
    message: str,
    webhook_key: str | None,
) -> dict:
    return {
        "channel": "teams",
        "message": message,
        "webhook_key": webhook_key,
    }


def build_notification_plan(
    *,
    alerting_config: dict,
    subject: str,
    message: str,
) -> list[dict]:
    plan = []

    if is_notification_channel_enabled(alerting_config, "email"):
        recipients = parse_recipients(alerting_config.get("recipients"))
        if recipients:
            plan.append(
                build_email_notification_payload(
                    recipients=recipients,
                    subject=subject,
                    body=message,
                )
            )

    if is_notification_channel_enabled(alerting_config, "slack"):
        plan.append(
            build_slack_notification_payload(
                message=message,
                webhook_key=alerting_config.get("slack_webhook_key"),
            )
        )

    if is_notification_channel_enabled(alerting_config, "teams"):
        plan.append(
            build_teams_notification_payload(
                message=message,
                webhook_key=alerting_config.get("teams_webhook_key"),
            )
        )

    return plan
