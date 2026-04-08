import smtplib
from email.mime.text import MIMEText

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


def build_smtp_settings(
    *,
    alerting_config: dict,
    secrets_resolver,
) -> dict:
    smtp_secret_scope = alerting_config.get("smtp_secret_scope", "")
    smtp_host_key = alerting_config.get("smtp_host_key", "")
    smtp_port_key = alerting_config.get("smtp_port_key", "")
    smtp_username_key = alerting_config.get("smtp_username_key", "")
    smtp_password_key = alerting_config.get("smtp_password_key", "")

    if not all([smtp_secret_scope, smtp_host_key, smtp_port_key, smtp_username_key, smtp_password_key]):
        raise ValueError("Configuracao SMTP incompleta para notificacao por email")

    return {
        "host": secrets_resolver(smtp_secret_scope, smtp_host_key),
        "port": int(secrets_resolver(smtp_secret_scope, smtp_port_key)),
        "username": secrets_resolver(smtp_secret_scope, smtp_username_key),
        "password": secrets_resolver(smtp_secret_scope, smtp_password_key),
    }


def send_email_notification(
    *,
    payload: dict,
    smtp_settings: dict,
    sender_email: str | None = None,
) -> str:
    recipients = payload.get("recipients", [])
    subject = payload.get("subject", "")
    body = payload.get("body", "")

    if not recipients:
        raise ValueError("Payload de email sem recipients")

    from_email = sender_email or smtp_settings["username"]

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = ", ".join(recipients)

    with smtplib.SMTP(smtp_settings["host"], smtp_settings["port"]) as server:
        server.starttls()
        server.login(smtp_settings["username"], smtp_settings["password"])
        server.sendmail(from_email, recipients, msg.as_string())

    return "SENT"


def send_notification_plan(
    *,
    plan: list[dict],
    alerting_config: dict,
    secrets_resolver,
) -> list[dict]:
    results = []

    smtp_settings = None
    email_needed = any(item["channel"] == "email" for item in plan)

    if email_needed:
        smtp_settings = build_smtp_settings(
            alerting_config=alerting_config,
            secrets_resolver=secrets_resolver,
        )

    for item in plan:
        try:
            if item["channel"] == "email":
                status = send_email_notification(
                    payload=item,
                    smtp_settings=smtp_settings,
                )
            else:
                status = "SKIPPED"

            results.append(
                {
                    **item,
                    "delivery_status": status,
                }
            )
        except Exception as exc:
            results.append(
                {
                    **item,
                    "delivery_status": "FAILED",
                    "delivery_error": f"{type(exc).__name__}: {str(exc)}",
                }
            )

    return results
