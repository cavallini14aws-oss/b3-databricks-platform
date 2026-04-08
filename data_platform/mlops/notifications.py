import json
import smtplib
import urllib.request
from email.mime.text import MIMEText

from data_platform.core.activation_control import get_activation_notifications_config
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


def resolve_webhook_url(
    *,
    alerting_config: dict,
    channel: str,
    secrets_resolver,
) -> str:
    webhook_secret_scope = alerting_config.get("webhook_secret_scope", "")

    if channel == "slack":
        webhook_key_name = alerting_config.get("slack_webhook_key", "")
    elif channel == "teams":
        webhook_key_name = alerting_config.get("teams_webhook_key", "")
    else:
        raise ValueError(f"Canal webhook invalido: {channel}")

    if not webhook_secret_scope or not webhook_key_name:
        raise ValueError(f"Configuracao de webhook incompleta para canal {channel}")

    return secrets_resolver(webhook_secret_scope, webhook_key_name)


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


def send_slack_notification(
    *,
    payload: dict,
    webhook_url: str,
) -> str:
    body = json.dumps({"text": payload["message"]}).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request) as response:
        status_code = getattr(response, "status", 200)
        if status_code >= 400:
            raise RuntimeError(f"Slack webhook retornou status {status_code}")

    return "SENT"


def send_teams_notification(
    *,
    payload: dict,
    webhook_url: str,
) -> str:
    body = json.dumps({"text": payload["message"]}).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request) as response:
        status_code = getattr(response, "status", 200)
        if status_code >= 400:
            raise RuntimeError(f"Teams webhook retornou status {status_code}")

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
            elif item["channel"] == "slack":
                webhook_url = resolve_webhook_url(
                    alerting_config=alerting_config,
                    channel="slack",
                    secrets_resolver=secrets_resolver,
                )
                status = send_slack_notification(
                    payload=item,
                    webhook_url=webhook_url,
                )
            elif item["channel"] == "teams":
                webhook_url = resolve_webhook_url(
                    alerting_config=alerting_config,
                    channel="teams",
                    secrets_resolver=secrets_resolver,
                )
                status = send_teams_notification(
                    payload=item,
                    webhook_url=webhook_url,
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


def load_alerting_config_from_activation_control(
    env: str | None = None,
    config_path: str = "config/activation/operational_control.yml",
) -> dict:
    notifications_cfg = get_activation_notifications_config(
        env=env,
        config_path=config_path,
    )

    email_cfg = notifications_cfg.get("email", {})
    slack_cfg = notifications_cfg.get("slack", {})
    teams_cfg = notifications_cfg.get("teams", {})

    recipients = email_cfg.get("recipients", [])
    recipients_str = ",".join(recipients)

    smtp_cfg = email_cfg.get("smtp", {})
    slack_webhook_cfg = slack_cfg.get("webhook", {})
    teams_webhook_cfg = teams_cfg.get("webhook", {})

    return {
        "enable_alerting": bool(notifications_cfg.get("enabled", False)),
        "severity_min": notifications_cfg.get("severity_min", "WARNING"),
        "email_enabled": bool(email_cfg.get("enabled", False)),
        "slack_enabled": bool(slack_cfg.get("enabled", False)),
        "teams_enabled": bool(teams_cfg.get("enabled", False)),
        "recipients": recipients_str,
        "smtp_secret_scope": smtp_cfg.get("scope", ""),
        "smtp_host_key": smtp_cfg.get("host_key", ""),
        "smtp_port_key": smtp_cfg.get("port_key", ""),
        "smtp_username_key": smtp_cfg.get("username_key", ""),
        "smtp_password_key": smtp_cfg.get("password_key", ""),
        "webhook_secret_scope": slack_webhook_cfg.get("scope", "") or teams_webhook_cfg.get("scope", ""),
        "slack_webhook_key": slack_webhook_cfg.get("key", ""),
        "teams_webhook_key": teams_webhook_cfg.get("key", ""),
    }
