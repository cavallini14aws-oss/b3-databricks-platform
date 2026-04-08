from data_platform.mlops.notifications import (
    build_email_notification_payload,
    build_notification_plan,
    build_slack_notification_payload,
    build_smtp_settings,
    build_teams_notification_payload,
    is_notification_channel_enabled,
    parse_recipients,
    resolve_webhook_url,
    send_notification_plan,
)


def test_parse_recipients_returns_clean_list():
    recipients = parse_recipients("a@test.com, b@test.com ,, c@test.com ")

    assert recipients == [
        "a@test.com",
        "b@test.com",
        "c@test.com",
    ]


def test_parse_recipients_returns_empty_list():
    assert parse_recipients("") == []
    assert parse_recipients(None) == []


def test_is_notification_channel_enabled_returns_expected_values():
    config = {
        "email_enabled": True,
        "slack_enabled": False,
        "teams_enabled": True,
    }

    assert is_notification_channel_enabled(config, "email") is True
    assert is_notification_channel_enabled(config, "slack") is False
    assert is_notification_channel_enabled(config, "teams") is True


def test_is_notification_channel_enabled_blocks_invalid_channel():
    config = {}

    try:
        is_notification_channel_enabled(config, "sms")
        assert False, "Era esperado ValueError"
    except ValueError as exc:
        assert "Canal de notificacao invalido" in str(exc)


def test_build_email_notification_payload_returns_expected_shape():
    payload = build_email_notification_payload(
        recipients=["a@test.com"],
        subject="Assunto",
        body="Mensagem",
    )

    assert payload["channel"] == "email"
    assert payload["recipients"] == ["a@test.com"]
    assert payload["subject"] == "Assunto"
    assert payload["body"] == "Mensagem"


def test_build_slack_notification_payload_returns_expected_shape():
    payload = build_slack_notification_payload(
        message="Mensagem",
        webhook_key="slack-key",
    )

    assert payload == {
        "channel": "slack",
        "message": "Mensagem",
        "webhook_key": "slack-key",
    }


def test_build_teams_notification_payload_returns_expected_shape():
    payload = build_teams_notification_payload(
        message="Mensagem",
        webhook_key="teams-key",
    )

    assert payload == {
        "channel": "teams",
        "message": "Mensagem",
        "webhook_key": "teams-key",
    }


def test_build_notification_plan_prioritizes_enabled_channels():
    config = {
        "email_enabled": True,
        "slack_enabled": True,
        "teams_enabled": False,
        "recipients": "a@test.com,b@test.com",
        "slack_webhook_key": "slack-key",
        "teams_webhook_key": "teams-key",
    }

    plan = build_notification_plan(
        alerting_config=config,
        subject="Alerta",
        message="Drift critico detectado",
    )

    assert len(plan) == 2
    assert plan[0]["channel"] == "email"
    assert plan[1]["channel"] == "slack"


def test_build_notification_plan_skips_email_without_recipients():
    config = {
        "email_enabled": True,
        "slack_enabled": False,
        "teams_enabled": False,
        "recipients": "",
    }

    plan = build_notification_plan(
        alerting_config=config,
        subject="Alerta",
        message="Mensagem",
    )

    assert plan == []


def test_build_smtp_settings_returns_expected_values():
    config = {
        "smtp_secret_scope": "scope-x",
        "smtp_host_key": "host-key",
        "smtp_port_key": "port-key",
        "smtp_username_key": "user-key",
        "smtp_password_key": "pass-key",
    }

    fake_secrets = {
        ("scope-x", "host-key"): "smtp.test.com",
        ("scope-x", "port-key"): "587",
        ("scope-x", "user-key"): "user@test.com",
        ("scope-x", "pass-key"): "secret-pass",
    }

    settings = build_smtp_settings(
        alerting_config=config,
        secrets_resolver=lambda scope, key: fake_secrets[(scope, key)],
    )

    assert settings == {
        "host": "smtp.test.com",
        "port": 587,
        "username": "user@test.com",
        "password": "secret-pass",
    }


def test_build_smtp_settings_blocks_incomplete_config():
    config = {
        "smtp_secret_scope": "",
        "smtp_host_key": "",
    }

    try:
        build_smtp_settings(
            alerting_config=config,
            secrets_resolver=lambda scope, key: "x",
        )
        assert False, "Era esperado ValueError"
    except ValueError as exc:
        assert "Configuracao SMTP incompleta" in str(exc)


def test_resolve_webhook_url_returns_expected_value():
    config = {
        "webhook_secret_scope": "scope-webhook",
        "slack_webhook_key": "slack-key",
    }

    webhook_url = resolve_webhook_url(
        alerting_config=config,
        channel="slack",
        secrets_resolver=lambda scope, key: "https://hooks.slack.test",
    )

    assert webhook_url == "https://hooks.slack.test"


def test_send_notification_plan_marks_email_as_sent(monkeypatch):
    plan = [
        {
            "channel": "email",
            "recipients": ["a@test.com"],
            "subject": "Alerta",
            "body": "Mensagem",
        }
    ]

    monkeypatch.setattr(
        "data_platform.mlops.notifications.build_smtp_settings",
        lambda **kwargs: {
            "host": "smtp.test.com",
            "port": 587,
            "username": "user@test.com",
            "password": "secret-pass",
        },
    )
    monkeypatch.setattr(
        "data_platform.mlops.notifications.send_email_notification",
        lambda **kwargs: "SENT",
    )

    results = send_notification_plan(
        plan=plan,
        alerting_config={},
        secrets_resolver=lambda scope, key: "x",
    )

    assert len(results) == 1
    assert results[0]["delivery_status"] == "SENT"


def test_send_notification_plan_marks_email_as_failed(monkeypatch):
    plan = [
        {
            "channel": "email",
            "recipients": ["a@test.com"],
            "subject": "Alerta",
            "body": "Mensagem",
        }
    ]

    monkeypatch.setattr(
        "data_platform.mlops.notifications.build_smtp_settings",
        lambda **kwargs: {
            "host": "smtp.test.com",
            "port": 587,
            "username": "user@test.com",
            "password": "secret-pass",
        },
    )

    def fail_email(**kwargs):
        raise RuntimeError("SMTP error")

    monkeypatch.setattr(
        "data_platform.mlops.notifications.send_email_notification",
        fail_email,
    )

    results = send_notification_plan(
        plan=plan,
        alerting_config={},
        secrets_resolver=lambda scope, key: "x",
    )

    assert len(results) == 1
    assert results[0]["delivery_status"] == "FAILED"
    assert "SMTP error" in results[0]["delivery_error"]


def test_send_notification_plan_marks_slack_as_sent(monkeypatch):
    plan = [
        {
            "channel": "slack",
            "message": "Drift critico",
            "webhook_key": "slack-key",
        }
    ]

    monkeypatch.setattr(
        "data_platform.mlops.notifications.resolve_webhook_url",
        lambda **kwargs: "https://hooks.slack.test",
    )
    monkeypatch.setattr(
        "data_platform.mlops.notifications.send_slack_notification",
        lambda **kwargs: "SENT",
    )

    results = send_notification_plan(
        plan=plan,
        alerting_config={"webhook_secret_scope": "scope-x", "slack_webhook_key": "slack-key"},
        secrets_resolver=lambda scope, key: "x",
    )

    assert len(results) == 1
    assert results[0]["delivery_status"] == "SENT"


def test_send_notification_plan_marks_slack_as_failed(monkeypatch):
    plan = [
        {
            "channel": "slack",
            "message": "Drift critico",
            "webhook_key": "slack-key",
        }
    ]

    monkeypatch.setattr(
        "data_platform.mlops.notifications.resolve_webhook_url",
        lambda **kwargs: "https://hooks.slack.test",
    )

    def fail_slack(**kwargs):
        raise RuntimeError("Slack webhook error")

    monkeypatch.setattr(
        "data_platform.mlops.notifications.send_slack_notification",
        fail_slack,
    )

    results = send_notification_plan(
        plan=plan,
        alerting_config={"webhook_secret_scope": "scope-x", "slack_webhook_key": "slack-key"},
        secrets_resolver=lambda scope, key: "x",
    )

    assert len(results) == 1
    assert results[0]["delivery_status"] == "FAILED"
    assert "Slack webhook error" in results[0]["delivery_error"]


def test_send_notification_plan_marks_teams_as_sent(monkeypatch):
    plan = [
        {
            "channel": "teams",
            "message": "Drift critico",
            "webhook_key": "teams-key",
        }
    ]

    monkeypatch.setattr(
        "data_platform.mlops.notifications.resolve_webhook_url",
        lambda **kwargs: "https://hooks.teams.test",
    )
    monkeypatch.setattr(
        "data_platform.mlops.notifications.send_teams_notification",
        lambda **kwargs: "SENT",
    )

    results = send_notification_plan(
        plan=plan,
        alerting_config={"webhook_secret_scope": "scope-x", "teams_webhook_key": "teams-key"},
        secrets_resolver=lambda scope, key: "x",
    )

    assert len(results) == 1
    assert results[0]["delivery_status"] == "SENT"


def test_send_notification_plan_marks_teams_as_failed(monkeypatch):
    plan = [
        {
            "channel": "teams",
            "message": "Drift critico",
            "webhook_key": "teams-key",
        }
    ]

    monkeypatch.setattr(
        "data_platform.mlops.notifications.resolve_webhook_url",
        lambda **kwargs: "https://hooks.teams.test",
    )

    def fail_teams(**kwargs):
        raise RuntimeError("Teams webhook error")

    monkeypatch.setattr(
        "data_platform.mlops.notifications.send_teams_notification",
        fail_teams,
    )

    results = send_notification_plan(
        plan=plan,
        alerting_config={"webhook_secret_scope": "scope-x", "teams_webhook_key": "teams-key"},
        secrets_resolver=lambda scope, key: "x",
    )

    assert len(results) == 1
    assert results[0]["delivery_status"] == "FAILED"
    assert "Teams webhook error" in results[0]["delivery_error"]
