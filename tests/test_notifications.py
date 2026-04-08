from data_platform.mlops.notifications import (
    build_email_notification_payload,
    build_notification_plan,
    build_slack_notification_payload,
    build_teams_notification_payload,
    is_notification_channel_enabled,
    parse_recipients,
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
