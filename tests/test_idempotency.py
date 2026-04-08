from data_platform.mlops.idempotency import (
    build_cycle_execution_key,
    is_duplicate_cycle_execution,
)


def test_build_cycle_execution_key_returns_expected_string():
    key = build_cycle_execution_key(
        cycle_name="postprod",
        model_name="clientes_status_classifier",
        model_version="v123",
        window_start="2026-04-01",
        window_end="2026-04-08",
        severity="CRITICAL",
    )

    assert "postprod" in key
    assert "clientes_status_classifier" in key
    assert "v123" in key
    assert "CRITICAL" in key


def test_is_duplicate_cycle_execution_returns_true_for_same_key():
    existing_keys = [
        "postprod|clientes_status_classifier|v123|2026-04-01|2026-04-08|CRITICAL"
    ]

    assert is_duplicate_cycle_execution(
        existing_keys=existing_keys,
        cycle_name="postprod",
        model_name="clientes_status_classifier",
        model_version="v123",
        window_start="2026-04-01",
        window_end="2026-04-08",
        severity="CRITICAL",
    ) is True


def test_is_duplicate_cycle_execution_returns_false_for_different_key():
    existing_keys = [
        "postprod|clientes_status_classifier|v123|2026-04-01|2026-04-08|WARNING"
    ]

    assert is_duplicate_cycle_execution(
        existing_keys=existing_keys,
        cycle_name="postprod",
        model_name="clientes_status_classifier",
        model_version="v123",
        window_start="2026-04-01",
        window_end="2026-04-08",
        severity="CRITICAL",
    ) is False
