from data_platform.core.execution_order import (
    get_execution_order,
    validate_execution_order,
)


def test_get_execution_order_returns_expected_sequence():
    order = get_execution_order()
    assert order == [
        "drift_cycle",
        "postprod_cycle",
        "retraining_cycle",
        "operational_cycle",
    ]


def test_validate_execution_order_accepts_correct_order():
    assert validate_execution_order(
        ["drift_cycle", "postprod_cycle", "retraining_cycle", "operational_cycle"]
    ) is True


def test_validate_execution_order_rejects_wrong_order():
    assert validate_execution_order(
        ["postprod_cycle", "drift_cycle", "retraining_cycle", "operational_cycle"]
    ) is False
