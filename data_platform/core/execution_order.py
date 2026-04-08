EXECUTION_ORDER = [
    "drift_cycle",
    "postprod_cycle",
    "retraining_cycle",
    "operational_cycle",
]


def get_execution_order() -> list[str]:
    return list(EXECUTION_ORDER)


def validate_execution_order(cycles: list[str]) -> bool:
    expected = set(EXECUTION_ORDER)
    provided = set(cycles)

    if expected != provided:
        return False

    indices = [cycles.index(item) for item in EXECUTION_ORDER]
    return indices == sorted(indices)
