def build_cycle_execution_key(
    *,
    cycle_name: str,
    model_name: str,
    model_version: str | None,
    window_start: str | None,
    window_end: str | None,
    severity: str | None = None,
) -> str:
    parts = [
        cycle_name,
        model_name,
        model_version or "",
        window_start or "",
        window_end or "",
        severity or "",
    ]
    return "|".join(parts)


def is_duplicate_cycle_execution(
    existing_keys: list[str],
    *,
    cycle_name: str,
    model_name: str,
    model_version: str | None,
    window_start: str | None,
    window_end: str | None,
    severity: str | None = None,
) -> bool:
    current_key = build_cycle_execution_key(
        cycle_name=cycle_name,
        model_name=model_name,
        model_version=model_version,
        window_start=window_start,
        window_end=window_end,
        severity=severity,
    )
    return current_key in set(existing_keys)
