from datetime import date
from pathlib import Path

from data_platform.core.config_loader import load_yaml_config


def _should_ignore(path: Path, ignore_paths: list[str]) -> bool:
    path_str = str(path)
    return any(part in path_str for part in ignore_paths)


def _load_exceptions(exceptions_file: str) -> list[dict]:
    path = Path(exceptions_file)
    if not path.exists():
        return []

    data = load_yaml_config(exceptions_file)
    return data.get("exceptions", [])


def _exception_matches(environment: str, pattern: str, exceptions: list[dict]) -> dict | None:
    today = date.today()

    for item in exceptions:
        if item.get("environment") != environment:
            continue
        if item.get("package_or_pattern") != pattern:
            continue

        valid_until = item.get("valid_until")
        if valid_until:
            try:
                expiry = date.fromisoformat(valid_until)
                if expiry < today:
                    continue
            except Exception:
                continue

        return item

    return None


def scan_manual_install_patterns(environment: str) -> dict:
    contract = load_yaml_config("config/platform_contracts/runtime_install_contract.yml")
    rules = contract["runtime_install_governance"]

    if environment not in {"dev", "hml", "prd"}:
        return {
            "valid": False,
            "errors": [f"environment inválido: {environment}"],
            "matches": [],
            "approved_matches": [],
        }

    allow_manual = rules[environment]["manual_pip_install_allowed"]
    exception_required = rules[environment]["approved_exception_required"]
    patterns = rules["forbidden_manual_install_patterns"]
    targets = rules["scan_targets"]
    ignore_paths = rules["ignore_paths"]
    exceptions_file = rules["exceptions_file"]
    exceptions = _load_exceptions(exceptions_file)

    matches = []
    approved_matches = []

    for target in targets:
        target_path = Path(target)
        if not target_path.exists():
            continue

        for path in target_path.rglob("*"):
            if not path.is_file():
                continue
            if _should_ignore(path, ignore_paths):
                continue

            try:
                text = path.read_text(encoding="utf-8")
            except Exception:
                continue

            for pattern in patterns:
                if pattern in text:
                    match = {
                        "file": str(path),
                        "pattern": pattern,
                    }
                    matches.append(match)

                    approved = _exception_matches(environment, pattern, exceptions)
                    if approved:
                        approved_matches.append(
                            {
                                **match,
                                "approved_by": approved.get("approved_by"),
                                "reason": approved.get("reason"),
                                "valid_until": approved.get("valid_until"),
                            }
                        )

    errors = []

    if environment == "dev":
        return {
            "valid": True,
            "environment": environment,
            "manual_pip_install_allowed": allow_manual,
            "errors": [],
            "matches": matches,
            "approved_matches": approved_matches,
        }

    if matches:
        unapproved = [
            m for m in matches
            if not any(
                a["file"] == m["file"] and a["pattern"] == m["pattern"]
                for a in approved_matches
            )
        ]

        if unapproved and exception_required:
            errors.append(
                f"manual pip install encontrado em {environment} sem exceção aprovada: {len(unapproved)} ocorrência(s)"
            )

    return {
        "valid": not errors,
        "environment": environment,
        "manual_pip_install_allowed": allow_manual,
        "errors": errors,
        "matches": matches,
        "approved_matches": approved_matches,
    }
