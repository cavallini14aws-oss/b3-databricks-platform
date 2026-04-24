import json
import os

from data_platform.orchestration.ci_provider_config import get_active_ci_provider
from data_platform.orchestration.ci_secrets_contract import get_provider_all_secrets
from data_platform.orchestration.validate_ci_secrets import _validate_variable


def is_pull_request_context() -> bool:
    return os.getenv("GITHUB_EVENT_NAME", "") == "pull_request"


def build_validation_payload(provider_name: str) -> dict:
    provider_data = get_provider_all_secrets(provider_name)

    result = {
        "provider": provider_name,
        "valid": True,
        "environments": {},
    }

    for env, secret_data in provider_data.items():
        missing = []
        invalid = []
        present = []

        for variable in secret_data.required:
            value = os.getenv(variable)
            is_valid, error = _validate_variable(variable, value)

            if value is None or not str(value).strip():
                missing.append(variable)
            elif is_valid:
                present.append(variable)
            else:
                invalid.append(
                    {
                        "name": variable,
                        "reason": error,
                    }
                )

        env_valid = len(missing) == 0 and len(invalid) == 0
        if not env_valid:
            result["valid"] = False

        result["environments"][env] = {
            "required": secret_data.required,
            "present": present,
            "missing": missing,
            "invalid": invalid,
            "valid": env_valid,
        }

    return result


def main() -> None:
    active_provider = get_active_ci_provider()
    payload = build_validation_payload(active_provider.name)

    print(json.dumps(payload, indent=2, ensure_ascii=False))

    if not payload["valid"]:
        if is_pull_request_context():
            print("[WARN] Missing CI secrets in pull_request context. Skipping hard failure.")
            return

        raise SystemExit(1)


if __name__ == "__main__":
    main()
