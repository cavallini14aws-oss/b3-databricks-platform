import json

from b3_platform.orchestration.ci_provider_config import get_active_ci_provider
from b3_platform.orchestration.ci_secrets_contract import get_provider_all_secrets
import os


def build_validation_payload(provider_name: str) -> dict:
    provider_data = get_provider_all_secrets(provider_name)

    result = {
        "provider": provider_name,
        "valid": True,
        "environments": {},
    }

    for env, secret_data in provider_data.items():
        missing = []
        present = []

        for variable in secret_data.required:
            if os.getenv(variable):
                present.append(variable)
            else:
                missing.append(variable)

        if missing:
            result["valid"] = False

        result["environments"][env] = {
            "required": secret_data.required,
            "present": present,
            "missing": missing,
            "valid": len(missing) == 0,
        }

    return result


def main() -> None:
    active_provider = get_active_ci_provider()
    payload = build_validation_payload(active_provider.name)

    print(json.dumps(payload, indent=2, ensure_ascii=False))

    if not payload["valid"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
