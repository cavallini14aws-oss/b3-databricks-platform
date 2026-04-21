import argparse
import json
import os


from data_platform.orchestration.ci_secrets_contract import get_provider_all_secrets


PLACEHOLDER_MARKERS = {
    "placeholder",
    "changeme",
    "example",
    "fake",
    "test",
    "dummy",
    "sample",
    "todo",
}


def _looks_like_placeholder(value: str) -> bool:
    lowered = value.strip().lower()
    return any(marker in lowered for marker in PLACEHOLDER_MARKERS)


def _validate_variable(variable_name: str, value: str | None) -> tuple[bool, str | None]:
    if value is None or not value.strip():
        return False, "Variável ausente ou vazia."

    normalized = value.strip()

    if variable_name.endswith("_WORKSPACE_HOST"):
        if not normalized.startswith("https://"):
            return False, "WORKSPACE_HOST deve começar com https://"
        if _looks_like_placeholder(normalized):
            return False, "WORKSPACE_HOST parece placeholder."
        return True, None

    if variable_name.endswith("_DATABRICKS_TOKEN"):
        if len(normalized) < 10:
            return False, "DATABRICKS_TOKEN muito curto."
        if _looks_like_placeholder(normalized):
            return False, "DATABRICKS_TOKEN parece placeholder."
        return True, None

    if variable_name.endswith("_COMPUTE_MODE"):
        if len(normalized) < 3:
            return False, "CLUSTER_ID muito curto."
        if _looks_like_placeholder(normalized):
            return False, "CLUSTER_ID parece placeholder."
        return True, None

    if _looks_like_placeholder(normalized):
        return False, "Valor parece placeholder."

    return True, None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Valida se as variáveis obrigatórias do provider estão presentes no ambiente."
    )
    parser.add_argument(
        "--provider",
        required=True,
        choices=["github_actions", "azure_devops", "bitbucket", "aws"],
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    provider_data = get_provider_all_secrets(parsed.provider)

    result = {
        "provider": parsed.provider,
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

    print(json.dumps(result, indent=2, ensure_ascii=False))

    if not result["valid"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
