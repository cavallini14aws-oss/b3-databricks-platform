import argparse
import json
import os

from b3_platform.orchestration.ci_secrets_contract import get_provider_all_secrets


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

    print(json.dumps(result, indent=2, ensure_ascii=False))

    if not result["valid"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
