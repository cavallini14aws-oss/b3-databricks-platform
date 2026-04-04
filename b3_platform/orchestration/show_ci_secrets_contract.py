import argparse
import json

from b3_platform.orchestration.ci_secrets_contract import get_provider_all_secrets


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Mostra o contrato de secrets obrigatório por provider."
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

    payload = {
        "provider": parsed.provider,
        "environments": {
            env: {
                "required": data.required,
            }
            for env, data in get_provider_all_secrets(parsed.provider).items()
        },
    }

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
