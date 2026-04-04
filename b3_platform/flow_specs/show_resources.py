import argparse
import json

from b3_platform.flow_specs.generate_resources import build_resources_payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Mostra resources operacionais por ambiente."
    )
    parser.add_argument(
        "--environment",
        required=True,
        choices=["dev", "hml", "prd"],
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    payload = build_resources_payload(parsed.environment)
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
