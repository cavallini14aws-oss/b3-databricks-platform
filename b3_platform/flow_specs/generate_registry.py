import argparse
import json
from pathlib import Path

from b3_platform.flow_specs.discovery import discover_flow_specs


def build_registry_payload() -> dict:
    discovered = discover_flow_specs()

    registry_items = []
    for item in discovered:
        registry_items.append(
            {
                "flow_name": item["flow_name"],
                "flow_type": item["flow_type"],
                "project": item["project"],
                "domain": item["domain"],
                "layer": item["layer"],
                "description": item["description"],
                "spec_module": item["spec_module"],
                "entrypoint": item["entrypoint"],
                "callable_name": item["callable_name"],
                "enabled": item["enabled"],
                "tags": item["tags"],
            }
        )

    return {
        "registry_version": 1,
        "flow_count": len(registry_items),
        "flows": registry_items,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera registry operacional consolidado a partir dos FLOW_SPEC."
    )
    parser.add_argument(
        "--output-path",
        default="artifacts/generated_flow_registry.json",
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    payload = build_registry_payload()

    output_path = Path(parsed.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
