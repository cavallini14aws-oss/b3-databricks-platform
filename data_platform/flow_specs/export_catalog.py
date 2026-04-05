import argparse
import json
from pathlib import Path

from data_platform.flow_specs.discovery import discover_flow_specs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Descobre flow specs e exporta catálogo consolidado."
    )
    parser.add_argument(
        "--output-path",
        default="artifacts/flow_specs_catalog.json",
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    catalog = {
        "flow_specs": discover_flow_specs(),
    }

    output_path = Path(parsed.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(catalog, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(catalog, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
