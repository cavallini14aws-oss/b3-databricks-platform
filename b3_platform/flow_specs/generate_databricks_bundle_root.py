import argparse
from pathlib import Path

from b3_platform.flow_specs.generate_bundle_targets import build_bundle_targets_payload


def _yaml_scalar(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace('"', '\\"')
    return f'"{text}"'


def build_bundle_root_yaml() -> str:
    targets_payload = build_bundle_targets_payload()

    lines = []
    lines.append("bundle:")
    lines.append('  name: "b3-databricks-platform"')
    lines.append("")
    lines.append("include:")
    lines.append('  - "artifacts/generated_resources_dev.yml"')
    lines.append('  - "artifacts/generated_resources_hml.yml"')
    lines.append('  - "artifacts/generated_resources_prd.yml"')
    lines.append("")
    lines.append("targets:")

    for env, target in targets_payload["targets"].items():
        lines.append(f"  {env}:")
        lines.append(f"    default: {_yaml_scalar(target['default'])}")
        lines.append(
            f"    mode: {'development' if env == 'dev' else 'production'}"
        )
        lines.append("    workspace:")
        lines.append(f"      host: {_yaml_scalar(target['workspace']['host_placeholder'])}")
        lines.append(f"      profile: {_yaml_scalar(target['workspace']['profile_placeholder'])}")
        lines.append(f"      root_path: {_yaml_scalar(target['workspace']['root_path'])}")
        lines.append("    variables:")
        for key, value in target["bundle_variables"].items():
            lines.append(f"      {key}: {_yaml_scalar(value)}")

    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera databricks.yml raiz final para Bundles."
    )
    parser.add_argument(
        "--output-path",
        default="artifacts/generated_databricks_root.yml",
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    yaml_text = build_bundle_root_yaml()

    output_path = Path(parsed.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml_text, encoding="utf-8")

    print(yaml_text)


if __name__ == "__main__":
    main()
