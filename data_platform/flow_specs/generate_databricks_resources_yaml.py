import argparse
from pathlib import Path

from data_platform.flow_specs.generate_bundle_resources import build_bundle_resources_payload


def _yaml_scalar(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace('"', '\\"')
    return f'"{text}"'


def build_resources_yaml(environment: str) -> str:
    payload = build_bundle_resources_payload(environment)

    lines = []
    lines.append("resources:")
    lines.append("  jobs:")

    for job_name, job in payload["resources"]["jobs"].items():
        lines.append(f"    {job_name}:")
        lines.append(f"      name: {_yaml_scalar(job['name'])}")
        lines.append("      tags:")
        for key, value in job["tags"].items():
            lines.append(f"        {key}: {_yaml_scalar(value)}")
        lines.append("      tasks:")
        for task in job["tasks"]:
            lines.append(f"        - task_key: {_yaml_scalar(task['task_key'])}")
            lines.append("          spark_python_task:")
            lines.append(
                f"            python_file: {_yaml_scalar(task['spark_python_task']['python_file_placeholder'])}"
            )
            lines.append("            parameters:")
            for param in task["spark_python_task"]["parameters"]:
                lines.append(f"              - {_yaml_scalar(param)}")
            lines.append(
                f"          existing_cluster_id: {_yaml_scalar(task['existing_cluster_id_placeholder'])}"
            )

    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera resources YAML por ambiente para Bundles."
    )
    parser.add_argument(
        "--environment",
        required=True,
        choices=["dev", "hml", "prd"],
    )
    parser.add_argument(
        "--output-path",
        default=None,
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    yaml_text = build_resources_yaml(parsed.environment)

    output_path = parsed.output_path or f"artifacts/generated_resources_{parsed.environment}.yml"
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(yaml_text, encoding="utf-8")

    print(yaml_text)


if __name__ == "__main__":
    main()
