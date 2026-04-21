import argparse
from pathlib import Path

from data_platform.flow_specs.generate_bundle_targets import build_bundle_targets_payload
from data_platform.flow_specs.generate_bundle_resources import build_bundle_resources_payload


def _yaml_scalar(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    escaped = text.replace('"', '\\"')
    return f'"{escaped}"'


def _emit_tags(tags: dict, indent: int = 0) -> list[str]:
    prefix = " " * indent
    lines = [f"{prefix}tags:"]
    for key, value in tags.items():
        lines.append(f"{prefix}  {key}: {_yaml_scalar(value)}")
    return lines


def _emit_compute(job: dict, task: dict, indent: int = 6) -> list[str]:
    prefix = " " * indent
    lines = []
    compute = task["compute"]
    mode = compute["resolved_mode"]

    if mode == "classic":
        cluster_key = f"{task['task_key']}_cluster"
        lines.append(f"{prefix}job_clusters:")
        lines.append(f"{prefix}  - job_cluster_key: {_yaml_scalar(cluster_key)}")
        lines.append(f"{prefix}    new_cluster:")
        for key, value in compute["classic"].items():
            lines.append(f"{prefix}      {key}: {_yaml_scalar(value)}")
    elif mode == "serverless":
        serverless = compute["serverless"]
        env_key = serverless.get("environment_key", "default")
        env_version = serverless.get("environment_version", "2")
        dependencies = serverless.get("dependencies", [])

        lines.append(f"{prefix}environments:")
        lines.append(f"{prefix}  - environment_key: {_yaml_scalar(env_key)}")
        lines.append(f"{prefix}    spec:")
        lines.append(f"{prefix}      environment_version: {_yaml_scalar(env_version)}")
        if dependencies:
            lines.append(f"{prefix}      dependencies:")
            for dep in dependencies:
                lines.append(f"{prefix}        - {_yaml_scalar(dep)}")

    return lines


def _emit_task(task: dict, indent: int = 8) -> list[str]:
    prefix = " " * indent
    lines = []
    compute = task["compute"]
    mode = compute["resolved_mode"]

    lines.append(f"{prefix}- task_key: {_yaml_scalar(task['task_key'])}")

    if mode == "classic":
        cluster_key = f"{task['task_key']}_cluster"
        lines.append(f"{prefix}  job_cluster_key: {_yaml_scalar(cluster_key)}")
    elif mode == "serverless":
        env_key = compute["serverless"].get("environment_key", "default")
        lines.append(f"{prefix}  environment_key: {_yaml_scalar(env_key)}")

    lines.append(f"{prefix}  spark_python_task:")
    lines.append(
        f"{prefix}    python_file: {_yaml_scalar(task['spark_python_task']['python_file'])}"
    )
    lines.append(f"{prefix}    parameters:")
    for param in task["spark_python_task"]["parameters"]:
        lines.append(f"{prefix}      - {_yaml_scalar(param)}")

    return lines


def build_databricks_bundle_yaml() -> str:
    targets_payload = build_bundle_targets_payload()

    env_resources = {
        "dev": build_bundle_resources_payload("dev"),
        "hml": build_bundle_resources_payload("hml"),
        "prd": build_bundle_resources_payload("prd"),
    }

    lines = []
    lines.append("bundle:")
    lines.append('  name: "b3-databricks-platform"')
    lines.append("")
    lines.append("targets:")

    for env, target in targets_payload["targets"].items():
        lines.append(f"  {env}:")
        lines.append(f"    default: {_yaml_scalar(target['default'])}")
        lines.append("    workspace:")
        lines.append(
            f"      host: {_yaml_scalar(target['workspace']['host_placeholder'])}"
        )
        lines.append(
            f"      profile: {_yaml_scalar(target['workspace']['profile_placeholder'])}"
        )
        lines.append(
            f"      root_path: {_yaml_scalar(target['workspace']['root_path'])}"
        )
        lines.append("    variables:")
        for key, value in target["bundle_variables"].items():
            lines.append(f"      {key}: {_yaml_scalar(value)}")

    lines.append("")
    lines.append("resources:")
    lines.append("  jobs:")

    for env, payload in env_resources.items():
        for job_name, job in payload["resources"]["jobs"].items():
            task = job["tasks"][0]
            lines.append(f"    {job_name}:")
            lines.append(f"      name: {_yaml_scalar(job['name'])}")
            lines.extend(_emit_tags(job["tags"], indent=6))
            lines.extend(_emit_compute(job, task, indent=6))
            lines.append("      tasks:")
            lines.extend(_emit_task(task, indent=8))

    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera databricks.yml final a partir dos targets e resources gerados."
    )
    parser.add_argument(
        "--output-path",
        default="artifacts/generated_databricks.yml",
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    yaml_text = build_databricks_bundle_yaml()

    output_path = Path(parsed.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml_text, encoding="utf-8")

    print(yaml_text)


if __name__ == "__main__":
    main()
