import argparse
import json
from pathlib import Path

from b3_platform.flow_specs.generate_resources import build_resources_payload


def build_bundle_resources_payload(environment: str) -> dict:
    resources_payload = build_resources_payload(environment)

    jobs = {}

    for item in resources_payload["resources"]:
        if item["resource_type"] != "job":
            continue

        jobs[item["job_name"]] = {
            "name": item["job_name"],
            "tags": item["tags"],
            "tasks": [
                {
                    "task_key": item["task_key"],
                    "spark_python_task": {
                        "python_file_placeholder": "${workspace.root_path}/b3_platform/flow_specs/run_flow_by_path.py",
                        "parameters": [
                            "--spec-module", item["spec_module"],
                            "--project", item["project"],
                            "--use-catalog", str(item["use_catalog"]).lower(),
                            "--config-path", item["config_path"],
                        ],
                    },
                    "existing_cluster_id_placeholder": f"${{{environment.upper()}_CLUSTER_ID}}",
                }
            ],
        }

    return {
        "bundle_resources_version": 1,
        "environment": environment,
        "resources": {
            "jobs": jobs,
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera bundle resources compatíveis a partir dos resources operacionais."
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

    payload = build_bundle_resources_payload(parsed.environment)

    output_path = parsed.output_path or f"artifacts/generated_bundle_resources_{parsed.environment}.json"
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
