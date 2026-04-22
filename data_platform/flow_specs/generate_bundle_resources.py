import argparse
import json
from copy import deepcopy
from pathlib import Path

import yaml

from data_platform.flow_specs.generate_resources import build_resources_payload


def _load_compute_matrix() -> dict:
    path = Path("config/databricks/compute_matrix.yml")
    if not path.exists():
        return {
            "version": 1,
            "defaults": {
                "mode": "auto",
                "classic": {},
                "serverless": {"environment_key": "default"},
            },
            "targets": {},
        }
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _deep_merge(base: dict, override: dict) -> dict:
    result = deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def _normalize_cluster_mode(mode: str | None) -> str | None:
    if mode is None:
        return None

    normalized = str(mode).strip().lower()

    mapping = {
        "existing_or_job_cluster": "classic",
        "existing": "classic",
        "job": "classic",
        "job_cluster": "classic",
        "classic": "classic",
        "serverless": "serverless",
        "auto": "auto",
    }

    return mapping.get(normalized, normalized)


def _resolve_compute(environment: str, resource_cluster_mode: str | None = None) -> dict:
    matrix = _load_compute_matrix()
    defaults = matrix.get("defaults", {})
    targets = matrix.get("targets", {})
    target_cfg = targets.get(environment, {})

    merged = _deep_merge(defaults, target_cfg)

    requested_mode = _normalize_cluster_mode(resource_cluster_mode) or _normalize_cluster_mode(
        merged.get("mode", "auto")
    ) or "auto"
    classic_cfg = merged.get("classic", {}) or {}
    serverless_cfg = merged.get("serverless", {}) or {"environment_key": "default"}

    if requested_mode == "serverless":
        resolved_mode = "serverless"
    elif requested_mode == "classic":
        resolved_mode = "classic"
    else:
        resolved_mode = "classic" if classic_cfg else "serverless"

    return {
        "requested_mode": requested_mode,
        "resolved_mode": resolved_mode,
        "classic": classic_cfg,
        "serverless": serverless_cfg,
    }


def build_bundle_resources_payload(environment: str) -> dict:
    resources_payload = build_resources_payload(environment)

    jobs = {}

    for item in resources_payload["resources"]:
        if item["resource_type"] != "job":
            continue

        compute = _resolve_compute(
            environment,
            resource_cluster_mode=item.get("cluster_mode"),
        )

        task = {
            "task_key": item["task_key"],
            "cluster_mode": _normalize_cluster_mode(item.get("cluster_mode")) or compute["resolved_mode"],
            "spark_python_task": {
                "python_file": "${workspace.root_path}/data_platform/flow_specs/run_flow_by_path.py",
                "parameters": [
                    "--spec-module", item["spec_module"],
                    "--project", item["project"],
                    "--use-catalog", str(item["use_catalog"]).lower(),
                    "--config-path", item["config_path"],
                ],
            },
            "compute": deepcopy(compute),
        }

        jobs[item["job_name"]] = {
            "name": item["job_name"],
            "tags": item["tags"],
            "tasks": [task],
        }

    environment_compute = _resolve_compute(environment)

    return {
        "bundle_resources_version": 2,
        "environment": environment,
        "compute": environment_compute,
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
