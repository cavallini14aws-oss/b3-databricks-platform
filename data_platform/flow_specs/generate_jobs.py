import argparse
import json
from pathlib import Path

from data_platform.core.job_config import load_job_config
from data_platform.flow_specs.generate_registry import build_registry_payload


def build_jobs_payload(environment: str) -> dict:
    registry = build_registry_payload()
    env_cfg = load_job_config(environment)

    jobs = []

    for flow in registry["flows"]:
        if not flow["enabled"]:
            continue

        jobs.append(
            {
                "job_name": f"{flow['flow_name']}_{environment}",
                "environment": environment,
                "project": flow["project"],
                "domain": flow["domain"],
                "flow_name": flow["flow_name"],
                "flow_type": flow["flow_type"],
                "spec_module": flow["spec_module"],
                "entrypoint": flow["entrypoint"],
                "callable_name": flow["callable_name"],
                "cluster_key": env_cfg.cluster_key,
                "cluster_mode": env_cfg.cluster_mode,
                "use_catalog": env_cfg.use_catalog,
                "config_path": env_cfg.default_config_path,
                "tags": flow["tags"],
            }
        )

    return {
        "jobs_version": 1,
        "environment": environment,
        "job_count": len(jobs),
        "jobs": jobs,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera job payloads por ambiente a partir do registry de flows."
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

    payload = build_jobs_payload(parsed.environment)

    output_path = parsed.output_path or f"artifacts/generated_jobs_{parsed.environment}.json"
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
