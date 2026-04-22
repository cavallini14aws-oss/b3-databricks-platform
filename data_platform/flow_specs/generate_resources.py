import argparse
import json
from pathlib import Path

from data_platform.flow_specs.generate_jobs import build_jobs_payload


def build_resources_payload(environment: str) -> dict:
    jobs_payload = build_jobs_payload(environment)

    resources = []

    for job in jobs_payload["jobs"]:
        resources.append(
            {
                "resource_type": "job",
                "job_name": job["job_name"],
                "environment": job["environment"],
                "project": job["project"],
                "domain": job["domain"],
                "flow_name": job["flow_name"],
                "flow_type": job["flow_type"],
                "task_key": f"{job['flow_name']}_{environment}_task",
                "spec_module": job["spec_module"],
                "entrypoint": job["entrypoint"],
                "callable_name": job["callable_name"],
                "config_path": job["config_path"],
                "cluster_mode": job.get("cluster_mode", "existing_or_job_cluster"),
                "use_catalog": job["use_catalog"],
                "tags": job["tags"],
            }
        )

    return {
        "resources_version": 2,
        "environment": environment,
        "resource_count": len(resources),
        "resources": resources,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera resources operacionais por ambiente a partir dos job payloads."
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

    payload = build_resources_payload(parsed.environment)

    output_path = parsed.output_path or f"artifacts/generated_resources_{parsed.environment}.json"
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
