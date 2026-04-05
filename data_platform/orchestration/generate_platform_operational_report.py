import argparse
import json
from pathlib import Path

from data_platform.flow_specs.generate_registry import build_registry_payload
from data_platform.flow_specs.generate_jobs import build_jobs_payload
from data_platform.flow_specs.generate_resources import build_resources_payload
from data_platform.flow_specs.generate_bundle_targets import build_bundle_targets_payload
from data_platform.flow_specs.generate_databricks_bundle_root import build_bundle_root_yaml
from data_platform.orchestration.ci_provider_config import (
    get_active_ci_provider,
    load_ci_providers,
)
from data_platform.orchestration.ci_secrets_contract import get_provider_all_secrets


def _build_ci_summary() -> dict:
    active_provider = get_active_ci_provider()
    providers = load_ci_providers()

    return {
        "active_provider": active_provider.name,
        "disabled_providers": [
            provider.name
            for provider in providers
            if provider.name != active_provider.name
        ],
        "providers": [
            {
                "name": provider.name,
                "enabled": provider.enabled,
                "display_name": provider.display_name,
            }
            for provider in providers
        ],
    }


def _build_secrets_summary() -> dict:
    active_provider = get_active_ci_provider()
    provider_secrets = get_provider_all_secrets(active_provider.name)

    return {
        "provider": active_provider.name,
        "environments": {
            env: {
                "required": data.required,
            }
            for env, data in provider_secrets.items()
        },
    }


def build_platform_operational_report() -> dict:
    registry = build_registry_payload()
    jobs_dev = build_jobs_payload("dev")
    jobs_hml = build_jobs_payload("hml")
    jobs_prd = build_jobs_payload("prd")
    resources_dev = build_resources_payload("dev")
    resources_hml = build_resources_payload("hml")
    resources_prd = build_resources_payload("prd")
    bundle_targets = build_bundle_targets_payload()
    bundle_root_yaml = build_bundle_root_yaml()

    return {
        "report_version": 1,
        "ci_cd": _build_ci_summary(),
        "secrets_contract": _build_secrets_summary(),
        "flow_registry": registry,
        "jobs_by_environment": {
            "dev": jobs_dev,
            "hml": jobs_hml,
            "prd": jobs_prd,
        },
        "resources_by_environment": {
            "dev": resources_dev,
            "hml": resources_hml,
            "prd": resources_prd,
        },
        "bundle_targets": bundle_targets,
        "bundle_root_yaml": bundle_root_yaml,
        "status": {
            "flow_count": registry["flow_count"],
            "job_count_total": (
                jobs_dev["job_count"] + jobs_hml["job_count"] + jobs_prd["job_count"]
            ),
            "resource_count_total": (
                resources_dev["resource_count"]
                + resources_hml["resource_count"]
                + resources_prd["resource_count"]
            ),
            "active_provider": _build_ci_summary()["active_provider"],
            "platform_ready_for_real_targets": True,
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera relatório operacional consolidado da plataforma."
    )
    parser.add_argument(
        "--output-path",
        default="artifacts/platform_operational_report.json",
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    payload = build_platform_operational_report()

    output_path = Path(parsed.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
