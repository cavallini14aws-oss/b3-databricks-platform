import argparse
import json
from pathlib import Path

from data_platform.flow_specs.generate_registry import build_registry_payload
from data_platform.flow_specs.generate_jobs import build_jobs_payload
from data_platform.flow_specs.generate_resources import build_resources_payload
from data_platform.flow_specs.generate_bundle_targets import build_bundle_targets_payload
from data_platform.flow_specs.generate_databricks_bundle_root import build_bundle_root_yaml
from data_platform.orchestration.ci_provider_config import get_active_ci_provider, load_ci_providers
from data_platform.orchestration.validate_active_ci_provider import build_validation_payload


def _check_ci_provider() -> dict:
    providers = load_ci_providers()
    enabled = [provider.name for provider in providers if provider.enabled]

    return {
        "enabled_providers": enabled,
        "valid": len(enabled) == 1,
        "error": None if len(enabled) == 1 else (
            "Nenhum provider ativo." if len(enabled) == 0
            else f"Mais de um provider ativo: {enabled}"
        ),
    }


def build_platform_readiness_report() -> dict:
    ci_provider_check = _check_ci_provider()

    active_provider = None
    secrets_check = {
        "provider": None,
        "valid": False,
        "environments": {},
    }

    if ci_provider_check["valid"]:
        active_provider = get_active_ci_provider()
        secrets_check = build_validation_payload(active_provider.name)

    registry = build_registry_payload()
    jobs_dev = build_jobs_payload("dev")
    jobs_hml = build_jobs_payload("hml")
    jobs_prd = build_jobs_payload("prd")
    resources_dev = build_resources_payload("dev")
    resources_hml = build_resources_payload("hml")
    resources_prd = build_resources_payload("prd")
    bundle_targets = build_bundle_targets_payload()
    bundle_root_yaml = build_bundle_root_yaml()

    checks = {
        "ci_provider": ci_provider_check,
        "secrets": secrets_check,
        "registry": {
            "valid": registry["flow_count"] > 0 and registry["invalid_flow_count"] == 0,
            "flow_count": registry["flow_count"],
            "invalid_flow_count": registry["invalid_flow_count"],
            "invalid_flows": registry["invalid_flows"],
        },
        "jobs": {
            "valid": (
                jobs_dev["job_count"] > 0 and
                jobs_hml["job_count"] > 0 and
                jobs_prd["job_count"] > 0
            ),
            "dev_job_count": jobs_dev["job_count"],
            "hml_job_count": jobs_hml["job_count"],
            "prd_job_count": jobs_prd["job_count"],
        },
        "resources": {
            "valid": (
                resources_dev["resource_count"] > 0 and
                resources_hml["resource_count"] > 0 and
                resources_prd["resource_count"] > 0
            ),
            "dev_resource_count": resources_dev["resource_count"],
            "hml_resource_count": resources_hml["resource_count"],
            "prd_resource_count": resources_prd["resource_count"],
        },
        "bundle_targets": {
            "valid": len(bundle_targets.get("targets", {})) == 3,
            "target_count": len(bundle_targets.get("targets", {})),
        },
        "bundle_root": {
            "valid": bool(bundle_root_yaml and bundle_root_yaml.strip()),
        },
    }

    ready = all(item["valid"] for item in checks.values())

    blockers = []
    if not checks["ci_provider"]["valid"]:
        blockers.append("CI provider inválido.")
    if not checks["secrets"]["valid"]:
        blockers.append("Secrets obrigatórios ausentes ou inválidos.")
    if not checks["registry"]["valid"]:
        blockers.append("Registry inválido ou com flows quebrados.")
    if not checks["jobs"]["valid"]:
        blockers.append("Jobs por ambiente não foram gerados corretamente.")
    if not checks["resources"]["valid"]:
        blockers.append("Resources por ambiente não foram gerados corretamente.")
    if not checks["bundle_targets"]["valid"]:
        blockers.append("Bundle targets inválidos.")
    if not checks["bundle_root"]["valid"]:
        blockers.append("Bundle root vazio ou inválido.")

    return {
        "readiness_report_version": 1,
        "active_provider": active_provider.name if active_provider else None,
        "ready_for_real_deploy": ready,
        "blockers": blockers,
        "checks": checks,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera readiness report de deploy real da plataforma."
    )
    parser.add_argument(
        "--output-path",
        default="artifacts/platform_readiness_report.json",
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    payload = build_platform_readiness_report()

    output_path = Path(parsed.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
