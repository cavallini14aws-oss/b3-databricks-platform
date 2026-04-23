import argparse
import json
from pathlib import Path

import yaml

from data_platform.core.job_config import load_job_config


def load_official_environment_contract() -> dict:
    path = Path("config/official_environment_contract.yml")
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def build_bundle_targets_payload() -> dict:
    environments = ["dev", "hml", "prd"]
    official_contract = load_official_environment_contract()
    target_contracts = official_contract.get("targets", {})
    targets = {}

    for env in environments:
        job_cfg = load_job_config(env)
        contract = target_contracts.get(env, {})
        workspace_contract = contract.get("workspace", {})
        run_as_contract = contract.get("run_as", {})

        target_payload = {
            "default": env == "dev",
            "mode": contract.get("mode", "development" if env == "dev" else "production"),
            "workspace": {
                "root_path": workspace_contract.get(
                    "root_path",
                    f"/Workspace/Users/${{workspace.current_user.userName}}/.bundle/b3_databricks_platform/{env}",
                ),
            },
            "bundle_variables": {
                "environment": env,
                "use_catalog": job_cfg.use_catalog,
                "default_config_path": job_cfg.default_config_path,
                "workspace_root": job_cfg.workspace_root,
            },
        }

        run_as_type = run_as_contract.get("type")
        run_as_value = run_as_contract.get("value")

        if run_as_type == "user_name" and run_as_value == "CURRENT_USER":
            target_payload["run_as"] = {"user_name": "${workspace.current_user.userName}"}
        elif run_as_type == "service_principal_name" and run_as_value and run_as_value != "TO_BE_DEFINED":
            target_payload["run_as"] = {"service_principal_name": run_as_value}

        targets[env] = target_payload

    return {
        "bundle_targets_version": 2,
        "targets": targets,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera targets de bundle com suporte ao official_environment_contract."
    )
    parser.add_argument(
        "--output-path",
        default="artifacts/generated_bundle_targets.json",
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    payload = build_bundle_targets_payload()

    output_path = Path(parsed.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
