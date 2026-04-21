import argparse
import json
from pathlib import Path

from data_platform.core.job_config import load_job_config


def build_bundle_targets_payload() -> dict:
    environments = ["dev", "hml", "prd"]
    targets = {}

    for env in environments:
        job_cfg = load_job_config(env)

        targets[env] = {
            "default": env == "dev",
            "workspace": {
                "root_path": f"/Workspace/Users/${{workspace.current_user.userName}}/.bundle/b3_databricks_platform/{env}",
            },
            "bundle_variables": {
                "environment": env,
                "use_catalog": job_cfg.use_catalog,
                "default_config_path": job_cfg.default_config_path,
                "workspace_root": job_cfg.workspace_root,
            },
        }

    return {
        "bundle_targets_version": 2,
        "targets": targets,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera targets de bundle com root_path dinâmico por ambiente."
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
