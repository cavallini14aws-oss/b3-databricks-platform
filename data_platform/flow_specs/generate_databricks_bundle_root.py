import argparse
from pathlib import Path

from data_platform.flow_specs.generate_bundle_targets import build_bundle_targets_payload


def _yaml_scalar(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace('"', '\\"')
    return f'"{text}"'


def _is_placeholder(value: str) -> bool:
    lowered = str(value).strip().lower()
    return lowered.startswith("<") and lowered.endswith(">")


def build_bundle_root_yaml() -> str:
    targets_payload = build_bundle_targets_payload()

    hml_run_sp_default = "<app-id-hml>"
    prd_run_sp_default = "<app-id-prd>"
    hml_admin_group_default = "<group-hml-admin>"
    prd_admin_group_default = "<group-prd-admin>"

    lines = []
    lines.append("bundle:")
    lines.append('  name: "b3_databricks_platform"')
    lines.append("  databricks_cli_version: '>= 0.275.0, < 1.0.0'")
    lines.append("")
    lines.append("include:")
    lines.append('  - "artifacts/generated_resources_dev.yml"')
    lines.append('  - "artifacts/generated_resources_hml.yml"')
    lines.append('  - "artifacts/generated_resources_prd.yml"')
    lines.append("")
    lines.append("variables:")
    lines.append("  deploy_user:")
    lines.append('    description: "Current deployment identity for bundle permissions"')
    lines.append('    default: ""')
    lines.append("  hml_run_sp:")
    lines.append('    description: "Service principal app id for HML run_as"')
    lines.append(f"    default: {_yaml_scalar(hml_run_sp_default)}")
    lines.append("  prd_run_sp:")
    lines.append('    description: "Service principal app id for PRD run_as"')
    lines.append(f"    default: {_yaml_scalar(prd_run_sp_default)}")
    lines.append("  hml_admin_group:")
    lines.append('    description: "Admin group for HML bundle permissions"')
    lines.append(f"    default: {_yaml_scalar(hml_admin_group_default)}")
    lines.append("  prd_admin_group:")
    lines.append('    description: "Admin group for PRD bundle permissions"')
    lines.append(f"    default: {_yaml_scalar(prd_admin_group_default)}")
    lines.append("  environment:")
    lines.append('    description: "Resolved deployment environment"')
    lines.append('    default: "dev"')
    lines.append("  use_catalog:")
    lines.append('    description: "Whether target uses Unity Catalog"')
    lines.append("    default: false")
    lines.append("  default_config_path:")
    lines.append('    description: "Default config path for flows"')
    lines.append('    default: "config/clientes_ml_pipeline.yml"')
    lines.append("  workspace_root:")
    lines.append('    description: "Workspace root for project files"')
    lines.append('    default: "/Workspace/Repos/b3-databricks-platform/b3-databricks-platform"')
    lines.append("")
    lines.append("targets:")

    for env, target in targets_payload["targets"].items():
        lines.append(f"  {env}:")
        lines.append(f"    default: {_yaml_scalar(target['default'])}")
        lines.append(f"    mode: {'development' if env == 'dev' else 'production'}")
        lines.append("    workspace:")
        lines.append(f"      root_path: {_yaml_scalar(target['workspace']['root_path'])}")

        if env == "dev":
            lines.append("    permissions:")
            lines.append("      - user_name: ${var.deploy_user}")
            lines.append("        level: CAN_MANAGE")

        elif env == "hml":
            lines.append("    permissions:")
            lines.append("      - user_name: ${var.deploy_user}")
            lines.append("        level: CAN_MANAGE")
            if not _is_placeholder(hml_admin_group_default):
                lines.append("      - level: CAN_MANAGE")
                lines.append("        group_name: ${var.hml_admin_group}")
            if not _is_placeholder(hml_run_sp_default):
                lines.append("    run_as:")
                lines.append("      service_principal_name: ${var.hml_run_sp}")
                lines.append("    permissions:")
                lines.append("      - user_name: ${var.deploy_user}")
                lines.append("        level: CAN_MANAGE")
                if not _is_placeholder(hml_admin_group_default):
                    lines.append("      - level: CAN_MANAGE")
                    lines.append("        group_name: ${var.hml_admin_group}")
                lines.append("      - level: CAN_MANAGE")
                lines.append("        service_principal_name: ${var.hml_run_sp}")

        elif env == "prd":
            lines.append("    permissions:")
            lines.append("      - user_name: ${var.deploy_user}")
            lines.append("        level: CAN_MANAGE")
            if not _is_placeholder(prd_admin_group_default):
                lines.append("      - level: CAN_MANAGE")
                lines.append("        group_name: ${var.prd_admin_group}")
            if not _is_placeholder(prd_run_sp_default):
                lines.append("    run_as:")
                lines.append("      service_principal_name: ${var.prd_run_sp}")
                lines.append("    permissions:")
                lines.append("      - user_name: ${var.deploy_user}")
                lines.append("        level: CAN_MANAGE")
                if not _is_placeholder(prd_admin_group_default):
                    lines.append("      - level: CAN_MANAGE")
                    lines.append("        group_name: ${var.prd_admin_group}")
                lines.append("      - level: CAN_MANAGE")
                lines.append("        service_principal_name: ${var.prd_run_sp}")

        lines.append("    variables:")
        for key, value in target["bundle_variables"].items():
            lines.append(f"      {key}: {_yaml_scalar(value)}")

    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera databricks.yml raiz final para Bundles."
    )
    parser.add_argument(
        "--output-path",
        default="artifacts/generated_databricks_root.yml",
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    yaml_text = build_bundle_root_yaml()

    output_path = Path(parsed.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml_text, encoding="utf-8")

    print(yaml_text)


if __name__ == "__main__":
    main()
