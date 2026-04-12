from pathlib import Path

from data_platform.core.config_loader import load_yaml_config
from data_platform.table_specs.base.base_table_spec import BaseTableSpec


def validate_sql_template_usage(sql_path: str) -> dict:
    sql_text = Path(sql_path).read_text(encoding="utf-8")

    required_snippets = [
        "USING DELTA",
        "'delta.enableDeletionVectors' = 'true'",
        "'delta.feature.appendOnly' = 'supported'",
        "'delta.feature.deletionVectors' = 'supported'",
        "'delta.feature.invariants' = 'supported'",
        "'delta.minReaderVersion' = '3'",
        "'delta.minWriterVersion' = '7'",
    ]

    missing = [snippet for snippet in required_snippets if snippet not in sql_text]

    return {
        "valid": not missing,
        "missing_sql_requirements": missing,
    }


def validate_table_spec_object(spec: BaseTableSpec) -> dict:
    errors = []

    if spec.using_format != "delta":
        errors.append("using_format deve ser delta")

    if not spec.table_description.strip():
        errors.append("table_description obrigatório")

    if not spec.columns:
        errors.append("ao menos uma coluna obrigatória")

    for column in spec.columns:
        if not column.name.strip():
            errors.append("column.name obrigatório")
        if not column.data_type.strip():
            errors.append(f"column.data_type obrigatório: {column.name}")
        if not column.comment.strip():
            errors.append(f"column.comment obrigatório: {column.name}")

    effective_props = spec.effective_table_properties()
    contract = load_yaml_config("config/platform_contracts/table_governance_contract.yml")
    mandatory_props = contract["table_governance"]["mandatory_delta_properties"]

    for prop in mandatory_props:
        if prop not in effective_props:
            errors.append(f"delta property obrigatória ausente: {prop}")

    return {
        "valid": not errors,
        "errors": errors,
    }
