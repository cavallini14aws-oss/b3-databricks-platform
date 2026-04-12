from pathlib import Path

from data_platform.governance.table_governance_validator import validate_sql_template_usage
from data_platform.table_specs.base.base_table_spec import BaseColumnSpec, BaseTableSpec


def test_validate_sql_template_usage_ok(tmp_path):
    sql_file = tmp_path / "table.sql"
    sql_file.write_text(
        """
CREATE TABLE x (id STRING)
USING DELTA
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.feature.appendOnly' = 'supported',
    'delta.feature.deletionVectors' = 'supported',
    'delta.feature.invariants' = 'supported',
    'delta.minReaderVersion' = '3',
    'delta.minWriterVersion' = '7'
);
""",
        encoding="utf-8",
    )

    result = validate_sql_template_usage(str(sql_file))
    assert result["valid"] is True


def test_validate_table_spec_object_ok():
    from data_platform.governance.table_governance_validator import validate_table_spec_object

    spec = BaseTableSpec(
        catalog="catalog",
        schema="silver",
        table_name="tb_example",
        table_description="Tabela de exemplo",
        columns=[
            BaseColumnSpec(name="id", data_type="STRING", comment="Identificador")
        ],
    )

    result = validate_table_spec_object(spec)
    assert result["valid"] is True
