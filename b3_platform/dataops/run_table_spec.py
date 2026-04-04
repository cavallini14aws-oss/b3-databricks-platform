from uuid import uuid4

from b3_platform.core.context import get_context
from b3_platform.dataops.sql_runner import log_sql_history
from b3_platform.dataops.table_builder import (
    build_create_table_sql,
    build_set_column_tags_sql_list,
    build_set_table_tags_sql,
)
from b3_platform.dataops.table_spec import TableSpec
from b3_platform.dataops.table_validator import validate_table_spec


def run_table_spec(
    spark,
    table_spec: TableSpec,
    project: str = "clientes",
    use_catalog: bool = False,
    run_id: str | None = None,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    resolved_run_id = run_id or str(uuid4())

    validate_table_spec(table_spec)

    create_sql = build_create_table_sql(
        table_spec=table_spec,
        project=project,
        use_catalog=use_catalog,
    )

    spark.sql(create_sql)

    log_sql_history(
        spark=spark,
        migration_type="table_spec_create",
        file_name=table_spec.table_name,
        file_path=f"table_spec::{table_spec.schema_name}.{table_spec.table_name}",
        status="SUCCESS",
        message="CREATE TABLE executado com sucesso.",
        run_id=resolved_run_id,
        project=project,
        use_catalog=use_catalog,
    )

    table_tags_sql = build_set_table_tags_sql(
        table_spec=table_spec,
        project=project,
        use_catalog=use_catalog,
    )

    if table_tags_sql:
        spark.sql(table_tags_sql)

        log_sql_history(
            spark=spark,
            migration_type="table_spec_table_tags",
            file_name=table_spec.table_name,
            file_path=f"table_spec::{table_spec.schema_name}.{table_spec.table_name}",
            status="SUCCESS",
            message="SET TAGS da tabela executado com sucesso.",
            run_id=resolved_run_id,
            project=project,
            use_catalog=use_catalog,
        )

    for column_name, sql in build_set_column_tags_sql_list(
        table_spec=table_spec,
        project=project,
        use_catalog=use_catalog,
    ):
        spark.sql(sql)

        log_sql_history(
            spark=spark,
            migration_type="table_spec_column_tags",
            file_name=f"{table_spec.table_name}.{column_name}",
            file_path=f"table_spec::{table_spec.schema_name}.{table_spec.table_name}.{column_name}",
            status="SUCCESS",
            message="SET TAGS da coluna executado com sucesso.",
            run_id=resolved_run_id,
            project=project,
            use_catalog=use_catalog,
        )
