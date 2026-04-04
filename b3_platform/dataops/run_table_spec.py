from uuid import uuid4

from b3_platform.dataops.sql_runner import ensure_sql_history_table, log_sql_history
from b3_platform.dataops.table_builder import (
    build_create_catalog_sql,
    build_create_schema_sql,
    build_create_table_sql,
    build_set_column_tags_sql_list,
    build_set_owner_sql,
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
    resolved_run_id = run_id or str(uuid4())

    validate_table_spec(table_spec)
    ensure_sql_history_table(spark, project=project, use_catalog=use_catalog)

    create_catalog_sql = build_create_catalog_sql(table_spec)
    if create_catalog_sql:
        spark.sql(create_catalog_sql)

        log_sql_history(
            spark=spark,
            migration_type="table_spec_create_catalog",
            file_name=table_spec.table_name,
            file_path=f"table_spec::{table_spec.catalog_name}",
            status="SUCCESS",
            message="CREATE CATALOG executado com sucesso.",
            run_id=resolved_run_id,
            project=project,
            use_catalog=use_catalog,
        )

    create_schema_sql = build_create_schema_sql(table_spec)
    if create_schema_sql:
        spark.sql(create_schema_sql)

        schema_path = (
            f"{table_spec.catalog_name}.{table_spec.schema_name}"
            if table_spec.catalog_name
            else table_spec.schema_name
        )

        log_sql_history(
            spark=spark,
            migration_type="table_spec_create_schema",
            file_name=table_spec.table_name,
            file_path=f"table_spec::{schema_path}",
            status="SUCCESS",
            message="CREATE SCHEMA executado com sucesso.",
            run_id=resolved_run_id,
            project=project,
            use_catalog=use_catalog,
        )

    create_table_sql = build_create_table_sql(table_spec)
    spark.sql(create_table_sql)

    table_path = (
        f"{table_spec.catalog_name}.{table_spec.schema_name}.{table_spec.table_name}"
        if table_spec.catalog_name
        else f"{table_spec.schema_name}.{table_spec.table_name}"
    )

    log_sql_history(
        spark=spark,
        migration_type="table_spec_create",
        file_name=table_spec.table_name,
        file_path=f"table_spec::{table_path}",
        status="SUCCESS",
        message="CREATE TABLE executado com sucesso.",
        run_id=resolved_run_id,
        project=project,
        use_catalog=use_catalog,
    )

    owner_sql = build_set_owner_sql(table_spec)
    if owner_sql:
        spark.sql(owner_sql)

        log_sql_history(
            spark=spark,
            migration_type="table_spec_owner",
            file_name=table_spec.table_name,
            file_path=f"table_spec::{table_path}",
            status="SUCCESS",
            message="OWNER aplicado com sucesso.",
            run_id=resolved_run_id,
            project=project,
            use_catalog=use_catalog,
        )

    table_tags_sql = build_set_table_tags_sql(table_spec)
    if table_tags_sql:
        spark.sql(table_tags_sql)

        log_sql_history(
            spark=spark,
            migration_type="table_spec_table_tags",
            file_name=table_spec.table_name,
            file_path=f"table_spec::{table_path}",
            status="SUCCESS",
            message="SET TAGS da tabela executado com sucesso.",
            run_id=resolved_run_id,
            project=project,
            use_catalog=use_catalog,
        )

    for column_name, sql in build_set_column_tags_sql_list(table_spec):
        spark.sql(sql)

        log_sql_history(
            spark=spark,
            migration_type="table_spec_column_tags",
            file_name=f"{table_spec.table_name}.{column_name}",
            file_path=f"table_spec::{table_path}.{column_name}",
            status="SUCCESS",
            message="SET TAGS da coluna executado com sucesso.",
            run_id=resolved_run_id,
            project=project,
            use_catalog=use_catalog,
        )
