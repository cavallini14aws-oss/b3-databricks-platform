from uuid import uuid4

from pyspark.sql import functions as F

from b3_platform.core.context import get_context
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


def _history_table_name(project: str = "clientes", use_catalog: bool = False) -> str:
    ctx = get_context(project=project, use_catalog=use_catalog)
    return ctx.naming.qualified_table(ctx.naming.schema_obs, "tb_sql_migration_history")


def _history_already_executed(
    spark,
    migration_type: str,
    file_name: str,
    run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> bool:
    table_name = _history_table_name(project=project, use_catalog=use_catalog)

    if not spark.catalog.tableExists(table_name):
        return False

    return (
        spark.table(table_name)
        .filter(F.col("migration_type") == migration_type)
        .filter(F.col("file_name") == file_name)
        .filter(F.col("run_id") == run_id)
        .filter(F.col("status") == "SUCCESS")
        .limit(1)
        .count() > 0
    )


def _log_if_needed(
    spark,
    migration_type: str,
    file_name: str,
    file_path: str,
    message: str,
    run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> bool:
    if _history_already_executed(
        spark=spark,
        migration_type=migration_type,
        file_name=file_name,
        run_id=run_id,
        project=project,
        use_catalog=use_catalog,
    ):
        return False

    log_sql_history(
        spark=spark,
        migration_type=migration_type,
        file_name=file_name,
        file_path=file_path,
        status="SUCCESS",
        message=message,
        run_id=run_id,
        project=project,
        use_catalog=use_catalog,
    )
    return True


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

    table_path = (
        f"{table_spec.catalog_name}.{table_spec.schema_name}.{table_spec.table_name}"
        if table_spec.catalog_name
        else f"{table_spec.schema_name}.{table_spec.table_name}"
    )

    create_catalog_sql = build_create_catalog_sql(table_spec)
    if create_catalog_sql:
        file_name = table_spec.table_name
        migration_type = "table_spec_create_catalog"

        if not _history_already_executed(
            spark=spark,
            migration_type=migration_type,
            file_name=file_name,
            run_id=resolved_run_id,
            project=project,
            use_catalog=use_catalog,
        ):
            spark.sql(create_catalog_sql)

            _log_if_needed(
                spark=spark,
                migration_type=migration_type,
                file_name=file_name,
                file_path=f"table_spec::{table_spec.catalog_name}",
                message="CREATE CATALOG executado com sucesso.",
                run_id=resolved_run_id,
                project=project,
                use_catalog=use_catalog,
            )

    create_schema_sql = build_create_schema_sql(table_spec)
    if create_schema_sql:
        file_name = table_spec.table_name
        migration_type = "table_spec_create_schema"

        if not _history_already_executed(
            spark=spark,
            migration_type=migration_type,
            file_name=file_name,
            run_id=resolved_run_id,
            project=project,
            use_catalog=use_catalog,
        ):
            spark.sql(create_schema_sql)

            schema_path = (
                f"{table_spec.catalog_name}.{table_spec.schema_name}"
                if table_spec.catalog_name
                else table_spec.schema_name
            )

            _log_if_needed(
                spark=spark,
                migration_type=migration_type,
                file_name=file_name,
                file_path=f"table_spec::{schema_path}",
                message="CREATE SCHEMA executado com sucesso.",
                run_id=resolved_run_id,
                project=project,
                use_catalog=use_catalog,
            )

    create_table_sql = build_create_table_sql(table_spec)
    if not _history_already_executed(
        spark=spark,
        migration_type="table_spec_create",
        file_name=table_spec.table_name,
        run_id=resolved_run_id,
        project=project,
        use_catalog=use_catalog,
    ):
        spark.sql(create_table_sql)

        _log_if_needed(
            spark=spark,
            migration_type="table_spec_create",
            file_name=table_spec.table_name,
            file_path=f"table_spec::{table_path}",
            message="CREATE TABLE executado com sucesso.",
            run_id=resolved_run_id,
            project=project,
            use_catalog=use_catalog,
        )

    owner_sql = build_set_owner_sql(table_spec)
    if owner_sql:
        if not _history_already_executed(
            spark=spark,
            migration_type="table_spec_owner",
            file_name=table_spec.table_name,
            run_id=resolved_run_id,
            project=project,
            use_catalog=use_catalog,
        ):
            spark.sql(owner_sql)

            _log_if_needed(
                spark=spark,
                migration_type="table_spec_owner",
                file_name=table_spec.table_name,
                file_path=f"table_spec::{table_path}",
                message="OWNER aplicado com sucesso.",
                run_id=resolved_run_id,
                project=project,
                use_catalog=use_catalog,
            )

    table_tags_sql = build_set_table_tags_sql(table_spec)
    if table_tags_sql:
        if not _history_already_executed(
            spark=spark,
            migration_type="table_spec_table_tags",
            file_name=table_spec.table_name,
            run_id=resolved_run_id,
            project=project,
            use_catalog=use_catalog,
        ):
            spark.sql(table_tags_sql)

            _log_if_needed(
                spark=spark,
                migration_type="table_spec_table_tags",
                file_name=table_spec.table_name,
                file_path=f"table_spec::{table_path}",
                message="SET TAGS da tabela executado com sucesso.",
                run_id=resolved_run_id,
                project=project,
                use_catalog=use_catalog,
            )

    for column_name, sql in build_set_column_tags_sql_list(table_spec):
        file_name = f"{table_spec.table_name}.{column_name}"

        if not _history_already_executed(
            spark=spark,
            migration_type="table_spec_column_tags",
            file_name=file_name,
            run_id=resolved_run_id,
            project=project,
            use_catalog=use_catalog,
        ):
            spark.sql(sql)

            _log_if_needed(
                spark=spark,
                migration_type="table_spec_column_tags",
                file_name=file_name,
                file_path=f"table_spec::{table_path}.{column_name}",
                message="SET TAGS da coluna executado com sucesso.",
                run_id=resolved_run_id,
                project=project,
                use_catalog=use_catalog,
            )
