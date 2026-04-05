from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from data_platform.core.context import get_context


@dataclass(frozen=True)
class SqlExecutionResult:
    file_name: str
    file_path: str
    status: str
    message: str


def _history_table_name(project: str = "clientes", use_catalog: bool = False) -> str:
    ctx = get_context(project=project, use_catalog=use_catalog)
    return ctx.naming.qualified_table(ctx.naming.schema_obs, "tb_sql_migration_history")


def ensure_sql_history_table(spark, project: str = "clientes", use_catalog: bool = False) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    schema_name = ctx.naming.qualified_schema(ctx.naming.schema_obs)
    table_name = _history_table_name(project=project, use_catalog=use_catalog)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_timestamp TIMESTAMP,
            env STRING,
            project STRING,
            migration_type STRING,
            file_name STRING,
            file_path STRING,
            status STRING,
            message STRING,
            run_id STRING
        )
        """
    )


def log_sql_history(
    spark,
    migration_type: str,
    file_name: str,
    file_path: str,
    status: str,
    message: str,
    run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    from pyspark.sql import Row
    from pyspark.sql import types as T

    ctx = get_context(project=project, use_catalog=use_catalog)
    table_name = _history_table_name(project=project, use_catalog=use_catalog)

    schema = T.StructType(
        [
            T.StructField("event_timestamp", T.TimestampType(), False),
            T.StructField("env", T.StringType(), False),
            T.StructField("project", T.StringType(), False),
            T.StructField("migration_type", T.StringType(), False),
            T.StructField("file_name", T.StringType(), False),
            T.StructField("file_path", T.StringType(), False),
            T.StructField("status", T.StringType(), False),
            T.StructField("message", T.StringType(), False),
            T.StructField("run_id", T.StringType(), False),
        ]
    )

    row = Row(
        event_timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
        env=ctx.env,
        project=ctx.project,
        migration_type=migration_type,
        file_name=file_name,
        file_path=file_path,
        status=status,
        message=message,
        run_id=run_id,
    )

    spark.createDataFrame([row], schema=schema).write.mode("append").saveAsTable(table_name)


def migration_already_executed(
    spark,
    migration_type: str,
    file_name: str,
    project: str = "clientes",
    use_catalog: bool = False,
) -> bool:
    from pyspark.sql import functions as F

    table_name = _history_table_name(project=project, use_catalog=use_catalog)

    if not spark.catalog.tableExists(table_name):
        return False

    return (
        spark.table(table_name)
        .filter(F.col("migration_type") == migration_type)
        .filter(F.col("file_name") == file_name)
        .filter(F.col("status") == "SUCCESS")
        .limit(1)
        .count() > 0
    )


def execute_sql_directory(
    spark,
    sql_dir: str,
    migration_type: str,
    run_id: str,
    project: str = "clientes",
    use_catalog: bool = False,
    stop_on_error: bool = True,
    skip_if_already_executed: bool = True,
) -> list[SqlExecutionResult]:
    ensure_sql_history_table(spark, project=project, use_catalog=use_catalog)

    base_path = Path(sql_dir)
    if not base_path.exists():
        raise FileNotFoundError(f"Diretório SQL não encontrado: {sql_dir}")

    results = []
    sql_files = sorted(base_path.glob("*.sql"))

    for sql_file in sql_files:
        file_name = sql_file.name
        file_path = str(sql_file)
        sql_text = sql_file.read_text(encoding="utf-8").strip()

        if not sql_text:
            result = SqlExecutionResult(
                file_name=file_name,
                file_path=file_path,
                status="SKIPPED",
                message="Arquivo SQL vazio.",
            )
            results.append(result)
            log_sql_history(
                spark=spark,
                migration_type=migration_type,
                file_name=file_name,
                file_path=file_path,
                status=result.status,
                message=result.message,
                run_id=run_id,
                project=project,
                use_catalog=use_catalog,
            )
            continue

        if skip_if_already_executed and migration_already_executed(
            spark=spark,
            migration_type=migration_type,
            file_name=file_name,
            project=project,
            use_catalog=use_catalog,
        ):
            result = SqlExecutionResult(
                file_name=file_name,
                file_path=file_path,
                status="SKIPPED",
                message="Migration já executada com sucesso anteriormente.",
            )
            results.append(result)
            continue

        try:
            spark.sql(sql_text)

            result = SqlExecutionResult(
                file_name=file_name,
                file_path=file_path,
                status="SUCCESS",
                message="SQL executado com sucesso.",
            )

            log_sql_history(
                spark=spark,
                migration_type=migration_type,
                file_name=file_name,
                file_path=file_path,
                status=result.status,
                message=result.message,
                run_id=run_id,
                project=project,
                use_catalog=use_catalog,
            )

            results.append(result)

        except Exception as exc:
            result = SqlExecutionResult(
                file_name=file_name,
                file_path=file_path,
                status="ERROR",
                message=str(exc),
            )

            log_sql_history(
                spark=spark,
                migration_type=migration_type,
                file_name=file_name,
                file_path=file_path,
                status=result.status,
                message=result.message,
                run_id=run_id,
                project=project,
                use_catalog=use_catalog,
            )

            results.append(result)

            if stop_on_error:
                raise

    return results
