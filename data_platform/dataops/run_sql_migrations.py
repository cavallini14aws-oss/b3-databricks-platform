import argparse
import json
from uuid import uuid4

from data_platform.dataops.sql_runner import execute_sql_directory


def _str_to_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value

    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False

    raise ValueError(f"Valor booleano inválido: {value}")


def _resolve_spark_session(explicit_spark=None):
    if explicit_spark is not None:
        return explicit_spark

    try:
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession()
    except Exception:
        return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Executa migrations SQL versionadas."
    )
    parser.add_argument(
        "--migration-type",
        required=True,
        choices=["create_tables", "alter_tables"],
    )
    parser.add_argument("--sql-dir", required=True)
    parser.add_argument("--project", default="clientes")
    parser.add_argument("--use-catalog", default="false")
    parser.add_argument("--stop-on-error", default="true")
    parser.add_argument("--skip-if-already-executed", default="true")
    parser.add_argument("--run-id", default=None)
    return parser


def main(args: list[str] | None = None, spark_session=None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    use_catalog = _str_to_bool(parsed.use_catalog)
    stop_on_error = _str_to_bool(parsed.stop_on_error)
    skip_if_already_executed = _str_to_bool(parsed.skip_if_already_executed)
    run_id = parsed.run_id or str(uuid4())

    resolved_spark = _resolve_spark_session(explicit_spark=spark_session)
    if resolved_spark is None:
        raise RuntimeError(
            "Spark session não encontrada. Execute em Databricks com Spark ativo "
            "ou passe spark_session explicitamente."
        )

    results = execute_sql_directory(
        spark=resolved_spark,
        sql_dir=parsed.sql_dir,
        migration_type=parsed.migration_type,
        run_id=run_id,
        project=parsed.project,
        use_catalog=use_catalog,
        stop_on_error=stop_on_error,
        skip_if_already_executed=skip_if_already_executed,
    )

    output = {
        "migration_type": parsed.migration_type,
        "sql_dir": parsed.sql_dir,
        "project": parsed.project,
        "run_id": run_id,
        "results": [
            {
                "file_name": item.file_name,
                "file_path": item.file_path,
                "status": item.status,
                "message": item.message,
            }
            for item in results
        ],
    }

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
