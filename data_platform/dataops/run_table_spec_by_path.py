import argparse
import importlib

from data_platform.dataops.run_table_spec import run_table_spec


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
        description="Executa uma TableSpec a partir do import path do módulo."
    )
    parser.add_argument("--spec-module", required=True)
    parser.add_argument("--project", default="clientes")
    parser.add_argument("--use-catalog", default="false")
    parser.add_argument("--run-id", default=None)
    return parser


def main(args: list[str] | None = None, spark_session=None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    use_catalog = _str_to_bool(parsed.use_catalog)

    resolved_spark = _resolve_spark_session(explicit_spark=spark_session)
    if resolved_spark is None:
        raise RuntimeError(
            "Spark session não encontrada. Execute em Databricks com Spark ativo "
            "ou passe spark_session explicitamente."
        )

    module = importlib.import_module(parsed.spec_module)

    if not hasattr(module, "TABLE_SPEC"):
        raise AttributeError(
            f"O módulo {parsed.spec_module} não possui TABLE_SPEC."
        )

    run_table_spec(
        spark=resolved_spark,
        table_spec=module.TABLE_SPEC,
        project=parsed.project,
        use_catalog=use_catalog,
        run_id=parsed.run_id,
    )


if __name__ == "__main__":
    main()
