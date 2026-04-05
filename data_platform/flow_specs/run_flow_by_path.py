import argparse
import json

from data_platform.flow_specs.flow_catalog import load_flow_callable, flow_spec_to_dict


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
        description="Executa um fluxo a partir do import path do módulo."
    )
    parser.add_argument("--spec-module", required=True)
    parser.add_argument("--project", default=None)
    parser.add_argument("--use-catalog", default="false")
    parser.add_argument("--config-path", default=None)
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

    flow_spec, flow_callable = load_flow_callable(parsed.spec_module)

    resolved_project = parsed.project or flow_spec.project

    output = {
        "flow_spec": flow_spec_to_dict(flow_spec),
        "execution": {
            "project": resolved_project,
            "use_catalog": use_catalog,
            "config_path": parsed.config_path,
        },
    }

    print(json.dumps(output, indent=2))

    kwargs = {
        "spark": resolved_spark,
        "project": resolved_project,
        "use_catalog": use_catalog,
    }

    if parsed.config_path is not None:
        kwargs["config_path"] = parsed.config_path

    flow_callable(**kwargs)


if __name__ == "__main__":
    main()
