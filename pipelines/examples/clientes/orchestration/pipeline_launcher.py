import argparse

from b3_platform.orchestration.pipeline_registry import resolve_pipeline_callable


def run_pipeline(
    spark,
    registry_path: str,
    pipeline_name: str,
) -> None:
    pipeline_callable, config_path = resolve_pipeline_callable(
        registry_path=registry_path,
        pipeline_name=pipeline_name,
    )

    pipeline_callable(
        spark=spark,
        config_path=config_path,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Executa pipeline registrado no registry YAML."
    )
    parser.add_argument(
        "--registry-path",
        dest="registry_path",
        default="config/pipelines_registry.yml",
    )
    parser.add_argument(
        "--pipeline-name",
        dest="pipeline_name",
        default="clientes_end_to_end",
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    try:
        spark  # type: ignore[name-defined]
    except NameError as exc:
        raise RuntimeError(
            "Spark session não encontrada. Execute este entrypoint em ambiente Databricks "
            "ou injete a variável global 'spark'."
        ) from exc

    run_pipeline(
        spark=spark,  # type: ignore[name-defined]
        registry_path=parsed.registry_path,
        pipeline_name=parsed.pipeline_name,
    )


if __name__ == "__main__":
    main()
