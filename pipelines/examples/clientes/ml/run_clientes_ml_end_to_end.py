import argparse
from uuid import uuid4

from b3_platform.core.context import get_context
from b3_platform.core.logger import PlatformLogger
from b3_platform.orchestration.pipeline_runner import run_with_observability
from pipelines.examples.clientes.ml.evaluate_clientes_model import run_evaluate_clientes_model
from pipelines.examples.clientes.ml.prepare_clientes_training_dataset import run_prepare_clientes_training_dataset
from pipelines.examples.clientes.ml.train_clientes_model import run_train_clientes_model


def _str_to_bool(value: str | bool | None) -> bool | None:
    if value is None or isinstance(value, bool):
        return value

    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False

    raise ValueError(f"Valor booleano inválido: {value}")


def run_clientes_ml_end_to_end(
    spark,
    project: str = "clientes",
    use_catalog: bool | None = None,
    config_path: str = "config/clientes_ml_pipeline.yml",
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="run_clientes_ml_end_to_end",
        env=ctx.env,
        project=ctx.project,
    )

    run_id = forced_run_id or base_logger.run_id

    def _run(logger: PlatformLogger):
        logger.info("Executando preparação do dataset de treino")

        run_prepare_clientes_training_dataset(
            spark=spark,
            project=project,
            use_catalog=ctx.naming.use_catalog,
            dataset_version="v2",
            parent_component="run_clientes_ml_end_to_end",
            parent_run_id=run_id,
            forced_run_id=str(uuid4()),
        )

        logger.info("Executando treino do modelo")

        model_version = run_train_clientes_model(
            spark=spark,
            project=project,
            use_catalog=ctx.naming.use_catalog,
            config_path=config_path,
            parent_component="run_clientes_ml_end_to_end",
            parent_run_id=run_id,
            forced_run_id=str(uuid4()),
        )

        logger.info(f"Executando avaliação do modelo: {model_version}")

        run_evaluate_clientes_model(
            spark=spark,
            model_version=model_version,
            project=project,
            use_catalog=ctx.naming.use_catalog,
            config_path=config_path,
            parent_component="run_clientes_ml_end_to_end",
            parent_run_id=run_id,
            forced_run_id=str(uuid4()),
        )

        logger.info("Pipeline ML end-to-end concluído com sucesso")

    run_with_observability(
        spark=spark,
        component="run_clientes_ml_end_to_end",
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=ctx.naming.use_catalog,
        parent_component=parent_component,
        parent_run_id=parent_run_id,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Executa pipeline ML end-to-end do exemplo clientes."
    )
    parser.add_argument("--project", default="clientes")
    parser.add_argument(
        "--config-path",
        dest="config_path",
        default="config/clientes_ml_pipeline.yml",
    )
    parser.add_argument(
        "--use-catalog",
        dest="use_catalog",
        default=None,
        help="true/false. Se omitido, usa o valor do ambiente.",
    )
    return parser


def main(args: list[str] | None = None) -> None:
    parser = build_parser()
    parsed = parser.parse_args(args=args)

    use_catalog = _str_to_bool(parsed.use_catalog)

    try:
        spark  # type: ignore[name-defined]
    except NameError as exc:
        raise RuntimeError(
            "Spark session não encontrada. Execute este entrypoint em ambiente Databricks "
            "ou injete a variável global 'spark'."
        ) from exc

    run_clientes_ml_end_to_end(
        spark=spark,  # type: ignore[name-defined]
        project=parsed.project,
        use_catalog=use_catalog,
        config_path=parsed.config_path,
    )


if __name__ == "__main__":
    main()
