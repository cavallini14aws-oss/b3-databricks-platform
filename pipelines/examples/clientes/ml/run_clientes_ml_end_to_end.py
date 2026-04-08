import argparse
from uuid import uuid4

from data_platform.core.config_loader import load_yaml_config
from data_platform.core.context import get_context
from data_platform.core.logger import PlatformLogger
from data_platform.mlops.datasets import get_scoring_dataset_table
from data_platform.mlops.registry import get_model_artifact_path
from data_platform.mlops.smoke_runs import log_smoke_run
from data_platform.orchestration.pipeline_runner import run_with_observability
from pipelines.examples.clientes.ml.evaluate_clientes_model import run_evaluate_clientes_model
from pipelines.examples.clientes.ml.prepare_clientes_scoring_dataset import run_prepare_clientes_scoring_dataset
from pipelines.examples.clientes.ml.prepare_clientes_training_dataset import run_prepare_clientes_training_dataset
from pipelines.examples.clientes.ml.train_clientes_model import run_train_clientes_model
from pipelines.template.ml.batch.batch_inference import run_batch_inference


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
    skip_train: bool = False,
    existing_model_version: str | None = None,
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
    config = load_yaml_config(config_path)
    model_name = config["model"]["name"]

    def _run(logger: PlatformLogger):
        logger.info(f"skip_train={skip_train}")
        logger.info(f"existing_model_version={existing_model_version}")

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

        if skip_train:
            if not existing_model_version:
                raise ValueError(
                    "existing_model_version deve ser informado quando skip_train=True"
                )

            model_version = existing_model_version
            logger.info(
                f"Pulando treino e reutilizando model_version existente: {model_version}"
            )
        else:
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

        artifact_path = get_model_artifact_path(
            spark=spark,
            model_name=model_name,
            model_version=model_version,
            project=project,
            use_catalog=ctx.naming.use_catalog,
        )
        logger.info(f"smoke_artifact_path={artifact_path}")

        logger.info("Executando preparação do scoring dataset")

        run_prepare_clientes_scoring_dataset(
            spark=spark,
            project=project,
            use_catalog=ctx.naming.use_catalog,
            dataset_version="v2",
            parent_component="run_clientes_ml_end_to_end",
            parent_run_id=run_id,
            forced_run_id=str(uuid4()),
        )

        scoring_input_table = get_scoring_dataset_table(
            project=project,
            use_catalog=ctx.naming.use_catalog,
            version="v2",
        )

        scoring_output_table = ctx.naming.qualified_table(
            ctx.naming.schema_mlops,
            f"tb_{model_name}_smoke_predictions_{ctx.env}",
        )

        logger.info(f"Executando batch inference de smoke: {model_version}")
        logger.info(f"scoring_input_table={scoring_input_table}")
        logger.info(f"scoring_output_table={scoring_output_table}")

        log_smoke_run(
            spark=spark,
            component="run_clientes_ml_end_to_end",
            model_name=model_name,
            model_version=model_version,
            status="STARTED",
            run_id=run_id,
            input_table=scoring_input_table,
            output_table=scoring_output_table,
            message="Smoke ML observavel iniciado",
            project=project,
            use_catalog=ctx.naming.use_catalog,
        )

        try:
            run_batch_inference(
                spark=spark,
                input_table=scoring_input_table,
                output_table=scoring_output_table,
                model_name=model_name,
                model_version=model_version,
                artifact_path=artifact_path,
                target_env=ctx.env,
                project=project,
                use_catalog=ctx.naming.use_catalog,
                parent_component="run_clientes_ml_end_to_end",
                parent_run_id=run_id,
                forced_run_id=str(uuid4()),
            )

            log_smoke_run(
                spark=spark,
                component="run_clientes_ml_end_to_end",
                model_name=model_name,
                model_version=model_version,
                status="SUCCESS",
                run_id=run_id,
                input_table=scoring_input_table,
                output_table=scoring_output_table,
                message="Smoke ML observavel concluido com sucesso",
                project=project,
                use_catalog=ctx.naming.use_catalog,
            )

            logger.info("Pipeline ML end-to-end concluído com sucesso")
        except Exception as exc:
            log_smoke_run(
                spark=spark,
                component="run_clientes_ml_end_to_end",
                model_name=model_name,
                model_version=model_version,
                status="ERROR",
                run_id=run_id,
                input_table=scoring_input_table,
                output_table=scoring_output_table,
                message=f"{type(exc).__name__}: {str(exc)}",
                project=project,
                use_catalog=ctx.naming.use_catalog,
            )
            raise

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
    parser.add_argument(
        "--skip-train",
        dest="skip_train",
        action="store_true",
        help="Pula o treino e reutiliza uma versao de modelo existente.",
    )
    parser.add_argument(
        "--existing-model-version",
        dest="existing_model_version",
        default=None,
        help="Model version existente para usar quando --skip-train estiver ativo.",
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
        skip_train=parsed.skip_train,
        existing_model_version=parsed.existing_model_version,
    )


if __name__ == "__main__":
    main()
