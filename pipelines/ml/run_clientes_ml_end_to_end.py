from uuid import uuid4

from b3_platform.core.context import get_context
from b3_platform.core.logger import PlatformLogger
from b3_platform.orchestration.pipeline_runner import run_with_observability
from pipelines.ml.evaluate_clientes_model import run_evaluate_clientes_model
from pipelines.ml.prepare_clientes_training_dataset import run_prepare_clientes_training_dataset
from pipelines.ml.train_clientes_model import run_train_clientes_model


def run_clientes_ml_end_to_end(
    spark,
    project: str = "clientes",
    use_catalog: bool = False,
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
            use_catalog=use_catalog,
            parent_component="run_clientes_ml_end_to_end",
            parent_run_id=run_id,
            forced_run_id=str(uuid4()),
        )

        logger.info("Executando treino do modelo")

        model_version = run_train_clientes_model(
            spark=spark,
            project=project,
            use_catalog=use_catalog,
            parent_component="run_clientes_ml_end_to_end",
            parent_run_id=run_id,
            forced_run_id=str(uuid4()),
        )

        logger.info(f"Executando avaliação do modelo: {model_version}")

        run_evaluate_clientes_model(
            spark=spark,
            model_version=model_version,
            project=project,
            use_catalog=use_catalog,
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
        use_catalog=use_catalog,
        parent_component=parent_component,
        parent_run_id=parent_run_id,
    )
