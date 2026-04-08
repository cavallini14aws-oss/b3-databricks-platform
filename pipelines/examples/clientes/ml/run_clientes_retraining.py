from uuid import uuid4

from data_platform.core.context import get_context
from data_platform.core.logger import PlatformLogger
from data_platform.mlops.retraining import execute_retraining_request
from data_platform.orchestration.pipeline_runner import run_with_observability
from pipelines.examples.clientes.ml.evaluate_clientes_model import run_evaluate_clientes_model
from pipelines.examples.clientes.ml.prepare_clientes_training_dataset import run_prepare_clientes_training_dataset
from pipelines.examples.clientes.ml.train_clientes_model import run_train_clientes_model


def run_clientes_retraining(
    spark,
    *,
    request_payload: dict,
    project: str = "clientes",
    use_catalog: bool = False,
    config_path: str = "config/clientes_ml_pipeline.yml",
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> dict:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="run_clientes_retraining",
        env=ctx.env,
        project=ctx.project,
    )

    run_id = forced_run_id or base_logger.run_id

    def _run(logger: PlatformLogger):
        if request_payload["request_status"] != "APPROVED":
            raise ValueError("Somente retraining request APPROVED pode ser executado")

        logger.info("Executando preparação do dataset de treino para retraining")
        run_prepare_clientes_training_dataset(
            spark=spark,
            project=project,
            use_catalog=ctx.naming.use_catalog,
            dataset_version="v2",
            parent_component="run_clientes_retraining",
            parent_run_id=run_id,
            forced_run_id=str(uuid4()),
        )

        logger.info("Executando treino do modelo em retraining")
        new_model_version = run_train_clientes_model(
            spark=spark,
            project=project,
            use_catalog=ctx.naming.use_catalog,
            config_path=config_path,
            parent_component="run_clientes_retraining",
            parent_run_id=run_id,
            forced_run_id=str(uuid4()),
        )

        logger.info(f"Executando avaliação do novo modelo: {new_model_version}")
        run_evaluate_clientes_model(
            spark=spark,
            model_version=new_model_version,
            project=project,
            use_catalog=ctx.naming.use_catalog,
            config_path=config_path,
            parent_component="run_clientes_retraining",
            parent_run_id=run_id,
            forced_run_id=str(uuid4()),
        )

        lifecycle = execute_retraining_request(
            spark=spark,
            model_name=request_payload["model_name"],
            model_version=new_model_version,
            trigger_type=request_payload["trigger_type"],
            trigger_source=request_payload["trigger_source"],
            trigger_severity=request_payload.get("trigger_severity"),
            reason=request_payload.get("reason"),
            requested_by=request_payload.get("requested_by"),
            run_id=run_id,
            project=project,
            use_catalog=ctx.naming.use_catalog,
        )

        logger.info(f"Retraining executado com sucesso: {new_model_version}")

        return {
            **lifecycle,
            "new_model_version": new_model_version,
        }

    return run_with_observability(
        spark=spark,
        component="run_clientes_retraining",
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=ctx.naming.use_catalog,
        parent_component=parent_component,
        parent_run_id=parent_run_id,
    )
