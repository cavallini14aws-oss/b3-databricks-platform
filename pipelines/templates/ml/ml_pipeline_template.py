from b3_platform.core.config_loader import load_yaml_config
from b3_platform.core.context import get_context
from b3_platform.core.logger import PlatformLogger
from b3_platform.orchestration.pipeline_runner import run_with_observability


def run_ml_pipeline_template(
    spark,
    project: str,
    component_name: str,
    config_path: str,
    use_catalog: bool | None = None,
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    config = load_yaml_config(config_path)

    base_logger = PlatformLogger(
        component=component_name,
        env=ctx.env,
        project=ctx.project,
    )

    run_id = forced_run_id or base_logger.run_id

    def _run(logger: PlatformLogger):
        logger.info(f"Executando template de ML pipeline: {component_name}")
        logger.info(f"env={ctx.env}")
        logger.info(f"use_catalog={ctx.naming.use_catalog}")
        logger.info(f"debug_mode={ctx.debug_mode}")
        logger.info(f"enable_registry={ctx.flags.enable_registry}")
        logger.info(f"enable_baseline={ctx.flags.enable_baseline}")
        logger.info(f"enable_confusion_matrix={ctx.flags.enable_confusion_matrix}")
        logger.info(f"enable_predictions_logging={ctx.flags.enable_predictions_logging}")
        logger.info(f"config_path={config_path}")
        logger.info(f"config_keys={list(config.keys())}")

        # TODO:
        # 1. Preparar dataset
        # 2. Treinar modelo
        # 3. Avaliar
        # 4. Persistir métricas/predictions
        # 5. Aplicar gates por ambiente

        logger.info("Template de ML pipeline executado com sucesso")

    run_with_observability(
        spark=spark,
        component=component_name,
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=ctx.naming.use_catalog,
        parent_component=parent_component,
        parent_run_id=parent_run_id,
    )
