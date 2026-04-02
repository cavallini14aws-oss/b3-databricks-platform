from b3_platform.core.context import get_context
from b3_platform.core.logger import PlatformLogger
from b3_platform.orchestration.pipeline_runner import run_with_observability


def run_data_pipeline_template(
    spark,
    project: str,
    component_name: str,
    use_catalog: bool | None = None,
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component=component_name,
        env=ctx.env,
        project=ctx.project,
    )

    run_id = forced_run_id or base_logger.run_id

    def _run(logger: PlatformLogger):
        logger.info(f"Executando template de data pipeline: {component_name}")
        logger.info(f"env={ctx.env}")
        logger.info(f"use_catalog={ctx.naming.use_catalog}")
        logger.info(f"debug_mode={ctx.debug_mode}")

        # TODO:
        # 1. Ler fonte
        # 2. Aplicar transformação
        # 3. Persistir destino
        # 4. Validar resultado

        logger.info("Template de data pipeline executado com sucesso")

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
