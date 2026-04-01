from b3_platform.context import get_context
from b3_platform.logger import PlatformLogger
from b3_platform.observability import log_pipeline_event


def run_log_pipeline_runs_demo(
    spark,
    project: str = "clientes",
    use_catalog: bool = False,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    logger = PlatformLogger(
        component="log_pipeline_runs_demo",
        env=ctx.env,
        project=ctx.project,
    )

    logger.info("Iniciando gravação de observabilidade")

    log_pipeline_event(
        spark=spark,
        component="log_pipeline_runs_demo",
        status="STARTED",
        run_id=logger.run_id,
        message="Pipeline iniciado",
        project=project,
        use_catalog=use_catalog,
    )

    log_pipeline_event(
        spark=spark,
        component="log_pipeline_runs_demo",
        status="SUCCESS",
        run_id=logger.run_id,
        message="Pipeline finalizado com sucesso",
        project=project,
        use_catalog=use_catalog,
    )

    logger.info("Observabilidade gravada com sucesso")