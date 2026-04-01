from b3_platform.logger import PlatformLogger
from b3_platform.observability import log_pipeline_event


def run_with_observability(
    spark,
    component: str,
    env: str,
    project: str,
    run_id: str,
    fn,
    use_catalog: bool = False,
):
    logger = PlatformLogger(
        component=component,
        env=env,
        project=project,
        run_id=run_id,
    )

    log_pipeline_event(
        spark=spark,
        component=component,
        status="STARTED",
        run_id=run_id,
        message="Pipeline iniciado",
        project=project,
        use_catalog=use_catalog,
    )

    try:
        result = fn(logger)

        log_pipeline_event(
            spark=spark,
            component=component,
            status="SUCCESS",
            run_id=run_id,
            message="Pipeline finalizado com sucesso",
            project=project,
            use_catalog=use_catalog,
        )

        return result

    except Exception as exc:
        log_pipeline_event(
            spark=spark,
            component=component,
            status="ERROR",
            run_id=run_id,
            message=str(exc),
            project=project,
            use_catalog=use_catalog,
        )
        raise