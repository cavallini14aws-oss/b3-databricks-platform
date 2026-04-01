from datetime import datetime

from b3_platform.core.logger import PlatformLogger
from b3_platform.orchestration.lineage import log_pipeline_lineage
from b3_platform.orchestration.observability import log_pipeline_event


def run_with_observability(
    spark,
    component: str,
    env: str,
    project: str,
    run_id: str,
    fn,
    use_catalog: bool = False,
    parent_component: str | None = None,
    parent_run_id: str | None = None,
):
    logger = PlatformLogger(
        component=component,
        env=env,
        project=project,
        run_id=run_id,
    )

    if parent_component and parent_run_id:
        log_pipeline_lineage(
            spark=spark,
            parent_component=parent_component,
            parent_run_id=parent_run_id,
            child_component=component,
            child_run_id=run_id,
            project=project,
            use_catalog=use_catalog,
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

    started_at = datetime.utcnow()

    try:
        result = fn(logger)

        duration_seconds = (datetime.utcnow() - started_at).total_seconds()

        log_pipeline_event(
            spark=spark,
            component=component,
            status="SUCCESS",
            run_id=run_id,
            message="Pipeline finalizado com sucesso",
            project=project,
            use_catalog=use_catalog,
            duration_seconds=duration_seconds,
        )

        return result

    except Exception as exc:
        duration_seconds = (datetime.utcnow() - started_at).total_seconds()

        log_pipeline_event(
            spark=spark,
            component=component,
            status="ERROR",
            run_id=run_id,
            message=f"{type(exc).__name__}: {str(exc)}",
            project=project,
            use_catalog=use_catalog,
            duration_seconds=duration_seconds,
        )
        raise
