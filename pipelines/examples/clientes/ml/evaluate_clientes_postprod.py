from data_platform.core.context import get_context
from data_platform.core.logger import PlatformLogger
from data_platform.mlops.datasets import get_postprod_labels_table
from data_platform.mlops.postprod_evaluation import evaluate_postprod_from_tables
from data_platform.orchestration.pipeline_runner import run_with_observability


def run_evaluate_clientes_postprod(
    spark,
    *,
    model_name: str = "clientes_status_classifier",
    model_version: str | None = None,
    predictions_table: str | None = None,
    project: str = "clientes",
    use_catalog: bool = False,
    window_start: str | None = None,
    window_end: str | None = None,
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="evaluate_clientes_postprod",
        env=ctx.env,
        project=ctx.project,
    )

    run_id = forced_run_id or base_logger.run_id

    def _run(logger: PlatformLogger):
        resolved_predictions_table = predictions_table or ctx.naming.qualified_table(
            ctx.naming.schema_mlops,
            "tb_model_predictions",
        )
        labels_table = get_postprod_labels_table(
            project=project,
            use_catalog=use_catalog,
        )

        logger.info(f"predictions_table={resolved_predictions_table}")
        logger.info(f"labels_table={labels_table}")
        logger.info(f"model_name={model_name}")
        logger.info(f"model_version={model_version}")
        logger.info(f"window_start={window_start}")
        logger.info(f"window_end={window_end}")

        metrics = evaluate_postprod_from_tables(
            spark=spark,
            predictions_table=resolved_predictions_table,
            labels_table=labels_table,
            join_keys=["id_cliente"],
            model_name=model_name,
            model_version=model_version,
            run_id=run_id,
            prediction_col="prediction",
            label_col="label_real",
            prediction_timestamp_col="event_timestamp",
            label_timestamp_col="label_snapshot_ts",
            window_start=window_start,
            window_end=window_end,
            metric_names=["accuracy", "f1", "precision", "recall"],
            project=project,
            use_catalog=use_catalog,
        )

        logger.info(f"postprod_metrics={metrics}")

    run_with_observability(
        spark=spark,
        component="evaluate_clientes_postprod",
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=use_catalog,
        parent_component=parent_component,
        parent_run_id=parent_run_id,
    )
