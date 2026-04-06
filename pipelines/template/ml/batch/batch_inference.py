from pyspark.ml import PipelineModel

from data_platform.core.context import get_context
from data_platform.core.logger import PlatformLogger
from data_platform.mlops.deployments import get_active_model_for_env
from data_platform.mlops.registry import (
    get_latest_valid_model_version,
    get_model_artifact_path,
)
from data_platform.mlops.scoring_runs import log_scoring_run
from data_platform.mlops.monitoring import log_feature_monitoring, log_prediction_monitoring
from data_platform.orchestration.pipeline_runner import run_with_observability


def run_batch_inference(
    spark,
    input_table: str,
    output_table: str,
    model_name: str = "template_model",
    model_version: str | None = None,
    artifact_path: str | None = None,
    target_env: str | None = None,
    project: str = "template",
    use_catalog: bool = False,
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="batch_inference",
        env=ctx.env,
        project=ctx.project,
    )

    run_id = forced_run_id or base_logger.run_id

    def _run(logger: PlatformLogger):
        resolved_model_version = model_version
        resolved_artifact_path = artifact_path
        resolved_target_env = target_env or ctx.env

        if resolved_artifact_path is None and target_env is not None:
            active_model = get_active_model_for_env(
                spark=spark,
                model_name=model_name,
                target_env=resolved_target_env,
                project=project,
                use_catalog=use_catalog,
            )

            if active_model is None:
                raise ValueError(
                    "Nenhum modelo ativo encontrado para "
                    f"model_name={model_name}, target_env={resolved_target_env}"
                )

            resolved_model_version = active_model["model_version"]
            resolved_artifact_path = active_model["artifact_path"]

        if resolved_artifact_path is None:
            if resolved_model_version is None:
                resolved_model_version = get_latest_valid_model_version(
                    spark=spark,
                    model_name=model_name,
                    project=project,
                    use_catalog=use_catalog,
                )

            resolved_artifact_path = get_model_artifact_path(
                spark=spark,
                model_name=model_name,
                model_version=resolved_model_version,
                project=project,
                use_catalog=use_catalog,
            )

        logger.info(f"model_name={model_name}")
        logger.info(f"model_version={resolved_model_version}")
        logger.info(f"target_env={resolved_target_env}")
        logger.info(f"input_table={input_table}")
        logger.info(f"output_table={output_table}")
        logger.info(f"artifact_path={resolved_artifact_path}")

        df = spark.table(input_table)
        input_count = df.count()
        logger.info(f"input_count={input_count}")

        model = PipelineModel.load(resolved_artifact_path)
        predictions = model.transform(df)

        prediction_count = predictions.count()
        logger.info(f"prediction_count={prediction_count}")

        predictions.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

        log_scoring_run(
            spark=spark,
            model_name=model_name,
            model_version=resolved_model_version,
            target_env=resolved_target_env,
            input_table=input_table,
            output_table=output_table,
            input_count=input_count,
            prediction_count=prediction_count,
            artifact_path=resolved_artifact_path,
            run_id=run_id,
            project=project,
            use_catalog=use_catalog,
        )

        log_prediction_monitoring(
            spark=spark,
            predictions_df=predictions,
            model_name=model_name,
            model_version=resolved_model_version,
            target_env=resolved_target_env,
            run_id=run_id,
            project=project,
            use_catalog=use_catalog,
        )

        log_feature_monitoring(
            spark=spark,
            feature_df=df,
            feature_columns=[
                "segmento_dominante",
                "source_type_dominante",
                "qtd_registros",
                "tem_file",
                "tem_table",
            ],
            model_name=model_name,
            model_version=resolved_model_version,
            target_env=resolved_target_env,
            run_id=run_id,
            project=project,
            use_catalog=use_catalog,
        )

        logger.info(f"Batch inference concluido com sucesso: {output_table}")

        del predictions
        del model
        del df

    run_with_observability(
        spark=spark,
        component="batch_inference",
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=use_catalog,
        parent_component=parent_component,
        parent_run_id=parent_run_id,
    )
