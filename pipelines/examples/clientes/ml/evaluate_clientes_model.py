from datetime import datetime, timezone
import gc

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from b3_platform.config_loader import load_clientes_ml_pipeline_config
from b3_platform.core.context import get_context
from b3_platform.core.logger import get_logger
from b3_platform.governance.promotion import evaluate_promotion_rules, log_promotion_decision
from b3_platform.mlops.baseline import compute_majority_baseline_accuracy, log_baseline_metric
from b3_platform.mlops.datasets import get_training_dataset_table
from b3_platform.mlops.registry import get_latest_valid_model_version, get_model_artifact_path
from b3_platform.mlops.evaluation import log_confusion_matrix, log_model_metric
from b3_platform.orchestration.pipeline_runner import run_with_observability


def run_evaluate_clientes_model(
    spark,
    model_version: str | None = None,
    project: str = "clientes",
    use_catalog: bool = False,
    config_path: str = "config/clientes_ml_pipeline.yml",
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)
    base_logger = get_logger("evaluate_clientes_model", project=project, run_id=forced_run_id)
    run_id = forced_run_id or base_logger.run_id

    def _run(logger):
        config = load_clientes_ml_pipeline_config(config_path)
        ml_config = config.mlops

        dataset_table = get_training_dataset_table(
            project=project,
            use_catalog=use_catalog,
        )
        predictions_table = ctx.naming.qualified_table(
            ctx.naming.schema_mlops,
            "tb_model_predictions",
        )
        mlops_schema = ctx.naming.qualified_schema(ctx.naming.schema_mlops)

        model_name = ml_config.model_registry.model_name
        dataset_version = ml_config.features.dataset_version
        feature_columns = ml_config.features.feature_columns
        label_column = ml_config.features.label_column
        job_config = ml_config.jobs["evaluate_model"]

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {mlops_schema}")

        logger.info(f"dataset_table={dataset_table}")

        resolved_model_version = model_version
        if resolved_model_version is None:
            resolved_model_version = get_latest_valid_model_version(
                spark=spark,
                model_name=model_name,
                project=project,
                use_catalog=use_catalog,
            )
            logger.info(
                "model_version nao informada. "
                f"Usando ultima versao valida: {resolved_model_version}"
            )

        logger.info(f"model_version={resolved_model_version}")
        logger.info(f"dataset_version={dataset_version}")
        logger.info(f"predictions_table={predictions_table}")
        logger.info(f"feature_columns={feature_columns}")
        logger.info(f"enable_baseline={job_config.enable_baseline}")
        logger.info(f"enable_confusion_matrix={job_config.enable_confusion_matrix}")
        logger.info(f"enable_predictions_logging={job_config.enable_predictions_logging}")
        logger.info(f"job_environment={job_config.environment}")

        promotion_target_env = (
            job_config.promotion_rules[0].target_env
            if job_config.promotion_rules
            else None
        )
        logger.info(f"promotion_target_env={promotion_target_env}")

        artifact_path = get_model_artifact_path(
            spark=spark,
            model_name=model_name,
            model_version=resolved_model_version,
            project=project,
            use_catalog=use_catalog,
        )

        logger.info(f"artifact_path={artifact_path}")

        df = spark.table(dataset_table)
        test_df = df.filter(F.col("dataset_split") == "test")

        logger.info(f"test_count={test_df.count()}")

        model = PipelineModel.load(artifact_path)
        predictions = model.transform(test_df)

        metric_values = {}

        evaluator_auc = BinaryClassificationEvaluator(
            labelCol=label_column,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        )
        auc_value = evaluator_auc.evaluate(predictions)
        metric_values["auc"] = auc_value
        log_model_metric(
            spark=spark,
            model_name=model_name,
            model_version=resolved_model_version,
            metric_name="auc",
            metric_value=auc_value,
            dataset_version=dataset_version,
            run_id=run_id,
            project=project,
            use_catalog=use_catalog,
        )

        evaluator_accuracy = MulticlassClassificationEvaluator(
            labelCol=label_column,
            predictionCol="prediction",
            metricName="accuracy",
        )
        accuracy_value = evaluator_accuracy.evaluate(predictions)
        metric_values["accuracy"] = accuracy_value
        log_model_metric(
            spark=spark,
            model_name=model_name,
            model_version=resolved_model_version,
            metric_name="accuracy",
            metric_value=accuracy_value,
            dataset_version=dataset_version,
            run_id=run_id,
            project=project,
            use_catalog=use_catalog,
        )

        evaluator_f1 = MulticlassClassificationEvaluator(
            labelCol=label_column,
            predictionCol="prediction",
            metricName="f1",
        )
        f1_value = evaluator_f1.evaluate(predictions)
        metric_values["f1"] = f1_value
        log_model_metric(
            spark=spark,
            model_name=model_name,
            model_version=resolved_model_version,
            metric_name="f1",
            metric_value=f1_value,
            dataset_version=dataset_version,
            run_id=run_id,
            project=project,
            use_catalog=use_catalog,
        )

        if job_config.enable_baseline:
            baseline_accuracy = compute_majority_baseline_accuracy(
                test_df=test_df,
                label_column=label_column,
            )
            log_baseline_metric(
                spark=spark,
                model_name=model_name,
                model_version=resolved_model_version,
                baseline_name="majority_class_accuracy",
                baseline_value=baseline_accuracy,
                dataset_version=dataset_version,
                run_id=run_id,
                project=project,
                use_catalog=use_catalog,
            )

        if job_config.enable_confusion_matrix:
            confusion_df = (
                predictions.groupBy(
                    F.col(label_column).alias("label"),
                    F.col("prediction"),
                )
                .count()
                .orderBy("label", "prediction")
            )

            confusion_rows = confusion_df.collect()
            log_confusion_matrix(
                spark=spark,
                model_name=model_name,
                model_version=resolved_model_version,
                rows=confusion_rows,
                dataset_version=dataset_version,
                run_id=run_id,
                project=project,
                use_catalog=use_catalog,
            )

        approved, reason = evaluate_promotion_rules(
            metric_values=metric_values,
            promotion_rules=job_config.promotion_rules,
        )
        logger.info(f"promotion_decision_approved={approved}")
        logger.info(f"promotion_decision_reason={reason}")

        log_promotion_decision(
            spark=spark,
            model_name=model_name,
            model_version=resolved_model_version,
            source_env=job_config.environment,
            target_env=promotion_target_env,
            approved=approved,
            reason=reason,
            metrics=metric_values,
            run_id=run_id,
            project=project,
            use_catalog=use_catalog,
        )

        if job_config.enable_predictions_logging:
            prediction_rows = []
            for row in predictions.select(*feature_columns, label_column, "prediction").collect():
                payload = {col: row[col] for col in feature_columns}
                payload["label"] = row[label_column]
                payload["prediction"] = row["prediction"]

                prediction_rows.append(
                    Row(
                        event_timestamp=datetime.now(timezone.utc),
                        model_name=model_name,
                        model_version=resolved_model_version,
                        dataset_version=dataset_version,
                        payload_json=str(payload),
                        run_id=run_id,
                    )
                )

            if prediction_rows:
                predictions_schema = T.StructType(
                    [
                        T.StructField("event_timestamp", T.TimestampType(), False),
                        T.StructField("model_name", T.StringType(), False),
                        T.StructField("model_version", T.StringType(), False),
                        T.StructField("dataset_version", T.StringType(), False),
                        T.StructField("payload_json", T.StringType(), False),
                        T.StructField("run_id", T.StringType(), False),
                    ]
                )
                prediction_df = spark.createDataFrame(prediction_rows, schema=predictions_schema)
                if spark.catalog.tableExists(predictions_table):
                    prediction_df.write.mode("append").saveAsTable(predictions_table)
                else:
                    prediction_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(predictions_table)

        del predictions
        del model
        del test_df
        del df
        del artifact_path
        gc.collect()

        logger.info("Avaliacao do modelo concluida com sucesso")

    run_with_observability(
        spark=spark,
        component="evaluate_clientes_model",
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=use_catalog,
        parent_component=parent_component,
        parent_run_id=parent_run_id,
    )
