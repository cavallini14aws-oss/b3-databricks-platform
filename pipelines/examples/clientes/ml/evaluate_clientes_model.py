from datetime import datetime, UTC
import gc

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from b3_platform.core.config_loader import load_yaml_config
from b3_platform.core.context import get_context
from b3_platform.core.job_config import load_job_config
from b3_platform.core.logger import PlatformLogger
from b3_platform.governance.promotion import (
    evaluate_ml_promotion,
    log_promotion_decision,
)
from b3_platform.mlops.baseline import compute_majority_baseline_accuracy, log_baseline_metric
from b3_platform.mlops.datasets import get_training_dataset_table
from b3_platform.mlops.registry import get_model_artifact_path
from b3_platform.mlops.evaluation import log_confusion_matrix, log_model_metric
from b3_platform.orchestration.pipeline_runner import run_with_observability


PREDICTIONS_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), False),
        T.StructField("id_cliente", T.LongType(), False),
        T.StructField("segmento_dominante", T.StringType(), True),
        T.StructField("source_type_dominante", T.StringType(), True),
        T.StructField("label", T.DoubleType(), True),
        T.StructField("prediction", T.DoubleType(), True),
        T.StructField("dataset_split", T.StringType(), True),
        T.StructField("run_id", T.StringType(), False),
    ]
)


def run_evaluate_clientes_model(
    spark,
    model_version: str,
    project: str = "clientes",
    use_catalog: bool = False,
    config_path: str = "config/clientes_ml_pipeline.yml",
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> None:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="evaluate_clientes_model",
        env=ctx.env,
        project=ctx.project,
    )

    run_id = forced_run_id or base_logger.run_id
    config = load_yaml_config(config_path)
    job_config = load_job_config(ctx.env)

    model_name = config["model"]["name"]
    feature_columns = config["dataset"]["feature_columns"]
    dataset_version = config["dataset"].get("version", "v2")
    metric_names = config["evaluation"]["metrics"]
    config_generate_confusion_matrix = config["evaluation"]["generate_confusion_matrix"]
    config_generate_baseline = config["evaluation"]["generate_baseline"]

    generate_confusion_matrix = (
        config_generate_confusion_matrix and ctx.flags.enable_confusion_matrix
    )
    generate_baseline = config_generate_baseline and ctx.flags.enable_baseline
    generate_predictions_logging = ctx.flags.enable_predictions_logging

    def _run(logger: PlatformLogger):
        dataset_table = get_training_dataset_table(
            project=project,
            use_catalog=use_catalog,
            version=dataset_version,
        )

        predictions_table = ctx.naming.qualified_table(
            ctx.naming.schema_mlops,
            "tb_model_predictions",
        )

        mlops_schema = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {mlops_schema}")

        logger.info(f"dataset_table={dataset_table}")
        logger.info(f"model_version={model_version}")
        logger.info(f"dataset_version={dataset_version}")
        logger.info(f"predictions_table={predictions_table}")
        logger.info(f"feature_columns={feature_columns}")
        logger.info(f"enable_baseline={generate_baseline}")
        logger.info(f"enable_confusion_matrix={generate_confusion_matrix}")
        logger.info(f"enable_predictions_logging={generate_predictions_logging}")
        logger.info(f"job_environment={job_config.environment}")
        promotion_target_env = job_config.promotion_rules[0].target_env if job_config.promotion_rules else None
        logger.info(f"promotion_target_env={promotion_target_env}")

        artifact_path = get_model_artifact_path(
            spark=spark,
            model_name=model_name,
            model_version=model_version,
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

        if "accuracy" in metric_names:
            metric_values["accuracy"] = MulticlassClassificationEvaluator(
                labelCol="label",
                predictionCol="prediction",
                metricName="accuracy",
            ).evaluate(predictions)

        if "f1" in metric_names:
            metric_values["f1"] = MulticlassClassificationEvaluator(
                labelCol="label",
                predictionCol="prediction",
                metricName="f1",
            ).evaluate(predictions)

        if "auc" in metric_names:
            metric_values["auc"] = BinaryClassificationEvaluator(
                labelCol="label",
                rawPredictionCol="rawPrediction",
                metricName="areaUnderROC",
            ).evaluate(predictions)

        accuracy = metric_values.get("accuracy")
        f1_score = metric_values.get("f1")
        auc = metric_values.get("auc")

        for metric_name, metric_value in metric_values.items():
            log_model_metric(
                spark=spark,
                model_name=model_name,
                model_version=model_version,
                metric_name=metric_name,
                metric_value=metric_value,
                run_id=run_id,
                project=project,
                use_catalog=use_catalog,
            )

        if generate_baseline:
            baseline_accuracy = compute_majority_baseline_accuracy(test_df)

            log_baseline_metric(
                spark=spark,
                model_name=model_name,
                model_version=model_version,
                baseline_name="majority_class",
                metric_name="accuracy",
                metric_value=baseline_accuracy,
                run_id=run_id,
                project=project,
                use_catalog=use_catalog,
            )

        if generate_confusion_matrix:
            confusion_rows = (
                predictions
                .groupBy("label", "prediction")
                .count()
                .withColumnRenamed("count", "record_count")
                .collect()
            )

            log_confusion_matrix(
                spark=spark,
                model_name=model_name,
                model_version=model_version,
                confusion_rows=confusion_rows,
                run_id=run_id,
                project=project,
                use_catalog=use_catalog,
            )

        if generate_predictions_logging:
            prediction_rows = [
                Row(
                    event_timestamp=datetime.now(UTC).replace(tzinfo=None),
                    env=ctx.env,
                    project=ctx.project,
                    model_name=model_name,
                    model_version=model_version,
                    id_cliente=int(row["id_cliente"]),
                    segmento_dominante=row["segmento_dominante"],
                    source_type_dominante=row["source_type_dominante"],
                    label=float(row["label"]) if row["label"] is not None else None,
                    prediction=float(row["prediction"]) if row["prediction"] is not None else None,
                    dataset_split=row["dataset_split"],
                    run_id=run_id,
                )
                for row in predictions.select(
                    "id_cliente",
                    "segmento_dominante",
                    "source_type_dominante",
                    "label",
                    "prediction",
                    "dataset_split",
                ).collect()
            ]

            if prediction_rows:
                prediction_df = spark.createDataFrame(
                    prediction_rows,
                    schema=PREDICTIONS_SCHEMA,
                )

                if spark.catalog.tableExists(predictions_table):
                    prediction_df.write.mode("append").saveAsTable(predictions_table)
                else:
                    prediction_df.write.mode("overwrite").option(
                        "overwriteSchema",
                        "true",
                    ).saveAsTable(predictions_table)

        promotion_target_env = job_config.promotion_rules[0].target_env if job_config.promotion_rules else None
        promotion_decision = evaluate_ml_promotion(
            job_config=job_config,
            source_env=ctx.env,
            target_env=promotion_target_env,
            accuracy=accuracy,
            f1=f1_score,
            auc=auc,
            tests_passed=True,
            manual_approval=False,
        )

        log_promotion_decision(
            spark=spark,
            model_name=model_name,
            model_version=model_version,
            decision=promotion_decision,
            run_id=run_id,
            source_env=ctx.env,
            target_env=promotion_target_env,
            accuracy=accuracy,
            f1=f1_score,
            auc=auc,
            tests_passed=True,
            manual_approval=False,
            project=project,
            use_catalog=use_catalog,
        )

        logger.info(
            f"promotion_decision_approved={promotion_decision.approved}"
        )
        logger.info(
            f"promotion_decision_reason={promotion_decision.reason}"
        )

        if not promotion_decision.approved and ctx.env in {"hml", "prd"}:
            raise RuntimeError(
                f"Quality gate failed for environment {ctx.env}: "
                f"{promotion_decision.reason}"
            )

        logger.info("Avaliação do modelo concluída com sucesso")

        del predictions
        del model
        del test_df
        del df
        del artifact_path
        gc.collect()

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
