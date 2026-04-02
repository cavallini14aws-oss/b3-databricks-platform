from datetime import datetime, UTC

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from b3_platform.core.context import get_context
from b3_platform.core.logger import PlatformLogger
from b3_platform.mlops.datasets import get_training_dataset_table
from b3_platform.mlops.evaluation import log_model_metric
from b3_platform.orchestration.pipeline_runner import run_with_observability


PREDICTIONS_SCHEMA = T.StructType(
    [
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("env", T.StringType(), False),
        T.StructField("project", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), False),
        T.StructField("id_cliente", T.LongType(), False),
        T.StructField("segmento", T.StringType(), True),
        T.StructField("source_type", T.StringType(), True),
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
    model_name = "clientes_status_classifier"

    def _run(logger: PlatformLogger):
        dataset_table = get_training_dataset_table(
            project=project,
            use_catalog=use_catalog,
        )

        predictions_table = ctx.naming.qualified_table(
            ctx.naming.schema_mlops,
            "tb_model_predictions",
        )

        mlops_schema = ctx.naming.qualified_schema(ctx.naming.schema_mlops)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {mlops_schema}")

        logger.info(f"dataset_table={dataset_table}")
        logger.info(f"model_version={model_version}")
        logger.info(f"predictions_table={predictions_table}")

        df = spark.table(dataset_table)
        train_df = df.filter(F.col("dataset_split") == "train")
        test_df = df.filter(F.col("dataset_split") == "test")

        logger.info(f"train_count={train_df.count()}")
        logger.info(f"test_count={test_df.count()}")

        segment_indexer = StringIndexer(
            inputCol="segmento",
            outputCol="segmento_idx",
            handleInvalid="keep",
        )

        source_indexer = StringIndexer(
            inputCol="source_type",
            outputCol="source_type_idx",
            handleInvalid="keep",
        )

        encoder = OneHotEncoder(
            inputCols=["segmento_idx", "source_type_idx"],
            outputCols=["segmento_ohe", "source_type_ohe"],
        )

        assembler = VectorAssembler(
            inputCols=["segmento_ohe", "source_type_ohe"],
            outputCol="features",
        )

        classifier = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            probabilityCol="probability",
            rawPredictionCol="rawPrediction",
            maxIter=10,
        )

        pipeline = Pipeline(
            stages=[
                segment_indexer,
                source_indexer,
                encoder,
                assembler,
                classifier,
            ]
        )

        model = pipeline.fit(train_df)
        predictions = model.transform(test_df)

        accuracy = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy",
        ).evaluate(predictions)

        f1_score = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="f1",
        ).evaluate(predictions)

        auc = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        ).evaluate(predictions)

        for metric_name, metric_value in [
            ("accuracy", accuracy),
            ("f1", f1_score),
            ("auc", auc),
        ]:
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

        prediction_rows = [
            Row(
                event_timestamp=datetime.now(UTC).replace(tzinfo=None),
                env=ctx.env,
                project=ctx.project,
                model_name=model_name,
                model_version=model_version,
                id_cliente=int(row["id_cliente"]),
                segmento=row["segmento"],
                source_type=row["source_type"],
                label=float(row["label"]) if row["label"] is not None else None,
                prediction=float(row["prediction"]) if row["prediction"] is not None else None,
                dataset_split=row["dataset_split"],
                run_id=run_id,
            )
            for row in predictions.select(
                "id_cliente",
                "segmento",
                "source_type",
                "label",
                "prediction",
                "dataset_split",
            ).collect()
        ]

        if prediction_rows:
            prediction_df = spark.createDataFrame(prediction_rows, schema=PREDICTIONS_SCHEMA)

            if spark.catalog.tableExists(predictions_table):
                prediction_df.write.mode("append").saveAsTable(predictions_table)
            else:
                prediction_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(predictions_table)

        logger.info("Avaliação do modelo concluída com sucesso")

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
