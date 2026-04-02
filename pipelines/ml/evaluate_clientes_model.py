from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

from b3_platform.core.context import get_context
from b3_platform.core.logger import PlatformLogger
from b3_platform.mlops.datasets import get_training_dataset_table
from b3_platform.mlops.evaluation import log_model_metric
from b3_platform.orchestration.pipeline_runner import run_with_observability


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

        logger.info(f"dataset_table={dataset_table}")
        logger.info(f"model_version={model_version}")

        df = spark.table(dataset_table)

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

        model = pipeline.fit(df)
        predictions = model.transform(df)

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
