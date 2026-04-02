from uuid import uuid4

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import functions as F

from b3_platform.core.config_loader import load_yaml_config
from b3_platform.core.context import get_context
from b3_platform.core.logger import PlatformLogger
from b3_platform.mlops.datasets import get_training_dataset_table
from b3_platform.mlops.registry import register_model
from b3_platform.orchestration.pipeline_runner import run_with_observability


def run_train_clientes_model(
    spark,
    project: str = "clientes",
    use_catalog: bool = False,
    config_path: str = "config/clientes_ml_pipeline.yml",
    parent_component: str | None = None,
    parent_run_id: str | None = None,
    forced_run_id: str | None = None,
) -> str:
    ctx = get_context(project=project, use_catalog=use_catalog)

    base_logger = PlatformLogger(
        component="train_clientes_model",
        env=ctx.env,
        project=ctx.project,
    )

    run_id = forced_run_id or base_logger.run_id
    config = load_yaml_config(config_path)

    model_name = config["model"]["name"]
    algorithm = config["model"]["algorithm"]
    feature_columns = config["dataset"]["feature_columns"]

    model_version = str(uuid4())

    def _run(logger: PlatformLogger):
        dataset_table = get_training_dataset_table(
            project=project,
            use_catalog=use_catalog,
        )

        logger.info(f"dataset_table={dataset_table}")
        logger.info(f"feature_columns={feature_columns}")

        df = spark.table(dataset_table)
        train_df = df.filter(F.col("dataset_split") == "train")

        logger.info(f"train_count={train_df.count()}")

        indexers = []
        encoder_inputs = []
        encoder_outputs = []

        for feature_name in feature_columns:
            idx_col = f"{feature_name}_idx"
            ohe_col = f"{feature_name}_ohe"

            indexers.append(
                StringIndexer(
                    inputCol=feature_name,
                    outputCol=idx_col,
                    handleInvalid="keep",
                )
            )

            encoder_inputs.append(idx_col)
            encoder_outputs.append(ohe_col)

        encoder = OneHotEncoder(
            inputCols=encoder_inputs,
            outputCols=encoder_outputs,
        )

        assembler = VectorAssembler(
            inputCols=encoder_outputs,
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
                *indexers,
                encoder,
                assembler,
                classifier,
            ]
        )

        model = pipeline.fit(train_df)

        register_model(
            spark=spark,
            model_name=model_name,
            model_version=model_version,
            algorithm=algorithm,
            run_id=run_id,
            status="TRAINED",
            artifact_path=None,
            project=project,
            use_catalog=use_catalog,
        )

        logger.info(f"Modelo treinado com sucesso: version={model_version}")

        del model
        del train_df
        del df

    run_with_observability(
        spark=spark,
        component="train_clientes_model",
        env=ctx.env,
        project=ctx.project,
        run_id=run_id,
        fn=_run,
        use_catalog=use_catalog,
        parent_component=parent_component,
        parent_run_id=parent_run_id,
    )

    return model_version
