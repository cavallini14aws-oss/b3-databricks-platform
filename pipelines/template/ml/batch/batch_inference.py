from pyspark.ml import PipelineModel

from data_platform.core.context import get_context
from data_platform.mlops.registry import (
    get_latest_valid_model_version,
    get_model_artifact_path,
)


def run_batch_inference(
    spark,
    input_table: str,
    output_table: str,
    model_name: str = "template_model",
    model_version: str | None = None,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)

    resolved_model_version = model_version
    if resolved_model_version is None:
        resolved_model_version = get_latest_valid_model_version(
            spark=spark,
            model_name=model_name,
            project=project,
            use_catalog=use_catalog,
        )

    artifact_path = get_model_artifact_path(
        spark=spark,
        model_name=model_name,
        model_version=resolved_model_version,
        project=project,
        use_catalog=use_catalog,
    )

    print(f"[template/ml/batch] batch_inference env={ctx.env} project={ctx.project}")
    print(f"[template/ml/batch] model_name={model_name}")
    print(f"[template/ml/batch] model_version={resolved_model_version}")
    print(f"[template/ml/batch] input_table={input_table}")
    print(f"[template/ml/batch] output_table={output_table}")
    print(f"[template/ml/batch] artifact_path={artifact_path}")

    df = spark.table(input_table)
    model = PipelineModel.load(artifact_path)
    predictions = model.transform(df)

    predictions.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

    del predictions
    del model
    del df
