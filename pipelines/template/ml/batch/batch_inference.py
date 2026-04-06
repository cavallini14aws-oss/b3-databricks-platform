from pyspark.ml import PipelineModel

from data_platform.core.context import get_context
from data_platform.mlops.deployments import get_active_model_for_env
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
    artifact_path: str | None = None,
    target_env: str | None = None,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)

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

    print(f"[template/ml/batch] batch_inference env={ctx.env} project={ctx.project}")
    print(f"[template/ml/batch] model_name={model_name}")
    print(f"[template/ml/batch] model_version={resolved_model_version}")
    print(f"[template/ml/batch] target_env={resolved_target_env}")
    print(f"[template/ml/batch] input_table={input_table}")
    print(f"[template/ml/batch] output_table={output_table}")
    print(f"[template/ml/batch] artifact_path={resolved_artifact_path}")

    df = spark.table(input_table)
    model = PipelineModel.load(resolved_artifact_path)
    predictions = model.transform(df)

    predictions.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

    del predictions
    del model
    del df
