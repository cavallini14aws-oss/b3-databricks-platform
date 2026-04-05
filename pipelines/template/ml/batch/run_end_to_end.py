from pipelines.template.ml.batch.prepare_dataset import run_prepare_dataset
from pipelines.template.ml.batch.train_model import run_train_model
from pipelines.template.ml.batch.evaluate_model import run_evaluate_model


def run_template_ml_batch_end_to_end(
    spark,
    project: str = "template",
    use_catalog: bool = False,
):
    run_prepare_dataset(spark=spark, project=project, use_catalog=use_catalog)
    run_train_model(spark=spark, project=project, use_catalog=use_catalog)
    run_evaluate_model(
        spark=spark,
        model_version=None,
        project=project,
        use_catalog=use_catalog,
    )
