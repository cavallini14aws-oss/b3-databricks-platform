from data_platform.core.context import get_context
from data_platform.mlops.registry import update_model_status
from data_platform.mlops.model_states import EVALUATED


def run_evaluate_model(
    spark,
    model_name: str = "template_model",
    model_version: str | None = None,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    print(
        f"[template/ml/batch] evaluate_model env={ctx.env} "
        f"project={ctx.project} model_version={model_version}"
    )

    if model_version is not None:
        update_model_status(
            spark=spark,
            model_name=model_name,
            model_version=model_version,
            status=EVALUATED,
            project=project,
            use_catalog=use_catalog,
        )
