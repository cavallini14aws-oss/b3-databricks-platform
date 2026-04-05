from data_platform.core.context import get_context


def run_evaluate_model(
    spark,
    model_version: str | None = None,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    print(
        f"[template/ml/batch] evaluate_model env={ctx.env} "
        f"project={ctx.project} model_version={model_version}"
    )
