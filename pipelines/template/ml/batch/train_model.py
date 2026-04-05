from data_platform.core.context import get_context


def run_train_model(
    spark,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    print(f"[template/ml/batch] train_model env={ctx.env} project={ctx.project}")
