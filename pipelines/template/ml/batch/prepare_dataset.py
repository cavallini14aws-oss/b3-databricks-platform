from data_platform.core.context import get_context


def run_prepare_dataset(
    spark,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    print(f"[template/ml/batch] prepare_dataset env={ctx.env} project={ctx.project}")
