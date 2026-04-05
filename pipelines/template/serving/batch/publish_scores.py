from data_platform.core.context import get_context


def run_publish_scores(
    spark,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    print(f"[template/serving/batch] publish_scores env={ctx.env} project={ctx.project}")
