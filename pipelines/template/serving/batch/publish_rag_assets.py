from data_platform.core.context import get_context


def run_publish_rag_assets(
    spark,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    print(f"[template/serving/batch] publish_rag_assets env={ctx.env} project={ctx.project}")
