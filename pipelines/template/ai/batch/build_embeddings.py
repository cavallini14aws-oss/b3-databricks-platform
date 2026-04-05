from data_platform.core.context import get_context


def run_build_embeddings(
    spark,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    print(f"[template/ai/batch] build_embeddings env={ctx.env} project={ctx.project}")
