from data_platform.core.context import get_context


def run_ingest_knowledge(
    spark,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    print(f"[template/ai/batch] ingest_knowledge env={ctx.env} project={ctx.project}")
