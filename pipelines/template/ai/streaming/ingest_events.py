from data_platform.core.context import get_context


def run_ingest_events(
    spark,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    print(f"[template/ai/streaming] ingest_events env={ctx.env} project={ctx.project}")
