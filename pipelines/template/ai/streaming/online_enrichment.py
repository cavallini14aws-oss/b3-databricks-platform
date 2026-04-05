from data_platform.core.context import get_context


def run_online_enrichment(
    spark,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    print(f"[template/ai/streaming] online_enrichment env={ctx.env} project={ctx.project}")
