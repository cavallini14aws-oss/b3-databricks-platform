from data_platform.core.context import get_context


def run_incremental_index_update(
    spark,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    print(f"[template/ai/streaming] incremental_index_update env={ctx.env} project={ctx.project}")
