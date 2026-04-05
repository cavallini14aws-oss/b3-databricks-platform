from data_platform.core.context import get_context


def run_stream_scoring(
    spark,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    print(f"[template/ml/streaming] stream_scoring env={ctx.env} project={ctx.project}")
