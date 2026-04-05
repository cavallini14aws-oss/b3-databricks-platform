from data_platform.core.context import get_context


def run_online_features(
    spark,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    print(f"[template/ml/streaming] online_features env={ctx.env} project={ctx.project}")
