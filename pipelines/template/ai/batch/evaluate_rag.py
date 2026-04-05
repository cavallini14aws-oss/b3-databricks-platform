from data_platform.core.context import get_context


def run_evaluate_rag(
    spark,
    project: str = "template",
    use_catalog: bool = False,
):
    ctx = get_context(project=project, use_catalog=use_catalog)
    print(f"[template/ai/batch] evaluate_rag env={ctx.env} project={ctx.project}")
