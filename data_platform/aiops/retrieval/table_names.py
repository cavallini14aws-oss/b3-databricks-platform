def resolve_silver_schema(ctx) -> str:
    naming = getattr(ctx, "naming", None)
    if naming is None:
        return "silver"

    if hasattr(naming, "silver_schema"):
        return naming.silver_schema

    if hasattr(naming, "silver"):
        return naming.silver

    return "silver"


def ai_table(project: str, suffix: str) -> str:
    return f"tb_{project}_ai_{suffix}"


def ai_table_fqn(ctx, project: str, suffix: str) -> str:
    schema = resolve_silver_schema(ctx)
    return f"{schema}.{ai_table(project, suffix)}"
