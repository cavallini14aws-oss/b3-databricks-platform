def resolve_schema(ctx) -> str:
    naming = getattr(ctx, "naming", None)
    if naming is None:
        return "silver"

    if hasattr(naming, "silver_schema"):
        return naming.silver_schema

    if hasattr(naming, "silver"):
        return naming.silver

    return "silver"
