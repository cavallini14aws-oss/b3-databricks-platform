from b3_platform.core.context import get_context


def get_training_dataset_table(
    project: str = "clientes",
    use_catalog: bool = False,
) -> str:
    ctx = get_context(project=project, use_catalog=use_catalog)
    return ctx.naming.qualified_table(ctx.naming.schema_feature, "tb_clientes_training_dataset")
