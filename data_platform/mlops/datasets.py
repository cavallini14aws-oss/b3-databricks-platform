from data_platform.core.context import get_context


def get_training_dataset_table(
    project: str = "clientes",
    use_catalog: bool = False,
    version: str = "v2",
) -> str:
    ctx = get_context(project=project, use_catalog=use_catalog)

    if version == "v1":
        table_name = "tb_clientes_training_dataset"
    elif version == "v2":
        table_name = "tb_clientes_training_dataset_v2"
    else:
        raise ValueError(f"Versão de dataset não suportada: {version}")

    return ctx.naming.qualified_table(ctx.naming.schema_feature, table_name)


def get_scoring_dataset_table(
    project: str = "clientes",
    use_catalog: bool = False,
    version: str = "v2",
) -> str:
    ctx = get_context(project=project, use_catalog=use_catalog)

    if version == "v2":
        table_name = "tb_clientes_scoring_dataset_v2"
    else:
        raise ValueError(f"Versão de scoring dataset não suportada: {version}")

    return ctx.naming.qualified_table(ctx.naming.schema_feature, table_name)


def get_postprod_labels_table(
    project: str = "clientes",
    use_catalog: bool = False,
) -> str:
    ctx = get_context(project=project, use_catalog=use_catalog)
    return ctx.naming.qualified_table(ctx.naming.schema_mlops, "tb_clientes_postprod_labels")
