from b3_platform.core.context import get_context
from b3_platform.dataops.table_spec import TableSpec


def render_catalog_name(catalog_template: str, env: str) -> str:
    return catalog_template.replace("{env}", env)


def build_catalog_name(
    table_spec: TableSpec,
    project: str = "clientes",
    use_catalog: bool = False,
) -> str | None:
    if not use_catalog:
        return None

    ctx = get_context(project=project, use_catalog=use_catalog)
    return render_catalog_name(table_spec.catalog_template, ctx.env)


def build_fully_qualified_schema_name(
    table_spec: TableSpec,
    project: str = "clientes",
    use_catalog: bool = False,
) -> str:
    catalog_name = build_catalog_name(
        table_spec=table_spec,
        project=project,
        use_catalog=use_catalog,
    )

    if use_catalog and catalog_name:
        return f"{catalog_name}.{table_spec.schema_name}"
    return table_spec.schema_name


def build_fully_qualified_table_name(
    table_spec: TableSpec,
    project: str = "clientes",
    use_catalog: bool = False,
) -> str:
    schema_name = build_fully_qualified_schema_name(
        table_spec=table_spec,
        project=project,
        use_catalog=use_catalog,
    )
    return f"{schema_name}.{table_spec.table_name}"


def build_create_catalog_sql(
    table_spec: TableSpec,
    project: str = "clientes",
    use_catalog: bool = False,
) -> str | None:
    catalog_name = build_catalog_name(
        table_spec=table_spec,
        project=project,
        use_catalog=use_catalog,
    )

    if not use_catalog or not catalog_name:
        return None

    return f"CREATE CATALOG IF NOT EXISTS {catalog_name}"


def build_create_schema_sql(
    table_spec: TableSpec,
    project: str = "clientes",
    use_catalog: bool = False,
) -> str:
    qualified_schema = build_fully_qualified_schema_name(
        table_spec=table_spec,
        project=project,
        use_catalog=use_catalog,
    )
    return f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}"


def build_create_table_sql(
    table_spec: TableSpec,
    project: str = "clientes",
    use_catalog: bool = False,
) -> str:
    qualified_name = build_fully_qualified_table_name(
        table_spec=table_spec,
        project=project,
        use_catalog=use_catalog,
    )

    columns_sql = ",\n      ".join(
        f"{col.name} {col.data_type} COMMENT '{col.comment}'"
        for col in table_spec.columns
    )

    properties_sql = ",\n      ".join(
        f"'{key}' = '{value}'"
        for key, value in table_spec.table_properties.items()
    )

    return f"""
    CREATE TABLE IF NOT EXISTS {qualified_name} (
      {columns_sql}
    )
    USING {table_spec.using_format}
    TBLPROPERTIES (
      {properties_sql}
    )
    """.strip()


def build_set_table_tags_sql(
    table_spec: TableSpec,
    project: str = "clientes",
    use_catalog: bool = False,
) -> str | None:
    if not table_spec.table_tags:
        return None

    qualified_name = build_fully_qualified_table_name(
        table_spec=table_spec,
        project=project,
        use_catalog=use_catalog,
    )

    tags_sql = ",\n      ".join(
        f"'{key}' = '{value}'"
        for key, value in table_spec.table_tags.items()
    )

    return f"""
    ALTER TABLE {qualified_name}
    SET TAGS (
      {tags_sql}
    )
    """.strip()


def build_set_column_tags_sql_list(
    table_spec: TableSpec,
    project: str = "clientes",
    use_catalog: bool = False,
) -> list[tuple[str, str]]:
    qualified_name = build_fully_qualified_table_name(
        table_spec=table_spec,
        project=project,
        use_catalog=use_catalog,
    )

    statements = []
    for column_name, tags in table_spec.column_tags.items():
        tags_sql = ", ".join(
            f"'{key}' = '{value}'"
            for key, value in tags.items()
        )
        sql = f"""
        ALTER TABLE {qualified_name}
        ALTER COLUMN {column_name}
        SET TAGS ({tags_sql})
        """.strip()
        statements.append((column_name, sql))

    return statements
