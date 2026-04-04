from b3_platform.dataops.table_spec import TableSpec


REQUIRED_TABLE_PROPERTIES = {
    "delta.enableDeletionVectors",
    "delta.minReaderVersion",
    "delta.minWriterVersion",
}

REQUIRED_TABLE_TAGS = {
    "descricao_tabela",
}

REQUIRED_COLUMN_TAGS = {
    "pii",
    "classificacao",
}


def validate_table_spec(table_spec: TableSpec) -> None:
    if table_spec.catalog_name is not None and not table_spec.catalog_name.strip():
        raise ValueError("catalog_name inválido.")

    if not table_spec.schema_name.strip():
        raise ValueError("schema_name é obrigatório.")

    if not table_spec.table_name.strip():
        raise ValueError("table_name é obrigatório.")

    if not table_spec.table_description.strip():
        raise ValueError("table_description é obrigatório.")

    if not table_spec.columns:
        raise ValueError("A tabela deve ter ao menos uma coluna.")

    seen_columns = set()
    for column in table_spec.columns:
        if not column.name.strip():
            raise ValueError("Nome de coluna vazio.")
        if not column.data_type.strip():
            raise ValueError(f"Tipo vazio para coluna {column.name}.")
        if not column.comment.strip():
            raise ValueError(f"Comment obrigatório para coluna {column.name}.")

        normalized = column.name.lower()
        if normalized in seen_columns:
            raise ValueError(f"Coluna duplicada detectada: {column.name}")
        seen_columns.add(normalized)

    missing_properties = REQUIRED_TABLE_PROPERTIES - set(table_spec.table_properties.keys())
    if missing_properties:
        raise ValueError(
            f"table_properties obrigatórias ausentes: {sorted(missing_properties)}"
        )

    if "descricao_tabela" not in table_spec.table_tags:
        raise ValueError("table_tags obrigatória ausente: descricao_tabela")

    if table_spec.table_tags["descricao_tabela"] != table_spec.table_description:
        raise ValueError(
            "table_description deve ser igual ao valor de table_tags['descricao_tabela']"
        )

    for required_tag in REQUIRED_TABLE_TAGS:
        if required_tag not in table_spec.table_tags:
            raise ValueError(f"table_tags obrigatória ausente: {required_tag}")

    column_names = {column.name.lower() for column in table_spec.columns}

    for column_name in column_names:
        if column_name not in {name.lower() for name in table_spec.column_tags.keys()}:
            raise ValueError(
                f"column_tags obrigatória ausente para coluna: {column_name}"
            )

    normalized_column_tags = {
        key.lower(): value for key, value in table_spec.column_tags.items()
    }

    for column_name, tags in normalized_column_tags.items():
        if column_name not in column_names:
            raise ValueError(
                f"column_tags referencia coluna inexistente: {column_name}"
            )

        missing_required_tags = REQUIRED_COLUMN_TAGS - set(tags.keys())
        if missing_required_tags:
            raise ValueError(
                f"column_tags da coluna {column_name} sem tags obrigatórias: "
                f"{sorted(missing_required_tags)}"
            )
