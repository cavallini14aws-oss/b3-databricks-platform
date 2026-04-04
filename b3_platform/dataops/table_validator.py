from b3_platform.dataops.table_spec import TableSpec


REQUIRED_TABLE_PROPERTIES = {
    "delta.enableDeletionVectors",
    "delta.minReaderVersion",
    "delta.minWriterVersion",
}


def validate_table_spec(table_spec: TableSpec) -> None:
    if not table_spec.catalog_template.strip():
        raise ValueError("catalog_template é obrigatório.")

    if not table_spec.schema_name.strip():
        raise ValueError("schema_name é obrigatório.")

    if not table_spec.table_name.strip():
        raise ValueError("table_name é obrigatório.")

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

    for column_name in table_spec.column_tags.keys():
        if column_name.lower() not in seen_columns:
            raise ValueError(
                f"column_tags referencia coluna inexistente: {column_name}"
            )
