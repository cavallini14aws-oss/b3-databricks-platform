from dataclasses import dataclass, field


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    data_type: str
    comment: str


@dataclass(frozen=True)
class TableSpec:
    catalog_name: str | None
    schema_name: str
    table_name: str
    table_description: str
    columns: list[ColumnSpec]
    owner: str | None = None
    table_properties: dict[str, str] = field(default_factory=dict)
    table_tags: dict[str, str] = field(default_factory=dict)
    column_tags: dict[str, dict[str, str]] = field(default_factory=dict)
    using_format: str = "delta"
    create_catalog_if_not_exists: bool = False
    create_schema_if_not_exists: bool = True
