from dataclasses import dataclass, field


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    data_type: str
    comment: str


@dataclass(frozen=True)
class TableSpec:
    catalog_template: str
    schema_name: str
    table_name: str
    columns: list[ColumnSpec]
    table_properties: dict[str, str] = field(default_factory=dict)
    table_tags: dict[str, str] = field(default_factory=dict)
    column_tags: dict[str, dict[str, str]] = field(default_factory=dict)
    using_format: str = "delta"
