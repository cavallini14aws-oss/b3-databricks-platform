from dataclasses import dataclass, field
from typing import Any


FIXED_DELTA_PROPERTIES = {
    "delta.enableDeletionVectors": "true",
    "delta.feature.appendOnly": "supported",
    "delta.feature.deletionVectors": "supported",
    "delta.feature.invariants": "supported",
    "delta.minReaderVersion": "3",
    "delta.minWriterVersion": "7",
}


@dataclass
class BaseColumnSpec:
    name: str
    data_type: str
    nullable: bool = True
    comment: str = ""
    pii: bool = False
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class BaseTableSpec:
    catalog: str
    schema: str
    table_name: str
    table_description: str
    columns: list[BaseColumnSpec]
    table_tags: dict[str, str] = field(default_factory=dict)
    table_properties: dict[str, str] = field(default_factory=dict)
    using_format: str = "delta"

    def effective_table_properties(self) -> dict[str, str]:
        merged = dict(FIXED_DELTA_PROPERTIES)
        merged.update(self.table_properties)
        return merged
