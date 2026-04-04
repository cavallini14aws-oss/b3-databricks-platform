from dataclasses import dataclass, field
from typing import Callable


@dataclass(frozen=True)
class FlowSpec:
    flow_name: str
    flow_type: str
    project: str
    domain: str
    layer: str | None
    description: str
    entrypoint: str
    callable_name: str
    tags: dict[str, str] = field(default_factory=dict)
    enabled: bool = True


VALID_FLOW_TYPES = {"data", "ml", "llm"}
