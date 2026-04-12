from types import SimpleNamespace

from data_platform.aiops.evaluations.ai_smoke_utils import resolve_schema


def test_resolve_schema_with_silver_schema():
    ctx = SimpleNamespace(naming=SimpleNamespace(silver_schema="clientes_silver"))
    assert resolve_schema(ctx) == "clientes_silver"


def test_resolve_schema_with_silver_attr():
    ctx = SimpleNamespace(naming=SimpleNamespace(silver="silver"))
    assert resolve_schema(ctx) == "silver"


def test_resolve_schema_fallback():
    ctx = SimpleNamespace(naming=SimpleNamespace())
    assert resolve_schema(ctx) == "silver"
