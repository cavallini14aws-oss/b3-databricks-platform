from types import SimpleNamespace

import pytest

from data_platform.core.secrets import (
    build_databricks_secrets_resolver,
    get_secret_scope,
    resolve_secret,
)


def test_get_secret_scope_returns_expected_value():
    context = SimpleNamespace(secret_scope="keyvault-dev-datahub")
    assert get_secret_scope(context) == "keyvault-dev-datahub"


def test_resolve_secret_uses_dbutils_successfully():
    context = SimpleNamespace(secret_scope="keyvault-dev-datahub")

    class FakeSecrets:
        @staticmethod
        def get(scope, key):
            assert scope == "keyvault-dev-datahub"
            assert key == "smtp-password"
            return "secret-value"

    class FakeDbutils:
        secrets = FakeSecrets()

    value = resolve_secret(
        context=context,
        key="smtp-password",
        dbutils=FakeDbutils(),
    )

    assert value == "secret-value"


def test_resolve_secret_blocks_missing_dbutils():
    context = SimpleNamespace(secret_scope="keyvault-dev-datahub")

    with pytest.raises(RuntimeError, match="dbutils não informado"):
        resolve_secret(
            context=context,
            key="smtp-password",
            dbutils=None,
        )


def test_build_databricks_secrets_resolver_returns_callable():
    class FakeSecrets:
        @staticmethod
        def get(scope, key):
            return f"{scope}:{key}"

    class FakeDbutils:
        secrets = FakeSecrets()

    resolver = build_databricks_secrets_resolver(
        context=SimpleNamespace(secret_scope="keyvault-dev-datahub"),
        dbutils=FakeDbutils(),
    )

    assert resolver("scope-a", "key-a") == "scope-a:key-a"
