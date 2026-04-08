from data_platform.core.hml_rehearsal_status import (
    get_hml_rehearsal_blockers,
    is_hml_rehearsal_ready,
)


def test_get_hml_rehearsal_blockers(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.hml_rehearsal_status.get_activation_environment_config",
        lambda env, config_path: {
            "go_live": {
                "blockers": ["rehearsal_hml_pendente"],
            }
        },
    )

    blockers = get_hml_rehearsal_blockers()
    assert blockers == ["rehearsal_hml_pendente"]


def test_is_hml_rehearsal_ready(monkeypatch):
    monkeypatch.setattr(
        "data_platform.core.hml_rehearsal_status.get_activation_environment_config",
        lambda env, config_path: {
            "go_live": {
                "blockers": [],
            }
        },
    )

    assert is_hml_rehearsal_ready() is True
