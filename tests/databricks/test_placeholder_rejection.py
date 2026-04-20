from pathlib import Path

def test_check_runtime_env_rejects_placeholders():
    text = Path("bin/check-runtime-env").read_text(encoding="utf-8")
    assert "reject_placeholder" in text
    assert "HOST_REAL_" in text
    assert "CLIENT_ID_REAL_" in text
    assert "CLIENT_SECRET_REAL_" in text
    assert "TO_BE_DEFINED" in text
