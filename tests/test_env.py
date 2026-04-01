import os

from b3_platform.env import get_env


def test_get_env_dev():
    os.environ["LOCAL_ENV"] = "dev"
    assert get_env() == "dev"
