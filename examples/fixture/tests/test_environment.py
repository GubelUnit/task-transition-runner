import sys


def test_baseline_environment() -> None:
    assert sys.version_info >= (3, 12)
