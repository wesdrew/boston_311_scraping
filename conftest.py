import pytest


def pytest_collection_modifyitems(items):
    for item in items:
        *_, parent_directory, directory, _ = item.fspath.parts
        if directory == "integration" and parent_directory == "tests":
            item.add_marker(pytest.mark.integration)
