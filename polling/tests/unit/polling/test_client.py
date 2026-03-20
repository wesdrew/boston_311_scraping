import pytest
from polling.client import ThreeOneOneClient, ThreeOneOneClientConfigException


@pytest.mark.parametrize("base_url", [None, ""])
def test_client_raises_on_invalid_base_url(base_url):
    with pytest.raises(ThreeOneOneClientConfigException):
        ThreeOneOneClient(base_url=base_url)
