from pathlib import Path

from datafetch.tools.http.core import SimpleHttpFetch
from datafetch.utils import FetchWithTemporaryExtensionMixin


def test_simplehttp(tmp_path):
    fetcher = SimpleHttpFetch(base_url="http://www.google.com")
    assert isinstance(fetcher, SimpleHttpFetch)
    assert isinstance(fetcher, FetchWithTemporaryExtensionMixin)

    # Fetch without any suffix
    r = fetcher.fetch(destination_dir=str(tmp_path))
    assert isinstance(r, Path)
    assert r.is_file()

    # Fetch with suffix
    r = fetcher.fetch(url_suffix="?q=plop",
                      destination_dir=str(tmp_path), destination_filename="plop.txt")
    assert r.is_file()
    assert r.name == "plop.txt"
