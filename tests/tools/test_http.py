from pathlib import Path

from datafetch.tools.http.core import SimpleHttpFetch


def test_simplehttp(tmp_path):
    fetch = SimpleHttpFetch(base_url="www.google.com")
    assert isinstance(fetch, SimpleHttpFetch)
    r = fetch.download_url(destination_dir=str(tmp_path))
    assert isinstance(r, Path)