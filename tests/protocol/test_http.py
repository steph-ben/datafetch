from pathlib import Path

from datafetch.protocol.http.core import SimpleHttpFetch
from datafetch.core import FetchWithTemporaryExtensionMixin


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


def test_simplehttp_raw(tmp_path):
    for use_raw in True, False:
        fetcher = SimpleHttpFetch(base_url="http://www.google.com", use_requests_raw=use_raw)
        assert isinstance(fetcher, SimpleHttpFetch)
        r = fetcher.fetch(tmp_path)
        assert r.is_file()


def test_download_with_db(tmp_path):
    s3api = SimpleHttpFetch(use_download_db=True)
    with s3api:
        r = s3api.download_timestep(
            date_day=date_day,
            run=0,
            timestep="00",
            download_dir=str(tmp_path)
        )
        assert isinstance(r, dict)
        r = s3api.download_timestep(
            date_day=date_day,
            run=0,
            timestep="00",
            download_dir=str(tmp_path)
        )
        assert isinstance(r, dict)
