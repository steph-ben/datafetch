from pathlib import Path

from datafetch.protocol.http.core import SimpleHttpFetch
from datafetch.core import FetchWithTemporaryExtensionMixin, DownloadedFileRecorderMixin


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
    fetcher = SimpleHttpFetch(
        base_url="http://www.google.com",
        use_download_db=True,
        db_dir=str(tmp_path)
    )
    assert isinstance(fetcher, (SimpleHttpFetch, DownloadedFileRecorderMixin))

    # Fetch a file and ensure db creation
    r = fetcher.fetch(destination_dir=str(tmp_path))
    assert fetcher.db_path.is_file()
    assert isinstance(r, Path)
    assert r.is_file()
    assert fetcher.temporary_extension not in r.suffix

    # Delete the file, it should not be there after fetching
    # since the db entry exists
    r.unlink()
    r = fetcher.fetch(destination_dir=str(tmp_path))
    assert isinstance(r, Path)
    assert not r.exists()
