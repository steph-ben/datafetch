from pathlib import Path

from datafetch.core import DownloadedFileRecorderMixin
from datafetch.utils.db import DownloadRecord


def test_downdb(tmp_path):
    db = DownloadedFileRecorderMixin(db_dir=str(tmp_path))
    assert isinstance(db, DownloadedFileRecorderMixin)
    with db:
        assert Path(db.db_path).exists()

        record, created = db.db_get_record(key="plop")

        assert isinstance(record, DownloadRecord)
        assert created is True

        record.__data__.update({'status': 'failed', 'error': '404'})
        assert record.status == "failed"
        assert record.error == "404"
        assert record.save() == 1

        record, created = db.db_get_record(key="plop")
        assert isinstance(record, DownloadRecord)
        assert created is False
        assert record.status == "failed"
        assert record.error == "404"


def test_downdb_record(tmp_path: Path):
    fp = tmp_path / "file.download"
    fp.write_text("some content")

    record = DownloadRecord(key="plop")
    assert isinstance(record, DownloadRecord)
    assert record.need_download() is True

    record.set_failed()
    assert record.need_download() is True

    record.set_start()
    assert record.need_download() is False
    assert record.date_start

    record.set_downloaded(fp)
    assert record.need_download() is False
    assert record.filepath == str(fp.absolute())
    assert record.size == fp.stat().st_size
    assert record.date_stop
