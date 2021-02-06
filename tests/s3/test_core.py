from pathlib import Path
from datetime import datetime, timedelta

from datafetch.s3 import NoaaGfsS3, S3ApiBucket

yesterday = datetime.today() - timedelta(days=1)
date_day = yesterday.strftime("%Y%m%d")


def test_generic():
    s3api = S3ApiBucket(bucket_name="any_bucket")
    r = s3api.filter(Prefix="plop")


def test_filter():
    s3api = NoaaGfsS3()
    r = s3api.filter(
        Prefix=s3api.get_daterun_prefix(date_day, "00")
    ).limit(count=1)
    assert isinstance(list(r), list)


def test_download():
    s3api = NoaaGfsS3()
    r = s3api.download(
        object_key=s3api.get_timestep_key(date_day, "00", "003"),
        destination_dir="/tmp/",
    )
    print(r)
    assert isinstance(r, Path)


def test_check_availability():
    s3api = NoaaGfsS3()
    r = s3api.check_timestep_availability(
        date_day=date_day,
        run=0,
        timestep="00"
    )
    print(r)
    assert isinstance(r, dict)


def test_download_with_db(tmp_path):
    s3api = NoaaGfsS3(use_download_db=True)
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
