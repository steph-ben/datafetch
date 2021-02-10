from pathlib import Path
from datetime import datetime, timedelta

from datafetch.weather.noaa.nwp import NoaaGfsS3

yesterday = datetime.today() - timedelta(days=1)
date_day = yesterday.strftime("%Y%m%d")


def test_filter():
    s3api = NoaaGfsS3()
    r = s3api.filter(
        Prefix=s3api.get_daterun_prefix(date_day, "00")
    ).limit(count=1)
    assert isinstance(list(r), list)


def test_download(tmp_path):
    s3api = NoaaGfsS3()
    r = s3api.fetch(
        object_key=s3api.get_timestep_key(date_day, "00", "003"),
        destination_dir=str(tmp_path))
    assert isinstance(r, Path)


def test_check_availability():
    s3api = NoaaGfsS3()
    r = s3api.check_timestep_availability(
        date_day=date_day,
        run=0,
        timestep="00"
    )
    assert isinstance(r, dict)


def test_download_with_db(tmp_path):
    s3api = NoaaGfsS3(use_download_db=True)
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
