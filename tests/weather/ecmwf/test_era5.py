from datetime import timedelta, datetime
from pathlib import Path

from datafetch.weather.ecmwf.core import EcmwfEra5S3, EcmwfEra5CDS


def test_era5_aws_filter(tmp_path):
    s3 = EcmwfEra5S3()

    yearmonth = datetime.utcnow() - timedelta(days=31)
    r = s3.check_param_availability(
        parameter_filename="precipitation_amount_1hour_Accumulation.nc",
        year=yearmonth.year,
        month=yearmonth.month
    )
    assert isinstance(r, bool)
    assert r is True


def test_era5_aws_fetch(tmp_path):
    s3 = EcmwfEra5S3()

    yearmonth = datetime.utcnow() - timedelta(days=31)
    r = s3.fetch_param(
        parameter_filename="precipitation_amount_1hour_Accumulation.nc",
        year=yearmonth.year,
        month=yearmonth.month,
        destination_dir=str(tmp_path)
    )
    assert isinstance(r, Path)


def test_era5_cds(tmp_path):
    cds = EcmwfEra5CDS(db_dir=str(tmp_path))
    assert isinstance(cds, EcmwfEra5CDS)

    cds.make_request_queue_for_latest(
        date_day=datetime.utcnow() - timedelta(days=1),
        destination_dir=str(tmp_path)
    )
    raise Exception