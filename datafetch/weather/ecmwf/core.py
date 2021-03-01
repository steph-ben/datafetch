import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union, Tuple, List

import pydantic

from datafetch.protocol import S3ApiBucket
from datafetch.protocol.cds import ClimateDataStoreApi
from datafetch.utils.db import DownloadRecord

logger = logging.getLogger(__name__)


class EcmwfEra5S3(S3ApiBucket, pydantic.BaseModel):
    """
    Download ERA5 reanalysis computed by ECMWF from AWS
    See reference links and documentation on :
        - https://registry.opendata.aws/ecmwf-era5/
        - https://github.com/planet-os/notebooks/blob/master/aws/era5-pds.md

    Example of usage:

        >>> s3 = EcmwfEra5S3()
        >>> s3.fetch_param(
                parameter_filename="precipitation_amount_1hour_Accumulation.nc",
                year="2020", month="12",
                destination_dir="/tmp"
            )
        PosixPath('/tmp/2020/12/data/precipitation_amount_1hour_Accumulation.nc')

    """
    bucket_name = "era5-pds"

    def check_param_availability(self, parameter_filename: str, year: str = None, month: str = None) -> bool:
        """
        Check if a particular ERA5 parameter is available for a given month

        :param parameter_filename:
        :param year:
        :param month:
        :return:
        """
        object_key = self.get_object_key(parameter_filename, year, month)
        r = self.filter(Prefix=object_key).limit(count=1)
        if len(list(r)) > 0:
            logger.info(f"{object_key} is available")
            return True
        else:
            logger.warning(f"{object_key} not available")
            return False

    def fetch_param(self,  parameter_filename: str, year: str = None, month: str = None, **kwargs) -> Union[Path, None]:
        """
        Fetch a single param

        :param parameter_filename:
        :param year:
        :param month:
        :return:
        """
        object_key = self.get_object_key(parameter_filename, year, month)
        return self.fetch(object_key=object_key, **kwargs)

    def get_object_key(self, parameter_filename: str, year: str = None, month: str = None):
        """
        Compute an object key from args

        :param parameter_filename:
        :param year:
        :param month:
        :return:
        """
        now = datetime.utcnow()
        if year is None:
            year = now.year
        if month is None:
            month = now.month

        month = str(month).zfill(2)

        return f"{year}/{month}/data/{parameter_filename}"


class EcmwfEra5CDS(ClimateDataStoreApi, pydantic.BaseModel):
    """
    Download ERA5 reanalysis computed by ECMWF from Climate Data Store (CDS)

    See reference links and documentation on :
        - https://confluence.ecmwf.int/display/CKB/How+to+download+ERA5
        - https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels

    """
    destination_dir: str
    cds_resources_list = [
        (
            # Pressure levels
            'reanalysis-era5-pressure-levels',
            {
                'product_type': 'reanalysis',
                'format': 'grib',
                'variable': 'temperature',
                'pressure_level': '850',
                'time': [
                    '00:00', '01:00', '02:00',
                    '03:00', '04:00', '05:00',
                    '06:00', '07:00', '08:00',
                    '09:00', '10:00', '11:00',
                    '12:00', '13:00', '14:00',
                    '15:00', '16:00', '17:00',
                    '18:00', '19:00', '20:00',
                    '21:00', '22:00', '23:00',
                ],
            },
        ),
        (
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'format': 'grib',
                'variable': 'total_precipitation',
                'time': [
                    '00:00', '01:00', '02:00',
                    '03:00', '04:00', '05:00',
                    '06:00', '07:00', '08:00',
                    '09:00', '10:00', '11:00',
                    '12:00', '13:00', '14:00',
                    '15:00', '16:00', '17:00',
                    '18:00', '19:00', '20:00',
                    '21:00', '22:00', '23:00',
                ],
            },
        )
    ]
    # Sync mode while fetching from CDS
    wait_until_complete: bool = True
    # Use sqlite db for keeping track of downloads
    use_download_db = True

    def make_request_queue_for_latest(self, date_info: dict = None) -> List[Tuple[DownloadRecord, bool]]:
        """
        :return:
        """
        db_list = []
        for resource in self.cds_resources_list:
            name, param = resource
            param = self.update_param_with_date(param, date_info)
            db_record, created = self.submit_to_queue(name, param)
            db_list.append((db_record, created))
        return db_list

    def check_queue_and_download(self, date_info: dict = None) -> List[Path]:
        """
        Check status of all requests

        :return:
        """
        fp_list = []
        for resource in self.cds_resources_list:
            name, param = resource
            param = self.update_param_with_date(param, date_info)
            db_record = self.check_queue(name, param, wait_until_complete=self.wait_until_complete)
            fp = self.download_result(name, param, destination_dir=self.destination_dir)
            fp_list.append((fp, param))
        return fp_list

    @staticmethod
    def update_param_with_date(param: dict, date_info: dict = None) -> dict:
        """
        Update param with date information

        :param param:
        :param date_info:
        :return:
        """
        if date_info is None:
            # By default, update with yesterday's date
            date_day = datetime.utcnow() - timedelta(days=10)
            date_info = {
                'year': date_day.year,
                'month': str(date_day.month).zfill(2),
                'day': str(date_day.day).zfill(2)
            }
        param['year'] = date_info['year']
        param['month'] = date_info['month']
        param['day'] = date_info['day']

        return param


def cli_era5():
    cds = EcmwfEra5CDS(
        destination_dir="/tmp/"
    )
    cds.make_request_queue_for_latest()
    cds.check_queue_and_download()


if __name__ == "__main__":
    logging.basicConfig()
    logger = logging.getLogger("datafetch")
    logger.setLevel(logging.INFO)

    cli_era5()
