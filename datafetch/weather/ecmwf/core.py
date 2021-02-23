import logging
from datetime import datetime
from pathlib import Path
from typing import Union

import pydantic

from datafetch.protocol import S3ApiBucket

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
