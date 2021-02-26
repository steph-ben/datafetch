"""
Core functionalities of downloading weather data from public AWS dataset, in particular NWP data
"""
import logging
from datetime import datetime
from typing import Union

import pydantic

from datafetch.protocol import S3ApiBucket


logger = logging.getLogger(__name__)


class S3Nwp(S3ApiBucket, pydantic.BaseModel):
    """
    Abstract class for fetch Numerical Weather Predication data from S3
    """
    def get_daterun_prefix(self, date_day: str, run: str) -> str:
        """
        Key prefix for a specific date_day / run
        eg. gfs.{date_day}/{run}/gfs.t{run}z.pgrb2.0p25"

        :param date_day:
        :param run:
        :return:
        """
        raise NotImplementedError

    def get_timestep_key(self, date_day: str, run: str, timestep: str) -> str:
        """
        Key for a specific timestep
        eg. eg. gfs.{date_day}/{run}/gfs.t{run}z.pgrb2.0p25.f003"

        :param date_day:
        :param run:
        :param timestep:
        :return:
        """
        raise NotImplementedError

    def check_run_availability(self, date_day: str = None, run: str = 0) -> Union[dict, None]:
        """
        Check if a particular run is available

        :param date_day:
        :param run:
        :return:
        """
        if date_day is None:
            date_day = datetime.today().strftime("%Y%m%d")

        daterun_prefix = self.get_daterun_prefix(date_day, run)
        logger.info(f"{date_day} / {run} : Checking run availability, prefix {daterun_prefix} ...")

        r = self.filter(Prefix=daterun_prefix).limit(count=1)
        if len(list(r)) > 0:
            logger.info(f"{date_day} / {run} : Run is available !")
            return {'date_day': date_day, 'run': run}
        else:
            logger.warning(f"Run {date_day} / {run} is not yet available")
            return None

    def check_timestep_availability(self, date_day: str, run: str, timestep: str) -> Union[dict, None]:
        """
        Check if a particular timestep is available

        :param date_day:
        :param run:
        :param timestep:
        :return:
        """
        timestep_key = self.get_timestep_key(date_day=date_day, run=run, timestep=timestep)
        logger.info(f"{date_day} / {run} / {timestep} : Checking timestep availability, key {timestep_key} ...")

        r = self.filter(Prefix=timestep_key)
        if len(list(r)) > 0:
            logger.info(f"{date_day} / {run} / {timestep} : Timestep available !")
            return {'date_day': date_day, 'run': run, 'timestep': timestep}
        else:
            logger.warning(f"{date_day} / {run} / {timestep} not yet available")
            return None

    def download_timestep(self, date_day: str, run: str, timestep: str, download_dir: str) -> dict:
        """
        Download a particular timestep

        :param date_day:
        :param run:
        :param timestep:
        :param download_dir:
        :return:
        """
        logger.info(f"{date_day} / {run} / {timestep} : Downloading to {download_dir} ...")
        fp = self.fetch(
            object_key=self.get_timestep_key(date_day=date_day, run=run, timestep=timestep),
            destination_dir=download_dir
        )
        if fp:
            return {'fp': str(fp.absolute())}
        else:
            return None


class NoaaGfsS3(S3Nwp, pydantic.BaseModel):
    """
    Helper for downloading weather GFS from NOAA

    Reference documentation:
        - Homepage : https://registry.opendata.aws/noaa-gfs-bdp-pds/
        - Browse bucket: https://noaa-gfs-bdp-pds.s3.amazonaws.com/index.html
    """
    bucket_name: str = "noaa-gfs-bdp-pds"

    def get_daterun_prefix(self, date_day: str, run: str):
        run = str(run).zfill(2)
        return f"gfs.{date_day}/{run}/gfs.t{run}z.pgrb2.0p25"

    def get_timestep_key(self, date_day: str, run: str, timestep: str):
        run = str(run).zfill(2)
        timestep = str(timestep).zfill(3)
        return f"gfs.{date_day}/{run}/gfs.t{run}z.pgrb2.0p25.f{timestep}"
