"""
Core functionalities of downloading weather data from public AWS dataset, in particular NWP data
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import Union

import pydantic
import boto3
import boto3.resources
import botocore
import botocore.client

logger = logging.getLogger(__name__)


class S3ApiBucket(pydantic.BaseModel):
    """
    An helper task for accessing Amazon WebService Storage buckets:
    - filtering objects
    - download objects
    """
    bucket_name: str = None
    _s3: object = None

    class Config:
        underscore_attrs_are_private = True

    @property
    def s3(self) -> boto3.resources.base.ServiceResource:
        """
        boto3 resource object

        :return:
        """
        if self._s3 is None:
            session = boto3.session.Session()
            self._s3 = session.resource('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))
        return self._s3

    @property
    def bucket(self) -> object:
        """
        Bucket object

        :return:
        """
        return self.s3.Bucket(name=self.bucket_name)

    def filter(self, **kwargs: dict):
        """
        Helper for filtering objects in current bucket

        Usage:
            r = s3api.filter(
                    Prefix="plop/plip"
            ).limit(count=1)

        :param kwargs:
        :return:
        """
        logger.debug(f"{self.bucket_name} : filtering {kwargs} ...")
        return self.bucket.objects.filter(**kwargs)

    def download(self, object_key: str,
                 destination_dir: str, destination_filename: str = None,
                 tmp_extension: str = ".tmp") -> Union[Path, None]:
        """
        Helper for download object from bucket

        :param object_key:
        :param destination_dir:
        :param destination_filename:
        :param tmp_extension:
        :return:
        """
        if destination_filename is None:
            destination_filename = object_key
        fp = Path(destination_dir, destination_filename)
        if not fp.parent.exists():
            fp.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"{self.bucket_name} : downloading {object_key} to {fp} ...")
        fp_tmp = fp
        if tmp_extension:
            fp_tmp = fp.parent / f"{fp.name}.tmp"
        try:
            self.bucket.download_file(object_key, str(fp_tmp))
        except botocore.exceptions.ClientError as exc:
            logger.error(f"Unable to download {object_key} : {str(exc)}")
            return None

        if tmp_extension:
            fp_tmp.rename(fp)
        return fp


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
        fp = self.download(
            object_key=self.get_timestep_key(date_day=date_day, run=run, timestep=timestep),
            destination_dir=download_dir
        )
        return {'fp': str(fp.absolute())}


class NoaaGfsS3(S3Nwp, pydantic.BaseModel):
    """
    Helper for downloading weather GFS from NOAA
    """
    bucket_name: str = "noaa-gfs-bdp-pds"

    def get_daterun_prefix(self, date_day: str, run: str):
        run = str(run).zfill(2)
        return f"gfs.{date_day}/{run}/gfs.t{run}z.pgrb2.0p25"

    def get_timestep_key(self, date_day: str, run: str, timestep: str):
        run = str(run).zfill(2)
        timestep = str(timestep).zfill(3)
        return f"gfs.{date_day}/{run}/gfs.t{run}z.pgrb2.0p25.f{timestep}"
