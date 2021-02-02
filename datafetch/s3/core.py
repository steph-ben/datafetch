import logging
from pathlib import Path

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
            self._s3 = boto3.resource('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))
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

    def download(self, object_key: str, destination_dir: str, destination_filename: str = None) -> Path:
        """
        Helper for download object from bucket

        :param object_key:
        :param destination_dir:
        :param destination_filename:
        :return:
        """
        if destination_filename is None:
            destination_filename = object_key
        fp = Path(destination_dir, destination_filename)
        if not fp.parent.exists():
            fp.parent.mkdir(parents=True)

        logger.info(f"{self.bucket_name} : downloading {object_key} to {fp} ...")
        self.bucket.download_file(object_key, str(fp))
        return fp


class NoaaGfsS3(S3ApiBucket, pydantic.BaseModel):
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

