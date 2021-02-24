"""
Helpers for fetching data from AWS S3
"""
import logging
from pathlib import Path
from typing import Union

import boto3
import boto3.resources
import botocore
import botocore.client
import pydantic

from datafetch.core import FetchWithTemporaryExtensionMixin, DownloadedFileRecorderMixin

logger = logging.getLogger(__name__)


class S3ApiBucket(FetchWithTemporaryExtensionMixin,
                  DownloadedFileRecorderMixin,
                  pydantic.BaseModel):
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

    def fetch(self, object_key: str, destination_dir: str,
              destination_filename: str = None,
              record_key: str = None,
              **kwargs) -> Union[Path, None]:
        """

        :param object_key:
        :param destination_dir:
        :param destination_filename:
        :param record_key:
        :param kwargs:
        :return:
        """
        if destination_filename is None:
            destination_filename = object_key

        return super().fetch(
            # For _fetch function below
            object_key=object_key,
            # For FetchWithTemporaryExtensionMixin
            destination_dir=destination_dir, destination_filename=destination_filename,
            # For DownloadFileRecorderMixin
            record_key=object_key,
            **kwargs)

    def _fetch(self, object_key: str,
               destination_fp: str = None, **kwargs) -> Union[Path, None]:
        """
        Actually download a S3 bucket resource

        :param object_key:
        :param destination_fp:
        :param kwargs:
        :return:
        """
        try:
            self.bucket.download_file(object_key, str(destination_fp))
        except Exception as exc:
            logger.error(f"Unable to fetch {self.bucket_name}/{object_key} to {destination_fp}: {str(exc)}")
            return None

        return Path(destination_fp)
