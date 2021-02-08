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
