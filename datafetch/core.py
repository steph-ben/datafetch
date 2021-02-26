"""
Core fetcher objects, including possible optional mixins
"""
import logging
from abc import ABC
from pathlib import Path
from typing import Union

import peewee
import pydantic

from .utils.db import DownloadRecord, db

logger = logging.getLogger(__name__)


class AbstractFetcher(pydantic.BaseModel, ABC):
    """
    Abstract class for fetcher
    """
    def fetch(self, **kwargs) -> Union[Path, None]:
        """
        Fetch a single file, with some possible pre and post actions

        When overrided in subclasses, this is the place to add some capabilities around download, eg.:
            - donwload file and rename with temporary extension
            - record download in a database

        :param kwargs:
        :return:
        """
        return self._fetch(**kwargs)

    def _fetch(self, destination_fp: str = None, **kwargs) -> Union[Path, None]:
        """
        Actually fetch a single file to `destination_fp`

        :param kwargs:
        :return:
        """
        raise NotImplementedError


class FetchWithTemporaryExtensionMixin(AbstractFetcher, pydantic.BaseModel, ABC):
    """
    Allow to run a download function by specifying a temporary extension while
    downloading the file
    """
    temporary_extension: str = "tmp"

    def fetch(self, destination_dir: str, destination_filename: str,
              **kwargs) -> Union[Path, None]:
        """
        Fetch a file with a temporary local extension

        :param destination_dir:
        :param destination_filename:
        :param kwargs:
        :return:
        """
        fp = Path(destination_dir) / destination_filename
        if not fp.parent.exists():
            fp.parent.mkdir(parents=True, exist_ok=True)

        fp_tmp = fp
        if self.temporary_extension:
            fp_tmp = fp.parent / f"{fp.name}.{self.temporary_extension}"
            logger.debug(f"Using temporary filename {fp_tmp} ...")

        fp_downloaded = super().fetch(destination_fp=str(fp_tmp), **kwargs)

        if fp_downloaded is not None:
            if self.temporary_extension:
                if fp_downloaded.is_file():
                    logger.info(f"Renaming {fp_downloaded} to {fp}")
                    fp_downloaded.rename(fp)
                fp_downloaded = fp

            if not fp_downloaded.is_file():
                logger.warning(f"File {fp_downloaded} was downloaded, but doesn't exists anymore")
                fp = None
        else:
            logger.error(f"File was not downloaded")
            fp = fp_downloaded

        return fp


class DownloadedFileRecorderMixin(AbstractFetcher, pydantic.BaseModel, ABC):
    """
    Helper to keep track of downloaded files into a database
    """
    use_download_db: bool = False

    db_name: str = None
    db_dir: str = "/tmp/"
    _db: peewee.Database = None

    def fetch(self, record_key: str, **kwargs) -> Union[Path, None]:
        """
        Record download into a database, identified by a unique `record_key`
        If a `record_key` already exists, the fetch won't be done again

        :param record_key:
        :param kwargs:
        :return:
        """
        if not self.use_download_db:
            # Don't check anything with db, fetch anyway
            return super().fetch(**kwargs)

        with self:
            logger.debug(f"{record_key} Checking if already downloaded ...")
            downdb_record, _ = self.db_get_record(key=record_key)
            if downdb_record.need_download():
                logger.info(f"{downdb_record} : Need download")
                try:
                    downdb_record.set_start()
                    fp = super().fetch(**kwargs)
                    downdb_record.set_downloaded(fp)
                except Exception as exc:
                    downdb_record.set_failed(error=str(exc))
                    logger.error(str(exc), exc_info=exc)

                downdb_record.save()
            else:
                logger.info(f"{record_key} : Already downloaded {downdb_record.filepath} ...")
                fp = Path(downdb_record.filepath)

        return fp

    @property
    def db_path(self) -> Path:
        """
        Path of sqlite file

        :return:
        """
        if self.db_name is None:
            self.db_name = self.__class__.__name__
        return Path(self.db_dir) / f"{self.db_name}.db"

    def __enter__(self):
        logger.debug(f"Using database {self.db_path} ...")
        db.init(database=self.db_path)
        db.connect()
        db.create_tables([DownloadRecord])

    def __exit__(self, exc_type, exc_val, exc_tb):
        db.close()

    @staticmethod
    def db_get_record(key: str) -> DownloadRecord:
        """
        Get a DownDb record from key

        :param key:
        :return:
        """
        return DownloadRecord.get_or_create(key=key)
