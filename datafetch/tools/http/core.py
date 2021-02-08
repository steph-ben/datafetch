"""
Helpers for fetching files through HTTP
"""
import logging
from pathlib import Path
from typing import Union

import pydantic

logger = logging.getLogger(__name__)


class SimpleHttpFetch(pydantic.BaseModel):
    """
    Simply download an url
    """
    base_url: str

    def download_url(self, destination_dir: str,
                     suffix: str = None,
                     destination_filename: str = None,
                     tmp_extention: str = ".tmp") -> Union[Path, None]:
        """
        Download data from remote url

        :param suffix:
        :param destination_dir:
        :param destination_filename:
        :param tmp_extention:
        :return:
        """
        url = Path(self.base_url)
        if suffix is not None:
            url = Path(self.base_url) / suffix

        if destination_filename is None:
            destination_filename = url.name

        fp = Path(destination_dir) / destination_filename
        if not fp.parent.exists():
            fp.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"{self.__class__.__name__} : Downloading {url} to {fp} ...")
        fp_tmp = fp
        if tmp_extention:
            fp_tmp = fp.parent / f"{fp.name}.tmp"
        try:
            # TODO : actually download the file
            pass
        except Exception as exc:
            logger.error(f"Unable to download {url} : {str(exc)}")
            return None

        if tmp_extention:
            fp_tmp.rename(fp)

        return fp
