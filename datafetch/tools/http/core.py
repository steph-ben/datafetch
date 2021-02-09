"""
Helpers for fetching files through HTTP
"""
import logging
import shutil
from pathlib import Path
from typing import Union

import pydantic
import requests

from datafetch.utils import FetchWithTemporaryExtensionMixin

logger = logging.getLogger(__name__)


class SimpleHttpFetch(FetchWithTemporaryExtensionMixin, pydantic.BaseModel):
    """
    Simply download an url

    Example of usage :

        >>> fetcher = SimpleHttpFetch(base_url="http://www.google.com")
        >>> fetcher.fetch(url_suffix="?q=plop", destination_dir="/tmp")
    """
    base_url: str

    # Use python requests raw bytes when download
    # It can make it faster when downloading large file. However, it doesn't gunzip and deflate.
    # cf. https://2.python-requests.org/en/master/user/quickstart/#binary-response-content
    use_requests_raw: bool = False

    def fetch(self, destination_dir: str,
              url_suffix: str = None, destination_filename: str = None,
              **kwargs: str) -> Union[Path, None]:
        """
        Download data from remote url

        :param destination_dir:
        :param url_suffix:
        :param destination_filename:
        :return:
        """
        # Handle optional url suffix
        url = self.base_url
        if url_suffix is not None:
            url = f"{self.base_url}/{url_suffix}"

        # Default destination filename from url suffix
        if destination_filename is None:
            destination_filename = url.split("/")[-1]

        return super().fetch(destination_dir=destination_dir, destination_filename=destination_filename,
                             url=url, **kwargs)

    def _fetch(self, url: str, destination_fp: str) -> Union[Path, None]:
        """
        Actually download an url to a file

        :param url:
        :param destination_fp:
        :return:
        """
        destination_fp = Path(destination_fp)
        logger.info(f"{self.__class__.__name__} : Downloading {url} to {destination_fp} ...")

        try:
            # cf. https://stackoverflow.com/a/39217788/554374
            with requests.get(url, stream=True) as r:
                with destination_fp.open('wb') as fd:
                    if self.use_requests_raw:
                        shutil.copyfileobj(r.raw, fd)
                    else:
                        for chunk in r.iter_content(chunk_size=128):
                            fd.write(chunk)
        except Exception as exc:
            logger.error(f"Unable to download {url} to {destination_fp}: {str(exc)}")
            return None

        return destination_fp
