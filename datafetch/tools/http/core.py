"""
Helpers for fetching files through HTTP
"""
from pathlib import Path
from typing import Union

import pydantic


class SimpleHTTPFetch(pydantic.BaseModel):
    base_url: str

    def download_url(self, suffix: str = None,
                 destination_dir: str = None, destination_filename: str = None,
                 tmp_extention: str = ".tmp") -> Union[Path, None]:
        """
        Download data from remote url

        :return:
        """
        pass
