"""
Fetch observation data from MeteoFrance public data
"""
from pathlib import Path
from typing import Union

from datafetch.tools.http.core import SimpleHTTPFetch


class MeteoFranceObservation(SimpleHTTPFetch):
    base_url = "https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/synop.2021020812.csv"

    def download_synop_for(self, datetime: str, **kwargs) -> Union[Path, None]:
        """
        Download synop csv data for a particular datetime
        :param datetime: eg. 2021020812
        :return:
        """
        url_suffix = "Txt" / "Synop" / f"synop.{datetime}.csv"
        return self.download_url(suffix=url_suffix, **kwargs)
