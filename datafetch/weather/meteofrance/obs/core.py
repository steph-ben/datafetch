"""
Fetch observation data from MeteoFrance public data
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union

import pydantic

from datafetch.protocol import SimpleHttpFetch


class MeteoFranceObservationFetch(SimpleHttpFetch, pydantic.BaseModel):
    """
    Download MeteoFrance Observation from public dataset
    """
    base_url = "https://donneespubliques.meteofrance.fr/donnees_libres/"

    observation_type_url_suffix = {
        'synop': 'Txt/Synop'
    }

    def fetch(self, destination_dir: str,
              observation_type: str = "synop", datetime_ref: str = None,
              **kwargs) -> Union[Path, None]:
        """
        Download synop csv data for a particular datetime

        :param destination_dir:
        :param observation_type: eg. synop
        :param datetime_ref: eg. 2021020812
        :param kwargs:
        :return:
        """
        # Handle observation type
        if observation_type not in self.observation_type_url_suffix:
            raise NotImplementedError(f"Observation type {observation_type} not implemented")
        obs_type_suffix = self.observation_type_url_suffix[observation_type]

        # Default datetime
        if datetime_ref is None:
            now = datetime.utcnow() - timedelta(days=1)
            # TODO : get the latest synoptic hour from current time
            datetime_ref = now.strftime("%Y%m%d00")

        # Build full url suffix
        url_suffix = "/".join([obs_type_suffix, f"{observation_type}.{datetime_ref}.csv"])

        return super().fetch(destination_dir=destination_dir, url_suffix=url_suffix, **kwargs)
