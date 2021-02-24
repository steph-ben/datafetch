from pathlib import Path

from datafetch.protocol.http.core import SimpleHttpFetch
from datafetch.core import FetchWithTemporaryExtensionMixin
from datafetch.weather.meteofrance.obs.core import MeteoFranceObservationFetch


def test_meteofrance_obs(tmp_path):
    fetcher = MeteoFranceObservationFetch()
    assert isinstance(fetcher, (MeteoFranceObservationFetch,
                                SimpleHttpFetch, FetchWithTemporaryExtensionMixin))

    # Fetch by default
    r = fetcher.fetch(destination_dir=str(tmp_path))
    assert isinstance(r, Path)
    assert r.is_file()
