import logging
import pydantic
import cdsapi
from cdsapi.api import Result

logger = logging.getLogger(__name__)


class ClimateDataStoreApi(pydantic.BaseModel):
    """
    A simple wrapper for cdsapi library
    See :
        - https://github.com/ecmwf/cdsapi
        - https://github.com/ecmwf/cdsapi/blob/master/examples/example-era5-update.py
    """
    api_uid: str = None
    api_key: str = None
    url: str = "https://cds.climate.copernicus.eu/api/v2"
    retrieve_and_wait_until_complete: bool = False
    _cds: cdsapi.Client = None

    class Config:
        underscore_attrs_are_private = True

    @property
    def cds(self):
        if self._cds is None:
            cds_args = {
                'verify': 0,
                'wait_until_complete': self.retrieve_and_wait_until_complete
            }
            if self.api_uid is not None and self.api_key is not None:
                key = f"{self.api_uid}:{self.api_key}"
                cds_args['key'] = key
            if self.url:
                cds_args['url'] = self.url

            logger.info(f"Logging to CDS {cds_args} ...")
            self._cds = cdsapi.Client(**cds_args)

        return self._cds

    def check_result_status(self, result: cdsapi.api.Result):
        """
        Check a CDS request status

        :param result:
        :return:
        """
        result.update()
        print(result.reply)
        return result

    def check_request_id_status(self, request_id: str):
        """
        Check status for a particular request id

        :param request_id:
        :return:
        """
        r = Result(client=self.cds, reply=None)
        r.update(request_id=request_id)
        print(r.reply)
