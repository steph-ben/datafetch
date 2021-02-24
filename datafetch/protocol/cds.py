import logging
from pathlib import Path
from typing import Union

import pydantic
import cdsapi
from cdsapi.api import Result

from datafetch.core import FetchWithTemporaryExtensionMixin, DownloadedFileRecorderMixin
from datafetch.utils.db import DownloadRecord

logger = logging.getLogger(__name__)


class ClimateDataStoreApi(FetchWithTemporaryExtensionMixin,
                          DownloadedFileRecorderMixin,
                          pydantic.BaseModel):
    """
    A simple wrapper for cdsapi library
    See :
        - https://github.com/ecmwf/cdsapi
        - https://github.com/ecmwf/cdsapi/blob/master/examples/example-era5-update.py
    """

    # For connecting CDS
    url: str = "https://cds.climate.copernicus.eu/api/v2"
    api_uid: str = None
    api_key: str = None
    _cds: cdsapi.Client = None

    # Do synchrone request or not
    retrieve_and_wait_until_complete: bool = False

    class Config:
        underscore_attrs_are_private = True

    @property
    def cds(self) -> cdsapi.Client:
        """
        Helper for creating a cdsapi Client
        UID and API key can be found here : https://cds.climate.copernicus.eu/user/

        Example of usage :
            >>> api = ClimateDataStoreApi(api_uid=xxx, api_key=xxx)
            >>> api.cds

        Otherwise, follow the doc at https://github.com/ecmwf/cdsapi#configure
            $ cat ~/.cdsapirc
            url: https://cds.climate.copernicus.eu/api/v2
            key: <UID>:<API key>
            verify: 0


        :return:
        """
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

    def fetch(self,
              cds_resource_name: str, cds_resource_param: dict,
              destination_dir: str, destination_filename: str,
              **kwargs) -> Union[Path, None]:
        """
        Fetching ...

        :param cds_resource_name:
        :param cds_resource_param:
        :param destination_dir:
        :param destination_filename:
        :param kwargs:
        :return:
        """
        queue_id = self.queue_request_if_needed(cds_resource_name, cds_resource_param)

        if queue_id is not None:
            self.check_queue_status(queue_id)
            super().fetch(url, destination_dir, destination_filename, **kwargs)

    def queue_request_if_needed(self, cds_resource_name: str, cds_resource_param: dict) -> DownloadRecord:
        """
        Trigger a request on CDS if needed, and record request id in a database

        :param cds_resource_name:
        :param cds_resource_param:
        :param dict:
        :return:
        """
        record_key = {'name': cds_resource_name, 'param': cds_resource_param}
        with self:
            logger.debug(f"{cds_resource_name} Checking if queuing to CDS is needed ...")
            downdb_record, created = self.db_get_record(key=record_key)
            if downdb_record.need_queue():
                # No request has been made yet
                try:
                    logger.info(f"{cds_resource_name} : Making request queue to CDS ...")
                    queue_id = self.queue_request(cds_resource_name, cds_resource_param)

                    if queue_id:
                        downdb_record.set_queued(queue_id)
                        logger.info(f"{cds_resource_name} : Request in queue ({queue_id}), saving to db ...")
                        downdb_record.save()
                    else:
                        logger.error(f"{cds_resource_name} : Queuing request failed")

                except KeyError as exc:
                    logger.error(f"{cds_resource_name} : Queuing request failed : {str(exc)}")
            else:
                logger.info(f"{cds_resource_name} Request already in queue at CDS ({downdb_record.queue_id})")

        return downdb_record, created

    def queue_request(self, cds_resource_name: str, cds_resource_param: dict):
        """
        Trigger a request on CDS

        :param cds_resource_name:
        :param cds_resource_param:
        :return:
        """
        r = self.cds.retrieve(
            name=cds_resource_name,
            request=cds_resource_param,
            target=None
        )
        return self.check_result_status(r)

    def download_result(self):
        pass
















    def _fetch(self, destination_fp: str = None, **kwargs) -> Union[Path, None]:
        pass


    def check_result_status(self, result: cdsapi.api.Result) -> Union[str, None]:
        """
        Check a CDS request status

        :param result:
        :return:
        """
        # Update the result to get request_id
        result.update()

        if 'request_id' in result.reply:
            return result.reply['request_id']
        else:
            return None

    def check_request_id_status(self, request_id: str):
        """
        Check status for a particular request id

        :param request_id:
        :return:
        """
        r = Result(client=self.cds, reply=None)
        r.update(request_id=request_id)
        print(r.reply)

