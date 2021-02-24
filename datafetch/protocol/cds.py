import logging
import time
from pathlib import Path
from typing import Union, Tuple

import pydantic
import cdsapi
from cdsapi.api import Result

from datafetch.core import FetchWithTemporaryExtensionMixin, DownloadedFileRecorderMixin
from datafetch.protocol import SimpleHttpFetch
from datafetch.utils.db import DownloadRecord

logger = logging.getLogger(__name__)


class ClimateDataStoreApi(SimpleHttpFetch,
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
    _cds_internal_wait_until_complete: bool = False
    _cds: cdsapi.Client = None

    # Do synchrone request or not
    wait_until_complete: bool = False

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
                'wait_until_complete': self._cds_internal_wait_until_complete
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
              destination_dir: str, destination_filename: str = None,
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
        downdb_record, created = self.queue_request_if_needed(cds_resource_name, cds_resource_param)

        if downdb_record.queue_id:
            state, result = self.check_queue_id_status(downdb_record.queue_id,
                                                       wait_until_complete=self.wait_until_complete)
            if state == "completed":
                url = result['location']

                return super().fetch(
                    # For SimpleHttpFetch
                    url_suffix=url,
                    # For FetchWithTemporaryExtensionMixin
                    destination_dir=destination_dir, destination_filename=destination_filename,
                    # For DownloadedFileRecorderMixin
                    record_key=downdb_record.key,
                    **kwargs)

    def queue_request_if_needed(self, cds_resource_name: str, cds_resource_param: dict) -> Tuple[DownloadRecord, bool]:
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
            downdb_record, created = self.db_get_record(key=str(record_key))
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

    def queue_request(self, cds_resource_name: str, cds_resource_param: dict) -> Union[str, None]:
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
        return self.get_queue_id_from_result(r)

    def check_queue_id_status(self, queue_id: str,
                              wait_until_complete: bool = False,
                              sleep_seconds: int = 10,
                              max_try: int = 6) -> Tuple[str, dict]:
        """
        Check status for a particular request id

        :param queue_id:
        :param wait_until_complete:
        :param max_try:
        :param sleep_seconds:
        :return:
        """
        r = Result(client=self.cds, reply=None)
        if not wait_until_complete:
            r.update(request_id=queue_id)
            logger.debug(r.reply)
            return r.reply['state'], r.reply
        else:
            nb_try = 0
            while True:
                nb_try += 1
                r.update(request_id=queue_id)
                state = r.reply['state']

                if state == "completed":
                    logger.debug(f"{queue_id} : Completed")
                    return state, r.reply

                if state in ("queued", "running"):
                    nb_try += 1
                    if nb_try > max_try:
                        logger.error(f"{queue_id} : Too many attempts checking results ({nb_try} / {max_try}), exiting ...")
                        return state, r.reply

                    logger.info(f"{queue_id} : Still in progress : {state} ... (attempt {nb_try} / {max_try})")
                    time.sleep(sleep_seconds)
                    continue

                if state in ("failed",):
                    logger.error(f"Request failed : {r.reply['error']}")
                    return state, r.reply

                raise Exception(f"Unknown API state {state} ...")

    def get_queue_id_from_result(self, result: cdsapi.api.Result) -> Union[str, None]:
        """
        Extract the request id from a Result

        :param result:
        :return:
        """
        # Update the result to get request_id
        result.update()

        if 'request_id' in result.reply:
            return result.reply['request_id']
        else:
            return None

