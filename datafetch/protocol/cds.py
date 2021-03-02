import logging
import pprint
import time
from pathlib import Path
from typing import Union, Tuple

import pydantic
import cdsapi
import urllib3
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

        >>> cds = ClimateDataStoreApi(wait_until_complete=True)
        >>> fp = cds.fetch(
            cds_resource_name='reanalysis-era5-pressure-levels',
            cds_resource_param={
                'product_type': 'reanalysis',
                'format': 'grib',
                'variable': 'temperature',
                'pressure_level': '850',
                'year': '2021',
                'month': '02',
                'day': '18',
                'time': ['00:00'],
            },
            destination_dir='/tmp/'
        )

    """

    # For connecting CDS
    api_uid: str = None
    api_key: str = None
    _cds_url: str = "https://cds.climate.copernicus.eu/api/v2"
    _cds_internal_wait_until_complete: bool = False
    _cds: cdsapi.Client = None

    use_download_db = True

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
            # Disable urllib https warning
            # cf. https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
            urllib3.disable_warnings()

            cds_args = {
                # Don't check SSL
                'verify': 0,
                # Don't automatically wait forever
                'wait_until_complete': self._cds_internal_wait_until_complete,
                # Don't automatically delete request when python object is deltroyed
                'delete': False
            }
            if self.api_uid is not None and self.api_key is not None:
                key = f"{self.api_uid}:{self.api_key}"
                cds_args['key'] = key
            if self._cds_url:
                cds_args['url'] = self._cds_url

            logger.debug(f"Logging to CDS {cds_args} ...")
            self._cds = cdsapi.Client(**cds_args)

        return self._cds

    def fetch(self,
              cds_resource_name: str, cds_resource_param: dict,
              destination_dir: str, destination_filename: str = None,
              wait_until_complete: bool = True,
              force_new: bool = False,
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
        self.submit_to_queue(cds_resource_name, cds_resource_param, force_new=force_new)
        self.check_queue(cds_resource_name, cds_resource_param, wait_until_complete=wait_until_complete)
        return self.download_result(cds_resource_name, cds_resource_param, destination_dir=destination_dir, **kwargs)

    def submit_to_queue(
            self, cds_resource_name: str, cds_resource_param: dict,
            force_new: bool = False
    ) -> Union[str, Tuple[DownloadRecord, bool]]:
        """
        Submit a request to CDS queue

        :param cds_resource_name:
        :param cds_resource_param:
        :param force_new:
        :return:
        """
        if not self.use_download_db:
            return self._queue_request(cds_resource_name, cds_resource_param)
        else:
            return self._queue_request_with_db(cds_resource_name, cds_resource_param, force_new)

    def _queue_request_with_db(self,
                               cds_resource_name: str, cds_resource_param: dict,
                               force_new: bool
                               ) -> Tuple[DownloadRecord, bool]:
        """
        Trigger a request on CDS if needed, and record request id in a database

        :param cds_resource_name:
        :param cds_resource_param:
        :param dict:
        :return:
        """
        record_key = self.get_resource_key(cds_resource_name, cds_resource_param)
        with self:
            logger.debug(f"{cds_resource_name} Checking if queuing to CDS is needed ...")
            downdb_record, created = self.db_get_record(key=str(record_key))

            if not created and force_new:
                logger.info(f"Cleaning existing record {downdb_record}")
                downdb_record.delete().execute()
                downdb_record, created = self.db_get_record(key=str(record_key))

            if downdb_record.need_queue():
                # No request has been made yet
                try:
                    logger.info(f"{cds_resource_name} : Making request queue to CDS ...")
                    queue_id = self._queue_request(cds_resource_name, cds_resource_param)

                    if queue_id:
                        downdb_record.set_queued(queue_id)
                        logger.info(f"{queue_id} in queue")
                        logger.info(f"{queue_id} saved into db : {downdb_record}")
                        downdb_record.save()
                    else:
                        logger.error(f"{cds_resource_name} : Queuing request failed")

                except KeyError as exc:
                    logger.error(f"{cds_resource_name} : Queuing request failed : {str(exc)}")
            else:
                logger.info(f"{cds_resource_name} Request already exists in our sqlite : {downdb_record})")

        return downdb_record, created

    def _queue_request(self, cds_resource_name: str, cds_resource_param: dict) -> Union[str, None]:
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

    def check_queue(self, cds_resource_name: str, cds_resource_param: dict, **kwargs) -> Union[DownloadRecord, None]:
        """
        Check a queue status and update db accordingly

        :param cds_resource_name:
        :param cds_resource_param:
        :return:
        """
        if not self.use_download_db:
            logger.error(f"Cannot check queue status while not using sqlite database to track requests id")
            return None

        record_key = self.get_resource_key(cds_resource_name, cds_resource_param)
        downdb_record, created = self.db_get_record(key=str(record_key))
        if created:
            logger.error(f"Cannot find existing request id for {record_key} ...")
            return None

        state, result = self.check_queue_by_id(downdb_record.queue_id, **kwargs)
        if state == "completed":
            downdb_record.origin_url = result['location']
            if downdb_record.status == "queued":
                downdb_record.set_queued_and_ready()
        if state == "failed":
            downdb_record.set_failed(error=str(result))
            logger.debug(cds_resource_name)
            logger.debug(pprint.pformat(cds_resource_param))
            logger.debug(pprint.pformat(result))
        downdb_record.save()

        return downdb_record

    def check_queue_by_id(self, queue_id: str,
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
            logger.info(f"{queue_id} : {r.reply['state']}")
            return r.reply['state'], r.reply

        else:
            nb_try = 0
            while True:
                nb_try += 1
                r.update(request_id=queue_id)
                state = r.reply['state']

                if state == "completed":
                    logger.info(f"{queue_id} Completed")
                    return state, r.reply

                if state in ("queued", "running"):
                    nb_try += 1
                    if nb_try > max_try:
                        logger.error(
                            f"{queue_id} : Too many attempts checking results ({nb_try} / {max_try}), exiting ...")
                        return state, r.reply

                    logger.info(f"{queue_id} : Still in progress : {state} ... (attempt {nb_try} / {max_try})")
                    time.sleep(sleep_seconds)
                    continue

                if state in ("failed",):
                    logger.error(f"Request failed : {r.reply['error']}")
                    return state, r.reply

                raise Exception(f"Unknown API state {state} ...")

    def download_result(self,
                        cds_resource_name: str, cds_resource_param: dict,
                        destination_dir: str, destination_filename: str = None,
                        **kwargs) -> Union[Path, None]:
        """
        Download result locally

        :param cds_resource_name:
        :param cds_resource_param:
        :param destination_filename:
        :param destination_dir:
        :param kwargs:
        :return:
        """
        if not self.use_download_db:
            logger.error(f"Cannot check queue status while not using sqlite database to track requests id")
            raise ReferenceError()

        record_key = self.get_resource_key(cds_resource_name, cds_resource_param)
        downdb_record, created = self.db_get_record(key=str(record_key))
        if created:
            logger.error(f"Cannot find existing request id for {record_key} ...")
            return None

        if downdb_record.status == "queued_and_ready":
            fp = super().fetch(
                # For SimpleHttpFetch
                url_suffix=downdb_record.origin_url,
                # For FetchWithTemporaryExtensionMixin
                destination_dir=destination_dir, destination_filename=destination_filename,
                # For DownloadedFileRecorderMixin
                record_key=downdb_record.key,
                **kwargs)

            if fp:
                logger.info("Cleanup CDS request")
                r = Result(client=self.cds, reply=None)
                r.update(request_id=downdb_record.queue_id)
                r.delete()

            return fp
        elif downdb_record.status == "downloaded":
            logger.info(f"Request already downloaded {downdb_record}")
            return downdb_record.filepath
        elif downdb_record.status == "queued":
            logger.error(f"Request is not yet ready for download {downdb_record}")
            logger.debug(pprint.pformat(downdb_record.__dict__))
            return None
        else:
            logger.error(f"Unexpected status {downdb_record}")
            return None

    def download_result_by_id(self, queue_id: str,
                              destination_dir: str, destination_filename: str = None,
                              **kwargs):
        """

        :param destination_dir:
        :param destination_filename:
        :param queue_id:
        :param kwargs:
        :return:
        """
        state, reply = self.check_queue_by_id(queue_id=queue_id)
        if state == "completed" and 'location' in reply:
            url = reply['location']

            return super().fetch(
                # For SimpleHttpFetch
                url_suffix=url,
                # For FetchWithTemporaryExtensionMixin
                destination_dir=destination_dir, destination_filename=destination_filename,
                # For DownloadedFileRecorderMixin
                record_key=url,
                **kwargs
            )
        else:
            logger.error(f"Wrong status : {state} / {reply}")
            return None

    @staticmethod
    def get_queue_id_from_result(result: cdsapi.api.Result) -> Union[str, None]:
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

    @staticmethod
    def get_resource_key(cds_resource_name: str, cds_resource_param: dict) -> dict:
        """
        Generate a key from the CDS request

        :param cds_resource_name:
        :param cds_resource_param:
        :return:
        """
        return {
            'name': cds_resource_name,
            'param': cds_resource_param
        }
