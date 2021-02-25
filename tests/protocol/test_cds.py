from pathlib import Path

from datafetch.protocol.cds import ClimateDataStoreApi
from datafetch.utils.db import DownloadRecord

cds_test_resource = {
    'cds_resource_name': 'reanalysis-era5-pressure-levels',
    'cds_resource_param': {
        'product_type': 'reanalysis',
        'format': 'grib',
        'variable': 'temperature',
        'pressure_level': '850',
        'year': '2021',
        'month': '02',
        'day': '18',
        'time': [
            '00:00', '06:00', '12:00',
            '18:00',
        ],
    }
}


def test_cds_queue(tmp_path):
    cds = ClimateDataStoreApi(db_dir=str(tmp_path))
    db_record, created = cds.submit_to_queue(**cds_test_resource)
    assert isinstance(db_record, DownloadRecord)
    assert isinstance(created, bool)
    assert db_record.queue_id

    cds = ClimateDataStoreApi(db_dir=str(tmp_path))
    db_record = cds.check_queue(**cds_test_resource)
    assert isinstance(db_record, DownloadRecord)

    cds = ClimateDataStoreApi()
    state, reply = cds.check_queue_by_id(queue_id=db_record.queue_id)
    assert isinstance(state, str)
    assert isinstance(reply, dict)

    cds = ClimateDataStoreApi()
    state, reply = cds.check_queue_by_id(
        queue_id=db_record.queue_id,
        wait_until_complete=True,
        sleep_seconds=1,
        max_try=2
    )
    assert isinstance(state, str)
    assert isinstance(reply, dict)

    cds = ClimateDataStoreApi(db_dir=str(tmp_path))
    fp = cds.download_result(**cds_test_resource, destination_dir=str(tmp_path))
    assert isinstance(fp, Path)
    assert fp.is_file()


#
# def test_cds_queue_request(tmp_path):
#     cds = ClimateDataStoreApi(
#         db_dir=str(tmp_path)
#     )
#
#     r1, created = cds.queue_request_if_needed(
#         cds_resource_name=cds_test_resource['cds_resource_name'],
#         cds_resource_param=cds_test_resource['cds_resource_param']
#     )
#     assert isinstance(r1, DownloadRecord)
#     assert r1.status == "queued"
#     assert r1.queue_id
#     assert created is True
#
#     r2, created = cds.queue_request_if_needed(
#         cds_resource_name=cds_test_resource['cds_resource_name'],
#         cds_resource_param=cds_test_resource['cds_resource_param']
#     )
#     assert isinstance(r2, DownloadRecord)
#     assert created is False
#     assert r2 == r1
#     assert r1.queue_id == r2.queue_id
#
#     state, reply = cds.check_queue_id_status(queue_id=r2.queue_id, wait_until_complete=False)
#     assert isinstance(state, str)
#     assert isinstance(reply, dict)
#
#     state, reply = cds.check_queue_id_status(queue_id=r2.queue_id,
#                                              wait_until_complete=True,
#                                              sleep_seconds=1, max_try=2)
#     assert isinstance(state, str)
#     assert isinstance(reply, dict)
#
#
# def test_cds_fetch(tmp_path):
#     cds = ClimateDataStoreApi(
#         db_dir=str(tmp_path),
#         wait_until_complete=True
#     )
#     fp = cds.fetch(
#         cds_resource_name=cds_test_resource['cds_resource_name'],
#         cds_resource_param=cds_test_resource['cds_resource_param'],
#         destination_dir=str(tmp_path)
#     )
#     assert isinstance(fp, Path)
#     assert fp.exists()
