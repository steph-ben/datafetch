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
    # test_cds_submit
    cds = ClimateDataStoreApi(db_dir=str(tmp_path))
    db_record, created = cds.submit_to_queue(**cds_test_resource)
    assert isinstance(db_record, DownloadRecord)
    assert isinstance(created, bool)
    assert db_record.queue_id

    # test_cds_check_queue
    cds = ClimateDataStoreApi(db_dir=str(tmp_path))
    db_record = cds.check_queue(**cds_test_resource)
    assert isinstance(db_record, DownloadRecord)

    # test_cds_check_queue_id
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

    # test_cds_download
    cds = ClimateDataStoreApi(db_dir=str(tmp_path))
    fp = cds.download_result(**cds_test_resource, destination_dir=str(tmp_path))
    # FIXME : Ensure file is ready before testing ...
    #assert isinstance(fp, Path)
    #assert fp.is_file()

    # test_cds_download_by_id
    cds = ClimateDataStoreApi(db_dir=str(tmp_path))
    fp = cds.download_result_by_id(queue_id=db_record.queue_id, destination_dir=str(tmp_path))
    assert isinstance(fp, Path)
    assert fp.is_file()


def test_cds_force_new(tmp_path):
    # test_cds_submit
    cds = ClimateDataStoreApi(db_dir=str(tmp_path))
    db_record, created = cds.submit_to_queue(**cds_test_resource)
    assert isinstance(db_record, DownloadRecord)
    assert isinstance(created, bool)
    assert db_record.queue_id

    queue_id_1 = db_record.queue_id

    # test_cds_submit
    cds = ClimateDataStoreApi(db_dir=str(tmp_path))
    db_record, created = cds.submit_to_queue(**cds_test_resource, force_new=True)
    assert isinstance(db_record, DownloadRecord)
    assert isinstance(created, bool)
    assert db_record.queue_id
    assert db_record.queue_id != queue_id_1
