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


def test_cds_queue_request(tmp_path):
    cds = ClimateDataStoreApi(
        db_dir=str(tmp_path)
    )

    r, created = cds.queue_request_if_needed(
        cds_resource_name=cds_test_resource['cds_resource_name'],
        cds_resource_param=cds_test_resource['cds_resource_param']
    )
    assert isinstance(r, DownloadRecord)
    assert r.status == "queued"
    assert created is True

    r, created = cds.queue_request_if_needed(
        cds_resource_name=cds_test_resource['cds_resource_name'],
        cds_resource_param=cds_test_resource['cds_resource_param']
    )
    assert isinstance(r, DownloadRecord)
    assert created is False
