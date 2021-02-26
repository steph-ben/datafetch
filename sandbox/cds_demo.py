from datafetch.protocol.cds import ClimateDataStoreApi

import logging
loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
for logger in loggers:
    if "datafetch" in logger.name :
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.INFO)


cds_resource_name = 'reanalysis-era5-pressure-levels'
cds_resource_param = {
    'product_type': 'reanalysis',
    'format': 'grib',
    'variable': 'temperature',
    'pressure_level': '850',
    'year': ['2020', '2021', '2014'],
    'month': '02',
    'day': '18',
    'time': ['00:00'],
}


#################
cds = ClimateDataStoreApi()

db_record, created = cds.submit_to_queue(cds_resource_name, cds_resource_param, force_new=True)
db_record = cds.check_queue(cds_resource_name, cds_resource_param, wait_until_complete=True)
fp = cds.download_result(
    cds_resource_name, cds_resource_param,
    destination_dir="/tmp/"
)
print(fp)

print("\n" * 5)
print("#############################################")
print("\n" * 5)
#################
cds = ClimateDataStoreApi()
cds.fetch(
    cds_resource_name, cds_resource_param, destination_dir="/tmp/",
    force_new=True
)


##############
print("\n" * 5)
print("#############################################")
print("\n" * 5)
import cdsapi
cds = cdsapi.Client()
cds.retrieve(
    name=cds_resource_name,
    request=cds_resource_param,
    target="plop.plip",
)
