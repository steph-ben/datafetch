# Fetching data

Tools for fetching data, and providing ready-to-use [Prefect](https://prefect.io) flows.

Features:
- Fetch from various protocol (Amazon S3, Copernicus Climate Data Store, HTTP)
- Keep track of previously downloaded file using a sqlite
- Temporary renaming of downloaded file (eg. .tmp extention)
- Full-featured workflow using [Prefect](https://prefect.io)

Current available protocol :
- `datafetch.protocol.s3.S3ApiBucket` for fetching AWS buckets, in particular [AWS Opendata](https://registry.opendata.aws)
- `datafetch.protocol.cds.ClimateDataStoreApi` for fetching from [Copernicus Climate Data Store](https://cds.climate.copernicus.eu)
- `datafetch.protocol.http.SimpleHttpFetch` 

Current available weather-related fetchers:
- `datefetch.weather.noaa.nwp.NoaaGfsS3` for fetching  [NOAA GFS from AWS S3](https://registry.opendata.aws/noaa-gfs-bdp-pds/)
- `datefetch.weather.meteofrance.obs.MeteoFranceObservationFetch`


## Quickstart

* Installation
```
pip install git+https://github.com/steph-ben/datafetch.git
```

* Download a full GFS run using prefect flow

```python
>>> from datafetch.s3.flows import create_flow_download
>>> flow = create_flow_download()
>>> flow.run()
```

* Download single GFS file

```python
>>> from datafetch.s3 import NoaaGfsS3
>>> s3api = NoaaGfsS3()
NoaaGfsS3(bucket_name='noaa-gfs-bdp-pds')

# Check availability
>>> s3api.check_timestep_availability("20210201", "00", "003")
{'date_day': '20210201', 'run': '00', 'timestep': '003'}

# Launch download
>>> s3api.download_timestep("20210201", "00", "003", download_dir="/tmp/")
{'fp': '/tmp/gfs.20210201/00/gfs.t00z.pgrb2.0p25.f003'}

# Check file
$ ls -lh /tmp/gfs.20210201/00/gfs.t00z.pgrb2.0p25.f003
-rw-rw-r-- 1 steph steph 312M Feb  5 15:45 /tmp/gfs.20210201/00/gfs.t00z.pgrb2.0p25.f003
```

* Low-level API usage

```python
>>> from datafetch.s3 import NoaaGfsS3
>>> s3api = NoaaGfsS3()

# Check data availability
>>> r = s3api.filter(Prefix=s3api.get_daterun_prefix("20210202", "00"))
>>> list(r)[:3]
[s3.ObjectSummary(bucket_name='noaa-gfs-bdp-pds', key='gfs.20210202/00/gfs.t00z.pgrb2.0p25.anl'), 
 s3.ObjectSummary(bucket_name='noaa-gfs-bdp-pds', key='gfs.20210202/00/gfs.t00z.pgrb2.0p25.anl.idx'), 
 s3.ObjectSummary(bucket_name='noaa-gfs-bdp-pds', key='gfs.20210202/00/gfs.t00z.pgrb2.0p25.f000')]

# Download
>>> s3api.download('gfs.20210202/00/gfs.t00z.pgrb2.0p25.anl', destination_dir="/tmp/")
PosixPath('/tmp/gfs.20210202/00/gfs.t00z.pgrb2.0p25.anl')
```

## Fetching from AWS

TODO

## Fetching from Copernicus Climate Data Store (CDS)

Copernicus CDS call itself a place to "Dive into this wealth of information about the Earth's past, present and future climate."

You can browse and download all data from the official website. As well, a python API https://github.com/ecmwf/cdsapi is available
for downloading data from scripts.

The `datafetch.protocol.cds` package enhance `cdsapi` with the following features:
- Make asynchronous request and check request status later on, using a sqlite
- Keep track of previously downloaded file, using a sqlite
- Temporary renaming of downloaded file (eg. .tmp extention)

### Pre-requisites

In order to access those public data, you must:
    - Register a free account from https://cds.climate.copernicus.eu/user/register
    - Configure your user key, as defined here https://github.com/ecmwf/cdsapi#configure

Then you can :
    - Browse all online resources from https://cds.climate.copernicus.eu/cdsapp#!/search?type=dataset
    - Simulate the needed information to download the resources from Donwload data > Show API request, example:

```
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
```

### Usage

TODO

