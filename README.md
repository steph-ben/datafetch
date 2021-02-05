# Fetching data

Tools for fetching data, and providing ready-to-use [Prefect](https://prefect.io) flows.

Currently available:
* Fetch AWS, in particular OpenData like [NOAA GFS from AWS S3](https://registry.opendata.aws/noaa-gfs-bdp-pds/)


## Quickstart

* Installation
```
pip install git+https://github.com/steph-ben/datafetch.git
```

* Download a full run using prefect flow

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
