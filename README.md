# Fetching data

Tools for fetching data, and providing ready-to-use [Prefect](https://prefect.io) flows.

Currently available:
* Fetch AWS, in particular OpenData like [NOAA GFS from AWS S3](https://registry.opendata.aws/noaa-gfs-bdp-pds/)


## Quickstart

* Installation
```
pip install git+https://github.com/steph-ben/datafetch.git
```

* Usage

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

## Setup full stack with prefect scheduler

* Install conda & env
  
```shell
# Conda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
sh ./Miniconda3-latest-Linux-x86_64.sh
conda config --set auto_activate_base false
 
# Create env
conda create -n prefect
conda activate prefect
conda install -c conda-forge prefect
```

* Docker (for running prefect server only)
  
```shell
# Cf. https://docs.docker.com/engine/install/centos/
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo yum install docker-ce docker-ce-cli containerd.io -y
sudo systemctl enable --now docker
sudo docker run hello-world
sudo usermod -Ga docker <<you_user>>
```

* Python reqs

```shell
git clone https://github.com/steph-ben/fetch-with-prefect.git
pip install -r requirements.txt
python setup.py develop
```


* Test our flow locally, wihtout any prefect server
  
```shell
python fetcher.py run

[2020-12-16 22:04:09+0000] INFO - prefect.FlowRunner | Beginning Flow run for 'aws-gfs-download'
[2020-12-16 22:04:10+0000] INFO - prefect.TaskRunner | Task 'check_run_availability': Starting task run...
[2020-12-16 22:04:10+0000] INFO - prefect.TaskRunner | 20201215 / 0 : Checking run availability ...
[2020-12-16 22:04:26+0000] INFO - prefect.TaskRunner | 20201215 / 0 : Run is available !
...
[2020-12-16 22:05:30+0000] INFO - prefect.TaskRunner | Task 'timestep_9_download': Finished task run for task with final state: 'Success'
[2020-12-16 22:05:30+0000] INFO - prefect.TaskRunner | Task 'timestep_18_download': Finished task run for task with final state: 'Success'
[2020-12-16 22:05:30+0000] INFO - prefect.FlowRunner | Flow run SUCCESS: all reference tasks succeeded
```

Working great ? Now let's run it inside prefect server


* Launching prefect and one local agent

```shell
prefect server start
prefect agent local start
```

* Register and trigger our flow

```shell
# Needed once
$ prefect create project "gfs-fetcher"

# Install our app in virtualenv, so the local agent can access our lib
$ python setup.py develop

# Interracting with Prefect !
$ python fetcher.py register
Result check: OK
Flow URL: http://localhost:8080/default/flow/ec5a906b-6b03-46ce-b7d8-b02e73d30d98
 └── ID: 7f049f56-2c6d-49b4-8515-658b43850764
 └── Project: gfs-fetcher
 └── Labels: ['steph-laptop']
7f049f56-2c6d-49b4-8515-658b43850764

$ python fetcher.py trigger
{'data': {'flow': [{'id': '7f049f56-2c6d-49b4-8515-658b43850764', 'name': 'aws-gfs-download'}]}}
A new FlowRun has been triggered
Go check it on http://linux:8080/flow-run/1829c2f9-a5f4-43c4-ad3e-bd42a80068ea
```

Enjoy the view !