"""
A set of standard tasks & flows for fetching GFS from AWS S3

Example of usage :
    >>> from fetchers.s3.flows import flow_download
    >>> flow_download.run()

"""
import datetime
from pathlib import Path

import prefect
from prefect import Parameter
from prefect.engine import signals
from prefect.executors import LocalDaskExecutor
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from .core import NoaaGfsS3


@prefect.task(
    log_stdout=True,
    max_retries=5, retry_delay=datetime.timedelta(minutes=1)
)
def check_run_availability(run: Parameter, date_day: Parameter = None):
    """
    Check if a GFS run is available for a given day

    :param date_day:
    :param run:
    :return:
    """
    if date_day is None:
        date_day = prefect.context.scheduled_start_time.strftime("%Y%m%d")

    print(f"{date_day} / {run} : Checking run availability ...")
    s3api = NoaaGfsS3()
    r = s3api.filter(Prefix=s3api.get_daterun_prefix(str(date_day), str(run))).limit(count=1)
    if len(list(r)) > 0:
        print(f"{date_day} / {run} : Run is available !")
        return {'date_day': date_day, 'run': run}
    else:
        raise signals.FAIL(f"Run {date_day} / {run} is not yet available")


@prefect.task(
    log_stdout=True,
    max_retries=5, retry_delay=datetime.timedelta(minutes=1)
)
def check_timestep_availability(daterun_info: dict, timestep: str):
    """
    Check if a particular timestep for GFS is available

    :param daterun_info:
    :param timestep:
    :return:
    """
    print(f"{daterun_info} / {timestep} : Checking timestep availability ...")
    s3api = NoaaGfsS3()
    r = s3api.filter(Prefix=s3api.get_timestep_key(timestep=timestep, **daterun_info))
    if len(list(r)) > 0:
        daterun_info = dict(**daterun_info)
        daterun_info['timestep'] = timestep
        print(f"{daterun_info} : Timestep available !")
        return daterun_info
    else:
        raise ValueError(f"Timestep {timestep} not yet available")


@prefect.task(log_stdout=True)
def download_timestep(timestep_info: dict, download_dir: str) -> Path:
    """
    Download a specific timestep file

    :param timestep_info:
    :param download_dir:
    :return:
    """
    print(f"Downloading file {timestep_info} to {download_dir} ...")
    s3api = NoaaGfsS3()
    return s3api.download(
        object_key=s3api.get_timestep_key(**timestep_info),
        destination_dir=download_dir
    )


@prefect.task
def post_processing(fp: str):
    print(f"Do some post-processing on {fp} ... ")


#######################################################

def create_flow_download(
        flow_name: str = "aws-gfs-download",
        run: int = 0,
        timesteps: list = [3, 6],
        max_concurrent_download: int = 5,
        schedule: str = "",
        download_dir: str = '/tmp/plop'):
    """
    Create a prefect flow for downloading GFS
    with some configuration option

    :param flow_name:
    :param run:
    :param timesteps:
    :param max_concurrent_download:
    :param download_dir:
    :return:
    """

    with prefect.Flow(name=f"{flow_name}-run{run}") as flow_download:
        """
        A Flow for downloading data from AWS:
            - check if a run is available
            - download according to config
        """
        param_run = prefect.Parameter("run", default=run)

        daterun_avail = check_run_availability(run=param_run)

        for timestep in timesteps:
            timestep_avail = check_timestep_availability(
                daterun_info=daterun_avail, timestep=timestep,
                task_args={'name': f'timestep_{timestep}_check_availability'}
            )
            fp = download_timestep(
                timestep_info=timestep_avail,
                download_dir=download_dir,
                task_args={'name': f'timestep_{timestep}_download'}
            )

    # Scheduling on a daily basis, according to the run
    schedule = Schedule(clocks=[CronClock(f"0 {run} * * *")])
    flow_download.schedule = schedule

    # For choosing the right executor,
    # see https://docs.prefect.io/orchestration/flow_config/executors.html#choosing-an-executor
    flow_download.executor = LocalDaskExecutor(
        scheduler="threads",
        num_workers=max_concurrent_download
    )

    return flow_download
