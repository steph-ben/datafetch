"""
A set of standard tasks & flows for fetching GFS from AWS S3

Example of usage :
    >>> from datafetch.weather.noaa.nwp.flows import create_flow_download
    >>> flow_download = create_flow_download()
    >>> flow_download.run()

"""
import datetime

import prefect
from prefect import Parameter
from prefect.engine import signals
from prefect.executors import LocalDaskExecutor
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.tasks.prefect import StartFlowRun

from .core import NoaaGfsS3


@prefect.task(
    retry_delay=datetime.timedelta(minutes=10),     # Wait 10 minutes between each try
    max_retries=6*5,    # Wait maximum 5 hours
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

    s3api = NoaaGfsS3()
    r = s3api.check_run_availability(date_day, str(run))
    if not r:
        raise signals.FAIL(f"Run {date_day} / {run} is not yet available")
    return r


@prefect.task(
    max_retries=5, retry_delay=datetime.timedelta(minutes=5)
)
def check_timestep_availability(daterun_info: dict, timestep: str):
    """
    Check if a particular timestep for GFS is available

    :param daterun_info:
    :param timestep:
    :return:
    """
    s3api = NoaaGfsS3()
    r = s3api.check_timestep_availability(timestep=timestep, **daterun_info)
    if not r:
        raise signals.FAIL(f"Timestep {timestep} not yet available")
    return r


@prefect.task(
    max_retries=5, retry_delay=datetime.timedelta(minutes=5)
)
def download_timestep(timestep_info: dict, download_dir: str) -> dict:
    """
    Download a specific timestep file

    :param timestep_info:
    :param download_dir:
    :return:
    """
    s3api = NoaaGfsS3()
    r = s3api.download_timestep(download_dir=download_dir, **timestep_info)
    return r


#######################################################


def create_flow_download(
        flow_name: str = "aws-gfs-download",
        run: int = 0,
        timesteps: list = None,
        max_concurrent_download: int = 5,
        download_dir: str = '/tmp/plop',
        post_flowrun: StartFlowRun = None) -> prefect.Flow:
    """
    Create a prefect flow for downloading GFS
    with some configuration option

    :param flow_name:
    :param run:
    :param timesteps:
    :param max_concurrent_download:
    :param download_dir:
    :param post_flowrun:
    :return:
    """
    if not timesteps:
        # Set default
        timesteps = [3, 6]

    with prefect.Flow(name=f"{flow_name}-run{run}") as flow_download:
        """
        A Flow for downloading data from AWS:
            - check if a run is available
            - download according to config
        """
        param_run = prefect.Parameter("run", default=run)
        date_day = prefect.Parameter("date_day", default=None)

        daterun_avail = check_run_availability(run=param_run, date_day=date_day)

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

            if post_flowrun is not None:
                post_flowrun(run_name=f"process_{fp}", parameters=fp, idempotency_key=str(fp))

    # Scheduling on a daily basis, according to the run
    schedule = Schedule(clocks=[CronClock(f"0 {run} * * *")])
    flow_download.schedule = schedule

    # For choosing the right executor,
    # see https://docs.prefect.io/orchestration/flow_config/executors.html#choosing-an-executor
    flow_download.executor = LocalDaskExecutor(
        scheduler="threads",
        num_workers=max_concurrent_download
    )

    # Setup prefect logging to catch current package logs
    prefect.utilities.logging._create_logger("datafetch")

    return flow_download
