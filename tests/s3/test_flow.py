from datetime import timedelta, datetime
from prefect.tasks.prefect import StartFlowRun
from datafetch.s3.flows import create_flow_download


yesterday = datetime.today() - timedelta(days=1)
date_day = yesterday.strftime("%Y%m%d")


def test_flow():
    flow_download = create_flow_download()
    flow_download.schedule = None
    flow_run = flow_download.run(date_day=date_day)
    print(type(flow_run))


def test_flow_post_process():
    post = StartFlowRun(flow_name="gfs-post-processing", project_name="laptop-gfs-project")
    flow_download = create_flow_download(
        post_flowrun=post
    )
    flow_download.schedule = None
    flow_run = flow_download.run(date_day=date_day)
    print(type(flow_run))