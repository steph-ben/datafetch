from datetime import timedelta, datetime

from datafetch.s3.flows import create_flow_download


yesterday = datetime.today() - timedelta(days=1)
date_day = yesterday.strftime("%Y%m%d")


def test_flow():
    flow_download = create_flow_download()
    flow_download.schedule = None
    flow_run = flow_download.run(date_day=date_day)
    print(type(flow_run))
