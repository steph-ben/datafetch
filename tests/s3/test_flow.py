from datafetch.s3.flows import create_flow_download


def test_flow():
    flow_download = create_flow_download()
    flow_download.schedule = None
    flow_run = flow_download.run()
    print(type(flow_run))
