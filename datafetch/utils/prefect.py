import argparse
import sys
from pathlib import Path
from typing import List

import prefect


def prefect_cli_helper_parser():
    """
    Argument Parser for prefect cli helper
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", help="Project project name", default="<project_name>")
    parser.add_argument("--label", help="Labels", default="<label>")
    parser.add_argument("--run-locally", help="Run the flow on the current machine, without any server", default=False)
    args = parser.parse_args()

    return args


def show_prefect_cli_helper(flow_list: List[prefect.Flow], args = None):
    """
    Show a reminder of prefect cli commands to register and trigger flows from current file

    :return:
    """
    if args is None:
        args = prefect_cli_helper_parser()

    if args.run_locally:
        for flow in flow_list:
            flow.schedule = None
            flow.run()

    print()
    print("Test your flow locally with:")
    print(f"    $ {' '.join(sys.argv)} --run-locally")

    print()
    print("Create your project with:")
    print(f"    $ prefect project create --name {args.project}")

    print()
    print("Register your flow(s) with:")
    for flow in flow_list:
        print(f"    $ prefect register flow --file {Path(sys.argv[0])} --name {flow.name} --project {args.project} --label {args.label}")

    print()
    print("Trigger your flow(s) with:")
    for flow in flow_list:
        print(f"    $ prefect run flow --name {flow.name} --project {args.project}")


def get_prefect_flow_id(flow_name: str):
    """
    Get internal prefect flow id from it's name

    If several version of the flow exists, get the most recent

    :param flow_name: str
    :return: str
    """
    client = prefect.Client()
    query = client.graphql({
        'query':
            {'flow(where: {archived: {_eq: false}, name: {_eq: "%s"}})' % flow_name: ['id', 'name']}
    })
    print(query)
    flow_id = query['data']['flow'][0]['id']

    return flow_id


def trigger_prefect_flow(flow_name: str, **kwargs):
    """
    Trigger a prefect flow, with some parameters

    :param flow_name:
    :param kwargs:
    :return:
    """
    flow_id = get_prefect_flow_id(flow_name)

    client = prefect.Client()
    r = client.create_flow_run(flow_id=flow_id, **kwargs)

    server_url = "{}:{}".format(
        prefect.context.config.server.host,
        prefect.context.config.server.port
    )

    print(f"A new FlowRun has been triggered")
    print(f"Go check it on http://{server_url}/flow-run/{r}")
