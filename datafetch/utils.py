"""
Various utils for datafetch
"""
import sys
import logging
import argparse
from abc import ABC
from pathlib import Path
from typing import Union, List

import prefect
import pydantic

logger = logging.getLogger(__name__)


class AbstractFetcher(pydantic.BaseModel, ABC):
    """
    Abstract class for fetcher
    """
    def fetch(self, **kwargs) -> Union[Path, None]:
        """
        Fetch a single file, with some possible pre and post actions

        When overrided in subclasses, this is the place to add some capabilities around download, eg.:
            - donwload file and rename with temporary extension
            - record download in a database

        :param kwargs:
        :return:
        """
        return self._fetch(**kwargs)

    def _fetch(self, destination_fp: str = None, **kwargs) -> Union[Path, None]:
        """
        Actually fetch a single file to `destination_fp`

        :param kwargs:
        :return:
        """
        raise NotImplementedError


class FetchWithTemporaryExtensionMixin(AbstractFetcher, pydantic.BaseModel, ABC):
    """
    Allow to run a download function by specifying a temporary extension while
    downloading the file
    """
    temporary_extension: str = "tmp"

    def fetch(self, destination_dir: str, destination_filename: str,
              **kwargs) -> Union[Path, None]:
        """
        Fetch a file with a temporary local extension

        :param destination_dir:
        :param destination_filename:
        :param kwargs:
        :return:
        """
        fp = Path(destination_dir) / destination_filename
        if not fp.parent.exists():
            fp.parent.mkdir(parents=True, exist_ok=True)

        fp_tmp = fp
        if self.temporary_extension:
            fp_tmp = fp.parent / f"{fp.name}.{self.temporary_extension}"
            logger.debug(f"Using temporary filename {fp_tmp} ...")

        fp_downloaded = super().fetch(destination_fp=str(fp_tmp), **kwargs)

        if fp_downloaded is None or (isinstance(fp_downloaded, Path) and not fp_downloaded.exists()):
            # Something went wrong
            fp = fp_downloaded
        else:
            if self.temporary_extension:
                logger.debug(f"Renaming {fp_downloaded} to {fp} ...")
                fp_downloaded.rename(fp)

        return fp


#######################################################
# Prefect related

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
