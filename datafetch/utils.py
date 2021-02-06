from datetime import timedelta, datetime
from pathlib import Path
import logging

import prefect
import pydantic
import peewee


logger = logging.getLogger(__name__)


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
            {'flow(where: {archived: {_eq: false}, name: {_eq: "%s"}})' % flow_name:
                 ['id', 'name']
             }
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


############################


db = peewee.SqliteDatabase(None)


class BaseDbModel(peewee.Model):
    class Meta:
        database = db


class DownloadRecord(BaseDbModel):
    """
    Represent a download record in the database
    """
    key = peewee.CharField(unique=True)
    filepath = peewee.CharField(null=True)
    size = peewee.FloatField(null=True)
    origin_url = peewee.CharField(null=True)
    date_start = peewee.DateTimeField(null=True)
    date_stop = peewee.DateTimeField(null=True)
    status = peewee.CharField(default="empty", choices=[
        ("empty", "empty"),
        ("downloading", "downloading"),
        ("downloaded", "downloaded"),
        ("failed", "failed")
    ])
    nb_try = peewee.IntegerField(default=0)
    error = peewee.CharField(null=True)

    def download_time(self) -> timedelta:
        """
        Elapsed download time
        :return:
        """
        return self.date_stop - self.date_start

    def need_download(self) -> bool:
        """
        Check if we need to download this record or not

        :return:
        """
        if self.status in ("empty", "failed"):
            return True
        else:
            return False

    def set_start(self):
        """
        Set download start

        :return:
        """
        self.date_start = datetime.utcnow()
        self.status = "downloading"

    def set_downloaded(self, fp: Path = None):
        """
        Set current record as downloaded

        :return:
        """
        if fp:
            self.filepath = str(fp.absolute())
            self.size = fp.stat().st_size
        self.status = "downloaded"
        self.date_stop = datetime.now()

    def set_failed(self, error: str = None):
        """
        Set failed status

        :param error:
        :return:
        """
        self.status = "failed"
        self.date_stop = datetime.now()
        if error:
            self.error = error


class DownloadedFileRecorderMixin(pydantic.BaseModel):
    """
    Helper to keep track of downloaded files into a database
    """
    db_name: str = None
    db_dir: str = "/tmp/"

    @property
    def db_path(self) -> Path:
        """
        Path of sqlite file

        :return:
        """
        if self.db_name is None:
            self.db_name = self.__class__.__name__
        return Path(self.db_dir) / f"{self.db_name}.db"

    def __enter__(self):
        logger.debug(f"Initializing database {self.db_path} ...")
        print(f"Initializing database {self.db_path} ...")
        db.init(database=self.db_path)
        db.connect()
        db.create_tables([DownloadRecord])

    def __exit__(self, exc_type, exc_val, exc_tb):
        db.close()

    def db_get_record(self, key: str) -> DownloadRecord:
        """
        Get a DownDb record from key

        :param key:
        :return:
        """
        return DownloadRecord.get_or_create(key=key)
