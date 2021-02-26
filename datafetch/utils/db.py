from datetime import timedelta, datetime
from pathlib import Path

import peewee

# FIXME: don't use a global variable ?
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
    date_queued = peewee.DateTimeField(null=True)
    date_queued_and_ready = peewee.DateTimeField(null=True)
    queue_id = peewee.CharField(null=True)
    date_start = peewee.DateTimeField(null=True)
    date_stop = peewee.DateTimeField(null=True)
    status = peewee.CharField(default="empty", choices=[
        ("empty", "empty"),
        ("queued", "queued"),
        ("queued_and_ready", "queued_and_ready"),
        ("downloading", "downloading"),
        ("downloaded", "downloaded"),
        ("failed", "failed")
    ])
    nb_try = peewee.IntegerField(default=0)
    error = peewee.CharField(null=True)

    def __str__(self):
        r = f"<{self.key[:20]}> // {self.status}"
        if self.queue_id:
            r += f" // id {self.queue_id} on {self.date_queued} "
        if self.filepath:
            r += f" // {self.filepath}"
        return r

    def download_time(self) -> timedelta:
        """
        Elapsed download time
        :return:
        """
        return self.date_stop - self.date_start

    def need_queue(self) -> bool:
        """
        Check if we need to queue this record or not

        :return:
        """
        if self.status in ("empty",):
            return True
        else:
            return False

    def need_download(self) -> bool:
        """
        Check if we need to download this record or not

        :return:
        """
        if self.status in ("empty", "failed", "queued_and_ready"):
            return True
        else:
            return False

    def set_queued(self, queue_id: str):
        """
        Set status as queued

        :param queue_id:
        :return:
        """
        self.date_queued = datetime.utcnow()
        self.status = "queued"
        self.queue_id = queue_id

    def set_queued_and_ready(self):
        """
        Queuing is over, download is ready

        :return:
        """
        self.date_queued_and_ready = datetime.utcnow()
        self.status = "queued_and_ready"

    def check_queued_and_ready(self):
        """
        Check if a queued result is available for download
        :return:
        """
        if self.status == "queued_and_ready":
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
