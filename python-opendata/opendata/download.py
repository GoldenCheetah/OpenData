from collections import namedtuple
import glob
from os import listdir, path, remove
from shutil import rmtree
import zipfile

from osfclient import api, cli
import pandas as pd


PROJECT_ID = '6hfpz'


Args = namedtuple(
    'Args',
    ['project', 'remote', 'local', 'username', 'password', 'force', 'output'],
)


class OpenDataClient:
    def __init__(self, project_id=PROJECT_ID):
        self.args = Args(
            project=project_id,
            remote=None,
            local=None,
            username=None,
            password=None,
            force=False,
            output=None,
        )
        self.osf = api.OSF()
        self.project = self.osf.project(project_id)
        self.store = next(self.project.storages)

    def download_athlete(self, athlete_id):
        for f in self.store.files:
            if f.name.rstrip('.zip') == athlete_id:
                break
        file_path = path.join('.opendata_storage/', f.name)
        args = self.args._replace(
            remote=f.path,
            local=file_path,
        )
        cli.fetch(args)

        self._unzip_and_delete_file(file_path)

    def list_athletes(self):
        """
        Assumes that each file belongs to one athlete.
        """
        return [f.name.rstrip('.zip') for f in self.store.files]

    def _unzip_file(self, filename):
        with zipfile.ZipFile(filename, "r") as zip_ref:
            zip_ref.extractall(
                path.join(
                    '.opendata_storage',
                    path.basename(filename).rstrip('.zip')
                )
            )

    def _unzip_and_delete_file(self, filename):
        self._unzip_file(filename)
        remove(filename)


def download(athletes=None, limit=None):
    client = OpenDataClient()
    if athletes:
        for athlete in athletes:
            client.download_athlete(athlete)
    else:
        athletes = client.list_athletes()
        if limit:
            athletes = athletes[:limit]

        for athlete in athletes:
            client.download_athlete(athlete)


def list_all_athletes():
    return OpenDataClient().list_athletes()
