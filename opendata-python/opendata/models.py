import copy
import glob
from io import BytesIO
import json
import os
from zipfile import ZipFile

import boto3
from botocore.handlers import disable_signing
import pandas as pd

from . import utils
from .conf import settings


class Activity:
    def __init__(self, id, filepath_or_buffer, metadata):
        self.id = id
        self.filepath_or_buffer = filepath_or_buffer
        self.metadata = None

    def has_data(self):
        if isinstance(self.filepath_or_buffer, str) \
                and not os.path.exists(self.filepath_or_buffer):
            return False
        else:
            return True

    @utils.lazy_load
    def data(self):
        try:
            data = pd.read_csv(self.filepath_or_buffer)
        except FileNotFoundError:
            raise FileNotFoundError((
                f'Data for activity with id={self.id} was not found in local storage. '
                f'It seems that only metadata is downloaded. '
                f'Run \'RemoteAthlete({{athlete_id}}).store_locally()\' to download data '
                f'for this athlete.'
            ))

        return data


class BaseAthlete:
    def __init__(self, athlete_id):
        self.id = athlete_id

    def __hash__(self):
        return hash((self.id))

    def __eq__(self, other):
        if issubclass(other.__class__, BaseAthlete):
            return other.id == self.id
        else:
            return False

    def transform_metadata(self, metadata):
        rides_metadata = copy.deepcopy(metadata['RIDES'])
        metadata['RIDES'] = {}

        for ride in rides_metadata:
            metadata['RIDES'][ride['date']] = ride

        return metadata


class LocalAthlete(BaseAthlete):
    def has_data(self):
        return os.path.isdir(os.path.join(settings.local_storage, settings.data_prefix, self.id))

    def get_activity(self, activity_id):
        data_filepath = os.path.join(
            settings.local_storage,
            settings.data_prefix,
            self.id,
            activity_id)

        date_string = utils.match_filename_to_date_strings(
            filename=activity_id,
            date_strings=self.metadata['RIDES'].keys()
        )
        metadata = self.metadata['RIDES'][date_string]
        return Activity(activity_id, data_filepath, metadata)

    def activities_generator(self):
        if self.has_data():
            for filepath in glob.glob(os.path.join(settings.local_storage, settings.data_prefix,
                                                   self.id, '*.csv')):
                filename = os.path.split(filepath)[-1]
                yield self.get_activity(filename)
        else:
            for ride in self.metadata['RIDES'].keys():
                yield self.get_activity(utils.date_string_to_filename(ride))

    def activities(self):
        return self.activities_generator()

    def download_remote_data(self):
        a = RemoteAthlete(self.id)
        a.store_locally()

    @utils.lazy_load
    def metadata(self):
        with open(os.path.join(settings.local_storage, settings.metadata_prefix,
                               f'{{{self.id}}}.json')) as f:
            metadata = json.load(f)
        return self.transform_metadata(metadata)


class RemoteAthlete(BaseAthlete):
    def download_object_as_bytes(self, key):
        s3 = boto3.resource('s3')
        s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
        bucket = s3.Bucket(settings.bucket_name)
        obj = bucket.Object(key)
        return BytesIO(obj.get()['Body'].read())

    @property
    def data_key(self):
        return f'{settings.data_prefix}/{self.id}.zip'

    @property
    def metadata_key(self):
        return f'{settings.metadata_prefix}/{{{self.id}}}.json.zip'

    @utils.lazy_load
    def data_zip(self):
        return ZipFile(self.download_object_as_bytes(self.data_key))

    @utils.lazy_load
    def metadata_zip(self):
        return ZipFile(self.download_object_as_bytes(self.metadata_key))

    def get_activity(self, activity_id):
        date_string = utils.match_filename_to_date_strings(
            filename=activity_id,
            date_strings=self.metadata['RIDES'].keys()
        )
        metadata = self.metadata['RIDES'][date_string]

        return Activity(activity_id, self.data_zip.open(activity_id), metadata)

    def activities_generator(self):
        for i in self.data_zip.filelist:
            if i.filename.endswith('.json'):
                continue
            else:
                yield self.get_activity(i.filename)

    def activities(self):
        for i in self.data_zip.filelist:
            if i.filename.endswith('.json'):
                # For backwards compatibility. The metadata used to be in the zip.
                continue

        return self.activities_generator()

    @utils.lazy_load
    def metadata(self):
        for i in self.metadata_zip.filelist:
            if i.filename.endswith('.json'):
                with self.metadata_zip.open(i.filename) as f:
                    metadata = json.load(f)
            break
        return self.transform_metadata(metadata)

    def store_locally(self, data=True):
        """
        Parameters
        ----------
        data: bool (default=True)
            When True saves the data alongside the metadata, otherwise saves
            only the metadata
        """
        if data:
            self.data_zip.extractall(path=os.path.join(settings.local_storage,
                                                       settings.data_prefix,
                                                       self.id))  # noqa: E501
        self.metadata_zip.extractall(path=os.path.join(settings.local_storage, settings.metadata_prefix))  # noqa: E501
