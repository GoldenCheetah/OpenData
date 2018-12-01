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
    def __init__(self, id, filepath_or_buffer):
        self.id = id
        self.filepath_or_buffer = filepath_or_buffer
        self.metadata = None

    @utils.lazy_load
    def data(self):
        return pd.read_csv(self.filepath_or_buffer)


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
    def get_activity(self, activity_id):
        filepath = os.path.join(settings.local_storage, settings.data_prefix, self.id, activity_id)
        activity = Activity(activity_id, filepath)

        date_string = utils.match_filename_to_date_strings(
            filename=activity_id,
            date_strings=self.metadata['RIDES'].keys()
        )
        activity.metadata = self.metadata['RIDES'][date_string]

        return activity

    def activities_generator(self):
        for filepath in glob.glob(os.path.join(settings.local_storage, settings.data_prefix, self.id, '*.csv')):  # noqa: E501
            filename = os.path.split(filepath)[-1]
            yield self.get_activity(filename)

    def activities(self):
        if not os.path.isdir(os.path.join(settings.local_storage, settings.data_prefix, self.id)):
            raise FileNotFoundError((
                f'\'{self.id}\' does not exist. Consider running '
                f'\'Athlete.load(allow_remote=True)\' to ensure all data is downloaded.'
            ))

        return self.activities_generator()

    @utils.lazy_load
    def metadata(self):
        with open(os.path.join(settings.local_storage, settings.metadata_prefix, f'{{{self.id}}}.json')) as f:  # noqa: E501
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
        activity = Activity(activity_id, self.data_zip.open(activity_id))
        date_string = utils.match_filename_to_date_strings(
            filename=activity_id,
            date_strings=self.metadata['RIDES'].keys()
        )
        activity.metadata = self.metadata['RIDES'][date_string]
        return activity

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

    def store_locally(self):
        self.data_zip.extractall(path=os.path.join(settings.local_storage, settings.data_prefix, self.id))  # noqa: E501
        self.metadata_zip.extractall(path=os.path.join(settings.local_storage, settings.metadata_prefix))  # noqa: E501
