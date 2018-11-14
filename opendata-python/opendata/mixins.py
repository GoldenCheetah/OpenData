from io import BytesIO
import glob
import os

import boto3
from botocore.handlers import disable_signing

from .conf import settings


class RemoteMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        s3 = boto3.resource('s3')
        s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
        self.bucket = s3.Bucket(settings.bucket_name)

    def download_object_as_bytes(self, key):
        obj = self.bucket.Object(key)
        return BytesIO(obj.get()['Body'].read())

    def _list_objects(self, prefix):
        for obj in self.bucket.objects.filter(Prefix=f'{prefix}/'):
            if obj.key == f'{prefix}/':
                # For some reason the prefix itself is also returned as an object
                continue
            else:
                yield obj

    def _get_object(self, prefix, key):
        return self.bucket.Object(f'{prefix}/{key}')

    def _get_or_list_objects(self, prefix, key):
        if key is None:
            return self._list_objects(prefix)
        else:
            return self._get_object(prefix, key)

    def remote_data(self, key=None):
        return self._get_or_list_objects(settings.data_prefix, key)

    def remote_metadata(self, key=None):
        return self._get_or_list_objects(settings.metadata_prefix, key)

    def remote_datasets(self, key=None):
        raise NotImplementedError

    def object_key_to_athlete_id(self, key):
        return key[5:-4]


class LocalStorageMixin:
    def _list_directories(self, prefix):
        for directory in glob.glob(os.path.join(settings.local_storage, prefix, '*', '')):
            yield directory

    def _get_directory(self, prefix, dirname):
        return os.path.join(settings.local_storage, prefix, dirname)

    def _get_or_list_directories(self, prefix, dirname):
        if dirname is None:
            return self._list_directories(prefix)
        else:
            return self._get_directory(prefix, dirname)

    def _list_files(self, prefix):
        for filename in os.listdir(os.path.join(settings.local_storage, prefix)):
            yield filename

    def _get_file(self, prefix, filename):
        return os.path.join(settings.local_storage, prefix, filename)

    def _get_or_list_files(self, prefix, filename):
        if filename is None:
            return self._list_files(prefix)
        else:
            return self._get_file(prefix, filename)

    def local_data(self, dirname=None):
        return self._get_or_list_directories(settings.data_prefix, dirname)

    def local_metadata(self, filename=None):
        return self._get_or_list_files(settings.metadata_prefix, filename)

    def local_datasets(self, filename=None):
        raise NotImplementedError
