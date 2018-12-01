import os

from opendata import mixins
from opendata.conf import settings
import pytest


@pytest.mark.aws_s3
class TestRemoteMixin:
    def test___init__(self):
        instance = mixins.RemoteMixin()
        assert hasattr(instance, 'bucket')

    def test_remote_data(self):
        instance = mixins.RemoteMixin()
        objects = list(instance.remote_data())
        assert len(objects) > 1200
        assert hasattr(objects[0], 'key')
        assert objects[0].key.startswith('data/')

    def test_remote_data_key(self):
        instance = mixins.RemoteMixin()
        obj = instance.remote_data('009daa84-3253-4967-93fe-ebdb0fa93339.zip')
        assert obj.key == 'data/009daa84-3253-4967-93fe-ebdb0fa93339.zip'

    def test_remote_metadata(self):
        instance = mixins.RemoteMixin()
        objects = list(instance.remote_metadata())
        assert len(objects) > 1200
        assert hasattr(objects[0], 'key')
        assert objects[0].key.startswith('metadata/')

    def test_remote_metadata_key(self):
        instance = mixins.RemoteMixin()
        obj = instance.remote_metadata('{009daa84-3253-4967-93fe-ebdb0fa93339}.json')
        assert obj.key == 'metadata/{009daa84-3253-4967-93fe-ebdb0fa93339}.json'

    def test_remote_datasets(self):
        instance = mixins.RemoteMixin()
        with pytest.raises(NotImplementedError):
            instance.remote_datasets()

    def test_remote_datasets_key(self):
        instance = mixins.RemoteMixin()
        with pytest.raises(NotImplementedError):
            instance.remote_datasets('some key')


class TestLocalStorageMixin:
    def test_local_data(self, local_storage):
        instance = mixins.LocalStorageMixin()
        data = list(instance.local_data())
        assert len(data) == 2

    def test_local_data_dirname(self, local_storage):
        instance = mixins.LocalStorageMixin()
        data = instance.local_data('1970_01_01_00_00_00.csv')
        assert data == os.path.join(local_storage.strpath, settings.data_prefix, '1970_01_01_00_00_00.csv')

    def test_local_metadata(self, local_storage):
        instance = mixins.LocalStorageMixin()
        data = list(instance.local_metadata())
        assert len(data) == 2

    def test_local_metadata_filename(self, local_storage):
        instance = mixins.LocalStorageMixin()
        data = instance.local_metadata('some-athlete-id-1')
        assert data == os.path.join(local_storage.strpath, settings.metadata_prefix, 'some-athlete-id-1')

    def test_local_datasets(self, local_storage):
        instance = mixins.LocalStorageMixin()
        with pytest.raises(NotImplementedError):
            instance.local_datasets()

    def test_local_datasets_filename(self, local_storage):
        instance = mixins.LocalStorageMixin()
        with pytest.raises(NotImplementedError):
            instance.local_datasets('some filename')
