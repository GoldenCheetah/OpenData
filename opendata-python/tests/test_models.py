import copy
import os
from types import GeneratorType

from opendata import models
from opendata.conf import settings
import pytest


class TestActivity:
    def test_init(self, local_storage):
        activity = models.Activity(
            id='1970_01_01_00_00_00.csv',
            filepath_or_buffer=\
                os.path.join(local_storage.strpath,
                             settings.data_prefix,
                             'some-athlete-id-1',
                             '1970_01_01_00_00_00.csv'),
            metadata={})
        assert activity.id == '1970_01_01_00_00_00.csv'
        assert activity.filepath_or_buffer == \
            os.path.join(local_storage.strpath,
                         settings.data_prefix,
                         'some-athlete-id-1',
                         '1970_01_01_00_00_00.csv')
        assert 'secs' in activity.data
        assert 'power' in activity.data
        assert 'heartrate' in activity.data
        assert activity.data.power[0] == 200
        assert activity.metadata == {}

    def test_init__not_stored_locally(self, local_storage):
        activity = models.Activity(
            id='not_stored_activity.csv',
            filepath_or_buffer=\
                os.path.join(local_storage.strpath,
                             settings.data_prefix,
                             'some-athlete-id-1',
                             'not_stored_activity.csv'),
            metadata={},
        )
        assert activity.metadata == {}
        assert not activity.has_data()
        with pytest.raises(FileNotFoundError):
            activity.data


class TestBaseAthlete:
    def test___init__(self):
        base_athlete = models.BaseAthlete('some-athlete-id-1')

        assert base_athlete.id == 'some-athlete-id-1'

    def test___eq__(self):
        athlete_a = models.BaseAthlete('id a')
        athlete_b = models.BaseAthlete('id b')
        athlete_c = models.BaseAthlete('id a')
        athlete_d = models.RemoteAthlete('id d')
        athlete_e = models.RemoteAthlete('id e')
        athlete_f = models.LocalAthlete('id d')

        assert athlete_a != athlete_b
        assert athlete_a == athlete_c
        assert athlete_d != athlete_e
        assert athlete_d == athlete_f

    def test_transform_metadata(self):
        base_athlete = models.BaseAthlete('some-athlete-id-1')
        metadata = {
            'RIDES': [
                {
                    'date': 'blabla'
                }
            ]
        }
        response = base_athlete.transform_metadata(copy.deepcopy(metadata))

        assert isinstance(response['RIDES'], dict)
        assert 'blabla' in response['RIDES']


class TestLocalAthlete:
    @pytest.fixture
    def local_athlete(self):
        return models.LocalAthlete('some-athlete-id-1')

    def test_get_activity(self, local_athlete):
        activity = local_athlete.get_activity('1970_01_01_00_00_00.csv')
        assert isinstance(activity, models.Activity)

    def test_get_missing_activity(self, local_athlete):
        activity = local_athlete.get_activity('1900_01_01_00_00_00.csv')
        assert not activity.has_data()
        assert activity.metadata is not None
        assert 'date' in activity.metadata
        with pytest.raises(FileNotFoundError):
            activity.data

    def test_activities(self, local_athlete):
        activities = local_athlete.activities()

        assert isinstance(activities, GeneratorType)

        activities = list(activities)

        assert len(activities) == 3

    def test_metadata(self, local_athlete):
        assert not hasattr(local_athlete, '_lazy_metadata')
        assert local_athlete.metadata['ATHLETE'] == 'some metadata'
        assert hasattr(local_athlete, '_lazy_metadata')

    @pytest.mark.aws_s3
    def test_download_remote_data(self):
        # TOTO: to be implemented
        # local_athlete.download_remote_data()
        pass


@pytest.mark.aws_s3
class TestRemoteAthlete:
    @pytest.fixture
    def remote_athlete(self):
        return models.RemoteAthlete('009daa84-3253-4967-93fe-ebdb0fa93339')

    def test_get_activity(self, remote_athlete):
        activity = remote_athlete.get_activity('2016_01_05_17_49_23.csv')
        assert isinstance(activity, models.Activity)

    def test_activities(self, remote_athlete):
        activities = remote_athlete.activities()

        assert isinstance(activities, GeneratorType)

        activities = list(activities)

        assert len(activities) == 257

    def test_metadata(self, remote_athlete):
        assert not hasattr(remote_athlete, '_lazy_metadata')
        assert remote_athlete.metadata['ATHLETE']['id'] == f'{{{remote_athlete.id}}}'
        assert hasattr(remote_athlete, '_lazy_metadata')

    def test_store_locally(self, remote_athlete, local_storage):
        remote_athlete.store_locally()
        assert remote_athlete.id in \
            os.listdir(os.path.join(local_storage.strpath, settings.data_prefix))
        assert f'{{{remote_athlete.id}}}.json' in \
            os.listdir(os.path.join(local_storage.strpath, settings.metadata_prefix))

    def test_store_locally_wo_data(self, remote_athlete, local_storage):
        remote_athlete.store_locally(data=False)
        assert remote_athlete.id not in \
            os.listdir(os.path.join(local_storage.strpath, settings.data_prefix))
        assert f'{{{remote_athlete.id}}}.json' in \
            os.listdir(os.path.join(local_storage.strpath, settings.metadata_prefix))
