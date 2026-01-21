import os

from opendata.conf import settings
from opendata.db import main, models
from opendata.models import Activity, LocalAthlete
import pytest
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import IntegrityError


class TestOpenDataDB:

    def test_create_tables(self, test_db_empty):
        engine = create_engine(
            f'postgres://{settings.db_user}:{settings.db_password}@{settings.db_host}:{settings.db_port}/{test_db_empty}')

        metadata = MetaData()
        metadata.reflect(bind=engine)
        tables = metadata.tables
        assert 'athletes' not in tables
        assert 'activities' not in tables

        opendatadb = main.OpenDataDB(database=test_db_empty)
        opendatadb.create_tables()

        metadata = MetaData()
        metadata.reflect(bind=engine)
        tables = metadata.tables
        assert 'athletes' in tables
        assert 'activities' in tables

    def test_create_athlete(self, test_db, db_session):
        local_athlete = LocalAthlete('some-athlete-id-1')
        opendatadb = main.OpenDataDB(database=test_db)
        opendatadb.insert_athlete(local_athlete)

        assert db_session.query(models.Athlete).count() == 1
        athlete = db_session.query(models.Athlete).one()
        assert athlete.id == local_athlete.id
        assert athlete.meta == local_athlete.metadata

    @pytest.fixture
    def activity(self, local_storage):
        return Activity(
            id='1970_01_01_00_00_00.csv',
            filepath_or_buffer=\
                os.path.join(local_storage.strpath,
                             settings.data_prefix,
                             'some-athlete-id-1',
                             '1970_01_01_00_00_00.csv'),
            metadata=dict(foo='bar'),
        )

    def test_create_activity(self, test_db, db_session, activity):
        opendatadb = main.OpenDataDB(database=test_db)

        opendatadb.insert_activity(activity)

        assert db_session.query(models.Activity).count() == 1
        db_activity = db_session.query(models.Activity).one()
        assert db_activity.id == activity.id
        assert db_activity.athlete is None
        assert db_activity.meta == activity.metadata

    def test_create_activity_with_athlete(self, test_db, db_session, activity):
        athlete = LocalAthlete('some-athlete-id-1')
        opendatadb = main.OpenDataDB(database=test_db)
        opendatadb.insert_athlete(athlete)

        opendatadb.insert_activity(activity, athlete)

        db_activity = db_session.query(models.Activity).one()
        assert db_activity.athlete == athlete.id

    def test_create_activity_non_existent_athlete(self, test_db, db_session, activity):
        athlete = LocalAthlete('some-athlete-id-1')
        opendatadb = main.OpenDataDB(database=test_db)

        with pytest.raises(IntegrityError):
            opendatadb.insert_activity(activity, athlete)
