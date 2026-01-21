from datetime import datetime

import pytest
from sqlalchemy.exc import IntegrityError

from opendata.db import models


class TestAthlete:
    def test_insert_athlete(self, db_session):
        db_session.add(models.Athlete(
            id='some athlete id',
            meta=dict(foo='bar'),
        ))
        db_session.commit()

        assert db_session.query(models.Athlete).count() == 1
        athlete = db_session.query(models.Athlete).one()
        assert athlete.id == 'some athlete id'
        assert athlete.meta == dict(foo='bar')

    def test_activities_relationship(self, db_session):
        athlete = models.Athlete(
            id='some athlete id',
            meta=dict(foo='bar'),
        )

        db_session.add(athlete)
        db_session.commit()

        for i in range(2):
            db_session.add(models.Activity(
                id=f'some activity id {i}',
                athlete=athlete.id,
            ))
        db_session.commit()

        athlete = db_session.query(models.Athlete).filter(models.Athlete.id == 'some athlete id').one()
        assert len(athlete.activities) == 2
        assert athlete.activities[0].id.startswith('some activity id ')


class TestActivity:
    @pytest.fixture(scope='function')
    def db_athlete(self, db_session):
        athlete = models.Athlete(
            id='some athlete id',
            meta=dict(foo='bar'),
        )

        db_session.add(athlete)
        db_session.commit()
        return athlete

    def test_insert_activity(self, db_session, db_athlete):
        db_session.add(models.Activity(
            id='some activity id',
            athlete=db_athlete.id,
            datetime=datetime.now(),
            meta=dict(foo='bar'),
            metrics=dict(bar='foo'),
            time=[1, 2, 3],
            distance=[1, 2, 3],
            speed=[1, 2, 3],
            power=[1, 2, 3],
            cadence=[1, 2, 3],
            heartrate=[1, 2, 3],
            altitude=[1, 2, 3],
            slope=[1, 2, 3],
            temperature=[1, 2, 3],
        ))
        db_session.commit()

        assert db_session.query(models.Activity).count() == 1
        activity = db_session.query(models.Activity).one()
        assert activity.id == 'some activity id'
        assert activity.athlete == db_athlete.id
        assert activity.meta == dict(foo='bar')
        assert activity.power == [1, 2, 3]
