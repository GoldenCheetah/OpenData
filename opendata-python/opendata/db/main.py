from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from opendata.conf import settings
from opendata.utils import filename_to_datetime
from . import models
from .constants import csv_to_db_mapping


class OpenDataDB:
    def __init__(self, host=settings.db_host, port=settings.db_port,
                 user=settings.db_user, password=settings.db_password,
                 database=settings.db_name):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.Session = sessionmaker()

    def get_engine(self):
        return create_engine(
            f'postgres://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        )

    @contextmanager
    def engine(self):
        engine = self.get_engine()
        yield engine
        engine.dispose()

    def get_session(self):
        return self.Session(bind=self.get_engine())

    @contextmanager
    def session(self):
        session = self.get_session()
        yield session
        session.close()

    def create_tables(self):
        with self.session() as session, self.engine() as engine:
            models.Base.metadata.create_all(engine)
            session.commit()

    def insert_athlete(self, athlete):
        with self.session() as session:
            session.add(models.Athlete(
                id=athlete.id,
                meta=athlete.metadata
            ))
            session.commit()

    def insert_activity(self, activity, athlete=None):
        with self.session() as session:
            if activity.metadata is not None \
                    and 'METRICS' in activity.metadata:
                metrics = activity.metadata.pop('METRICS')
            else:
                metrics = None

            db_activity = models.Activity(
                id=activity.id,
                datetime=filename_to_datetime(activity.id),
                meta=activity.metadata,
                metrics=metrics,
            )

            if athlete is not None:
                db_activity.athlete = athlete.id

            for column in csv_to_db_mapping.keys():
                if column in activity.data:
                    setattr(
                        db_activity,
                        csv_to_db_mapping[column],
                        activity.data[column].values.tolist()
                    )

            session.add(db_activity)
            session.commit()
