from sqlalchemy import Column, Float, ForeignKey, String
from sqlalchemy.dialects import postgresql
from sqlalchemy.types import DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Athlete(Base):
    __tablename__ = 'athletes'

    id = Column(String, primary_key=True)
    meta = Column(postgresql.JSONB)
    activities = relationship('Activity')

    def __repr__(self):
        return f'<Athlete({self.id})'


class Activity(Base):
    __tablename__ = 'activities'

    id = Column(String, primary_key=True)
    athlete = Column(String, ForeignKey('athletes.id'))
    datetime = Column(DateTime)

    meta = Column(postgresql.JSONB)
    metrics = Column(postgresql.JSONB)

    time = Column(postgresql.ARRAY(Float, dimensions=1))
    distance = Column(postgresql.ARRAY(Float, dimensions=1))
    speed = Column(postgresql.ARRAY(Float, dimensions=1))
    power = Column(postgresql.ARRAY(Float, dimensions=1))
    cadence = Column(postgresql.ARRAY(Float, dimensions=1))
    heartrate = Column(postgresql.ARRAY(Float, dimensions=1))
    altitude = Column(postgresql.ARRAY(Float, dimensions=1))
    slope = Column(postgresql.ARRAY(Float, dimensions=1))
    temperature = Column(postgresql.ARRAY(Float, dimensions=1))

    def __repr__(self):
        return f'<Activity({self.id})'
