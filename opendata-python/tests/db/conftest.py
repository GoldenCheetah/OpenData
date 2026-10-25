from uuid import uuid4

import pytest
from opendata.conf import settings
from opendata.db import models, main
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope='function')
def test_db_empty():
    engine = create_engine(
        f'postgres://{settings.db_user}:{settings.db_password}@{settings.db_host}:{settings.db_port}')
    conn = engine.connect()
    conn.execute('commit')

    db_name = f'opendata_test_{uuid4().hex}'
    conn.execute(f'create database {db_name}')
    conn.execute('commit')

    yield db_name

    engine.dispose()
    conn.close()


@pytest.fixture(scope='function')
def test_db(test_db_empty):
    opendatadb = main.OpenDataDB(database=test_db_empty)
    opendatadb.create_tables()
    return test_db_empty


@pytest.fixture(scope='function')
def db_engine(test_db):
    engine = create_engine(
        f'postgres://{settings.db_user}:{settings.db_password}@{settings.db_host}:{settings.db_port}/{test_db}'
    )

    yield engine

    engine.dispose()



@pytest.fixture(scope='function')
def db_conn(db_engine):
    conn = engine.connect()
    conn.execute('commit')

    yield conn

    conn.close()


@pytest.fixture(scope='function')
def db_session(db_engine):
    Session = sessionmaker()
    session = Session(bind=db_engine)

    yield session

    session.close()
