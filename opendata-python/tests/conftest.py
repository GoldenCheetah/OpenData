import json
import os

from opendata.conf import settings
import pytest


def dummy_metadata(dir_name):
    metadata = {
        'ATHLETE': 'some metadata',
        'RIDES': [],
    }
    for activity in range(3):
        p = dir_name.join(f'1970_01_01_00_00_{activity:02}.csv')
        p.write('secs,power,heartrate\n1,200,100\n2,201,101')
        metadata['RIDES'].append(
            dict(
                date=f'1970/01/01 00:00:{activity:02} UTC'
            )
        )
    return metadata


@pytest.fixture
def local_storage(tmpdir):
    data_dir = tmpdir.mkdir(settings.data_prefix)
    metadata_dir = tmpdir.mkdir(settings.metadata_prefix)
    datasets_dir = tmpdir.mkdir(settings.datasets_prefix)
    for athlete in range(2):
        athlete_id = f'some-athlete-id-{athlete}'
        d = data_dir.mkdir(athlete_id)
        metadata = dummy_metadata(d)
        for d in [data_dir, metadata_dir]:
            p = d.join(f'{{{athlete_id}}}.json')
            p.write(json.dumps(metadata))

    settings.local_storage = tmpdir.strpath

    return tmpdir


def pytest_collection_modifyitems(items):
    # All tests requiring S3 are disabled by default.
    # Enable by setting environment variable TEST_AWS_S3 to true
    if os.environ.get('TEST_AWS_S3', 'false') == 'true':
        return

    for item in items:
        if item.get_marker('aws_s3'):
            item.add_marker(pytest.mark.skip)
