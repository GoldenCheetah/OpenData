import json
import glob
from os import listdir, path

import pandas as pd
from pandas.io.json import json_normalize

from .constants import QUANTITY_TAGS
from .files import downloaded_athletes
from .utils import datestring_to_activity_id


def _preprocess_rides(athlete_metadata):
    """
    pandas.io.json.json_normalize doesn't like to work with deeply nested 
    data so the data in METRICS needs to be put 1 level higher. 
    Also removing XDATA for now.
    """
    for ride in athlete_metadata['RIDES']:
        for key, value in ride['METRICS'].items():
            if not isinstance(value, list):
                ride[key] = value
        ride.pop('METRICS', None)
        ride.pop('XDATA', None)

    return athlete_metadata


def _preprocess_athlete(athlete_metadata):
    """
    pandas.io.json.json_normalize doesn't like to work with deeply nested 
    data so in order to use the data in ATHLETE as metadata, these data 
    need to be put 1 level higher. 
    """
    for key, value in athlete_metadata['ATHLETE'].items():
        if key == 'id':
            value = value[1:-1]
        athlete_metadata[key] = value
    athlete_metadata.pop('ATHLETE')

    return athlete_metadata


def _activity_ids(metadata):
    return metadata.date.apply(lambda x: datestring_to_activity_id(x))


def _data_checks(metadata):
    return {q: metadata.data.map(lambda x: t in x) for q, t in QUANTITY_TAGS.items()}


def generate_metadata():
    athlete_ids = downloaded_athletes()
    metadata = pd.DataFrame()

    for athlete_id in athlete_ids:
        athlete_path = path.join('.opendata_storage', athlete_id)
        athlete_metadata_filename = glob.glob(path.join(athlete_path, '*.json'))

        with open(athlete_metadata_filename[0], 'r') as f:
            athlete_metadata = json.load(f)

        athlete_metadata = _preprocess_rides(athlete_metadata)
        athlete_metadata = _preprocess_athlete(athlete_metadata)

        fields = set(athlete_metadata.keys()) - set(['RIDES'])

        metadata = metadata.append(
            json_normalize(
                data=athlete_metadata,
                record_path='RIDES',
                meta=list(fields)
            ),
            ignore_index=True,
            sort=False
        )

    metadata = metadata.assign(activity_ids=_activity_ids(metadata))
    metadata = metadata.assign(**_data_checks(metadata))

    return metadata


def generate_metadata_csv():
    metadata = generate_metadata()
    metadata.to_csv('metadata.csv')
