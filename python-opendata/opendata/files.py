import glob
from os import listdir, path, walk

import pandas as pd


def downloaded_athletes():
    athlete_ids = []
    for f in listdir(path.join('.opendata_storage')):
        if path.isdir(path.join('.opendata_storage', f)):
            athlete_ids.append(f)

    return athlete_ids


def activities_for_athlete(athlete_id):
    activity_ids = []
    for f in glob.glob(path.join('.opendata_storage', athlete_id, '*.csv')):
        activity_ids.append(path.basename(f).rstrip('.csv'))

    return activity_ids


def get_activity(athlete_id, activity_id):
    file_path = path.join('.opendata_storage', athlete_id, activity_id + '.csv')

    return pd.read_csv(file_path)


def storage_size():
    total_size = 0
    for dirpath, dirnames, filenames in walk('.opendata_storage'):
        for filename in filenames:
            file_path = path.join(dirpath, filename)
            total_size += path.getsize(file_path)

    return total_size
