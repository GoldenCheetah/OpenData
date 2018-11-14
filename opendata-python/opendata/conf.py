import configparser
import os

from pkgsettings import Settings

DEFAULT_LOCAL_STORAGE_PATH = os.path.join(os.path.expanduser('~'), 'opendatastorage')


config = configparser.ConfigParser()
config['Storage'] = {}
config['Storage']['local_storage_path'] = DEFAULT_LOCAL_STORAGE_PATH
config.read('opendata.ini')


local_storage = config['Storage']['local_storage_path']
if not os.path.isdir(local_storage):
    if local_storage == DEFAULT_LOCAL_STORAGE_PATH:
        os.makedirs(DEFAULT_LOCAL_STORAGE_PATH)
        print(f'Local storage directory ({DEFAULT_LOCAL_STORAGE_PATH}) created')
    else:
        error_message = f'The specified local storage path {local_storage} does not exist'
        raise OSError(error_message)


settings = Settings()
settings.configure(
    bucket_name='goldencheetah-opendata',
    data_prefix='data',
    metadata_prefix='metadata',
    datasets_prefix='datasets',
    local_storage=config['Storage']['local_storage_path']
)
