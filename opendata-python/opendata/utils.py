from datetime import datetime
import re

DATE_STRING_FORMAT = '%Y/%m/%d %H:%M:%S %Z'
FILENAME_FORMAT = '%Y_%m_%d_%H_%M_%S'
FILENAME_FORMAT_WITH_EXTENSION = FILENAME_FORMAT + '.csv'


def date_string_to_filename(date_string):
    suffix = datetime.strptime(date_string, DATE_STRING_FORMAT).strftime(FILENAME_FORMAT)
    return suffix + '.csv'


def filename_to_date_string(filename):
    dt = datetime.strptime(filename, FILENAME_FORMAT_WITH_EXTENSION)
    return dt.strftime(DATE_STRING_FORMAT) + 'UTC'


def match_filename_to_date_strings(filename, date_strings):
    """
    The date and time in the Open Data metadata are UTC but the filenames are constructed
    from a date and time that are not necessarily UTC so there might be differences there.
    This method tries to solve that by matching gradually less strict untill we find a match.
    It is not nice that we have to do this but it has to be done.
    """
    dt = datetime.strptime(filename, FILENAME_FORMAT_WITH_EXTENSION)

    patterns = [
        fr'{dt.year:04}\/{dt.month:02}\/{dt.day:02} {dt.hour:02}:{dt.minute:02}:{dt.second:02} UTC',
        fr'{dt.year:04}\/{dt.month:02}\/{dt.day:02} \d{{2}}:{dt.minute:02}:{dt.second:02} UTC',
        fr'{dt.year:04}\/{dt.month:02}\/\d{{2}} \d{{2}}:{dt.minute:02}:{dt.second:02} UTC',
        fr'{dt.year:04}\/\d{{2}}\/\d{{2}} \d{{2}}:{dt.minute:02}:{dt.second:02} UTC',
        fr'\d{{4}}\/\d{{2}}\/\d{{2}} \d{{2}}:{dt.minute:02}:{dt.second:02} UTC',
    ]
    for pattern in patterns:
        regex = re.compile(pattern)
        results = list(filter(regex.search, date_strings))
        if results:
            return results[0]


def lazy_load(func):
    '''
    Decorator that makes a property lazy-evaluated.
    '''
    attr_name = '_lazy_' + func.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, func(self))
        return getattr(self, attr_name)
    return _lazy_property
