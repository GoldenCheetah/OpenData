import pytest

from opendata import utils


@pytest.mark.parametrize('filename,date_strings,expected', [
    ['2018_12_31_23_59_59.csv', ['2018/12/31 23:59:59 UTC'], '2018/12/31 23:59:59 UTC'],
    ['2018_12_31_23_59_59.fit.csv', ['2018/12/31 23:59:59 UTC'], '2018/12/31 23:59:59 UTC'],
])
def test_match_filename_to_date_strings(filename, date_strings, expected):
    result = utils.match_filename_to_date_strings(filename, date_strings)
    assert result == expected
