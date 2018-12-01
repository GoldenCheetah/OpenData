import os

from opendata import main
import pytest


class TestOpenData:
    def test_init(self):
        od = main.OpenData()

        assert hasattr(od, 'bucket')

    @pytest.mark.aws_s3()
    def test_remote_athletes(self, local_storage):
        od = main.OpenData()
        athletes = list(od.remote_athletes())

        assert len(athletes) > 1200

    @pytest.mark.aws_s3()
    def test_get_remote_athlete(self):
        od = main.OpenData()
        athlete = od.get_remote_athlete('009daa84-3253-4967-93fe-ebdb0fa93339')

        assert athlete.id == '009daa84-3253-4967-93fe-ebdb0fa93339'

    def test_local_athletes(self, local_storage):
        od = main.OpenData()
        athletes = list(od.local_athletes())

        assert len(athletes) == 2
        assert athletes[0].id.startswith('some-athlete-id-')

    def test_get_local_athlete(self, local_storage):
        od = main.OpenData()
        athlete = od.get_local_athlete('brian')

        assert athlete.id == 'brian'

    @pytest.mark.aws_s3()
    def test_missing_athletes(self, local_storage):
        od = main.OpenData()
        local_athlete_count = len(list(od.local_athletes()))
        remote_athlete_count = len(list(od.remote_athletes()))

        assert remote_athlete_count - local_athlete_count > 1200
