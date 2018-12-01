import os

from .mixins import LocalStorageMixin, RemoteMixin
from .models import LocalAthlete, RemoteAthlete


class OpenData(LocalStorageMixin, RemoteMixin):
    def remote_athletes(self):
        for obj in self.remote_data():
            athlete_id = self.object_key_to_athlete_id(obj.key)
            yield RemoteAthlete(athlete_id)

    def get_remote_athlete(self, athlete_id):
        return RemoteAthlete(athlete_id)

    def local_athletes(self):
        for path in self.local_data():
            athlete_id = os.path.split(os.path.split(path)[0])[-1]
            yield LocalAthlete(athlete_id)

    def get_local_athlete(self, athlete_id):
        return LocalAthlete(athlete_id)

    def missing_athletes(self):
        return list(set(self.remote_athletes()) - set(self.local_athletes()))

    def remote_datasets(self):
        raise NotImplementedError

    def get_remote_dataset(self, athlete_id):
        raise NotImplementedError

    def local_datasets(self):
        raise NotImplementedError

    def get_local_dataset(self, athlete_id):
        raise NotImplementedError

    def missing_datasets(self):
        raise NotImplementedError
