import pandas as pd


class S3Storer:
    def store(self, data, path, **kwargs):
        data.to_csv(path, **kwargs)
        # code to upload to S3


class CSVStorer:
    def __init__(self):
        pass

    def store(self, data, path, **kwargs):
        data.to_csv(path, **kwargs)

    def load(self, path):
        return pd.read_csv(path)


class StorageHandler:
    def __init__(self, local=True):
        self.local = local
        self.storer = CSVStorer() if local else S3Storer()

    def store(self, data, path, **kwargs):
        self.storer.store(data, path, **kwargs)

    def load(self, path):
        return self.storer.load(path)
