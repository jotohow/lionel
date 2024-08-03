import pandas as pd
from lionel.data_load.constants import DATA, RAW, CLEANED, PROCESSED


class S3Storer:
    def __init__(self):
        self.data = ""  # wherever the S3 bucket is

    def store(self, data, path, **kwargs):
        data.to_csv(path, **kwargs)
        # code to upload to S3


class CSVStorer:
    def __init__(self):
        self.data = DATA

    def store(self, data, path, **kwargs):
        data.to_csv(self.data / path, **kwargs)

    def load(self, path):
        return pd.read_csv(self.data / path)


class StorageHandler:
    def __init__(self, local=True):
        self.local = local
        self.storer = CSVStorer() if local else S3Storer()

    def store(self, data, path, **kwargs):
        self.storer.store(data, path, **kwargs)

    def load(self, path):
        return self.storer.load(path)
