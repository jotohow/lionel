from sklearn.base import BaseEstimator


class Naive(BaseEstimator):
    def fit(self, X, y):
        return self

    def predict(self, X):
        return X["lag1"]
