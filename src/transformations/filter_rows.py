import pyspark.sql.functions as F


class FilterTransformation:
    def __init__(self, config):
        self.config = config

    def transform(self, df):
        condition = self.config["filter"]
        return df.filter(condition)
