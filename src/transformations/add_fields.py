import pyspark.sql.functions as F


class AddFieldsTransformation:
    def __init__(self, config):
        self.config = config

    def transform(self, df):
        for field in self.config.get("fields", []):
            field_name = field["name"]
            expr = field["expression"]
            df = df.withColumn(field_name, F.expr(expr))
        return df
