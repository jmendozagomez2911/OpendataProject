import pyspark.sql.functions as F


class AddFieldsTransformation:
    def __init__(self, config):
        self.config = config

    def transform(self, df):
        for field in self.config["fields"]:
            field_name = field["name"]
            expression = field["expression"]
            df = df.withColumn(field_name, F.expr(expression))
        return df
