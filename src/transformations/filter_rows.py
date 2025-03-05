class FilterTransformation:
    def __init__(self, config):
        self.config = config

    def transform(self, df):
        condition = self.config.get("filter")
        return df.filter(condition)
