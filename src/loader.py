class Loader:
    def __init__(self, spark_session):
        self.spark = spark_session

    def write_csv(self, df, output_path):
        df.write.csv(output_path, header=True)
