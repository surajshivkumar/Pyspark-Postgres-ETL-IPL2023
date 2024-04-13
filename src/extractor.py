class Extractor:
    def __init__(self, spark_session):
        self.spark = spark_session

    def read_csv(self, file_path):
        return self.spark.read.csv(file_path, header=True, inferSchema=True)
