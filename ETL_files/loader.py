from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from lib.logger import Log4j
import boto3, datetime, logging
from _columns_ import *

s3 = boto3.client("s3")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class SparkFileLoader:
    def __init__(self, spark: SparkSession, bucket_name: str):
        self.spark = spark
        self.logger = Log4j(self.spark)
        self.df = None
        self.bucket_name = bucket_name

    def add_raw_file_name(self, df):
        return df.withColumn("raw_file_name", input_file_name())

    def _read_json_from_s3(self, directory_name):
        path = f"s3a://{self.bucket_name}/{directory_name}/"
        return self.spark.read.json(path)

    def load_data(self, date=None):
        if date is None:
            date = datetime.datetime.now().strftime("%Y-%m-%d")
        bucket_name = self.bucket_name
        directory_name = f"APIRoyale/players/sub_type=battlelog/extracted_at={date}"
        path = f"s3a://{bucket_name}/{directory_name}/"
        self.df = self.spark.read.json(path)
        self.df = self.add_raw_file_name(self.df)