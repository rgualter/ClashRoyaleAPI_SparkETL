from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from lib.logger import Log4j
import boto3, datetime, logging
from _columns_ import *

s3 = boto3.client("s3")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class SparkFileLoader:
    def __init__(self):
        self.spark = (
            SparkSession.builder.master("local[3]").appName("SparkETL").getOrCreate()
        )
        self.logger = Log4j(self.spark)
        self.df = None
        self.spark._jsc.hadoopConfiguration().set(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        self.now = datetime.datetime.now()

    def add_raw_file_name(self, df):
        return df.withColumn("raw_file_name", input_file_name())

    def load_data(self, date=None):
        if date is None:
            date = self.now.strftime("%Y-%m-%d")
        bucket_name = "apiroyale-raw"
        directory_name = f"APIRoyale/players/sub_type=battlelog/extracted_at={date}"
        path = f"s3a://{bucket_name}/{directory_name}/"
        self.df = self.spark.read.json(path)
        self.df = self.add_raw_file_name(self.df)
