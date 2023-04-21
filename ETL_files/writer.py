import logging
import datetime
from io import BytesIO
from decouple import config
import boto3
from pyspark.sql.functions import *


class S3DataWriter:
    def __init__(self, data=None, format=None, bucket_name=None, sub_type=None):
        self.buffer = BytesIO()
        self.now = datetime.datetime.now()
        self.date_str = str(self.now.date())
        self.client = boto3.client("s3")
        self.bucket_name = bucket_name or config('BUCKET_NAME')
        self.format = format or 'parquet'
        self.sub_type = sub_type or 'battlelog'
        self.key = f"APIRoyale/players/sub_type={self.sub_type}/transformed_at={self.date_str}/{self.now}.{self.format}"
        self.data = self.date_str if data is None else data

    def write_to_s3(self):
        s3_path = self.key
        df = self.data
        if self.format == 'parquet':
            df.write.mode("overwrite").parquet(f"s3a://{self.bucket_name}/{s3_path}")
        elif self.format == 'csv':
            df.select("*").write.format("csv").mode("overwrite").option(
                "header", "true"
            ).save("data/output/teste.csv")
        else:
            raise ValueError(f"Format '{self.format}' not supported.")
        logging.info(f"Writing data to Bucket: {self.bucket_name}/{s3_path}")
