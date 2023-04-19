from io import BytesIO
from pyspark.sql.functions import *
import boto3, datetime, logging
from _columns_ import *

s3 = boto3.client("s3")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class S3DataWriter:
    def __init__(self, data, format):
        super().__init__()
        self.buffer = BytesIO()
        self.now = datetime.datetime.now()
        self.date_str = self.now.date()
        self.client = boto3.client("s3")
        self.bucket_name = "apiroyale-stage"
        self.format = f"{format}"
        self.key = f"APIRoyale/players/sub_type=battlelog/transformed_at={self.date_str}/{self.now}.{self.format}"
        self.data = data

    def write_parquet_to_s3(self):
        s3_path = self.key
        buffer = self.buffer
        df = self.data
        df.write.mode("overwrite").parquet(f"s3a://{self.bucket_name}/{s3_path}")
        self.client.upload_fileobj(buffer, self.bucket_name, self.key)
        logger.info(f"Writing parquet data to Bucket:{self.bucket_name}/{s3_path}")

    def write_csv_local(self):
        df = self.data
        df.select("*").write.format("csv").mode("overwrite").option(
            "header", "true"
        ).save("data/output/teste.csv")
